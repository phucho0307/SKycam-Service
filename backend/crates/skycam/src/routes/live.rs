//! Server-Sent Events for the live view: option B, measured against polling a
//! CDN-cached `/live` (option A). See SCALING.md, "Live view".
//!
//! SSE rather than WebSocket: the data only flows server -> browser (settings
//! changes are an occasional PUT), and SSE gives automatic reconnect with
//! `Last-Event-ID` over plain HTTP.
//!
//! Limitation stated up front: the hub is in-process. A frame arriving on pod A
//! reaches only pod A's streams, so with more than one replica this needs a
//! Redis channel per camera. The skycam Deployment runs one replica today.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocket::response::stream::{Event, EventStream};
use rocket::{Shutdown, State};
use tokio::sync::broadcast::{self, error::RecvError};

use crate::db::Db;
use crate::routes::read::{live_snapshot, LivePayload};
use crate::storage::Storage;

/// One message per stored frame, built once and shared by every open stream.
pub struct LiveEvent {
    pub device_id: String,
    pub frame_id: String,
    pub payload: LivePayload,
}

/// Fans new-frame events out to open SSE streams.
pub struct LiveHub {
    tx: broadcast::Sender<Arc<LiveEvent>>,
}

impl LiveHub {
    pub fn new() -> Self {
        // A slow client that falls 16 frames behind (~30s) is told it lagged
        // and is resynced from a fresh snapshot rather than replayed.
        let (tx, _) = broadcast::channel(16);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LiveEvent>> {
        self.tx.subscribe()
    }

    pub fn has_viewers(&self) -> bool {
        self.tx.receiver_count() > 0
    }

    /// Build the payload once and broadcast it. Called after a frame is stored.
    /// One snapshot query per frame, however many viewers: the alternative of
    /// each stream querying on each event is N queries per frame.
    pub fn publish_after_insert(
        self: &Arc<Self>,
        db: Db,
        storage: Storage,
        device_id: String,
        frame_id: String,
    ) {
        if !self.has_viewers() {
            return; // nobody watching: skip the query entirely
        }
        let hub = Arc::clone(self);
        tokio::spawn(async move {
            match live_snapshot(&db, Some(&storage), Some(&device_id)).await {
                Ok(payload) => {
                    // Err only means every receiver went away meanwhile.
                    let _ = hub.tx.send(Arc::new(LiveEvent {
                        device_id,
                        frame_id,
                        payload,
                    }));
                }
                Err(e) => tracing::warn!(error = %e, "live snapshot for broadcast failed"),
            }
        });
    }
}

/// A reconnect delay spread over 1-5s. Not security-sensitive, so the clock's
/// sub-second digits are random enough and avoid a dependency.
fn reconnect_delay() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    Duration::from_millis(1_000 + u64::from(nanos % 4_000))
}

/// `GET /skycam/live/stream?device_id=...` as `text/event-stream`.
///
/// Sends the current snapshot first, so a reconnect (automatic in the browser)
/// always resyncs, and a missed broadcast heals on the next frame ~2s later.
/// A comment heartbeat every 20s keeps proxies from closing an idle stream;
/// Cloudflare drops responses idle for ~100s.
#[get("/live/stream?<device_id>")]
pub async fn live_stream(
    db: &State<Db>,
    storage: Option<&State<Storage>>,
    hub: &State<Arc<LiveHub>>,
    device_id: Option<String>,
    mut shutdown: Shutdown,
) -> Result<EventStream![], rocket::http::Status> {
    let db = db.inner().clone();
    let storage = storage.map(|s| s.inner().clone());
    let initial = live_snapshot(&db, storage.as_ref(), device_id.as_deref())
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "live stream snapshot failed");
            rocket::http::Status::InternalServerError
        })?;
    let mut rx = hub.subscribe();
    let retry = reconnect_delay();

    Ok(EventStream! {
        // `retry` sets how long this browser waits before reconnecting. Without
        // it every EventSource uses the same default, so when a pod restarts
        // all its viewers reconnect at the same instant. Measured at 500
        // viewers: 500 reconnects (and 500 snapshot queries) in one burst.
        yield Event::json(&initial).event("live").with_retry(retry);
        loop {
            let next = tokio::select! {
                r = rx.recv() => r,
                _ = &mut shutdown => break,
            };
            match next {
                Ok(ev) => {
                    if device_id.as_deref().is_some_and(|d| d != ev.device_id) {
                        continue;
                    }
                    yield Event::json(&ev.payload).event("live").id(ev.frame_id.clone());
                }
                Err(RecvError::Lagged(_)) => {
                    if let Ok(s) = live_snapshot(&db, storage.as_ref(), device_id.as_deref()).await {
                        yield Event::json(&s).event("live");
                    }
                }
                Err(RecvError::Closed) => break,
            }
        }
    }
    .heartbeat(Duration::from_secs(20)))
}
