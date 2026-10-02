use std::time::Duration;

use bson::doc;
use futures::TryStreamExt;
use rocket::http::{ContentType, Header, Status};
use rocket::serde::json::Json;
use rocket::State;
use serde::Serialize;

use domain::{Frame, TelemetryReading};

use crate::db::Db;
use crate::storage::Storage;

// FITS downloads stay presigned: large, rarely fetched, and not worth caching.
const URL_TTL: Duration = Duration::from_secs(900); // 15 min

/// How long a CDN (and the browser) may reuse a `/live` response. A new frame
/// exists only every ~2s, so this costs at most one frame of staleness and
/// turns N viewers polling one camera into ~1 origin request per 2s per camera.
///
/// `stale-if-error=60`: while the origin is down (a deploy, a crash), the edge
/// keeps serving the last good response instead of a 502. Measured: an origin
/// restart under 500 polling viewers gave 34 errors without it.
pub const LIVE_CACHE: &str = "public, max-age=2, stale-if-error=60";

/// A preview is written once under its frame id and never changes, so it can
/// be cached forever. `immutable` also stops browsers revalidating on reload.
const PREVIEW_CACHE: &str = "public, max-age=31536000, immutable";

// ---- response DTOs (clean JSON for the frontend) --------------------------

#[derive(Serialize, Clone)]
pub struct FrameDto {
    id: String,
    device_id: String,
    captured_at: String,
    received_at: String,
    temperature_c: Option<f64>,
    probe_temp_c: Option<f64>,
    cloud_score: Option<f64>,
    is_cloudy: Option<bool>,
    size_bytes: Option<u64>,
    /// Stable URL of the JPEG preview (if any), served by `frame_preview`.
    /// Not presigned: a presigned URL differs on every request, so no CDN or
    /// browser cache can ever reuse it, and every viewer pulled every preview
    /// from S3 itself.
    preview_url: Option<String>,
    /// Presigned URL to download the full FITS (if this frame has one).
    fits_url: Option<String>,
}

#[derive(Serialize)]
pub struct ReadingDto {
    recorded_at: String,
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
    probe_temp_c: Option<f64>,
}

async fn frame_to_dto(f: Frame, storage: Option<&Storage>) -> FrameDto {
    let id = f.id.map(|o| o.to_hex()).unwrap_or_default();
    let preview_url = f
        .preview_key
        .as_ref()
        .map(|_| format!("/skycam/frames/{id}/preview.jpg"));
    let fits_url = match (storage, &f.s3_key) {
        (Some(s), Some(k)) => s.presign_get(k, URL_TTL).await.ok(),
        _ => None,
    };
    FrameDto {
        id,
        device_id: f.device_id,
        captured_at: f.captured_at.to_rfc3339(),
        received_at: f.received_at.to_rfc3339(),
        temperature_c: f.temperature_c,
        probe_temp_c: f.probe_temp_c,
        cloud_score: f.cloud_score,
        is_cloudy: f.is_cloudy,
        size_bytes: f.size_bytes,
        preview_url,
        fits_url,
    }
}

/// What the live view needs, in one response: the newest frame (the picture)
/// and the newest *scored* frame (the sky status, which lags the picture while
/// detection runs). Previously the GUI made two requests for this, one of them
/// listing 20 frames just to find the scored one.
#[derive(Serialize, Clone)]
pub struct LivePayload {
    pub latest: Option<FrameDto>,
    pub last_scored: Option<FrameDto>,
}

pub async fn live_snapshot(
    db: &Db,
    storage: Option<&Storage>,
    device_id: Option<&str>,
) -> Result<LivePayload, mongodb::error::Error> {
    let frames = db.database.collection::<Frame>("frames");
    let by_device = match device_id {
        Some(d) => doc! { "device_id": d },
        None => doc! {},
    };
    let mut scored = by_device.clone();
    scored.insert("is_cloudy", doc! { "$ne": null });

    // Both walk frames{device_id, captured_at} (or {captured_at}) newest first
    // and stop at the first match: recent frames are scored within seconds,
    // so the second query reads a handful of index entries, not the collection.
    let latest = frames
        .find_one(by_device)
        .sort(doc! { "captured_at": -1 })
        .await?;
    let last_scored = frames
        .find_one(scored)
        .sort(doc! { "captured_at": -1 })
        .await?;

    Ok(LivePayload {
        latest: match latest {
            Some(f) => Some(frame_to_dto(f, storage).await),
            None => None,
        },
        last_scored: match last_scored {
            Some(f) => Some(frame_to_dto(f, storage).await),
            None => None,
        },
    })
}

#[derive(Responder)]
pub struct LiveResponse {
    inner: Json<LivePayload>,
    cache: Header<'static>,
}

#[derive(Responder)]
pub struct PreviewResponse {
    inner: (ContentType, Vec<u8>),
    cache: Header<'static>,
}

// ---- endpoints ------------------------------------------------------------

/// The live view's poll target. Cacheable for 2s by a CDN, so origin load is
/// per camera, not per viewer.
///
/// Note for Cloudflare: it caches by file extension by default, so this
/// extensionless JSON path needs a Cache Rule marking it eligible for cache
/// before the header has any effect at the edge. The `.jpg` preview path is
/// cached by default.
#[get("/live?<device_id>")]
pub async fn live(
    db: &State<Db>,
    storage: Option<&State<Storage>>,
    device_id: Option<String>,
) -> Result<LiveResponse, Status> {
    let payload = live_snapshot(db, storage.map(|s| s.inner()), device_id.as_deref())
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "live snapshot failed");
            Status::InternalServerError
        })?;
    Ok(LiveResponse {
        inner: Json(payload),
        cache: Header::new("Cache-Control", LIVE_CACHE),
    })
}

/// A frame's JPEG preview at a stable URL. Read from S3 once per CDN location,
/// then served from cache for a year.
///
/// Exposure note: previews were already reachable by anyone who could call the
/// read API (which is unauthenticated); this makes them cacheable, not newly
/// public. FITS downloads remain presigned.
#[get("/frames/<id>/preview.jpg")]
pub async fn frame_preview(
    db: &State<Db>,
    storage: Option<&State<Storage>>,
    id: &str,
) -> Result<PreviewResponse, Status> {
    let storage = storage.ok_or(Status::ServiceUnavailable)?;
    let oid = bson::oid::ObjectId::parse_str(id).map_err(|_| Status::NotFound)?;
    let frame = db
        .database
        .collection::<Frame>("frames")
        .find_one(doc! { "_id": oid })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "preview lookup failed");
            Status::InternalServerError
        })?
        .ok_or(Status::NotFound)?;
    let key = frame.preview_key.ok_or(Status::NotFound)?;
    let bytes = storage
        .get_bytes(&key)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, key = %key, "preview fetch from S3 failed");
            Status::BadGateway
        })?
        // Expired by the bucket's lifecycle rule: a 404, not an outage. Before
        // this, every expired preview was a 502 and an ERROR log line.
        .ok_or(Status::NotFound)?;
    Ok(PreviewResponse {
        inner: (ContentType::JPEG, bytes),
        cache: Header::new("Cache-Control", PREVIEW_CACHE),
    })
}

#[get("/frames/latest")]
pub async fn frames_latest(
    db: &State<Db>,
    storage: Option<&State<Storage>>,
) -> Result<Json<Option<FrameDto>>, Status> {
    let frame = db
        .database
        .collection::<Frame>("frames")
        .find_one(doc! {})
        .sort(doc! { "captured_at": -1 })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "frames/latest query failed");
            Status::InternalServerError
        })?;

    let s = storage.map(|s| s.inner());
    Ok(Json(match frame {
        Some(f) => Some(frame_to_dto(f, s).await),
        None => None,
    }))
}

#[get("/frames?<from>&<to>&<limit>")]
pub async fn frames_list(
    db: &State<Db>,
    storage: Option<&State<Storage>>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
) -> Result<Json<Vec<FrameDto>>, Status> {
    let mut range = doc! {};
    if let Some(f) = from {
        range.insert("$gte", f);
    }
    if let Some(t) = to {
        range.insert("$lte", t);
    }
    let filter = if range.is_empty() {
        doc! {}
    } else {
        doc! { "captured_at": range }
    };
    let lim = limit.unwrap_or(100).clamp(1, 500);

    let frames: Vec<Frame> = db
        .database
        .collection::<Frame>("frames")
        .find(filter)
        .sort(doc! { "captured_at": -1 })
        .limit(lim)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "frames list query failed");
            Status::InternalServerError
        })?
        .try_collect()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "frames list collect failed");
            Status::InternalServerError
        })?;

    let s = storage.map(|s| s.inner());
    let mut out = Vec::with_capacity(frames.len());
    for f in frames {
        out.push(frame_to_dto(f, s).await);
    }
    Ok(Json(out))
}

#[get("/telemetry?<from>&<to>&<limit>")]
pub async fn telemetry_list(
    db: &State<Db>,
    from: Option<String>,
    to: Option<String>,
    limit: Option<i64>,
) -> Result<Json<Vec<ReadingDto>>, Status> {
    let mut range = doc! {};
    if let Some(f) = from {
        range.insert("$gte", f);
    }
    if let Some(t) = to {
        range.insert("$lte", t);
    }
    let filter = if range.is_empty() {
        doc! {}
    } else {
        doc! { "recorded_at": range }
    };
    let lim = limit.unwrap_or(200).clamp(1, 1000);

    // newest first from Mongo, then reverse to chronological for charting
    let mut readings: Vec<TelemetryReading> = db
        .database
        .collection::<TelemetryReading>("telemetry")
        .find(filter)
        .sort(doc! { "recorded_at": -1 })
        .limit(lim)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "telemetry query failed");
            Status::InternalServerError
        })?
        .try_collect()
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "telemetry collect failed");
            Status::InternalServerError
        })?;
    readings.reverse();

    let out = readings
        .into_iter()
        .map(|r| ReadingDto {
            recorded_at: r.recorded_at.to_rfc3339(),
            temperature_c: r.temperature_c,
            humidity_pct: r.humidity_pct,
            probe_temp_c: r.probe_temp_c,
        })
        .collect();
    Ok(Json(out))
}
