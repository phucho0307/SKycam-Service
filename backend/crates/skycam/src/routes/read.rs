use std::time::Duration;

use bson::doc;
use futures::TryStreamExt;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use serde::Serialize;

use domain::{Frame, TelemetryReading};

use crate::db::Db;
use crate::storage::Storage;

// Presigned URLs are short-lived; the GUI re-fetches metadata often enough.
const URL_TTL: Duration = Duration::from_secs(900); // 15 min

// ---- response DTOs (clean JSON for the frontend) --------------------------

#[derive(Serialize)]
pub struct FrameDto {
    id: String,
    device_id: String,
    captured_at: String,
    received_at: String,
    temperature_c: Option<f64>,
    is_cloudy: Option<bool>,
    size_bytes: u64,
    /// Presigned, browser-reachable URL to the JPEG preview (if any).
    preview_url: Option<String>,
    /// Presigned URL to download the full FITS.
    fits_url: Option<String>,
}

#[derive(Serialize)]
pub struct ReadingDto {
    recorded_at: String,
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
}

async fn frame_to_dto(f: Frame, storage: Option<&Storage>) -> FrameDto {
    let (preview_url, fits_url) = match storage {
        Some(s) => {
            let preview = match &f.preview_key {
                Some(k) => s.presign_get(k, URL_TTL).await.ok(),
                None => None,
            };
            let fits = s.presign_get(&f.s3_key, URL_TTL).await.ok();
            (preview, fits)
        }
        None => (None, None),
    };
    FrameDto {
        id: f.id.map(|o| o.to_hex()).unwrap_or_default(),
        device_id: f.device_id,
        captured_at: f.captured_at.to_rfc3339(),
        received_at: f.received_at.to_rfc3339(),
        temperature_c: f.temperature_c,
        is_cloudy: None, // set once cloud detection exists
        size_bytes: f.size_bytes,
        preview_url,
        fits_url,
    }
}

// ---- endpoints ------------------------------------------------------------

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
        })
        .collect();
    Ok(Json(out))
}
