use chrono::{DateTime, Utc};
use rocket::form::Form;
use rocket::fs::TempFile;
use rocket::http::Status;
use rocket::request::{FromRequest, Outcome, Request};
use rocket::serde::json::Json;
use rocket::State;
use serde::{Deserialize, Serialize};

use domain::{Frame, TelemetryReading};

use crate::config::Config;
use crate::db::Db;
use crate::storage::Storage;

/// Request guard: requires `Authorization: Bearer <INGEST_TOKEN>`.
/// Fails closed with 503 if no token is configured on the server.
pub struct Ingest;

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Ingest {
    type Error = ();

    async fn from_request(req: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let cfg = match req.rocket().state::<Config>() {
            Some(c) => c,
            None => return Outcome::Error((Status::InternalServerError, ())),
        };
        let expected = match &cfg.ingest_token {
            Some(t) => t,
            None => {
                tracing::warn!("ingest request rejected: INGEST_TOKEN not configured");
                return Outcome::Error((Status::ServiceUnavailable, ()));
            }
        };
        match req.headers().get_one("authorization") {
            Some(h) if h.strip_prefix("Bearer ").map_or(false, |t| t == expected) => {
                Outcome::Success(Ingest)
            }
            _ => Outcome::Error((Status::Unauthorized, ())),
        }
    }
}

#[derive(Serialize)]
pub struct Ack {
    status: &'static str,
    id: String,
}

// ---- temperature / environmental telemetry --------------------------------

#[derive(Deserialize)]
pub struct TelemetryInput {
    device_id: String,
    /// ISO-8601; defaults to server time if omitted.
    recorded_at: Option<DateTime<Utc>>,
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
}

#[post("/telemetry", format = "json", data = "<input>")]
pub async fn telemetry(
    _auth: Ingest,
    db: &State<Db>,
    input: Json<TelemetryInput>,
) -> Result<Json<Ack>, Status> {
    let now = Utc::now();
    let reading = TelemetryReading {
        id: None,
        device_id: input.device_id.clone(),
        recorded_at: input.recorded_at.unwrap_or(now),
        received_at: now,
        temperature_c: input.temperature_c,
        humidity_pct: input.humidity_pct,
    };

    let res = db
        .database
        .collection::<TelemetryReading>("telemetry")
        .insert_one(&reading)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "telemetry insert failed");
            Status::InternalServerError
        })?;

    let id = res
        .inserted_id
        .as_object_id()
        .map(|o| o.to_hex())
        .unwrap_or_default();
    Ok(Json(Ack {
        status: "stored",
        id,
    }))
}

// ---- camera frame upload --------------------------------------------------

#[derive(FromForm)]
pub struct FrameUpload<'r> {
    device_id: String,
    /// ISO-8601 capture time; defaults to server time if omitted.
    captured_at: Option<String>,
    temperature_c: Option<f64>,
    exposure_ms: Option<f64>,
    gain: Option<i64>,
    /// Optional explicit extension for the object key (e.g. "fits", "jpg").
    ext: Option<String>,
    /// The full-resolution frame (e.g. FITS).
    file: TempFile<'r>,
    /// Optional small web-viewable JPEG preview.
    preview: Option<TempFile<'r>>,
}

/// Persist a multipart part to a real temp path and stream it into S3.
async fn upload_part(
    storage: &Storage,
    part: &mut TempFile<'_>,
    key: &str,
    content_type: &str,
    tmp_name: &str,
) -> Result<(), Status> {
    let tmp = std::env::temp_dir().join(tmp_name);
    part.persist_to(&tmp).await.map_err(|e| {
        tracing::error!(error = %e, "failed to buffer upload to disk");
        Status::InternalServerError
    })?;
    let res = storage.put_file(key, &tmp, content_type).await;
    let _ = tokio::fs::remove_file(&tmp).await;
    res.map_err(|e| {
        tracing::error!(error = %e, key = %key, "s3 upload failed");
        Status::BadGateway
    })
}

#[post("/frames", data = "<upload>")]
pub async fn frames(
    _auth: Ingest,
    db: &State<Db>,
    storage: &State<Storage>,
    mut upload: Form<FrameUpload<'_>>,
) -> Result<Json<Ack>, Status> {
    let now = Utc::now();
    let captured_at = upload
        .captured_at
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(now);

    let device = upload.device_id.clone();
    let ts = captured_at.format("%Y%m%dT%H%M%SZ").to_string();

    let content_type = upload
        .file
        .content_type()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let size_bytes = upload.file.len();
    let ext = upload
        .ext
        .clone()
        .or_else(|| {
            upload
                .file
                .content_type()
                .and_then(|c| c.extension().map(|e| e.as_str().to_string()))
        })
        .unwrap_or_else(|| "bin".to_string());

    // One id shared by the object keys and the Mongo document.
    let oid = bson::oid::ObjectId::new();
    let key = format!("frames/{}/{}-{}.{}", device, ts, oid.to_hex(), ext);

    // Full frame first.
    upload_part(
        storage,
        &mut upload.file,
        &key,
        &content_type,
        &format!("frame-{}", oid.to_hex()),
    )
    .await?;

    // Optional preview.
    let preview_key = if upload.preview.is_some() {
        let pkey = format!("previews/{}/{}-{}.jpg", device, ts, oid.to_hex());
        let part = upload.preview.as_mut().unwrap();
        upload_part(storage, part, &pkey, "image/jpeg", &format!("preview-{}", oid.to_hex())).await?;
        Some(pkey)
    } else {
        None
    };

    let frame = Frame {
        id: Some(oid),
        device_id: device,
        captured_at,
        received_at: now,
        s3_key: key,
        preview_key,
        content_type,
        size_bytes,
        temperature_c: upload.temperature_c,
        exposure_ms: upload.exposure_ms,
        gain: upload.gain,
    };

    db.database
        .collection::<Frame>("frames")
        .insert_one(&frame)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "frame metadata insert failed");
            Status::InternalServerError
        })?;

    Ok(Json(Ack {
        status: "stored",
        id: oid.to_hex(),
    }))
}
