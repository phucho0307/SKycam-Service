use bson::doc;
use chrono::Utc;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::State;
use serde::Deserialize;

use domain::CameraSettings;

use crate::db::Db;

// NOTE: these are currently open (no auth), consistent with the other read
// endpoints. `PUT` should get user-auth once login exists (Phase 2) so random
// callers can't change a camera's settings.

#[get("/settings?<device_id>")]
pub async fn get_settings(
    db: &State<Db>,
    device_id: Option<String>,
) -> Result<Json<Option<CameraSettings>>, Status> {
    let dev = device_id.unwrap_or_else(|| "skycam".to_string());
    let s = db
        .database
        .collection::<CameraSettings>("settings")
        .find_one(doc! { "device_id": &dev })
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "settings get failed");
            Status::InternalServerError
        })?;
    Ok(Json(s))
}

#[derive(Deserialize)]
pub struct SettingsInput {
    device_id: String,
    exposure_ms: Option<f64>,
    gain: Option<i64>,
    preview_gamma: Option<f64>,
    preview_contrast: Option<f64>,
    preview_brightness: Option<f64>,
}

#[put("/settings", format = "json", data = "<input>")]
pub async fn put_settings(
    db: &State<Db>,
    input: Json<SettingsInput>,
) -> Result<Json<CameraSettings>, Status> {
    let settings = CameraSettings {
        id: None,
        device_id: input.device_id.clone(),
        exposure_ms: input.exposure_ms,
        gain: input.gain,
        preview_gamma: input.preview_gamma,
        preview_contrast: input.preview_contrast,
        preview_brightness: input.preview_brightness,
        updated_at: Utc::now(),
    };

    let set_doc = bson::to_document(&settings).map_err(|e| {
        tracing::error!(error = %e, "settings serialize failed");
        Status::InternalServerError
    })?;

    db.database
        .collection::<CameraSettings>("settings")
        .update_one(
            doc! { "device_id": &input.device_id },
            doc! { "$set": set_doc },
        )
        .upsert(true)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "settings upsert failed");
            Status::InternalServerError
        })?;

    Ok(Json(settings))
}
