use bson::oid::ObjectId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Guest,
    Member,
    ImagingRequester,
    ObservatoryOperator,
    Admin,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub google_sub: String,
    pub email: String,
    pub display_name: String,
    pub roles: Vec<Role>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Release {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub product: String,
    pub version: String,
    pub channel: ReleaseChannel,
    pub notes_md: String,
    pub assets: Vec<ReleaseAsset>,
    pub published_at: DateTime<Utc>,
    pub source_repo: String,
    pub source_tag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReleaseChannel {
    Stable,
    Beta,
    Nightly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseAsset {
    pub platform: Platform,
    pub arch: Arch,
    pub filename: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub s3_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Windows,
    Linux,
    Macos,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Arch {
    X86_64,
    Aarch64,
}

/// A single environmental reading from a device (e.g. AHT10 on the sky camera Pi).
/// Small and high-frequency — stored as its own time-series in the `telemetry` collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryReading {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub device_id: String,
    /// When the device sampled the value.
    pub recorded_at: DateTime<Utc>,
    /// When the server received it (stamped server-side, never trusted from the client).
    pub received_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature_c: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub humidity_pct: Option<f64>,
}

/// Metadata for one captured camera frame. The image bytes live in object storage
/// (S3/R2) under `s3_key`; only this metadata lives in the `frames` collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub device_id: String,
    pub captured_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    /// Object key of the full FITS in the bucket (bytes are not stored in Mongo).
    pub s3_key: String,
    /// Object key of the small web-viewable JPEG preview, if the device sent one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview_key: Option<String>,
    pub content_type: String,
    pub size_bytes: u64,
    /// Environmental / capture metadata stamped onto the frame at capture time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature_c: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exposure_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gain: Option<i64>,
}
