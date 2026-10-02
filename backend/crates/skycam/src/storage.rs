use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::config::S3Config;

/// Wrapper over an S3-compatible store. Holds two clients:
/// - `client` uses the internal endpoint for uploads (server -> S3),
/// - `presign_client` uses the public endpoint so presigned URLs are
///   reachable by the browser.
#[derive(Clone)]
pub struct Storage {
    client: Client,
    presign_client: Client,
    bucket: String,
}

impl Storage {
    pub fn new(cfg: &S3Config) -> Self {
        let mk = |endpoint: String| {
            let creds = Credentials::new(
                cfg.access_key.clone(),
                cfg.secret_key.clone(),
                None,
                None,
                "static",
            );
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(cfg.region.clone()))
                .endpoint_url(endpoint)
                .credentials_provider(creds)
                // Custom endpoints (R2/MinIO) want path-style addressing.
                .force_path_style(true)
                .build()
        };

        Self {
            client: Client::from_conf(mk(cfg.endpoint.clone())),
            presign_client: Client::from_conf(mk(cfg.public_endpoint.clone())),
            bucket: cfg.bucket.clone(),
        }
    }

    /// Stream a file from disk into the bucket under `key`.
    pub async fn put_file(&self, key: &str, path: &Path, content_type: &str) -> Result<()> {
        let body = ByteStream::from_path(path).await?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(body)
            .send()
            .await?;
        Ok(())
    }

    /// Read a whole object over the internal endpoint. Only used for previews
    /// (tens of KB), which are then served at a stable, CDN-cacheable path, so
    /// each preview is read from S3 roughly once per CDN location rather than
    /// once per viewer. Never use this for FITS.
    ///
    /// `Ok(None)` when the object does not exist. That is routine, not a fault:
    /// the bucket's lifecycle rules delete previews after their retention
    /// period while the frame's metadata (and so its URL) lives on.
    pub async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        match resp {
            Ok(obj) => Ok(Some(obj.body.collect().await?.into_bytes().to_vec())),
            Err(e) if e.as_service_error().is_some_and(|se| se.is_no_such_key()) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// A short-lived, browser-reachable URL to GET an object (signed against
    /// the public endpoint).
    pub async fn presign_get(&self, key: &str, expires: Duration) -> Result<String> {
        let presigned = self
            .presign_client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(expires)?)
            .await?;
        Ok(presigned.uri().to_string())
    }
}
