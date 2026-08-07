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
