use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Builder, Client};
use aws_smithy_types::byte_stream::ByteStream;

use crate::config::Config;

// public_endpoint + public_url are read by Phase 1c.2 (the /download
// endpoint that 302-redirects to the direct MinIO URL).
#[derive(Clone)]
#[allow(dead_code)]
pub struct Storage {
    pub client: Client,
    pub bucket: String,
    pub public_endpoint: String,
}

impl Storage {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        let creds =
            Credentials::from_keys(cfg.s3_access_key.clone(), cfg.s3_secret_key.clone(), None);

        // Path-style addressing is required for MinIO at a bare endpoint URL
        // (MinIO doesn't do <bucket>.<host>-style routing by default).
        let s3_config = Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.s3_region.clone()))
            .endpoint_url(cfg.s3_endpoint.clone())
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        // Sanity-ping the bucket so misconfig fails on boot, not on first sync.
        client
            .head_bucket()
            .bucket(&cfg.s3_bucket)
            .send()
            .await
            .with_context(|| format!("head_bucket({})", cfg.s3_bucket))?;
        tracing::info!(bucket = %cfg.s3_bucket, "S3 bucket reachable");

        Ok(Self {
            client,
            bucket: cfg.s3_bucket.clone(),
            public_endpoint: cfg.s3_public_endpoint.clone(),
        })
    }

    pub async fn put_object(
        &self,
        key: &str,
        body: bytes::Bytes,
        content_type: &str,
    ) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(body))
            .send()
            .await
            .with_context(|| format!("put_object key={key}"))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn public_url(&self, key: &str) -> String {
        format!(
            "{}/{}/{}",
            self.public_endpoint.trim_end_matches('/'),
            self.bucket,
            key
        )
    }
}
