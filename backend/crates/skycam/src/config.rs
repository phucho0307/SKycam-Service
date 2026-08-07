use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub mongodb_uri: String,
    pub mongodb_db: String,
    pub environment: String,
    /// Shared bearer token that ingest devices (the Pi) must present.
    /// If unset, ingest endpoints fail closed (503).
    pub ingest_token: Option<String>,
    /// Object-storage settings; `None` when not fully configured.
    pub s3: Option<S3Config>,
}

#[derive(Clone)]
pub struct S3Config {
    /// Internal endpoint the service uses to upload (e.g. in-cluster MinIO).
    pub endpoint: String,
    /// Browser-reachable endpoint used to sign presigned GET URLs.
    /// Defaults to `endpoint` when unset.
    pub public_endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Config {
    fn from_env() -> Option<Self> {
        let endpoint = std::env::var("S3_ENDPOINT").ok()?;
        let bucket = std::env::var("S3_BUCKET").ok()?;
        let access_key = std::env::var("S3_ACCESS_KEY").ok()?;
        let secret_key = std::env::var("S3_SECRET_KEY").ok()?;
        // R2 ignores region but the SDK requires one; "auto" is the R2 convention.
        let region = std::env::var("S3_REGION").unwrap_or_else(|_| "auto".into());
        // Endpoint used when signing browser-facing URLs; falls back to internal.
        let public_endpoint =
            std::env::var("S3_PUBLIC_ENDPOINT").unwrap_or_else(|_| endpoint.clone());
        // Guard against the unfilled `<S3_ENDPOINT>` placeholder / empty values.
        if endpoint.is_empty() || endpoint.starts_with('<') || bucket.is_empty() {
            return None;
        }
        Some(Self {
            endpoint,
            public_endpoint,
            bucket,
            region,
            access_key,
            secret_key,
        })
    }
}

// Manual Debug so the secret key never lands in logs.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("public_endpoint", &self.public_endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8002),
            mongodb_uri: std::env::var("MONGODB_URI")
                .unwrap_or_else(|_| "mongodb://localhost:27017".into()),
            mongodb_db: std::env::var("MONGODB_DB").unwrap_or_else(|_| "observatory".into()),
            environment: std::env::var("APP_ENV").unwrap_or_else(|_| "dev".into()),
            ingest_token: std::env::var("INGEST_TOKEN").ok().filter(|s| !s.is_empty()),
            s3: S3Config::from_env(),
        })
    }
}
