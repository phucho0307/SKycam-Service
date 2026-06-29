use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub environment: String,
    pub mongodb_uri: String,
    pub mongodb_db: String,
    pub s3_endpoint: String,
    pub s3_public_endpoint: String,
    pub s3_bucket: String,
    pub s3_region: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub github_app_id: u64,
    pub github_app_installation_id: u64,
    pub github_app_private_key_pem: String,
    pub products_config_path: String,
    pub sync_interval_seconds: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            port: env_parse("PORT", 8001)?,
            environment: env_str("APP_ENV", "dev"),
            mongodb_uri: env_str("MONGODB_URI", "mongodb://localhost:27017"),
            mongodb_db: env_str("MONGODB_DB", "observatory"),
            s3_endpoint: env_required("S3_ENDPOINT")?,
            s3_public_endpoint: env_required("S3_PUBLIC_ENDPOINT")?,
            s3_bucket: env_required("S3_BUCKET")?,
            s3_region: env_str("S3_REGION", "us-east-1"),
            s3_access_key: env_required("S3_ACCESS_KEY")?,
            s3_secret_key: env_required("S3_SECRET_KEY")?,
            github_app_id: env_parse_required("GITHUB_APP_ID")?,
            github_app_installation_id: env_parse_required("GITHUB_APP_INSTALLATION_ID")?,
            github_app_private_key_pem: env_required("GITHUB_APP_PRIVATE_KEY")?,
            products_config_path: env_str("PRODUCTS_CONFIG_PATH", "/config/products.yaml"),
            sync_interval_seconds: env_parse("SYNC_INTERVAL_SECONDS", 300)?,
        })
    }
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var {key}"))
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v.parse().map_err(|e| anyhow::anyhow!("env {key}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_parse_required<T: std::str::FromStr>(key: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let raw = env_required(key)?;
    raw.parse().map_err(|e| anyhow::anyhow!("env {key}: {e}"))
}
