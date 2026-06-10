use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    pub mongodb_uri: String,
    pub mongodb_db: String,
    pub environment: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            port: std::env::var("PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8000),
            mongodb_uri: std::env::var("MONGODB_URI")
                .unwrap_or_else(|_| "mongodb://localhost:27017".into()),
            mongodb_db: std::env::var("MONGODB_DB").unwrap_or_else(|_| "observatory".into()),
            environment: std::env::var("APP_ENV").unwrap_or_else(|_| "dev".into()),
        })
    }
}
