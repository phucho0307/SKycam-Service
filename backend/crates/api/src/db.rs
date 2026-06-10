use anyhow::Result;
use mongodb::{Client, Database};

use crate::config::Config;

#[derive(Clone)]
pub struct Db {
    pub database: Database,
}

impl Db {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        let client = Client::with_uri_str(&cfg.mongodb_uri).await?;
        let database = client.database(&cfg.mongodb_db);
        database.run_command(bson::doc! { "ping": 1 }).await?;
        tracing::info!(db = %cfg.mongodb_db, "connected to MongoDB");
        Ok(Self { database })
    }
}
