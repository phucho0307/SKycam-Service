use anyhow::Result;
use bson::doc;
use chrono::{DateTime, Utc};
use mongodb::{options::IndexOptions, Client, Collection, Database};
use serde::{Deserialize, Serialize};

use crate::config::Config;

#[derive(Clone)]
pub struct Db {
    pub database: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncedRelease {
    pub product_id: String,
    pub source_repo: String,
    pub source_tag: String,
    pub release_name: String,
    pub notes_md: String,
    pub assets: Vec<SyncedAsset>,
    pub published_at: DateTime<Utc>,
    pub synced_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncedAsset {
    pub filename: String,
    pub size_bytes: u64,
    pub content_type: String,
    pub s3_key: String,
}

impl Db {
    pub async fn connect(cfg: &Config) -> Result<Self> {
        let client = Client::with_uri_str(&cfg.mongodb_uri).await?;
        let database = client.database(&cfg.mongodb_db);
        database.run_command(doc! { "ping": 1 }).await?;
        tracing::info!(db = %cfg.mongodb_db, "connected to MongoDB");

        let releases: Collection<SyncedRelease> = database.collection("releases");
        releases
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(doc! { "product_id": 1, "source_tag": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
            )
            .await?;

        Ok(Self { database })
    }

    pub fn releases(&self) -> Collection<SyncedRelease> {
        self.database.collection("releases")
    }
}
