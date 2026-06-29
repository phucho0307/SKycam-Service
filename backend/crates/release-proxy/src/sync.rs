use anyhow::{Context, Result};
use bson::doc;
use chrono::Utc;
use mongodb::options::ReplaceOptions;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::db::{Db, SyncedAsset, SyncedRelease};
use crate::github::{GithubAppClient, Release};
use crate::products::Product;
use crate::storage::Storage;

pub async fn run(
    cfg: Config,
    db: Db,
    storage: Storage,
    gh: Arc<GithubAppClient>,
    products: Arc<Vec<Product>>,
) {
    let interval = Duration::from_secs(cfg.sync_interval_seconds);
    tracing::info!(
        products = products.len(),
        interval_seconds = cfg.sync_interval_seconds,
        "release sync loop starting"
    );

    loop {
        for product in products.iter() {
            if let Err(e) = sync_product(&db, &storage, &gh, product).await {
                tracing::error!(product = %product.id, error = %e, "product sync failed");
            }
        }
        tokio::time::sleep(interval).await;
    }
}

async fn sync_product(
    db: &Db,
    storage: &Storage,
    gh: &GithubAppClient,
    product: &Product,
) -> Result<()> {
    let (owner, repo) = product.owner_and_repo()?;
    let releases = gh.list_releases(owner, repo).await?;

    for release in releases.iter().filter(|r| !r.draft) {
        if let Err(e) = sync_release(db, storage, gh, product, owner, repo, release).await {
            tracing::warn!(
                product = %product.id,
                tag = %release.tag_name,
                error = %e,
                "release sync skipped"
            );
        }
    }
    Ok(())
}

async fn sync_release(
    db: &Db,
    storage: &Storage,
    gh: &GithubAppClient,
    product: &Product,
    owner: &str,
    repo: &str,
    release: &Release,
) -> Result<()> {
    let coll = db.releases();
    let existing = coll
        .find_one(doc! {
            "product_id": &product.id,
            "source_tag": &release.tag_name,
        })
        .await
        .context("query existing release")?;

    if existing.is_some() {
        return Ok(());
    }

    tracing::info!(
        product = %product.id,
        tag = %release.tag_name,
        assets = release.assets.len(),
        "mirroring new release"
    );

    let mut synced_assets = Vec::with_capacity(release.assets.len());
    for asset in &release.assets {
        let key = format!(
            "releases/{}/{}/{}",
            product.id, release.tag_name, asset.name
        );
        let (bytes, content_type) = gh.download_asset(owner, repo, asset.id).await?;
        storage.put_object(&key, bytes, &content_type).await?;
        synced_assets.push(SyncedAsset {
            filename: asset.name.clone(),
            size_bytes: asset.size,
            content_type,
            s3_key: key,
        });
    }

    let record = SyncedRelease {
        product_id: product.id.clone(),
        source_repo: product.github_repo.clone(),
        source_tag: release.tag_name.clone(),
        release_name: release
            .name
            .clone()
            .unwrap_or_else(|| release.tag_name.clone()),
        notes_md: release.body.clone().unwrap_or_default(),
        assets: synced_assets,
        published_at: release.published_at.unwrap_or_else(Utc::now),
        synced_at: Utc::now(),
    };

    coll.replace_one(
        doc! {
            "product_id": &record.product_id,
            "source_tag": &record.source_tag,
        },
        &record,
    )
    .with_options(ReplaceOptions::builder().upsert(true).build())
    .await
    .context("upsert release record")?;

    tracing::info!(
        product = %product.id,
        tag = %release.tag_name,
        "release mirrored"
    );
    Ok(())
}
