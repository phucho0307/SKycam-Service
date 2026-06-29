use bson::doc;
use mongodb::options::FindOptions;
use rocket::futures::TryStreamExt;
use rocket::http::Status;
use rocket::response::Redirect;
use rocket::serde::json::Json;
use rocket::State;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::db::{Db, SyncedRelease};
use crate::github::GithubAppClient;
use crate::products::Product;
use crate::storage::Storage;

#[derive(Serialize)]
pub struct ProductSummary {
    id: String,
    display_name: String,
    github_repo: String,
    winget_package_id: String,
    homebrew_formula: String,
    latest_tag: Option<String>,
    latest_published_at: Option<chrono::DateTime<chrono::Utc>>,
    release_count: u64,
}

#[derive(Serialize)]
pub struct ProductDetail {
    id: String,
    display_name: String,
    github_repo: String,
    winget_package_id: String,
    homebrew_formula: String,
    releases: Vec<ReleaseView>,
}

#[derive(Serialize)]
pub struct ReleaseView {
    tag: String,
    name: String,
    notes_md: String,
    published_at: chrono::DateTime<chrono::Utc>,
    assets: Vec<AssetView>,
}

#[derive(Serialize)]
pub struct AssetView {
    filename: String,
    size_bytes: u64,
    content_type: String,
    download_url: String,
}

#[get("/products")]
pub async fn list_products(
    db: &State<Db>,
    products: &State<Arc<Vec<Product>>>,
) -> Result<Json<Vec<ProductSummary>>, Status> {
    let coll = db.releases();
    let mut out = Vec::with_capacity(products.len());
    for p in products.iter() {
        let count = coll
            .count_documents(doc! { "product_id": &p.id })
            .await
            .map_err(internal_error)?;
        let latest = coll
            .find_one(doc! { "product_id": &p.id })
            .with_options(
                mongodb::options::FindOneOptions::builder()
                    .sort(doc! { "published_at": -1 })
                    .build(),
            )
            .await
            .map_err(internal_error)?;
        out.push(ProductSummary {
            id: p.id.clone(),
            display_name: p.display_name.clone(),
            github_repo: p.github_repo.clone(),
            winget_package_id: p.winget_package_id.clone(),
            homebrew_formula: p.homebrew_formula.clone(),
            latest_tag: latest.as_ref().map(|r| r.source_tag.clone()),
            latest_published_at: latest.as_ref().map(|r| r.published_at),
            release_count: count,
        });
    }
    Ok(Json(out))
}

#[get("/products/<id>")]
pub async fn product_detail(
    id: &str,
    db: &State<Db>,
    products: &State<Arc<Vec<Product>>>,
) -> Result<Json<ProductDetail>, Status> {
    let product = products
        .iter()
        .find(|p| p.id == id)
        .ok_or(Status::NotFound)?;
    let coll = db.releases();
    let releases: Vec<SyncedRelease> = coll
        .find(doc! { "product_id": id })
        .with_options(
            FindOptions::builder()
                .sort(doc! { "published_at": -1 })
                .build(),
        )
        .await
        .map_err(internal_error)?
        .try_collect()
        .await
        .map_err(internal_error)?;

    let releases = releases
        .into_iter()
        .map(|r| ReleaseView {
            tag: r.source_tag.clone(),
            name: r.release_name,
            notes_md: r.notes_md,
            published_at: r.published_at,
            assets: r
                .assets
                .into_iter()
                .map(|a| AssetView {
                    download_url: format!(
                        "/releases/api/products/{}/{}/{}",
                        id, r.source_tag, a.filename
                    ),
                    filename: a.filename,
                    size_bytes: a.size_bytes,
                    content_type: a.content_type,
                })
                .collect(),
        })
        .collect();

    Ok(Json(ProductDetail {
        id: product.id.clone(),
        display_name: product.display_name.clone(),
        github_repo: product.github_repo.clone(),
        winget_package_id: product.winget_package_id.clone(),
        homebrew_formula: product.homebrew_formula.clone(),
        releases,
    }))
}

#[get("/products/<id>/<tag>/<asset>")]
pub async fn download_asset(
    id: &str,
    tag: &str,
    asset: &str,
    db: &State<Db>,
    storage: &State<Storage>,
) -> Result<Redirect, Status> {
    let coll = db.releases();
    let release = coll
        .find_one(doc! { "product_id": id, "source_tag": tag })
        .await
        .map_err(internal_error)?
        .ok_or(Status::NotFound)?;
    let asset_record = release
        .assets
        .iter()
        .find(|a| a.filename == asset)
        .ok_or(Status::NotFound)?;
    let target = storage.public_url(&asset_record.s3_key);
    tracing::info!(product = %id, tag, asset, "asset download redirect");
    Ok(Redirect::found(target))
}

#[derive(Deserialize)]
pub struct FeatureRequest {
    product_id: String,
    title: String,
    body: String,
    reporter_name: Option<String>,
    reporter_email: Option<String>,
}

#[derive(Serialize)]
pub struct FeatureRequestCreated {
    issue_url: String,
}

#[post("/feature-requests", data = "<req>")]
pub async fn create_feature_request(
    req: Json<FeatureRequest>,
    products: &State<Arc<Vec<Product>>>,
    gh: &State<Arc<GithubAppClient>>,
) -> Result<Json<FeatureRequestCreated>, Status> {
    let req = req.into_inner();
    if req.title.trim().is_empty()
        || req.title.len() > 200
        || req.body.trim().is_empty()
        || req.body.len() > 8000
    {
        return Err(Status::BadRequest);
    }
    let product = products
        .iter()
        .find(|p| p.id == req.product_id)
        .ok_or(Status::NotFound)?;
    let (owner, repo) = product
        .owner_and_repo()
        .map_err(|_| Status::InternalServerError)?;

    let footer = match (req.reporter_name.as_deref(), req.reporter_email.as_deref()) {
        (Some(name), Some(email)) if !name.is_empty() && !email.is_empty() => {
            format!("\n\n---\nReported by {name} <{email}> via observatory.services")
        }
        (Some(name), _) if !name.is_empty() => {
            format!("\n\n---\nReported by {name} via observatory.services")
        }
        _ => "\n\n---\nReported anonymously via observatory.services".to_string(),
    };
    let full_body = format!("{}{}", req.body.trim(), footer);

    let url = gh
        .create_issue(
            owner,
            repo,
            req.title.trim(),
            &full_body,
            &["feature-request", "observatory-services"],
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "feature request creation failed");
            Status::BadGateway
        })?;

    Ok(Json(FeatureRequestCreated { issue_url: url }))
}

fn internal_error<E: std::fmt::Display>(e: E) -> Status {
    tracing::error!(error = %e, "internal error");
    Status::InternalServerError
}
