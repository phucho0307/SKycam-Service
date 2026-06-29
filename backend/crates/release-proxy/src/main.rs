#[macro_use]
extern crate rocket;

mod config;
mod db;
mod github;
mod products;
mod routes;
mod storage;
mod sync;
mod turnstile;

use std::sync::Arc;

use config::Config;
use db::Db;
use github::GithubAppClient;
use storage::Storage;

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration");
    tracing::info!(env = %cfg.environment, port = cfg.port, "release-proxy starting");

    let db = Db::connect(&cfg).await.expect("mongodb connection failed");
    let storage = Storage::connect(&cfg).await.expect("s3 connection failed");
    let gh = Arc::new(
        GithubAppClient::new(
            cfg.github_app_id,
            cfg.github_app_installation_id,
            &cfg.github_app_private_key_pem,
        )
        .expect("github app client init failed"),
    );

    let products_file =
        products::load(&cfg.products_config_path).expect("products config load failed");
    let products: Arc<Vec<products::Product>> = Arc::new(products_file.products);
    tracing::info!(
        products = products.len(),
        path = %cfg.products_config_path,
        "loaded product registry"
    );

    let sync_cfg = cfg.clone();
    let sync_db = db.clone();
    let sync_storage = storage.clone();
    let sync_gh = gh.clone();
    let sync_products = products.clone();
    tokio::spawn(async move {
        sync::run(sync_cfg, sync_db, sync_storage, sync_gh, sync_products).await;
    });

    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", cfg.port));

    let turnstile_cfg = turnstile::Config {
        sitekey: cfg.turnstile_sitekey.clone(),
        secret: cfg.turnstile_secret.clone(),
    };

    rocket::custom(figment)
        .manage(db)
        .manage(storage)
        .manage(gh)
        .manage(products)
        .manage(turnstile_cfg)
        .manage(cfg)
        .mount("/releases", routes::root())
        .mount("/releases/api", routes::api_routes())
}
