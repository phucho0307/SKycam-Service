#[macro_use]
extern crate rocket;

mod config;
mod db;
mod routes;

use config::Config;
use db::Db;

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration");
    tracing::info!(env = %cfg.environment, port = cfg.port, "api starting");
    let db = Db::connect(&cfg).await.expect("mongodb connection failed");

    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", cfg.port));

    rocket::custom(figment)
        .manage(db)
        .manage(cfg)
        .mount("/api", routes::all())
}
