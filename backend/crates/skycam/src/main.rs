#[macro_use]
extern crate rocket;

mod config;
mod db;
mod routes;
mod storage;

use config::Config;
use db::Db;
use rocket::data::{Limits, ToByteUnit};
use storage::Storage;

#[launch]
async fn rocket() -> _ {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env().expect("invalid configuration");
    tracing::info!(env = %cfg.environment, port = cfg.port, "skycam starting");
    let db = Db::connect(&cfg).await.expect("mongodb connection failed");

    // Raise body limits so full-res FITS frames (tens of MB) fit through the
    // multipart upload. Keep this in sync with any Traefik body-size middleware.
    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", cfg.port))
        .merge((
            "limits",
            Limits::new()
                .limit("file", 64.mebibytes())
                .limit("data-form", 64.mebibytes()),
        ));

    let mut app = rocket::custom(figment)
        .manage(db)
        .mount("/skycam", routes::all());

    match &cfg.s3 {
        Some(s3) => {
            tracing::info!(bucket = %s3.bucket, "s3 storage enabled");
            app = app.manage(Storage::new(s3));
        }
        None => {
            tracing::warn!("S3 not configured; POST /skycam/frames will return 500");
        }
    }

    app.manage(cfg)
}
