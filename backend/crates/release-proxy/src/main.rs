#[macro_use]
extern crate rocket;

use rocket::serde::json::Json;
use serde::Serialize;

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[get("/healthz")]
fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[launch]
fn rocket() -> _ {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8001);

    let figment = rocket::Config::figment()
        .merge(("address", "0.0.0.0"))
        .merge(("port", port));

    // TODO Phase 1c:
    //  - GitHub App auth (poll + webhook)
    //  - Sync releases from configured private repos
    //  - Cache binaries to S3 (`<S3_ENDPOINT>` / `<S3_BUCKET>`)
    //  - GET /releases/:product, /releases/:product/:version, /download/:product/:version/:asset
    //  - POST /feature-requests -> create GitHub Issue
    //  - GET /winget/:product/manifest.yaml
    //  - Push to Homebrew tap repo

    rocket::custom(figment).mount("/releases", routes![healthz])
}
