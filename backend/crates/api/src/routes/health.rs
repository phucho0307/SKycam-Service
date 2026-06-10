use rocket::serde::json::Json;
use rocket::State;
use serde::Serialize;

use crate::db::Db;

#[derive(Serialize)]
pub struct Health {
    status: &'static str,
}

#[get("/healthz")]
pub fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[get("/readyz")]
pub async fn readyz(db: &State<Db>) -> Result<Json<Health>, rocket::http::Status> {
    match db.database.run_command(bson::doc! { "ping": 1 }).await {
        Ok(_) => Ok(Json(Health { status: "ready" })),
        Err(e) => {
            tracing::warn!(error = %e, "readiness probe: mongo ping failed");
            Err(rocket::http::Status::ServiceUnavailable)
        }
    }
}
