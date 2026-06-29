use rocket::serde::json::Json;
use serde::Serialize;

#[derive(Serialize)]
pub struct Health {
    status: &'static str,
}

#[get("/healthz")]
pub fn healthz() -> Json<Health> {
    Json(Health { status: "ok" })
}
