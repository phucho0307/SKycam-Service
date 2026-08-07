use rocket::Route;

pub mod health;
pub mod ingest;
pub mod read;

pub fn all() -> Vec<Route> {
    routes![
        health::healthz,
        health::readyz,
        // write (device, bearer-token)
        ingest::telemetry,
        ingest::frames,
        // read (GUI)
        read::frames_latest,
        read::frames_list,
        read::telemetry_list,
    ]
}
