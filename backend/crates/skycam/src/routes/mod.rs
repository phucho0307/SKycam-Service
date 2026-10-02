use rocket::Route;

pub mod health;
pub mod ingest;
pub mod live;
pub mod read;
pub mod settings;

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
        // live view: cacheable poll target, stable previews, and SSE
        read::live,
        read::frame_preview,
        live::live_stream,
        // camera settings (GUI edits, Pi fetches)
        settings::get_settings,
        settings::put_settings,
    ]
}
