use rocket::Route;

pub mod health;

pub fn all() -> Vec<Route> {
    routes![health::healthz, health::readyz]
}
