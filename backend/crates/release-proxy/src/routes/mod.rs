use rocket::Route;

pub mod api;
pub mod health;

pub fn root() -> Vec<Route> {
    routes![health::healthz]
}

pub fn api_routes() -> Vec<Route> {
    routes![
        api::list_products,
        api::product_detail,
        api::download_asset,
        api::create_feature_request,
    ]
}
