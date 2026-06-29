use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone)]
pub struct Config {
    pub sitekey: String,
    pub secret: String,
}

#[derive(Deserialize)]
struct VerifyResponse {
    success: bool,
    #[serde(rename = "error-codes", default)]
    error_codes: Vec<String>,
}

/// Verify a Turnstile token against Cloudflare's siteverify endpoint.
/// Returns Ok(true) if the token is valid for this site, Ok(false) otherwise.
pub async fn verify(secret: &str, token: &str) -> Result<bool> {
    let http = reqwest::Client::new();
    let form = [("secret", secret), ("response", token)];
    let resp = http
        .post("https://challenges.cloudflare.com/turnstile/v0/siteverify")
        .form(&form)
        .send()
        .await
        .context("turnstile siteverify request")?;
    let body: VerifyResponse = resp
        .json()
        .await
        .context("parsing turnstile siteverify response")?;
    if !body.success {
        tracing::warn!(errors = ?body.error_codes, "turnstile verification failed");
    }
    Ok(body.success)
}
