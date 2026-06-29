use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

const USER_AGENT: &str = "observatory-services-release-proxy/0.1";

// Fields tagged allow(dead_code) are not yet read but will be used by the
// Phase 1c.2 download/listing API; keeping them deserialized now avoids a
// second pass to re-derive the parser.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Release {
    pub id: u64,
    pub tag_name: String,
    pub name: Option<String>,
    pub body: Option<String>,
    pub draft: bool,
    pub prerelease: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub assets: Vec<Asset>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Asset {
    pub id: u64,
    pub name: String,
    pub size: u64,
    pub content_type: String,
    pub url: String,
    pub browser_download_url: String,
}

#[derive(Serialize)]
struct AppClaims {
    iat: u64,
    exp: u64,
    iss: u64,
}

#[derive(Deserialize)]
struct InstallationTokenResponse {
    token: String,
    expires_at: DateTime<Utc>,
}

pub struct GithubAppClient {
    http: reqwest::Client,
    app_id: u64,
    installation_id: u64,
    encoding_key: EncodingKey,
    cached_token: Mutex<Option<(String, DateTime<Utc>)>>,
}

impl GithubAppClient {
    pub fn new(app_id: u64, installation_id: u64, private_key_pem: &str) -> Result<Self> {
        let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
            .context("parsing GitHub App private key (PEM RSA)")?;
        let http = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            http,
            app_id,
            installation_id,
            encoding_key,
            cached_token: Mutex::new(None),
        })
    }

    fn make_jwt(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before epoch")?
            .as_secs();
        let claims = AppClaims {
            iat: now - 60,
            exp: now + 540,
            iss: self.app_id,
        };
        let header = Header::new(Algorithm::RS256);
        encode(&header, &claims, &self.encoding_key).context("encoding App JWT")
    }

    pub async fn installation_token(&self) -> Result<String> {
        let mut cached = self.cached_token.lock().await;
        if let Some((token, expires)) = cached.as_ref() {
            // refresh a minute before expiry
            if *expires > Utc::now() + chrono::Duration::seconds(60) {
                return Ok(token.clone());
            }
        }

        let jwt = self.make_jwt()?;
        let resp = self
            .http
            .post(format!(
                "https://api.github.com/app/installations/{}/access_tokens",
                self.installation_id
            ))
            .bearer_auth(&jwt)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .context("requesting installation token")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "installation token request failed: {status} body={body}"
            ));
        }

        let body: InstallationTokenResponse =
            resp.json().await.context("parsing installation token")?;
        *cached = Some((body.token.clone(), body.expires_at));
        Ok(body.token)
    }

    pub async fn list_releases(&self, owner: &str, repo: &str) -> Result<Vec<Release>> {
        let token = self.installation_token().await?;
        let url = format!("https://api.github.com/repos/{owner}/{repo}/releases?per_page=100");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .with_context(|| format!("listing releases for {owner}/{repo}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "list releases failed for {owner}/{repo}: {status} body={body}"
            ));
        }

        let releases: Vec<Release> = resp.json().await.context("parsing releases")?;
        Ok(releases)
    }

    /// Stream-download a release asset by its API id. Returns bytes + content-type.
    pub async fn download_asset(
        &self,
        owner: &str,
        repo: &str,
        asset_id: u64,
    ) -> Result<(bytes::Bytes, String)> {
        let token = self.installation_token().await?;
        let url = format!("https://api.github.com/repos/{owner}/{repo}/releases/assets/{asset_id}");
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&token)
            .header("Accept", "application/octet-stream")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .timeout(Duration::from_secs(300))
            .send()
            .await
            .with_context(|| format!("downloading asset {asset_id} for {owner}/{repo}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!(
                "asset download failed for {owner}/{repo}#{asset_id}: {status} body={body}"
            ));
        }

        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let bytes = resp.bytes().await.context("reading asset bytes")?;
        Ok((bytes, content_type))
    }
}
