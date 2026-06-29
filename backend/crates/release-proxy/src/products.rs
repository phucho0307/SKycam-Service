use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ProductsFile {
    pub products: Vec<Product>,
}

// display_name / winget_package_id / homebrew_formula are read by Phase 1c.2
// (public product cards) and Phase 1c.3 (winget + brew tap generation).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Product {
    pub id: String,
    pub github_repo: String,
    pub display_name: String,
    pub winget_package_id: String,
    pub homebrew_formula: String,
}

impl Product {
    pub fn owner_and_repo(&self) -> Result<(&str, &str)> {
        let mut parts = self.github_repo.splitn(2, '/');
        let owner = parts.next().context("github_repo missing owner")?;
        let repo = parts.next().context("github_repo missing repo name")?;
        Ok((owner, repo))
    }
}

pub fn load(path: &str) -> Result<ProductsFile> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading products file {path}"))?;
    let parsed: ProductsFile =
        serde_yaml::from_str(&raw).with_context(|| format!("parsing products file {path}"))?;
    Ok(parsed)
}
