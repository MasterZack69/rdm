//! The shared HTTP client and configuration.
//!
//! Both are built once per process: the client so its connection pool is
//! reused across downloads, the config because loading it touches the disk.

use anyhow::{Context, Result};
use std::sync::OnceLock;
use std::time::Duration;

use crate::config::Config;

static SHARED_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

pub(super) fn shared_client() -> Result<&'static reqwest::Client> {
    if let Some(c) = SHARED_CLIENT.get() {
        return Ok(c);
    }
    let client = reqwest::Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build HTTP client")?;
    Ok(SHARED_CLIENT.get_or_init(|| client))
}

static SHARED_CONFIG: OnceLock<Config> = OnceLock::new();

pub(super) fn shared_config() -> &'static Config {
    SHARED_CONFIG.get_or_init(Config::load)
}
