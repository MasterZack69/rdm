//! Credentials are read at execution time, never stored in queue items.

use anyhow::{Context, Result, ensure};
use std::{path::PathBuf, sync::Arc};

use crate::config::Config;

pub(crate) struct Authentication {
    pub known_hosts: PathBuf,
    pub identity: Option<PathBuf>,
    pub passphrase: Option<String>,
    pub password: Option<String>,
}

#[derive(Clone)]
pub struct SftpOptions {
    pub(crate) authentication: Arc<Authentication>,
    pub(crate) download_root: PathBuf,
    pub(crate) max_retries: u32,
}

impl SftpOptions {
    pub fn from_config(cfg: &Config) -> Result<Self> {
        let known_hosts = match std::env::var_os("RDM_SFTP_KNOWN_HOSTS") {
            Some(path) => PathBuf::from(path),
            None => dirs::home_dir()
                .context("Cannot locate ~/.ssh/known_hosts; set RDM_SFTP_KNOWN_HOSTS")?
                .join(".ssh/known_hosts"),
        };
        ensure!(!known_hosts.as_os_str().is_empty(), "RDM_SFTP_KNOWN_HOSTS must not be empty");
        let identity = std::env::var_os("RDM_SFTP_IDENTITY_FILE").map(PathBuf::from);
        ensure!(
            identity.as_ref().is_none_or(|path| !path.as_os_str().is_empty()),
            "RDM_SFTP_IDENTITY_FILE must not be empty"
        );
        Ok(Self {
            authentication: Arc::new(Authentication {
                known_hosts,
                identity,
                passphrase: secret("RDM_SFTP_KEY_PASSPHRASE")?,
                password: secret("RDM_SFTP_PASSWORD")?,
            }),
            download_root: std::path::absolute(&cfg.download_dir)
                .context("Invalid download directory")?,
            max_retries: cfg.max_retries,
        })
    }
}

fn secret(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => {
            ensure!(!value.contains('\0'), "{name} contains NUL");
            Ok(Some(value)) // Password whitespace is significant.
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => anyhow::bail!("{name} must be UTF-8"),
    }
}
