use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Every field added after 0.2.1 needs a `#[serde(default = ...)]`.
///
/// [`Config::load`] falls back to `Config::default()` for *any* parse error,
/// so a missing field in an older `config.toml` would silently throw away the
/// user's whole configuration instead of just filling in the new value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub connections: usize,
    pub download_dir: String,
    pub max_retries: u32,
    pub queue_parallel: usize,

    /// Parallel chunk workers for a MEGA download.
    #[serde(default = "default_mega_workers")]
    pub mega_workers: usize,

    /// Verify the CBC-MAC of a finished MEGA file against the meta-MAC in the
    /// link. Catches silent corruption that plain HTTP downloads cannot see.
    #[serde(default = "default_true")]
    pub mega_verify_mac: bool,

    /// While waiting out a MEGA HTTP 509 (bandwidth quota), poll the public IP
    /// and retry immediately when it changes — i.e. when the user turns on a
    /// VPN. Turn this off on NAT-behind-NAT setups where detection misfires.
    #[serde(default = "default_true")]
    pub mega_resume_on_ip_change: bool,

    /// How many GoFile files to download at once.
    ///
    /// Files, not chunks: GoFile rate-limits per connection, so throughput
    /// comes from running several files side by side rather than splitting
    /// one.
    #[serde(default = "default_gofile_workers")]
    pub gofile_workers: usize,

    /// A GoFile account token, for people who have an account and want their
    /// quota rather than a throwaway guest one. Empty means "create a guest
    /// account per run", which is what the website does for visitors.
    #[serde(default)]
    pub gofile_token: String,

    /// Files of a OneDrive folder share to download at once.
    ///
    /// Files, not chunks: each file takes its own connection, so a folder
    /// share fills the pipe by running several files side by side.
    #[serde(default = "default_onedrive_workers")]
    pub onedrive_workers: usize,
}

fn default_mega_workers() -> usize {
    crate::mega::WORKERS_DEFAULT
}

fn default_gofile_workers() -> usize {
    crate::hoster::gofile::WORKERS_DEFAULT
}

fn default_onedrive_workers() -> usize {
    crate::hoster::onedrive::WORKERS_DEFAULT
}

fn default_true() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        let download_dir = dirs::download_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join("Downloads")))
            .unwrap_or_else(|| PathBuf::from("."))
            .to_string_lossy()
            .to_string();

        Self {
            connections: 8,
            download_dir,
            max_retries: 6,
            queue_parallel: 3,
            mega_workers: default_mega_workers(),
            mega_verify_mac: true,
            mega_resume_on_ip_change: true,
            gofile_workers: default_gofile_workers(),
            gofile_token: String::new(),
            onedrive_workers: default_onedrive_workers(),
        }
    }
}

pub fn config_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("rdm")
        .join("config.toml")
}

impl Config {
    pub fn load() -> Self {
        let path = config_path();
        match std::fs::read_to_string(&path) {
            Ok(contents) => toml::from_str(&contents).unwrap_or_default(),
            Err(_) => {
                let cfg = Config::default();
                let _ = cfg.save();
                cfg
            }
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .context("Failed to create config directory")?;
        }
        let toml = toml::to_string_pretty(self)
            .context("Failed to serialize config")?;
        std::fs::write(&path, toml)
            .context("Failed to write config file")?;
        Ok(())
    }

    pub fn resolve_output_path(&self, filename: &str) -> String {
        let path = std::path::Path::new(filename);
        if path.is_absolute() {
            filename.to_owned()
        } else {
            PathBuf::from(&self.download_dir)
                .join(filename)
                .to_string_lossy()
                .to_string()
        }
    }

    pub fn print(&self) {
        eprintln!("  Config     : {}", config_path().display());
        eprintln!("  Download   : {}", self.download_dir);
        eprintln!("  Connections: {}", self.connections);
        eprintln!("  Max retries: {}", self.max_retries);
        eprintln!("  Queue par. : {}", self.queue_parallel);
        eprintln!("  MEGA slots : {}", self.mega_workers);
        eprintln!("  MEGA verify: {}", self.mega_verify_mac);
        eprintln!("  MEGA VPN   : {}", self.mega_resume_on_ip_change);
        eprintln!("  GoFile     : {} file(s) at a time", self.gofile_workers);
        eprintln!("  OneDrive   : {} file(s) at a time", self.onedrive_workers);
        // Never print the token itself: config output gets pasted into bug
        // reports.
        eprintln!(
            "  GoFile acct: {}",
            if self.gofile_token.trim().is_empty() {
                "guest"
            } else {
                "account token set"
            }
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 0.2.1 config file has none of the mega_* or gofile_* keys. It must
    /// still load, keeping the values the user did set.
    #[test]
    fn older_config_files_still_parse() {
        let old = r#"
connections = 12
download_dir = "/tmp/dl"
max_retries = 69
queue_parallel = 5
"#;
        let cfg: Config = toml::from_str(old).expect("old config should still parse");
        assert_eq!(cfg.connections, 12);
        assert_eq!(cfg.download_dir, "/tmp/dl");
        assert_eq!(cfg.max_retries, 69);
        assert_eq!(cfg.queue_parallel, 5);
        assert_eq!(cfg.mega_workers, crate::mega::WORKERS_DEFAULT);
        assert!(cfg.mega_verify_mac);
        assert!(cfg.mega_resume_on_ip_change);
        assert_eq!(
            cfg.gofile_workers,
            crate::hoster::gofile::WORKERS_DEFAULT
        );
        assert_eq!(
            cfg.onedrive_workers,
            crate::hoster::onedrive::WORKERS_DEFAULT
        );
        assert!(cfg.gofile_token.is_empty());
    }

    #[test]
    fn mega_settings_round_trip() {
        let cfg = Config {
            mega_workers: 12,
            mega_verify_mac: false,
            ..Default::default()
        };
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.mega_workers, 12);
        assert!(!back.mega_verify_mac);
    }

    #[test]
    fn gofile_settings_round_trip() {
        let cfg = Config {
            gofile_workers: 3,
            gofile_token: "abc123".to_owned(),
            ..Default::default()
        };
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.gofile_workers, 3);
        assert_eq!(back.gofile_token, "abc123");
    }
}
