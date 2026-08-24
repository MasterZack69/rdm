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

    /// Files of a Google Drive folder to download at once.
    ///
    /// Files, not chunks again, and here the reason is quota rather than
    /// bandwidth: Drive counts requests per key and per second, so a high
    /// number buys 403s rather than throughput.
    #[serde(default = "default_gdrive_workers")]
    pub gdrive_workers: usize,

    /// A Google Drive API key. Optional: a file whose link you already hold
    /// downloads without one, but listing a folder does not, and neither does
    /// learning a file's real name without a round trip through the warning
    /// page. `RDM_GDRIVE_API_KEY` overrides this for a single run.
    ///
    /// A key is a quota identity rather than a credential — a restricted share
    /// stays unreadable with or without one — but it is billable, so it is
    /// treated like the GoFile token and never printed.
    #[serde(default)]
    pub gdrive_api_key: String,

    /// What a Google Doc, Sheet, Slide deck or Drawing is exported as: an
    /// extension (`pdf`, `docx`, `xlsx`, `csv`, `png`, …), or `office` for
    /// whichever Microsoft format the kind has. A Google document is rendered
    /// on request rather than stored, so there is no original to fall back on
    /// and something has to choose.
    #[serde(default = "default_gdrive_doc_format")]
    pub gdrive_doc_format: String,

    /// Files of a pixeldrain list to download at once.
    ///
    /// Files, not chunks, and for a reason particular to pixeldrain: an
    /// anonymous transfer allowance is shared across connections, so splitting
    /// one file mostly divides the same bandwidth between its own chunks.
    #[serde(default = "default_pixeldrain_workers")]
    pub pixeldrain_workers: usize,

    /// A pixeldrain account API key.
    ///
    /// Optional, and it buys speed rather than access: pixeldrain caps
    /// anonymous transfers and lifts the cap for an account. Empty means
    /// anonymous, which works perfectly well and is slower.
    ///
    /// `RDM_PIXELDRAIN_API_KEY` takes precedence, for people who would rather
    /// not keep a credential in a file at all.
    #[serde(default)]
    pub pixeldrain_api_key: String,
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

fn default_gdrive_workers() -> usize {
    crate::hoster::gdrive::WORKERS_DEFAULT
}

/// PDF, because it is the one format every Google document kind exports as and
/// the one the Docs "File \u{2192} Download" menu offers first.
fn default_gdrive_doc_format() -> String {
    "pdf".to_owned()
}

fn default_pixeldrain_workers() -> usize {
    crate::hoster::pixeldrain::WORKERS_DEFAULT
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
            gdrive_workers: default_gdrive_workers(),
            gdrive_api_key: String::new(),
            gdrive_doc_format: default_gdrive_doc_format(),
            pixeldrain_workers: default_pixeldrain_workers(),
            pixeldrain_api_key: String::new(),
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

    /// The Drive API key for this run, environment first.
    ///
    /// `RDM_GDRIVE_API_KEY` wins over the config file, so a key can be given
    /// for one run without being written to disk. A blank value on either side
    /// is not a key: sending an empty one turns every call into a 400 instead
    /// of falling back to anonymous access.
    pub fn gdrive_key(&self) -> Option<String> {
        std::env::var("RDM_GDRIVE_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty())
            .or_else(|| Some(self.gdrive_api_key.clone()).filter(|key| !key.trim().is_empty()))
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
        eprintln!("  Drive      : {} file(s) at a time", self.gdrive_workers);
        eprintln!("  Drive docs : exported as {}", self.gdrive_doc_format);
        // Whether a key is set, never the key: same rule as the GoFile token
        // below.
        eprintln!(
            "  pixeldrain : {} file(s) at a time ({})",
            self.pixeldrain_workers,
            if self.pixeldrain_api_key.trim().is_empty() {
                "anonymous"
            } else {
                "API key set"
            }
        );
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
        // Same rule, and the same reason a folder download can refuse before
        // it starts.
        eprintln!(
            "  Drive key  : {}",
            if self.gdrive_api_key.trim().is_empty() {
                "none (folders unavailable)"
            } else {
                "set"
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
        assert_eq!(
            cfg.gdrive_workers,
            crate::hoster::gdrive::WORKERS_DEFAULT
        );
        assert_eq!(
            cfg.pixeldrain_workers,
            crate::hoster::pixeldrain::WORKERS_DEFAULT
        );
        assert!(cfg.gofile_token.is_empty());
        // No key means anonymous access, which is a working configuration for
        // everything except a folder.
        assert!(cfg.gdrive_api_key.is_empty());
        assert_eq!(cfg.gdrive_doc_format, "pdf");
        assert!(cfg.pixeldrain_api_key.is_empty());
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

    #[test]
    fn gdrive_settings_round_trip() {
        let cfg = Config {
            gdrive_workers: 7,
            gdrive_api_key: "AIzaSyExampleKey".to_owned(),
            gdrive_doc_format: "office".to_owned(),
            ..Default::default()
        };
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.gdrive_workers, 7);
        assert_eq!(back.gdrive_api_key, "AIzaSyExampleKey");
        assert_eq!(back.gdrive_doc_format, "office");
    }

    #[test]
    fn pixeldrain_settings_round_trip() {
        let cfg = Config {
            pixeldrain_workers: 6,
            pixeldrain_api_key: "deadbeef".to_owned(),
            ..Default::default()
        };
        let text = toml::to_string_pretty(&cfg).unwrap();
        let back: Config = toml::from_str(&text).unwrap();
        assert_eq!(back.pixeldrain_workers, 6);
        assert_eq!(back.pixeldrain_api_key, "deadbeef");
    }
}
