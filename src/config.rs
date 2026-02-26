use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub connections: usize,
    pub download_dir: String,
    pub max_retries: u32,
    pub queue_parallel: usize,
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
            queue_parallel: 1,
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
            filename.to_string()
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
    }
}
