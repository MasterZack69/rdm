use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Deserialize;

use crate::queue_state::Priority;

#[derive(Debug, Clone)]
pub struct Config {
    pub connections: usize,
    pub max_jobs: usize,
    pub download_dir: Option<PathBuf>,
    pub default_priority: Priority,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            connections: 8,
            max_jobs: 3,
            download_dir: None,
            default_priority: Priority::Normal,
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    connections: Option<usize>,
    max_jobs: Option<usize>,
    download_dir: Option<String>,
    default_priority: Option<String>,
}

impl Config {
    pub fn load() -> Self {
        match Self::try_load() {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("  ⚠ Config: {:#}", e);
                eprintln!("  Using default configuration.");
                Config::default()
            }
        }
    }

    pub fn load_from(path: &Path) -> Self {
        match Self::try_load_from(path) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("  ⚠ Config ({}): {:#}", path.display(), e);
                eprintln!("  Using default configuration.");
                Config::default()
            }
        }
    }

    fn try_load() -> Result<Self> {
        let path = config_path();

        if !path.exists() {
            return Ok(Config::default());
        }

        Self::try_load_from(&path)
    }

    fn try_load_from(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read {}: {}", path.display(), e))?;

        Self::from_toml(&content)
    }

    fn from_toml(content: &str) -> Result<Self> {
        let raw: RawConfig = toml::from_str(content)
            .map_err(|e| anyhow::anyhow!("invalid TOML: {}", e))?;

        let defaults = Config::default();

        let connections = raw.connections.unwrap_or(defaults.connections).max(1);
        let max_jobs = raw.max_jobs.unwrap_or(defaults.max_jobs).max(1);

        let download_dir = raw.download_dir
            .map(|s| expand_tilde(&s))
            .filter(|p| !p.as_os_str().is_empty());

        let default_priority = raw.default_priority
            .as_deref()
            .map(parse_priority)
            .unwrap_or(Some(defaults.default_priority))
            .unwrap_or(defaults.default_priority);

        Ok(Config {
            connections,
            max_jobs,
            download_dir,
            default_priority,
        })
    }

    pub fn resolve_output_path(&self, filename: &str) -> String {
        match &self.download_dir {
            Some(dir) => {
                let p = dir.join(filename);
                p.to_string_lossy().to_string()
            }
            None => filename.to_string(),
        }
    }
}

pub fn config_path() -> PathBuf {
    let base = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".config")
        });

    base.join("rdm").join("config.toml")
}

fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        return home_dir();
    }

    if let Some(rest) = path.strip_prefix("~/") {
        return home_dir().join(rest);
    }

    PathBuf::from(path)
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}

fn parse_priority(s: &str) -> Option<Priority> {
    match s.trim().to_lowercase().as_str() {
        "high" | "h" => Some(Priority::High),
        "normal" | "n" => Some(Priority::Normal),
        "low" | "l" => Some(Priority::Low),
        _ => {
            eprintln!("  ⚠ Config: unknown priority '{}', using normal", s);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.connections, 8);
        assert_eq!(cfg.max_jobs, 3);
        assert_eq!(cfg.download_dir, None);
        assert_eq!(cfg.default_priority, Priority::Normal);
    }

    #[test]
    fn test_full_toml() {
        let toml = r#"
            connections = 12
            max_jobs = 5
            download_dir = "/tmp/downloads"
            default_priority = "high"
        "#;

        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.connections, 12);
        assert_eq!(cfg.max_jobs, 5);
        assert_eq!(cfg.download_dir, Some(PathBuf::from("/tmp/downloads")));
        assert_eq!(cfg.default_priority, Priority::High);
    }

    #[test]
    fn test_partial_toml_uses_defaults() {
        let toml = r#"
            connections = 16
        "#;

        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.connections, 16);
        assert_eq!(cfg.max_jobs, 3);
        assert_eq!(cfg.download_dir, None);
        assert_eq!(cfg.default_priority, Priority::Normal);
    }

    #[test]
    fn test_empty_toml() {
        let cfg = Config::from_toml("").expect("parse failed");
        assert_eq!(cfg.connections, 8);
        assert_eq!(cfg.max_jobs, 3);
    }

    #[test]
    fn test_invalid_toml_returns_error() {
        let result = Config::from_toml("not valid {{{{ toml!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_toml_load_returns_defaults() {
        let tmp = std::env::temp_dir().join("rdm_test_invalid_config.toml");
        std::fs::write(&tmp, "broken {{[[ toml").expect("write failed");

        let cfg = Config::load_from(&tmp);
        assert_eq!(cfg.connections, 8);
        assert_eq!(cfg.max_jobs, 3);

        std::fs::remove_file(&tmp).expect("cleanup");
    }

    #[test]
    fn test_missing_file_returns_defaults() {
        let path = PathBuf::from("/tmp/rdm_no_such_config_file.toml");
        let cfg = Config::load_from(&path);
        assert_eq!(cfg.connections, 8);
    }

    #[test]
    fn test_tilde_expansion() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());

        assert_eq!(expand_tilde("~/Downloads"), PathBuf::from(format!("{}/Downloads", home)));
        assert_eq!(expand_tilde("~"), PathBuf::from(&home));
        assert_eq!(expand_tilde("/absolute/path"), PathBuf::from("/absolute/path"));
        assert_eq!(expand_tilde("relative/path"), PathBuf::from("relative/path"));
        assert_eq!(expand_tilde("~/a/b/c"), PathBuf::from(format!("{}/a/b/c", home)));
    }

    #[test]
    fn test_tilde_in_download_dir() {
        let toml = r#"
            download_dir = "~/Downloads"
        "#;

        let cfg = Config::from_toml(toml).expect("parse failed");
        let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
        assert_eq!(cfg.download_dir, Some(PathBuf::from(format!("{}/Downloads", home))));
    }

    #[test]
    fn test_connections_clamped_to_min_1() {
        let toml = r#"connections = 0"#;
        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.connections, 1);
    }

    #[test]
    fn test_max_jobs_clamped_to_min_1() {
        let toml = r#"max_jobs = 0"#;
        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.max_jobs, 1);
    }

    #[test]
    fn test_unknown_priority_falls_back() {
        let toml = r#"default_priority = "urgent""#;
        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.default_priority, Priority::Normal);
    }

    #[test]
    fn test_priority_case_insensitive() {
        for (input, expected) in [("High", Priority::High), ("LOW", Priority::Low), ("NORMAL", Priority::Normal)] {
            let toml = format!(r#"default_priority = "{}""#, input);
            let cfg = Config::from_toml(&toml).expect("parse failed");
            assert_eq!(cfg.default_priority, expected);
        }
    }

    #[test]
    fn test_priority_shorthand() {
        for (input, expected) in [("h", Priority::High), ("n", Priority::Normal), ("l", Priority::Low)] {
            let toml = format!(r#"default_priority = "{}""#, input);
            let cfg = Config::from_toml(&toml).expect("parse failed");
            assert_eq!(cfg.default_priority, expected);
        }
    }

    #[test]
    fn test_resolve_output_with_dir() {
        let cfg = Config {
            download_dir: Some(PathBuf::from("/home/user/Downloads")),
            ..Config::default()
        };
        assert_eq!(cfg.resolve_output_path("file.zip"), "/home/user/Downloads/file.zip");
    }

    #[test]
    fn test_resolve_output_without_dir() {
        let cfg = Config::default();
        assert_eq!(cfg.resolve_output_path("file.zip"), "file.zip");
    }

    #[test]
    fn test_empty_download_dir_treated_as_none() {
        let toml = r#"download_dir = """#;
        let cfg = Config::from_toml(toml).expect("parse failed");
        assert_eq!(cfg.download_dir, None);
    }

    #[test]
    fn test_config_path_xdg() {
        let path = config_path();
        let s = path.to_string_lossy();
        assert!(s.ends_with("rdm/config.toml"));
    }

    #[test]
    fn test_extra_unknown_keys_ignored() {
        let toml = r#"
            connections = 4
            unknown_key = "value"
            another = 42
        "#;
        let cfg = Config::from_toml(toml);
        assert!(cfg.is_err() || cfg.unwrap().connections == 4);
    }
}
