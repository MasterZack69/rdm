use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    High = 0,
    Normal = 1,
    Low = 2,
}

impl Priority {
    pub fn label(&self) -> &'static str {
        match self {
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Low => "low",
        }
    }

    pub fn icon(&self) -> &'static str {
        match self {
            Priority::High => "🔴",
            Priority::Normal => "🟡",
            Priority::Low => "🟢",
        }
    }
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedJob {
    pub id: u64,
    pub url: String,
    pub output_path: String,
    pub connections: usize,
    pub priority: Priority,
    pub state: JobState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobState {
    Pending,
    Paused,
    Active,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueSnapshot {
    pub next_id: u64,
    pub max_concurrent: usize,
    pub jobs: Vec<PersistedJob>,
}

impl QueueSnapshot {
    pub fn empty(max_concurrent: usize) -> Self {
        Self {
            next_id: 1,
            max_concurrent,
            jobs: Vec::new(),
        }
    }

    pub fn pending_jobs(&self) -> Vec<&PersistedJob> {
        self.jobs.iter().filter(|j| j.state == JobState::Pending).collect()
    }

    pub fn paused_jobs(&self) -> Vec<&PersistedJob> {
        self.jobs.iter().filter(|j| j.state == JobState::Paused).collect()
    }

    pub fn active_jobs(&self) -> Vec<&PersistedJob> {
        self.jobs.iter().filter(|j| j.state == JobState::Active).collect()
    }
}

pub fn default_queue_path() -> PathBuf {
    let base = std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
            PathBuf::from(home).join(".local/share")
        });
    base.join("rdm").join("queue.json")
}

pub async fn save(path: &Path, snapshot: &QueueSnapshot) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let json = serde_json::to_string_pretty(snapshot)
        .context("Failed to serialize queue state")?;

    let tmp_path = path.with_extension("json.tmp");

    let write_result = async {
        let mut file = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("Failed to create temp file: {}", tmp_path.display()))?;

        file.write_all(json.as_bytes()).await.context("Failed to write queue state")?;
        file.flush().await.context("Failed to flush queue state")?;
        file.sync_all().await.context("Failed to sync queue state")?;
        drop(file);

        fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("Failed to rename queue state to {}", path.display()))?;

        let parent = path.parent().unwrap_or(Path::new("."));
        let dir = fs::File::open(parent)
            .await
            .with_context(|| format!("Failed to open dir: {}", parent.display()))?;
        dir.sync_all().await.context("Failed to sync directory")?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = &write_result {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(anyhow::anyhow!("{:#}", e));
    }

    Ok(())
}

pub async fn load(path: &Path) -> Result<QueueSnapshot> {
    let data = fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read queue state: {}", path.display()))?;
    let snapshot: QueueSnapshot = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse queue state: {}", path.display()))?;
    Ok(snapshot)
}

pub async fn load_or_default(path: &Path, max_concurrent: usize) -> QueueSnapshot {
    match load(path).await {
        Ok(s) => s,
        Err(_) => QueueSnapshot::empty(max_concurrent),
    }
}

pub async fn delete(path: &Path) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("Failed to delete: {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_snapshot() -> QueueSnapshot {
        QueueSnapshot {
            next_id: 5,
            max_concurrent: 3,
            jobs: vec![
                PersistedJob {
                    id: 1, url: "https://a.com/1.zip".into(), output_path: "1.zip".into(),
                    connections: 8, priority: Priority::High, state: JobState::Pending,
                },
                PersistedJob {
                    id: 2, url: "https://b.com/2.zip".into(), output_path: "2.zip".into(),
                    connections: 4, priority: Priority::Normal, state: JobState::Paused,
                },
                PersistedJob {
                    id: 3, url: "https://c.com/3.zip".into(), output_path: "3.zip".into(),
                    connections: 8, priority: Priority::Low, state: JobState::Active,
                },
            ],
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let snap = sample_snapshot();
        let json = serde_json::to_string_pretty(&snap).expect("serialize");
        let parsed: QueueSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(snap, parsed);
    }

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::High < Priority::Normal);
        assert!(Priority::Normal < Priority::Low);
    }

    #[test]
    fn test_priority_default() {
        assert_eq!(Priority::default(), Priority::Normal);
    }

    #[test]
    fn test_priority_labels() {
        assert_eq!(Priority::High.label(), "high");
        assert_eq!(Priority::Normal.label(), "normal");
        assert_eq!(Priority::Low.label(), "low");
    }

    #[test]
    fn test_filter_by_state() {
        let snap = sample_snapshot();
        assert_eq!(snap.pending_jobs().len(), 1);
        assert_eq!(snap.paused_jobs().len(), 1);
        assert_eq!(snap.active_jobs().len(), 1);
    }

    #[test]
    fn test_empty_snapshot() {
        let snap = QueueSnapshot::empty(4);
        assert_eq!(snap.next_id, 1);
        assert!(snap.jobs.is_empty());
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let path = PathBuf::from("/tmp/rdm_test_qs_priority.json");
        let _ = fs::remove_file(&path).await;

        let snap = sample_snapshot();
        save(&path, &snap).await.expect("save");
        let loaded = load(&path).await.expect("load");
        assert_eq!(snap, loaded);
        assert_eq!(loaded.jobs[0].priority, Priority::High);
        assert_eq!(loaded.jobs[2].priority, Priority::Low);

        fs::remove_file(&path).await.expect("cleanup");
    }

    #[tokio::test]
    async fn test_load_or_default_missing() {
        let path = PathBuf::from("/tmp/rdm_no_such_prio.json");
        let snap = load_or_default(&path, 5).await;
        assert_eq!(snap.next_id, 1);
    }

    #[tokio::test]
    async fn test_delete_ok() {
        let path = PathBuf::from("/tmp/rdm_test_del_prio.json");
        fs::write(&path, b"{}").await.expect("setup");
        delete(&path).await.expect("delete");
        assert!(fs::metadata(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_ok() {
        let path = PathBuf::from("/tmp/rdm_no_del_prio.json");
        assert!(delete(&path).await.is_ok());
    }
}
