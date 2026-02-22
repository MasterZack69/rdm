use anyhow::{Context, Result};
use crate::queue_state::{self, JobState, Priority, QueueSnapshot};

pub async fn list() -> Result<()> {
    let path = queue_state::default_queue_path();

    let snap = match queue_state::load(&path).await {
        Ok(s) => s,
        Err(_) => {
            eprintln!("  No queue state found.");
            eprintln!("  Start a queue with: rdm queue <URL> ...");
            return Ok(());
        }
    };

    if snap.jobs.is_empty() {
        eprintln!("  Queue is empty.");
        return Ok(());
    }

    eprintln!("  ──────────────────────────────────────────────────────────");
    eprintln!("  {:>4}  {:<8}  {:<8}  {}", "ID", "Priority", "State", "Output");
    eprintln!("  ──────────────────────────────────────────────────────────");

    for j in &snap.jobs {
        let state_icon = match j.state {
            JobState::Pending => "⏳ pending",
            JobState::Active => "▶ active",
            JobState::Paused => "⏸ paused",
        };
        let prio_icon = match j.priority {
            Priority::High => "🔴 high",
            Priority::Normal => "🟡 normal",
            Priority::Low => "🟢 low",
        };

        eprintln!("  {:>4}  {:<10}  {:<12}  {}", j.id, prio_icon, state_icon, j.output_path);
    }

    eprintln!("  ──────────────────────────────────────────────────────────");

    let pending = snap.jobs.iter().filter(|j| j.state == JobState::Pending).count();
    let active = snap.jobs.iter().filter(|j| j.state == JobState::Active).count();
    let paused = snap.jobs.iter().filter(|j| j.state == JobState::Paused).count();

    eprintln!("  {} pending, {} active, {} paused", pending, active, paused);

    Ok(())
}

pub async fn pause(id: u64) -> Result<()> {
    let path = queue_state::default_queue_path();

    let mut snap = load_or_bail(&path).await?;

    let job = snap.jobs.iter_mut()
        .find(|j| j.id == id)
        .with_context(|| format!("Job #{} not found in queue", id))?;

    match job.state {
        JobState::Paused => {
            eprintln!("  Job #{} is already paused.", id);
            return Ok(());
        }
        JobState::Pending | JobState::Active => {
            job.state = JobState::Paused;
        }
    }

    queue_state::save(&path, &snap).await?;
    eprintln!("  ⏸ Job #{} paused: {}", id, job.output_path);

    if snap.jobs.iter().any(|j| j.id == id && matches!(j.state, JobState::Paused)) {
        eprintln!("  Note: If the queue is running, changes take effect on next restart.");
    }

    Ok(())
}

pub async fn resume(id: u64) -> Result<()> {
    let path = queue_state::default_queue_path();

    let mut snap = load_or_bail(&path).await?;

    let job = snap.jobs.iter_mut()
        .find(|j| j.id == id)
        .with_context(|| format!("Job #{} not found in queue", id))?;

    match job.state {
        JobState::Pending | JobState::Active => {
            eprintln!("  Job #{} is not paused (state: {:?}).", id, job.state);
            return Ok(());
        }
        JobState::Paused => {
            job.state = JobState::Pending;
        }
    }

    queue_state::save(&path, &snap).await?;
    eprintln!("  ▶ Job #{} resumed: {}", id, job.output_path);
    eprintln!("  Run `rdm queue` to start processing.");

    Ok(())
}

pub async fn cancel(id: u64) -> Result<()> {
    let path = queue_state::default_queue_path();

    let mut snap = load_or_bail(&path).await?;

    let before = snap.jobs.len();
    let removed: Vec<_> = snap.jobs.iter()
        .filter(|j| j.id == id)
        .map(|j| j.output_path.clone())
        .collect();

    if removed.is_empty() {
        anyhow::bail!("Job #{} not found in queue", id);
    }

    snap.jobs.retain(|j| j.id != id);

    if snap.jobs.is_empty() {
        queue_state::delete(&path).await?;
        eprintln!("  ✖ Job #{} removed: {}", id, removed[0]);
        eprintln!("  Queue is now empty (state file deleted).");
    } else {
        queue_state::save(&path, &snap).await?;
        eprintln!("  ✖ Job #{} removed: {}", id, removed[0]);
        eprintln!("  {} jobs remaining.", snap.jobs.len());
    }

    Ok(())
}

pub async fn set_priority(id: u64, priority: Priority) -> Result<()> {
    let path = queue_state::default_queue_path();

    let mut snap = load_or_bail(&path).await?;

    let job = snap.jobs.iter_mut()
        .find(|j| j.id == id)
        .with_context(|| format!("Job #{} not found in queue", id))?;

    let old = job.priority;
    job.priority = priority;

    queue_state::save(&path, &snap).await?;
    eprintln!(
        "  Job #{} priority: {} → {} ({})",
        id, old, priority, job.output_path,
    );

    Ok(())
}

pub async fn clear() -> Result<()> {
    let path = queue_state::default_queue_path();

    match queue_state::load(&path).await {
        Ok(snap) if !snap.jobs.is_empty() => {
            let active = snap.jobs.iter().filter(|j| j.state == JobState::Active).count();
            if active > 0 {
                anyhow::bail!(
                    "Cannot clear: {} active job(s). Stop the queue first.",
                    active,
                );
            }

            queue_state::delete(&path).await?;
            eprintln!("  ✖ Cleared {} jobs from queue.", snap.jobs.len());
        }
        Ok(_) => {
            eprintln!("  Queue is already empty.");
        }
        Err(_) => {
            eprintln!("  No queue state found.");
        }
    }

    Ok(())
}

async fn load_or_bail(path: &std::path::Path) -> Result<QueueSnapshot> {
    queue_state::load(path)
        .await
        .context("No queue state found. Start a queue with: rdm queue <URL> ...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_state::PersistedJob;
    use std::path::PathBuf;
    use tokio::fs;

    fn test_snap() -> QueueSnapshot {
        QueueSnapshot {
            next_id: 5,
            max_concurrent: 3,
            jobs: vec![
                PersistedJob {
                    id: 1, url: "https://a.com/1".into(), output_path: "1.zip".into(),
                    connections: 8, priority: Priority::High, state: JobState::Pending,
                },
                PersistedJob {
                    id: 2, url: "https://b.com/2".into(), output_path: "2.zip".into(),
                    connections: 4, priority: Priority::Normal, state: JobState::Active,
                },
                PersistedJob {
                    id: 3, url: "https://c.com/3".into(), output_path: "3.zip".into(),
                    connections: 8, priority: Priority::Low, state: JobState::Paused,
                },
            ],
        }
    }

    async fn save_test(name: &str) -> PathBuf {
        let path = PathBuf::from(format!("/tmp/rdm_test_cmd_{}.json", name));
        let _ = fs::remove_file(&path).await;
        queue_state::save(&path, &test_snap()).await.expect("save");
        path
    }

    #[tokio::test]
    async fn test_pause_pending_job() {
        let path = save_test("pause_pending").await;

        let mut snap = queue_state::load(&path).await.unwrap();
        let job = snap.jobs.iter_mut().find(|j| j.id == 1).unwrap();
        assert_eq!(job.state, JobState::Pending);

        job.state = JobState::Paused;
        queue_state::save(&path, &snap).await.unwrap();

        let reloaded = queue_state::load(&path).await.unwrap();
        let j = reloaded.jobs.iter().find(|j| j.id == 1).unwrap();
        assert_eq!(j.state, JobState::Paused);

        fs::remove_file(&path).await.expect("cleanup");
    }

    #[tokio::test]
    async fn test_resume_paused_job() {
        let path = save_test("resume_paused").await;

        let mut snap = queue_state::load(&path).await.unwrap();
        let job = snap.jobs.iter_mut().find(|j| j.id == 3).unwrap();
        assert_eq!(job.state, JobState::Paused);

        job.state = JobState::Pending;
        queue_state::save(&path, &snap).await.unwrap();

        let reloaded = queue_state::load(&path).await.unwrap();
        let j = reloaded.jobs.iter().find(|j| j.id == 3).unwrap();
        assert_eq!(j.state, JobState::Pending);

        fs::remove_file(&path).await.expect("cleanup");
    }

    #[tokio::test]
    async fn test_cancel_removes_job() {
        let path = save_test("cancel_remove").await;

        let mut snap = queue_state::load(&path).await.unwrap();
        snap.jobs.retain(|j| j.id != 1);
        queue_state::save(&path, &snap).await.unwrap();

        let reloaded = queue_state::load(&path).await.unwrap();
        assert_eq!(reloaded.jobs.len(), 2);
        assert!(reloaded.jobs.iter().all(|j| j.id != 1));

        fs::remove_file(&path).await.expect("cleanup");
    }

    #[tokio::test]
    async fn test_cancel_last_deletes_file() {
        let path = PathBuf::from("/tmp/rdm_test_cmd_cancel_last.json");
        let _ = fs::remove_file(&path).await;

        let snap = QueueSnapshot {
            next_id: 2,
            max_concurrent: 2,
            jobs: vec![PersistedJob {
                id: 1, url: "https://a.com/1".into(), output_path: "1.zip".into(),
                connections: 4, priority: Priority::Normal, state: JobState::Pending,
            }],
        };
        queue_state::save(&path, &snap).await.unwrap();

        let mut loaded = queue_state::load(&path).await.unwrap();
        loaded.jobs.retain(|j| j.id != 1);
        assert!(loaded.jobs.is_empty());

        queue_state::delete(&path).await.unwrap();
        assert!(fs::metadata(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_set_priority() {
        let path = save_test("set_priority").await;

        let mut snap = queue_state::load(&path).await.unwrap();
        let job = snap.jobs.iter_mut().find(|j| j.id == 1).unwrap();
        assert_eq!(job.priority, Priority::High);

        job.priority = Priority::Low;
        queue_state::save(&path, &snap).await.unwrap();

        let reloaded = queue_state::load(&path).await.unwrap();
        let j = reloaded.jobs.iter().find(|j| j.id == 1).unwrap();
        assert_eq!(j.priority, Priority::Low);

        fs::remove_file(&path).await.expect("cleanup");
    }

    #[tokio::test]
    async fn test_clear_no_active() {
        let path = PathBuf::from("/tmp/rdm_test_cmd_clear.json");
        let _ = fs::remove_file(&path).await;

        let snap = QueueSnapshot {
            next_id: 3,
            max_concurrent: 2,
            jobs: vec![
                PersistedJob {
                    id: 1, url: "https://a.com/1".into(), output_path: "1.zip".into(),
                    connections: 4, priority: Priority::Normal, state: JobState::Pending,
                },
                PersistedJob {
                    id: 2, url: "https://b.com/2".into(), output_path: "2.zip".into(),
                    connections: 4, priority: Priority::Low, state: JobState::Paused,
                },
            ],
        };
        queue_state::save(&path, &snap).await.unwrap();

        queue_state::delete(&path).await.unwrap();
        assert!(fs::metadata(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_list_empty_no_panic() {
        // Just verify list() doesn't panic when no file exists
        // (We can't easily test default path in unit tests,
        //  so we test the underlying logic)
        let path = PathBuf::from("/tmp/rdm_nonexistent_list.json");
        assert!(queue_state::load(&path).await.is_err());
    }
}
