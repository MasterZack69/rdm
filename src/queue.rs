use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::cli;
use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Pending,
    Downloading,
    Complete,
    Failed { reason: String, attempts: u32 },
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub url: String,
    pub output: Option<String>,
    pub connections: Option<usize>,
    pub status: Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Queue {
    next_id: u64,
    items: Vec<Item>,
}

impl Default for Queue {
    fn default() -> Self {
        Self { next_id: 1, items: Vec::new() }
    }
}


fn dir() -> PathBuf {
    crate::config::config_path()
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn queue_file() -> PathBuf { dir().join("queue.json") }
fn queue_lock_file() -> PathBuf { dir().join("queue.lock") }
fn processor_lock_file() -> PathBuf { dir().join("processor.lock") }
fn signal_file() -> PathBuf { dir().join("queue.signal") }


fn pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let ret = unsafe { libc::kill(pid as i32, 0) };
        if ret == 0 {
            return true;
        }
        let err = std::io::Error::last_os_error();
        err.raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        true
    }
}


fn read_lock_pid(path: &PathBuf) -> Option<u32> {
    fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
}

pub struct FileLock {
    path: PathBuf,
}

impl FileLock {
    fn acquire(path: PathBuf, timeout_ms: u64) -> Result<Self> {
        fs::create_dir_all(dir())?;

        let max_attempts = (timeout_ms / 100).max(1);
        let mut stale_removals = 0u32;
        const MAX_STALE_REMOVALS: u32 = 3;

        for attempt in 0..max_attempts {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut f) => {
                    let _ = write!(f, "{}", std::process::id());
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Only attempt stale removal a few times
                    if stale_removals < MAX_STALE_REMOVALS {
                        let is_stale = match read_lock_pid(&path) {
                            Some(pid) => !pid_alive(pid),
                            None => {
                                // Can't read PID — check age as last resort
                                fs::metadata(&path)
                                    .ok()
                                    .and_then(|m| m.modified().ok())
                                    .and_then(|t| t.elapsed().ok())
                                    .map(|age| age > Duration::from_secs(86400))
                                    .unwrap_or(false)
                            }
                        };

                        if is_stale {
                            let _ = fs::remove_file(&path);
                            stale_removals += 1;
                            continue; // retry immediately
                        }
                    }

                    if attempt < max_attempts - 1 {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
                Err(e) => return Err(e).context(format!("Failed to acquire lock: {}", path.display())),
            }
        }

        anyhow::bail!(
            "Could not acquire lock {} after {}ms — another rdm instance is running (PID: {})",
            path.display(),
            timeout_ms,
            read_lock_pid(&path).map(|p| p.to_string()).unwrap_or_else(|| "unknown".into()),
        )
    }

    fn transaction() -> Result<Self> {
        Self::acquire(queue_lock_file(), 5000)
    }

    fn processor() -> Result<Self> {
        Self::acquire(processor_lock_file(), 2000)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn atomic_write(path: &PathBuf, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");

    let mut f = fs::File::create(&tmp)
        .with_context(|| format!("Failed to create temp file: {}", tmp.display()))?;

    f.write_all(data)
        .context("Failed to write temp file")?;

    f.sync_all()
        .context("Failed to sync temp file")?;

    fs::rename(&tmp, path)
        .with_context(|| format!("Failed to rename {} → {}", tmp.display(), path.display()))?;

    if let Some(parent) = path.parent() {
        if let Ok(dir) = fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}

// zack here, saying hi to readers.

impl Queue {
    fn load_inner() -> Self {
        fs::read_to_string(queue_file())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save_inner(&self) -> Result<()> {
        fs::create_dir_all(dir())
            .context("Failed to create config directory")?;
        let json = serde_json::to_string_pretty(self)
            .context("Failed to serialize queue")?;
        atomic_write(&queue_file(), json.as_bytes())
    }

    pub fn locked<F, T>(f: F) -> Result<T>
    where
        F: FnOnce(&mut Queue) -> Result<T>,
    {
        let _lock = FileLock::transaction()?;
        let mut queue = Self::load_inner();
        let result = f(&mut queue)?;
        queue.save_inner()?;
        Ok(result)
    }

    pub fn load_readonly() -> Self {
        Self::load_inner()
    }

    pub fn add(&mut self, url: String, output: Option<String>, connections: Option<usize>) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.items.push(Item {
            id,
            url,
            output,
            connections,
            status: Status::Pending,
        });
        id
    }

    pub fn remove(&mut self, id: u64) -> bool {
        let len = self.items.len();
        self.items.retain(|i| i.id != id);
        self.items.len() < len
    }

    pub fn clear_finished(&mut self) -> usize {
        let len = self.items.len();
        self.items.retain(|i| matches!(i.status, Status::Pending | Status::Downloading));
        len - self.items.len()
    }

    pub fn clear_pending(&mut self) -> usize {
        let len = self.items.len();
        self.items.retain(|i| i.status != Status::Pending);
        len - self.items.len()
    }

    pub fn clear_all(&mut self) -> usize {
        let len = self.items.len();
        self.items.clear();
        self.next_id = 1;
        len
    }

    pub fn retry_failed(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if matches!(item.status, Status::Failed { .. }) {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_skipped(&mut self) -> usize {
        let mut count = 0;
        for item in &mut self.items {
            if item.status == Status::Skipped {
                item.status = Status::Pending;
                count += 1;
            }
        }
        count
    }

    pub fn retry_item(&mut self, id: u64) -> bool {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            match item.status {
                Status::Failed { .. } | Status::Skipped => {
                    item.status = Status::Pending;
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }

    fn next_pending(&self) -> Option<&Item> {
        self.items.iter().find(|i| i.status == Status::Pending)
    }

    fn set_status(&mut self, id: u64, status: Status) {
        if let Some(item) = self.items.iter_mut().find(|i| i.id == id) {
            item.status = status;
        }
    }

    pub fn pending_count(&self) -> usize {
        self.items.iter().filter(|i| i.status == Status::Pending).count()
    }

    pub fn print_list(&self) {
        if self.items.is_empty() {
            eprintln!("  Queue is empty.");
            return;
        }

        eprintln!();
        eprintln!("  {:>4}  {:<16}  {}", "ID", "Status", "URL");
        eprintln!("  {}  {}  {}",
            "────", "────────────────", "───────────────────────────────────────────");

        for item in &self.items {
            let status = match &item.status {
                Status::Pending          => "⏳ pending".to_string(),
                Status::Downloading      => "⬇  downloading".to_string(),
                Status::Complete         => "✅ complete".to_string(),
                Status::Failed { .. }    => "❌ failed".to_string(),
                Status::Skipped          => "⏭  skipped".to_string(),
            };

            let url = if item.url.len() > 55 {
                let mut end = 52;
                while end > 0 && !item.url.is_char_boundary(end) {
                    end -= 1;
                }
                format!("{}…", &item.url[..end])
            } else {
                item.url.clone()
            };

            eprintln!("  {:>4}  {:<16}  {}", item.id, status, url);

            if let Some(ref o) = item.output {
                eprintln!("                        → {}", o);
            }
            if let Status::Failed { ref reason, attempts } = item.status {
                eprintln!("                        error ({} attempt{}): {}",
                    attempts,
                    if attempts == 1 { "" } else { "s" },
                    reason,
                );
            }
        }

        let pending = self.pending_count();
        let complete = self.items.iter().filter(|i| i.status == Status::Complete).count();
        let failed = self.items.iter().filter(|i| matches!(i.status, Status::Failed { .. })).count();
        let skipped = self.items.iter().filter(|i| i.status == Status::Skipped).count();

        eprintln!();
        eprintln!("  {} total | {} pending | {} complete | {} failed | {} skipped",
            self.items.len(), pending, complete, failed, skipped);
    }
}

pub fn send_signal(sig: &str) -> Result<()> {
    fs::create_dir_all(dir())?;
    atomic_write(&signal_file(), sig.as_bytes())
}

fn read_signal() -> Option<String> {
    fs::read_to_string(signal_file())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn clear_signal() {
    let _ = fs::remove_file(signal_file());
}

pub async fn start(cfg: &Config, cancel: CancellationToken) -> Result<()> {
    let _processor_lock = FileLock::processor()
        .context("Another `rdm queue start` is already running")?;

    clear_signal();

    Queue::locked(|q| {
        for item in &mut q.items {
            if item.status == Status::Downloading {
                item.status = Status::Pending;
            }
        }
        Ok(())
    })?;

    let pending = Queue::load_readonly().pending_count();

    if pending == 0 {
        eprintln!("  Queue is empty — nothing to do.");
        return Ok(());
    }

    eprintln!("  🚀 Queue started — {} item(s) pending", pending);
    eprintln!();

    let mut completed = 0u32;
    let mut failed = 0u32;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        if let Some(ref sig) = read_signal() {
            if sig == "stop" {
                clear_signal();
                eprintln!("  ⏹  Stop signal received.");
                break;
            }
        }

        let next = Queue::locked(|q| {
            match q.next_pending().cloned() {
                Some(item) => {
                    q.set_status(item.id, Status::Downloading);
                    Ok(Some(item))
                }
                None => Ok(None),
            }
        })?;

        let next = match next {
            Some(item) => item,
            None => break,
        };

        let remaining = Queue::load_readonly().pending_count();
        let position = completed + failed + 1;
        let total = position + remaining as u32;

        eprintln!("  ▶ [{}/{}] #{}: {}",
            position,
            total,
            next.id,
            cli::percent_decode(&next.url),
        );

        let child = cancel.child_token();

        let watcher_token = child.clone();
        let watcher = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if watcher_token.is_cancelled() { break; }
                match read_signal() {
                    Some(ref s) if s == "skip" || s == "stop" => {
                        watcher_token.cancel();
                        break;
                    }
                    _ => {}
                }
            }
        });

            let output = {
            let raw_path = match &next.output {
                Some(o) => cli::percent_decode(o),
                None => {
                    let raw = next.url.split('?').next()
                        .and_then(|p| p.rsplit('/').next())
                        .filter(|s| !s.is_empty())
                        .unwrap_or("download.bin");
                    cli::percent_decode(raw)
                }
            };

            let full_path = cfg.resolve_output_path(&raw_path);

            if let Some(parent) = std::path::Path::new(&full_path).parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent).ok();
                }
            }

            Some(full_path)
        };

        let result = cli::run_download(
            next.url.clone(),
            output,
            next.connections.unwrap_or(cfg.connections),
            child.clone(),
        ).await;

        watcher.abort();

        // Ctrl+C — mark pending, save, exit
        if cancel.is_cancelled() {
            Queue::locked(|q| {
                q.set_status(next.id, Status::Pending);
                Ok(())
            })?;
            eprintln!();
            eprintln!("  ⚠ Queue interrupted — progress saved. Run `rdm queue start` to resume.");
            break;
        }

        let was_skipped = child.is_cancelled();
        let should_stop = read_signal().map(|s| s == "stop").unwrap_or(false);
        clear_signal();

        // Locked: update final status
        Queue::locked(|q| {
            if was_skipped {
                q.set_status(next.id, Status::Skipped);
            } else {
                match &result {
                    Ok(_) => {
                        q.set_status(next.id, Status::Complete);
                    }
                    Err(e) => {
                        let prev_attempts = match q.items.iter().find(|i| i.id == next.id) {
                            Some(Item { status: Status::Failed { attempts, .. }, .. }) => *attempts,
                            _ => 0,
                        };
                        q.set_status(next.id, Status::Failed {
                            reason: format!("{:#}", e),
                            attempts: prev_attempts + 1,
                        });
                    }
                }
            }
            Ok(())
        })?;

        if was_skipped {
            eprintln!("  ⏭  Skipped #{}", next.id);
        } else {
            match &result {
                Ok(_) => completed += 1,
                Err(e) => {
                    failed += 1;
                    eprintln!("  ❌ #{} failed: {:#}", next.id, e);
                }
            }
        }

        eprintln!();

        if should_stop {
            eprintln!("  ⏹  Stop signal received.");
            break;
        }
    }

    eprintln!("  Done. {} completed, {} failed.", completed, failed);
    Ok(())
}
