//! Cross-process locks.
//!
//! One is held for the length of a queue.json read-modify-write, the other for
//! the length of a `queue start` run. Each lock file holds the owning PID, so a
//! lock left behind by a crashed process can be told apart from a live one
//! instead of blocking every later run.

use anyhow::{Context, Result};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use super::store::{dir, processor_lock_file, queue_lock_file};

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
                    if stale_removals < MAX_STALE_REMOVALS {
                        let is_stale = match read_lock_pid(&path) {
                            Some(pid) => !pid_alive(pid),
                            None => fs::metadata(&path)
                                .ok()
                                .and_then(|m| m.modified().ok())
                                .and_then(|t| t.elapsed().ok())
                                .map(|age| age > Duration::from_secs(86400))
                                .unwrap_or(false),
                        };

                        if is_stale {
                            let _ = fs::remove_file(&path);
                            stale_removals += 1;
                            continue;
                        }
                    }

                    if attempt < max_attempts - 1 {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
                Err(e) => {
                    return Err(e).context(format!("Failed to acquire lock: {}", path.display()));
                }
            }
        }

        anyhow::bail!(
            "Could not acquire lock {} after {}ms — another rdm instance is running (PID: {})",
            path.display(),
            timeout_ms,
            read_lock_pid(&path)
                .map(|p| p.to_string())
                .unwrap_or_else(|| "unknown".into()),
        )
    }

    pub(super) fn transaction() -> Result<Self> {
        Self::acquire(queue_lock_file(), 5000)
    }

    pub(super) fn processor() -> Result<Self> {
        Self::acquire(processor_lock_file(), 2000)
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

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
