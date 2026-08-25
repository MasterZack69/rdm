//! Where the queue state lives on disk, and how it is written.
//!
//! Every write goes out as a temp file plus a rename, so a crash mid-write
//! leaves the previous queue.json intact rather than half of the new one.

use anyhow::{Context, Result};
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub(super) fn dir() -> PathBuf {
    crate::config::config_path()
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

pub(super) fn queue_file() -> PathBuf {
    dir().join("queue.json")
}

pub(super) fn queue_lock_file() -> PathBuf {
    dir().join("queue.lock")
}

pub(super) fn processor_lock_file() -> PathBuf {
    dir().join("processor.lock")
}

pub(super) fn signal_file() -> PathBuf {
    dir().join("queue.signal")
}

pub(super) fn atomic_write(path: &PathBuf, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");

    let mut f = fs::File::create(&tmp)
        .with_context(|| format!("Failed to create temp file: {}", tmp.display()))?;

    f.write_all(data).context("Failed to write temp file")?;

    f.sync_all().context("Failed to sync temp file")?;

    fs::rename(&tmp, path)
        .with_context(|| format!("Failed to rename {} → {}", tmp.display(), path.display()))?;

    if let Some(parent) = path.parent()
        && let Ok(dir) = fs::File::open(parent)
    {
        let _ = dir.sync_all();
    }

    Ok(())
}
