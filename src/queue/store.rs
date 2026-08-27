//! Where the queue state lives on disk, and how it is written.
//!
//! Every write goes out as a temp file plus a rename, so a crash mid-write
//! leaves the previous queue.json intact rather than half of the new one.
//!
//! queue.json holds every queued URL, and for a private share the URL *is*
//! the credential: a MEGA link carries its decryption key in the fragment, a
//! OneDrive direct link carries a tempauth signature. So the temp file is
//! created owner-only and the rename carries that mode onto queue.json.

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
    // Creating the directory here as well as in the callers keeps this
    // function able to stand on its own, and is what tightens the rdm
    // directory to 0700 for the runs that never touch Config::save.
    if let Some(parent) = path.parent() {
        crate::secret_file::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
    }

    let tmp = path.with_extension("tmp");

    // Owner-only, and set here rather than after the rename: a mode applied
    // to the finished file would leave a window in which it is readable by
    // everyone. Going through the temp file also repairs a queue.json an
    // earlier version left at 0644, because the rename replaces it outright.
    let mut f = crate::secret_file::create(&tmp)
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// queue.json lists every queued URL, so a world-readable one hands the
    /// rest of the machine a set of private share links.
    #[test]
    fn the_written_file_is_readable_only_by_its_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");

        atomic_write(&path, b"{\"next_id\":1,\"items\":[]}").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    /// The rename replaces the inode, so a queue.json left at 0644 by an
    /// earlier version does not keep those permissions.
    #[test]
    fn a_previously_world_readable_queue_is_replaced_not_reused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");

        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        atomic_write(&path, b"{\"next_id\":1,\"items\":[]}").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn no_temp_file_is_left_behind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");

        atomic_write(&path, b"{}").unwrap();

        assert!(!path.with_extension("tmp").exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"{}");
    }
}
