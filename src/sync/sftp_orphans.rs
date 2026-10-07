//! Fail-closed orphan discovery for a complete SFTP mirror.

use anyhow::{Context, Result, ensure};
use std::collections::HashSet;
use std::fs::Metadata;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

use crate::safe_file::{self, EntryKind};
use crate::sftp::local::{Destination, STATE_DIR, lock_output, path_text};

pub(super) struct Orphan {
    pub relative: PathBuf,
    metadata: Metadata,
}

pub(super) fn matches_extension(path: &str, extensions: &Option<HashSet<String>>) -> bool {
    match extensions {
        None => true,
        Some(extensions) => Path::new(path).extension().and_then(|value| value.to_str())
            .is_some_and(|extension| extensions.contains(&extension.to_ascii_lowercase())),
    }
}

pub(super) fn collect(
    root: &Path,
    keep: &HashSet<String>,
    extensions: &Option<HashSet<String>>,
    cancel: &CancellationToken,
) -> Result<Vec<Orphan>> {
    let mut pending = vec![(PathBuf::new(), 0_usize)];
    let mut result = Vec::new();
    let mut visited = 0;
    while let Some((relative, depth)) = pending.pop() {
        ensure!(!cancel.is_cancelled(), "SFTP sync cancelled before deletion");
        ensure!(depth <= 64, "Local mirror exceeds the directory-depth limit");
        // Enumerated through the descriptor the directory was resolved on,
        // anchored at the mirror root. Verifying the directory and then
        // reading `root.join(relative)` by pathname left a gap in which the
        // directory could be replaced with a symlink, and this listing is a
        // list of files to delete.
        let entries = safe_file::read_dir_beneath(root, &relative)
            .context("Cannot enumerate local mirror")?;
        for entry in entries {
            ensure!(!cancel.is_cancelled(), "SFTP sync cancelled before deletion");
            visited += 1;
            ensure!(visited <= 200_000, "Local mirror exceeds the entry limit");
            if entry.name == STATE_DIR { continue; }
            let path = relative.join(&entry.name);
            match entry.kind {
                EntryKind::Dir => pending.push((path, depth + 1)),
                // Never followed or removed: links and special files are the
                // user's, not this sweep's.
                EntryKind::Other => {}
                EntryKind::File => {
                    if !keep.contains(path_text(&path)?)
                        && matches_extension(path_text(&path)?, extensions)
                        && let Some(metadata) = safe_file::metadata_beneath(root, &path)?
                    {
                        result.push(Orphan { relative: path, metadata });
                    }
                }
            }
        }
    }
    result.sort_by(|left, right| left.relative.cmp(&right.relative));
    Ok(result)
}

pub(super) fn remove(root: &Path, orphan: &Orphan) -> Result<()> {
    // Coordinate with a queue item downloading to the same output.
    let _lock = lock_output(root, &orphan.relative)?;
    let destination = Destination::beneath(root, path_text(&orphan.relative)?)?;
    let Some(now) = destination.metadata()? else { return Ok(()); };
    ensure!(
        now.dev() == orphan.metadata.dev() && now.ino() == orphan.metadata.ino()
            && now.len() == orphan.metadata.len()
            && now.modified().ok() == orphan.metadata.modified().ok(),
        "Local orphan changed after planning; leaving it untouched"
    );
    safe_file::unlink_beneath(root, &orphan.relative)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_filter_is_case_insensitive() {
        let extensions = Some(HashSet::from([String::from("flac")]));
        assert!(matches_extension("disc/track.FLAC", &extensions));
        assert!(!matches_extension("notes.txt", &extensions));
        assert!(!matches_extension("README", &extensions));
    }

    #[test]
    fn orphan_scan_preserves_state_symlinks_and_out_of_scope_files() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("keep.flac"), b"keep").unwrap();
        std::fs::write(root.path().join("gone.flac"), b"gone").unwrap();
        std::fs::write(root.path().join("notes.txt"), b"notes").unwrap();
        std::fs::create_dir(root.path().join(STATE_DIR)).unwrap();
        std::fs::write(root.path().join(STATE_DIR).join("saved.part"), b"partial").unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();
        let keep = HashSet::from([String::from("keep.flac")]);
        let extensions = Some(HashSet::from([String::from("flac")]));
        let orphans = collect(root.path(), &keep, &extensions, &CancellationToken::new()).unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].relative, Path::new("gone.flac"));
        remove(root.path(), &orphans[0]).unwrap();
        assert!(root.path().join("keep.flac").exists());
        assert!(root.path().join("notes.txt").exists());
        assert!(root.path().join(STATE_DIR).join("saved.part").exists());
        assert!(root.path().join("linked").symlink_metadata().unwrap().is_symlink());
    }
}
