//! What `--delete` may remove.
//!
//! Every walk here goes through [`safe_file::read_dir_beneath`]: the directory
//! is resolved by descriptor, anchored at the root, and both the `readdir` and
//! the per-entry stat happen through that descriptor.
//!
//! The reason is that the sweep's input decides what gets deleted. Walking by
//! pathname with `std::fs::read_dir` re-resolves every component on every
//! level, so a directory inside the mirror could be replaced with a symlink
//! between one level and the next and the files behind it would be enumerated
//! as part of the mirror — and handed to the deletion phase. The older version
//! of this module fixed the easy half of that by reading `DirEntry::file_type`
//! instead of `Path::is_dir`, so a link reported as a link; it still trusted
//! the pathname it was enumerating.
//!
//! Symlinks are skipped outright rather than followed: not recursed into, and
//! not reported as orphans either. rdm only ever writes regular files, so a
//! link inside the destination is something the user put there and removing it
//! is not this sweep's business.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::safe_file::{self, EntryKind};

use super::paths::file_has_ext;

/// A relative path as the listing comparisons want it: `/`-separated, lossily
/// decoded. Entry names come from readdir, so they are bytes on Unix.
fn relative_text(relative: &Path) -> String {
    relative.to_string_lossy().replace('\\', "/")
}

/// Local files under `root`/`base` that the share no longer contains.
///
/// `root` is the trust boundary — the download directory the user configured —
/// and `base` is the relative path of the mirror inside it. Every level below
/// `base` is re-resolved from `root`, so no part of the walk depends on a
/// pathname staying what it was a moment ago.
///
/// Temp files are recognised structurally rather than by suffix: anything that
/// is a kept path plus a dot-suffix (`a.jpg.part`, `a.jpg.mctemp`, whatever
/// the downloader happens to use) belongs to a file we are keeping, so it is
/// left alone without this function needing to know the naming scheme.
///
/// Callers must not reach here with an incomplete listing: `keep` would be
/// missing those files' names and their local copies would be reported as
/// orphans. MEGA's undecryptable nodes and OneDrive's unwalkable children are
/// both that case.
pub(super) fn collect_listing_orphans(
    root: &Path,
    base: &Path,
    keep: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    for (relative, kind) in walk(root, base) {
        if kind != EntryKind::File {
            continue;
        }

        let Ok(suffix) = relative.strip_prefix(base) else {
            continue;
        };
        let suffix = relative_text(suffix);

        if suffix.is_empty() || keep.contains(&suffix) {
            continue;
        }

        let is_temp_of_kept = keep.iter().any(|kept| {
            suffix.len() > kept.len()
                && suffix.starts_with(kept)
                && suffix.as_bytes()[kept.len()] == b'.'
        });
        if is_temp_of_kept {
            continue;
        }

        if let Some(exts) = ext_filter.as_ref() {
            let name = relative.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !file_has_ext(name, exts) {
                continue;
            }
        }

        out.push(suffix);
    }
}

/// The HTTP path's sweep. `remote_decoded` holds listing paths that include
/// the mirror's own folder name, so each local path is compared with that
/// folder put back in front of it.
pub(super) fn collect_orphan_files(
    root: &Path,
    base: &Path,
    remote_decoded: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let Some(folder) = base.file_name().and_then(|n| n.to_str()) else {
        return;
    };

    for (relative, kind) in walk(root, base) {
        if kind != EntryKind::File {
            continue;
        }

        let name = relative.file_name().and_then(|n| n.to_str()).unwrap_or("");

        // Partial downloads and resume state belong to a transfer, not to the
        // listing.
        if name.ends_with(".part") || name.ends_with(".rdm") {
            continue;
        }

        if let Some(exts) = ext_filter.as_ref()
            && !file_has_ext(name, exts)
        {
            continue;
        }

        let Ok(suffix) = relative.strip_prefix(base) else {
            continue;
        };
        let suffix = relative_text(suffix);

        if suffix.is_empty() {
            continue;
        }

        if !remote_decoded.contains(&format!("{}/{}", folder, suffix)) {
            out.push(suffix);
        }
    }
}

/// Every file beneath `base`, as paths relative to `root`.
///
/// Depth- and entry-limited so that a mirror that has grown pathological —
/// or a directory loop made of hard-linked directories on a filesystem that
/// allows them — cannot turn a sweep into an unbounded walk.
fn walk(root: &Path, base: &Path) -> Vec<(PathBuf, EntryKind)> {
    const MAX_DEPTH: usize = 64;
    const MAX_ENTRIES: usize = 200_000;

    let mut pending = vec![(base.to_path_buf(), 0usize)];
    let mut out = Vec::new();

    while let Some((dir, depth)) = pending.pop() {
        let Ok(entries) = safe_file::read_dir_beneath(root, &dir) else {
            continue;
        };

        for entry in entries {
            if out.len() >= MAX_ENTRIES {
                return out;
            }

            let relative = dir.join(&entry.name);

            match entry.kind {
                EntryKind::Dir if depth < MAX_DEPTH => {
                    pending.push((relative.clone(), depth + 1));
                    out.push((relative, EntryKind::Dir));
                }
                // Links, FIFOs, devices: never followed, never deleted.
                EntryKind::Dir | EntryKind::Other => {}
                EntryKind::File => out.push((relative, EntryKind::File)),
            }
        }
    }

    out
}

/// Removes directories that the sweep has emptied, deepest first.
///
/// Only directories that are genuinely empty go, and only through the same
/// anchored resolution as everything else, so a link to a directory outside
/// the root is neither descended into nor removed.
pub(super) fn remove_empty_dirs(root: &Path, base: &Path) {
    let mut dirs: Vec<PathBuf> = walk(root, base)
        .into_iter()
        .filter(|(_, kind)| *kind == EntryKind::Dir)
        .map(|(relative, _)| relative)
        .collect();

    // Deepest first, so a directory whose only contents were empty
    // directories is itself empty by the time it is reached.
    dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));

    for dir in dirs {
        let _ = safe_file::remove_dir_beneath(root, &dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, b"x").unwrap();
    }

    fn keep_set(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn mega_orphans_are_paths_the_share_no_longer_has() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("keep.jpg"));
        touch(&base.join("sub/nested.jpg"));
        touch(&base.join("gone.jpg"));
        touch(&base.join("sub/also-gone.jpg"));

        let keep = keep_set(&["keep.jpg", "sub/nested.jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &keep, &None, &mut out);
        out.sort();

        assert_eq!(out, vec!["gone.jpg", "sub/also-gone.jpg"]);
    }

    /// Part files and resume state belong to a file we are keeping, so they
    /// must survive the sweep \u{2014} deleting them silently throws away resumable
    /// progress. Matching on "kept path plus a dot-suffix" means this holds
    /// whatever the downloader names them.
    #[test]
    fn mega_orphans_leave_temp_files_of_kept_paths_alone() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("movie.mkv"));
        touch(&base.join("movie.mkv.part"));
        touch(&base.join("movie.mkv.rdm"));
        touch(&base.join("movie.mkv.mctemp"));
        touch(&base.join("stray.mkv.part"));

        let keep = keep_set(&["movie.mkv"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &keep, &None, &mut out);

        // Only the leftover with no kept file behind it is an orphan.
        assert_eq!(out, vec!["stray.mkv.part"]);
    }

    #[test]
    fn mega_orphans_respect_the_extension_filter() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("gone.jpg"));
        touch(&base.join("notes.txt"));

        let exts: HashSet<String> = keep_set(&["jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &HashSet::new(), &Some(exts), &mut out);

        // notes.txt was never in scope for this sync, so it is not an orphan.
        assert_eq!(out, vec!["gone.jpg"]);
    }

    /// The hazard the undecryptable guard in `run_mega` exists for: a file
    /// whose node key stops resolving drops out of `keep`, and this function
    /// then cannot tell it from a file the share genuinely dropped. Proving
    /// that here is what makes the guard load-bearing rather than decorative.
    #[test]
    fn a_file_missing_from_keep_is_indistinguishable_from_an_orphan() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();

        touch(&base.join("readable.jpg"));
        touch(&base.join("key-no-longer-opens-this.jpg"));

        // Only the readable node made it into the listing.
        let keep = keep_set(&["readable.jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &keep, &None, &mut out);

        assert_eq!(
            out,
            vec!["key-no-longer-opens-this.jpg"],
            "a perfectly good file looks like an orphan, which is why run_mega \
             refuses to delete when any node is undecryptable"
        );
    }

    /// The escape this module's walking rule exists for. Before it, the link
    /// was followed, the files behind it were reported as orphans, and the
    /// deletion phase reconstructed and unlinked them.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_is_not_walked_as_part_of_the_mirror() {
        let outside = tempfile::tempdir().unwrap();
        touch(&outside.path().join("precious.jpg"));
        touch(&outside.path().join("nested/also-precious.jpg"));

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        touch(&base.join("keep.jpg"));
        std::os::unix::fs::symlink(outside.path(), base.join("elsewhere")).unwrap();

        let keep = keep_set(&["keep.jpg"]);
        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &keep, &None, &mut out);

        assert!(
            out.is_empty(),
            "nothing outside the mirror may be reported as an orphan, got {:?}",
            out
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_is_not_walked_by_the_http_sweep_either() {
        let outside = tempfile::tempdir().unwrap();
        touch(&outside.path().join("precious.jpg"));

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("mirror");
        std::fs::create_dir_all(&base).unwrap();
        touch(&base.join("keep.jpg"));
        std::os::unix::fs::symlink(outside.path(), base.join("elsewhere")).unwrap();

        let remote = keep_set(&["mirror/keep.jpg"]);
        let mut out = Vec::new();
        collect_orphan_files(dir.path(), Path::new("mirror"), &remote, &None, &mut out);

        assert!(
            out.is_empty(),
            "nothing outside the mirror may be reported as an orphan, got {:?}",
            out
        );
    }

    /// A link to a file is skipped rather than deleted. The sweep removes what
    /// rdm downloaded, and rdm only ever writes regular files.
    #[cfg(unix)]
    #[test]
    fn a_symlinked_file_is_left_alone() {
        let outside = tempfile::tempdir().unwrap();
        let target = outside.path().join("precious.jpg");
        touch(&target);

        let dir = tempfile::tempdir().unwrap();
        let base = dir.path();
        std::os::unix::fs::symlink(&target, base.join("linked.jpg")).unwrap();

        let mut out = Vec::new();
        collect_listing_orphans(base, Path::new(""), &HashSet::new(), &None, &mut out);

        assert!(out.is_empty(), "got {:?}", out);
    }

    #[cfg(unix)]
    #[test]
    fn removing_empty_dirs_does_not_descend_through_a_link() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outside.path().join("empty-but-not-ours")).unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("elsewhere")).unwrap();

        remove_empty_dirs(dir.path(), Path::new(""));

        assert!(
            outside.path().join("empty-but-not-ours").exists(),
            "an empty directory outside the mirror was removed through a link"
        );
    }
}
