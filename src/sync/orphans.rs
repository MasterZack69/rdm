//! What `--delete` may remove.
//!
//! Everything here walks with `DirEntry::file_type` rather than with
//! `Path::is_dir`/`Path::is_file`. That pair follows symlinks, so a link
//! planted inside the destination was walked as though it were part of the
//! mirror, and every path found underneath was handed to the deletion phase to
//! reconstruct and unlink. One `ln -s ~ dest/pics` aimed `--delete` at a home
//! directory. `file_type` is metadata the readdir call already returned and
//! follows nothing, so a link reports as a link.
//!
//! Symlinks are then skipped outright rather than followed: not recursed into,
//! and not reported as orphans either. rdm only ever writes regular files, so
//! a link inside the destination is something the user put there and removing
//! it is not this sweep's business.

use std::collections::HashSet;
use std::path::Path;

use super::paths::file_has_ext;

/// Local files under `base` that the share no longer contains.
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
    dir: &Path,
    base: &Path,
    keep: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        // Never followed, never deleted. See the module comment.
        if kind.is_symlink() {
            continue;
        }

        let path = entry.path();

        if kind.is_dir() {
            collect_listing_orphans(&path, base, keep, ext_filter, out);
            continue;
        }
        if !kind.is_file() {
            continue;
        }

        let relative = match path.strip_prefix(base) {
            Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
            Err(_) => continue,
        };
        if relative.is_empty() || keep.contains(&relative) {
            continue;
        }

        let is_temp_of_kept = keep.iter().any(|k| {
            relative.len() > k.len()
                && relative.starts_with(k)
                && relative.as_bytes()[k.len()] == b'.'
        });
        if is_temp_of_kept {
            continue;
        }

        if let Some(exts) = ext_filter.as_ref() {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !file_has_ext(name, exts) {
                continue;
            }
        }

        out.push(relative);
    }
}

/// The HTTP path's sweep. `remote_decoded` holds listing paths that include
/// the mirror's own folder name, so each local path is compared with that
/// folder put back in front of it.
pub(super) fn collect_orphan_files(
    dir: &Path,
    base: &Path,
    remote_decoded: &HashSet<String>,
    ext_filter: &Option<HashSet<String>>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_symlink() {
            continue;
        }

        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && (name.ends_with(".part") || name.ends_with(".rdm"))
        {
            continue;
        }
        if kind.is_dir() {
            collect_orphan_files(&path, base, remote_decoded, ext_filter, out);
        } else if kind.is_file() {
            if let Some(exts) = ext_filter.as_ref() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !file_has_ext(name, exts) {
                    continue;
                }
            }
            let relative = match path.strip_prefix(base) {
                Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
                Err(_) => continue,
            };
            if relative.is_empty() {
                continue;
            }
            let folder = match base.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => return,
            };
            let full = format!("{}/{}", folder, relative);
            if !remote_decoded.contains(&full) {
                out.push(relative);
            }
        }
    }
}

pub(super) fn remove_empty_dirs(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        // A linked directory is not ours to descend into or to remove.
        if kind.is_symlink() || !kind.is_dir() {
            continue;
        }
        let path = entry.path();
        remove_empty_dirs(&path);
        let _ = std::fs::remove_dir(&path);
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
        collect_listing_orphans(base, base, &keep, &None, &mut out);
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
        collect_listing_orphans(base, base, &keep, &None, &mut out);

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
        collect_listing_orphans(base, base, &HashSet::new(), &Some(exts), &mut out);

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
        collect_listing_orphans(base, base, &keep, &None, &mut out);

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
        collect_listing_orphans(base, base, &keep, &None, &mut out);

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
        collect_orphan_files(&base, &base, &remote, &None, &mut out);

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
        collect_listing_orphans(base, base, &HashSet::new(), &None, &mut out);

        assert!(out.is_empty(), "got {:?}", out);
    }

    #[cfg(unix)]
    #[test]
    fn removing_empty_dirs_does_not_descend_through_a_link() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outside.path().join("empty-but-not-ours")).unwrap();

        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("elsewhere")).unwrap();

        remove_empty_dirs(dir.path());

        assert!(
            outside.path().join("empty-but-not-ours").exists(),
            "an empty directory outside the mirror was removed through a link"
        );
    }
}
