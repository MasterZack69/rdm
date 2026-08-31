//! Turning listing entries into local paths.

use anyhow::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::safe_path;

/// Joins share-relative components onto the destination.
///
/// Components were sanitised when the node tree was decrypted, so this does
/// not re-sanitise; it exists so the MEGA path does not go through
/// [`local_path`], which resolves against the config.
pub(super) fn join_relative(base: &Path, components: &[String]) -> PathBuf {
    let mut path = base.to_path_buf();
    for part in components {
        path.push(part);
    }
    path
}

/// Resolves a listing-relative path to somewhere beneath the download
/// directory, or refuses.
///
/// This used to call `percent_decode` first. That was the second half of the
/// double decode: the scraper had already decoded and validated the value, so
/// decoding again turned `%2Fhome%2Fuser%2F.config%2Ftarget` — a legal
/// single component — back into an absolute path, which
/// `resolve_output_path` then honoured verbatim.
///
/// So: no decoding, and the join goes through
/// [`safe_path::resolve_under`], which refuses `RootDir`, `Prefix`,
/// `ParentDir` and `CurDir` components and requires the result to stay beneath
/// the download directory. Returning `Result` rather than a `PathBuf` means a
/// rejected entry is skipped rather than written somewhere unintended.
pub(super) fn local_path(cfg: &Config, relative: &str) -> Result<PathBuf> {
    safe_path::resolve_under(Path::new(&cfg.download_dir), relative)
}

/// A listing path in the form `collect_listing_orphans` derives from disk:
/// slash-separated and relative to the mirror root.
pub(super) fn relative_key(relative: &Path) -> String {
    relative.to_string_lossy().replace('\\', "/")
}

pub(super) fn extract_filename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_owned()
}

pub(super) fn file_has_ext(filename: &str, exts: &HashSet<String>) -> bool {
    let lower = filename.to_lowercase();
    exts.iter().any(|ext| lower.ends_with(&format!(".{}", ext)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_components_join_in_order() {
        assert_eq!(
            join_relative(Path::new("/tmp/dl"), &["a".to_owned(), "b.jpg".to_owned()]),
            PathBuf::from("/tmp/dl/a/b.jpg")
        );
    }

    fn cfg_with_dir(dir: &str) -> Config {
        let mut cfg = Config::default();
        cfg.download_dir = dir.to_owned();
        cfg
    }

    #[test]
    fn ordinary_entries_land_under_the_download_dir() {
        let cfg = cfg_with_dir("/tmp/dl");

        assert_eq!(
            local_path(&cfg, "file.mkv").unwrap(),
            PathBuf::from("/tmp/dl/file.mkv")
        );
        assert_eq!(
            local_path(&cfg, "show/s01/e01.mkv").unwrap(),
            PathBuf::from("/tmp/dl/show/s01/e01.mkv")
        );
    }

    /// The critical regression: this is the value the scraper legitimately
    /// produces from a `%252F...` listing entry. It must stay one filename
    /// under the download directory, not become an absolute path.
    #[test]
    fn a_singly_decoded_entry_is_not_decoded_again() {
        let cfg = cfg_with_dir("/tmp/dl");
        let path = local_path(&cfg, "%2Fhome%2Fuser%2F.config%2Ftarget").unwrap();

        assert_eq!(
            path,
            PathBuf::from("/tmp/dl/%2Fhome%2Fuser%2F.config%2Ftarget")
        );
        assert_eq!(path.parent(), Some(Path::new("/tmp/dl")));
        assert!(path.starts_with("/tmp/dl"));
    }

    #[test]
    fn escapes_are_refused_rather_than_resolved() {
        let cfg = cfg_with_dir("/tmp/dl");

        for bad in [
            "/home/user/.config/target",
            "../../../etc/passwd",
            "a/../../b",
            "a/b/../../../../etc/shadow",
            "..",
            "./x",
            "C:\\Windows\\evil.dll",
            "",
        ] {
            assert!(
                local_path(&cfg, bad).is_err(),
                "{bad:?} must not resolve to a path"
            );
        }
    }
}
