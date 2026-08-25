//! Turning listing entries into local paths.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::engine;

/// Joins share-relative components onto the destination.
///
/// Components were sanitised when the node tree was decrypted, so this does
/// not re-sanitise; it exists so the MEGA path does not go through
/// [`local_path`], which percent-decodes and resolves against the config.
pub(super) fn join_relative(base: &Path, components: &[String]) -> PathBuf {
    let mut path = base.to_path_buf();
    for part in components {
        path.push(part);
    }
    path
}

pub(super) fn local_path(cfg: &Config, relative: &str) -> PathBuf {
    let decoded = engine::percent_decode(relative);
    PathBuf::from(cfg.resolve_output_path(&decoded))
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
}
