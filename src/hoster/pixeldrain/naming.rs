//! Turning names pixeldrain reports into names a filesystem will accept.
//!
//! Split out for the same reason GoFile's naming is: it is pure, it is where
//! all the sharp edges are, and it is worth testing without a network in
//! sight.

use std::collections::HashSet;
use std::path::Path;

use crate::engine;

/// Turns a remote name into exactly one safe path component.
///
/// Names come off the network, and a file called `../../.ssh/authorized_keys`
/// is somebody else's idea of a joke: everything before the last separator is
/// dropped, and separators that arrive percent-encoded are decoded *first* so
/// they cannot smuggle one past.
///
/// Unlike OneDrive, pixeldrain does not police filenames on upload, so the
/// characters Windows refuses (`:` `*` `?` `"` `<` `>` `|`) have to be handled
/// here rather than assumed absent. They become `_` instead of being dropped,
/// because dropping them turns `1:2` into `12`.
///
/// Returns `None` when nothing usable is left, so the caller can fall back to
/// something it trusts rather than this function inventing a name.
pub(super) fn safe_component(name: &str) -> Option<String> {
    let decoded = engine::percent_decode(name);
    let leaf = decoded.rsplit(['/', '\\']).next().unwrap_or_default();
    let cleaned: String = leaf
        .chars()
        .filter(|c| !c.is_control())
        .map(|c| {
            if matches!(c, ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    // Trailing dots go as well: `.` and `..` trim away to nothing and are
    // caught below, and Windows drops them from real names anyway, which would
    // leave the file somewhere we did not put it.
    let trimmed = cleaned.trim().trim_end_matches('.').trim_end();

    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// The name to save a file under.
///
/// The remote name reduced to one safe component, falling back to `fallback` —
/// in practice the file's id, which is never empty and, having been through
/// `checked_id`, can never be a path.
pub(super) fn choose(remote: Option<&str>, fallback: &str) -> String {
    remote
        .and_then(safe_component)
        .unwrap_or_else(|| fallback.to_owned())
}

/// Keeps two files in one list from becoming one file on disk.
///
/// An ordinary case rather than a defensive one: pixeldrain lists are flat and
/// names within them need not be unique, so an album holding two `cover.jpg`
/// is perfectly legal and must not quietly arrive as a single file.
pub(super) fn unique(taken: &mut HashSet<String>, candidate: String) -> String {
    if taken.insert(candidate.clone()) {
        return candidate;
    }

    let path = Path::new(&candidate);
    let stem = path
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default();
    let extension = path
        .extension()
        .map(|extension| extension.to_string_lossy().into_owned());
    let mut copy = 2u32;

    loop {
        let mut numbered = format!("{stem} ({copy})");
        if let Some(extension) = &extension {
            numbered.push('.');
            numbered.push_str(extension);
        }

        if taken.insert(numbered.clone()) {
            return numbered;
        }

        copy += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_cannot_climb_out_of_the_download_directory() {
        assert_eq!(
            safe_component("../../.ssh/authorized_keys").as_deref(),
            Some("authorized_keys")
        );
        assert_eq!(
            safe_component("C:\\Windows\\evil.dll").as_deref(),
            Some("evil.dll")
        );
        // Decoded before splitting, or the separator survives the split.
        assert_eq!(
            safe_component("..%2f..%2fpasswd").as_deref(),
            Some("passwd")
        );
    }

    #[test]
    fn names_with_nothing_left_in_them_are_rejected_rather_than_invented() {
        // The caller has an id to fall back on; a name made up here would be
        // worse than admitting there isn't one.
        assert_eq!(safe_component(""), None);
        assert_eq!(safe_component("   "), None);
        assert_eq!(safe_component("."), None);
        assert_eq!(safe_component(".."), None);
        assert_eq!(safe_component("/"), None);
        assert_eq!(choose(Some("."), "aBcD1"), "aBcD1");
        assert_eq!(choose(None, "aBcD1"), "aBcD1");
    }

    #[test]
    fn characters_windows_refuses_are_replaced_and_not_dropped() {
        // pixeldrain accepts these on upload where OneDrive does not, so they
        // genuinely turn up. Dropping them would turn `1:2` into `12`.
        assert_eq!(
            safe_component("ep 1:2 <final>.mkv").as_deref(),
            Some("ep 1_2 _final_.mkv")
        );
        assert_eq!(safe_component("a\u{7}b.bin").as_deref(), Some("ab.bin"));
    }

    #[test]
    fn two_files_with_one_name_stay_two_files() {
        let mut taken = HashSet::new();
        assert_eq!(unique(&mut taken, "cover.jpg".to_owned()), "cover.jpg");
        assert_eq!(unique(&mut taken, "cover.jpg".to_owned()), "cover (2).jpg");
        assert_eq!(unique(&mut taken, "cover.jpg".to_owned()), "cover (3).jpg");
        // Extensionless names get the same treatment without a stray dot.
        assert_eq!(unique(&mut taken, "README".to_owned()), "README");
        assert_eq!(unique(&mut taken, "README".to_owned()), "README (2)");
    }

    #[test]
    fn a_name_that_is_already_taken_by_a_numbered_copy_keeps_counting() {
        let mut taken = HashSet::new();
        assert_eq!(
            unique(&mut taken, "clip (2).mp4".to_owned()),
            "clip (2).mp4"
        );
        assert_eq!(unique(&mut taken, "clip.mp4".to_owned()), "clip.mp4");
        assert_eq!(unique(&mut taken, "clip.mp4".to_owned()), "clip (3).mp4");
    }
}
