//! A relative path that has been decoded exactly once and validated.
//!
//! ## Why this type exists
//!
//! The directory scraper used to percent-decode a listing-supplied path,
//! validate the result, and hand back a plain `String`. Downstream — the sync
//! mirror and the queue — then percent-decoded that already-sanitised value a
//! second time. A listing offering
//!
//! ```text
//! %252Fhome%252Fuser%252F.config%252Ftarget
//! ```
//!
//! survived the first decode as `%2Fhome%2Fuser%2F.config%2Ftarget`, which is
//! a single perfectly ordinary relative component: no separators, not
//! absolute, no `..`. The second decode turned it into
//! `/home/user/.config/target`, an absolute path, and `resolve_output_path`
//! honours absolute paths as-is. Because `rdm sync` removes the files it has
//! selected for redownload *before* queueing the replacements, that was an
//! arbitrary overwrite rather than merely an arbitrary create.
//!
//! So decoding happens once, at the network trust boundary, and the result is
//! carried in this type. Nothing here decodes anything. There is no method
//! that will, which is the point: the guarantee is a property of the type
//! rather than a rule someone has to remember.
//!
//! ## The second half of the fix
//!
//! A validated string is still only a string. [`resolve_under`] is the final
//! filesystem boundary: it refuses `RootDir`, `Prefix`, `ParentDir` and
//! `CurDir` components outright and requires the joined result to remain
//! beneath the root it was given. Symlink containment is a separate concern
//! and belongs to [`crate::safe_file`], which opens with `RESOLVE_BENEATH`.

use anyhow::{Result, bail};
use std::path::{Component, Path, PathBuf};

/// The longest single component ext4, APFS and NTFS all accept.
pub const MAX_COMPONENT_LEN: usize = 255;

/// A generous ceiling on the whole path, well under `PATH_MAX`.
pub const MAX_RELATIVE_PATH_LEN: usize = 4096;

/// A slash-separated relative path, already percent-decoded once, with every
/// component checked.
///
/// Construct with [`SafeRelativePath::validate`], which takes an *already
/// decoded* string. If you find yourself wanting to decode inside this module,
/// that is the bug this type exists to prevent.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SafeRelativePath(String);

impl SafeRelativePath {
    /// Validates an already-decoded relative path.
    ///
    /// Returns `None` rather than a repaired value: a caller that cannot use
    /// the path should skip the entry, not write a guess to disk.
    pub fn validate(decoded: &str) -> Option<Self> {
        if decoded.is_empty() || decoded.len() > MAX_RELATIVE_PATH_LEN {
            return None;
        }

        // NUL truncates the path at the syscall boundary, so the name the
        // kernel sees would not be the name that was checked. A backslash is
        // the Windows separator, so `..\..\etc` is a traversal that
        // `split('/')` would count as one component.
        if decoded.contains('\0') || decoded.contains('\\') {
            return None;
        }

        if decoded.starts_with('/') || has_windows_drive_or_unc(decoded) {
            return None;
        }

        let mut components = 0usize;
        for comp in decoded.split('/') {
            if !component_is_safe(comp) {
                return None;
            }
            components += 1;
        }

        if components == 0 {
            return None;
        }

        Some(Self(decoded.to_owned()))
    }

    /// Validates a single component, refusing anything containing a separator.
    pub fn validate_component(decoded: &str) -> Option<Self> {
        if decoded.contains('/') {
            return None;
        }
        Self::validate(decoded)
    }

    /// Prefixes `folder` onto this path, revalidating the join.
    ///
    /// Used by the scraper's `--wrap-in-folder`. The join is validated rather
    /// than assumed, so a folder name that is itself unusable cannot smuggle
    /// anything through.
    pub fn nested_under(&self, folder: &str) -> Option<Self> {
        Self::validate(&format!("{}/{}", folder, self.0))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    /// The first path component, which is the mirror root for `rdm sync`.
    pub fn first_component(&self) -> &str {
        self.0.split('/').next().unwrap_or(&self.0)
    }

    /// The final component, i.e. the filename.
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// Joins this path beneath `root`, refusing to escape it.
    pub fn resolve_under(&self, root: &Path) -> Result<PathBuf> {
        resolve_under(root, &self.0)
    }
}

impl std::fmt::Display for SafeRelativePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Joins an already-decoded relative path beneath `root`, or refuses.
///
/// This is the final filesystem boundary. Even for a path that came from
/// [`SafeRelativePath`] the components are walked again here, because this is
/// the line that turns a string into somewhere we are about to write.
pub fn resolve_under(root: &Path, relative: &str) -> Result<PathBuf> {
    let Some(safe) = SafeRelativePath::validate(relative) else {
        bail!("Refusing unsafe relative path: {:?}", elide(relative));
    };

    let mut out = root.to_path_buf();

    for component in Path::new(safe.as_str()).components() {
        match component {
            Component::Normal(part) => out.push(part),
            // `..` is the traversal, `/` and `C:` make the result absolute,
            // and `.` should never have survived validation. None of them are
            // recoverable, so none of them are skipped.
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!(
                    "Refusing path with a traversal or root component: {:?}",
                    elide(relative)
                )
            }
            Component::CurDir => {
                bail!("Refusing path with a '.' component: {:?}", elide(relative))
            }
        }
    }

    // No `..` survived the walk above, so a lexical containment check is
    // sufficient here. Symlinks are a different escape and are handled at open
    // time by `crate::safe_file`, which passes RESOLVE_BENEATH.
    if !out.starts_with(root) {
        bail!(
            "Refusing destination outside the download root: {}",
            out.display()
        );
    }

    Ok(out)
}

/// Keeps a rejected path out of logs at full length, and out of the terminal
/// as anything but printable text.
fn elide(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .filter(|c| !c.is_control())
        .take(120)
        .collect();
    cleaned
}

fn component_is_safe(comp: &str) -> bool {
    if comp.is_empty() || comp == "." || comp == ".." {
        return false;
    }

    if comp.len() > MAX_COMPONENT_LEN {
        return false;
    }

    // C0 controls and DEL. ESC would let a filename repaint the terminal, and
    // NUL would truncate the path at the syscall boundary.
    if comp.chars().any(|c| (c as u32) < 0x20 || c == '\u{7f}') {
        return false;
    }

    if is_windows_reserved(comp) {
        return false;
    }

    // Windows quirk: a name that is only dots and spaces is dropped or
    // reinterpreted, so `foo/...` can land somewhere we did not choose.
    if comp.trim_end_matches(['.', ' ']).is_empty() {
        return false;
    }

    true
}

fn has_windows_drive_or_unc(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return true;
    }
    s.starts_with("\\\\")
}

fn is_windows_reserved(name: &str) -> bool {
    let stem = name.split('.').next().unwrap_or(name).to_ascii_uppercase();
    matches!(
        stem.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(s: &str) -> String {
        SafeRelativePath::validate(s)
            .expect("should be accepted")
            .into_string()
    }

    #[test]
    fn ordinary_relative_paths_are_accepted_unchanged() {
        assert_eq!(ok("file.mkv"), "file.mkv");
        assert_eq!(ok("sub/file.mkv"), "sub/file.mkv");
        assert_eq!(ok("a/b/c/d.mp4"), "a/b/c/d.mp4");
        // A percent sign is an ordinary character in a name, and stays one.
        assert_eq!(ok("100%25 done.txt"), "100%25 done.txt");
    }

    #[test]
    fn plain_traversal_is_refused() {
        for bad in [
            "",
            "..",
            "/etc/passwd",
            "../../.ssh/keys",
            "sub/../../etc/passwd",
            "foo/.",
            "foo/...",
            "a//b",
            "C:/Windows/x",
            "..\\..\\windows\\system32",
            "good\0bad.txt",
            "CON",
            "Lpt1.txt",
        ] {
            assert!(
                SafeRelativePath::validate(bad).is_none(),
                "{bad:?} must be refused"
            );
        }
    }

    /// The heart of the double-decode bug. Each of these is what the *first*
    /// decode produced, and every one of them is a legal single component that
    /// only becomes dangerous if something decodes it again. Validation must
    /// accept them as names, and `resolve_under` must keep them as one
    /// component rather than letting them become a path.
    #[test]
    fn a_singly_decoded_double_encoding_stays_one_component() {
        let root = Path::new("/home/user/Downloads");

        for once_decoded in [
            "%2Fhome%2Fuser%2F.config%2Ftarget",
            "%2F%2Fetc%2Fcron.d%2Frdm",
            "%2e%2e%2F%2e%2e%2F.bashrc",
            "%2E%2E%2Fescape",
            "%5C%5Cserver%5Cshare",
            "%5c..%5c..%5cwindows",
            "%2e",
            "%2e%2e",
            "a%2F..%2F..%2Fb",
        ] {
            let resolved = resolve_under(root, once_decoded)
                .unwrap_or_else(|e| panic!("{once_decoded:?} should resolve as a name: {e}"));

            assert_eq!(
                resolved.parent(),
                Some(root),
                "{once_decoded:?} escaped its directory"
            );
            assert!(
                resolved.starts_with(root),
                "{once_decoded:?} left the download root"
            );
        }
    }

    #[test]
    fn resolve_under_keeps_legitimate_nesting() {
        let root = Path::new("/dl");
        assert_eq!(
            resolve_under(root, "show/s01/e01.mkv").unwrap(),
            PathBuf::from("/dl/show/s01/e01.mkv")
        );
    }

    #[test]
    fn resolve_under_refuses_every_escape() {
        let root = Path::new("/dl");
        for bad in [
            "../etc/passwd",
            "../../../../etc/shadow",
            "/etc/passwd",
            "a/../../b",
            "./a",
            "a/./b",
            "C:\\Windows\\evil.dll",
            "..",
        ] {
            assert!(
                resolve_under(root, bad).is_err(),
                "{bad:?} must not resolve"
            );
        }
    }

    /// Multiple nested traversal components, which is the case a single
    /// `starts_with("..")` style check misses.
    #[test]
    fn deeply_nested_traversal_is_refused() {
        let root = Path::new("/dl");
        assert!(resolve_under(root, "a/b/../../../../etc/passwd").is_err());
        assert!(resolve_under(root, "a/b/c/../../../..").is_err());
    }

    #[test]
    fn a_component_may_not_contain_a_separator() {
        assert!(SafeRelativePath::validate_component("sub/file.mkv").is_none());
        assert_eq!(
            SafeRelativePath::validate_component("file.mkv")
                .unwrap()
                .as_str(),
            "file.mkv"
        );
    }

    #[test]
    fn nesting_under_a_folder_revalidates_the_join() {
        let rel = SafeRelativePath::validate("a/b.mkv").unwrap();
        assert_eq!(rel.nested_under("album").unwrap().as_str(), "album/a/b.mkv");
        // A folder name that would make the join unsafe is refused outright.
        assert!(rel.nested_under("..").is_none());
        assert!(rel.nested_under("/etc").is_none());
    }

    #[test]
    fn accessors_do_not_decode() {
        let rel = SafeRelativePath::validate("dir/%2Fnot-a-path").unwrap();
        assert_eq!(rel.first_component(), "dir");
        assert_eq!(rel.file_name(), "%2Fnot-a-path");
        assert_eq!(rel.as_str(), "dir/%2Fnot-a-path");
    }

    #[test]
    fn oversized_paths_and_components_are_refused() {
        let long_component = "a".repeat(MAX_COMPONENT_LEN + 1);
        assert!(SafeRelativePath::validate(&long_component).is_none());

        let long_path = format!("{}/x", "a/".repeat(MAX_RELATIVE_PATH_LEN));
        assert!(SafeRelativePath::validate(&long_path).is_none());
    }
}
