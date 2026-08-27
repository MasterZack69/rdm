//! Path sanitization.
//!
//! (1) Decode first, then validate every component. Reject:
//! - empty / overly long paths
//! - null bytes / control chars
//! - backslashes (Windows separator), absolute paths, drive letters, UNC
//! - any "." or ".." component (post-decode)
//! - Windows reserved device names
//! - components that are only dots/spaces (Windows quirks)

use crate::engine;
use super::limits::{MAX_PATH_COMPONENT_LEN, MAX_RELATIVE_PATH_LEN};

pub(super) fn sanitize_relative_path(raw: &str) -> Option<String> {
    let decoded = engine::percent_decode(raw);

    if decoded.is_empty() || decoded.len() > MAX_RELATIVE_PATH_LEN {
        return None;
    }

    if decoded.contains('\0') || decoded.contains('\\') {
        return None;
    }

    if decoded.starts_with('/') {
        return None;
    }

    if has_windows_drive_or_unc(&decoded) {
        return None;
    }

    let mut clean: Vec<String> = Vec::new();
    for comp in decoded.split('/') {
        if comp.is_empty() || comp == "." || comp == ".." {
            return None;
        }

        if comp.len() > MAX_PATH_COMPONENT_LEN {
            return None;
        }

        if comp.chars().any(|c| (c as u32) < 0x20 || c == '\u{7f}') {
            return None;
        }

        if is_windows_reserved(comp) {
            return None;
        }

        // Windows quirk: filenames consisting only of dots and spaces are ignored
        // or can point to directories/volumes in dangerous ways.
        let stripped = comp.trim_end_matches(['.', ' ']);
        if stripped.is_empty() {
            return None;
        }

        clean.push(comp.to_owned());
    }

    if clean.is_empty() {
        None
    } else {
        Some(clean.join("/"))
    }
}

pub(super) fn sanitize_path_component(s: &str) -> Option<String> {
    if s.contains('/') {
        return None;
    }
    sanitize_relative_path(s)
}

fn has_windows_drive_or_unc(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
        return true;
    }
    // UNC paths; also covered by the backslash check elsewhere, but kept for explicitness.
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

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    // (1) Sanitization tests.
    #[test]
    fn test_sanitize_relative_path_safe() {
        assert_eq!(
            sanitize_relative_path("file.mkv").as_deref(),
            Some("file.mkv")
        );
        assert_eq!(
            sanitize_relative_path("sub/file.mkv").as_deref(),
            Some("sub/file.mkv")
        );
        assert_eq!(
            sanitize_relative_path("a/b/c/d.mp4").as_deref(),
            Some("a/b/c/d.mp4")
        );
    }

    #[test]
    fn test_sanitize_relative_path_unsafe() {
        assert!(sanitize_relative_path("").is_none());
        assert!(sanitize_relative_path("/etc/passwd").is_none());
        assert!(sanitize_relative_path("../../.ssh/keys").is_none());
        assert!(sanitize_relative_path("sub/../../etc/passwd").is_none());
        assert!(sanitize_relative_path("..").is_none());

        // Encoded traversal is decoded then rejected.
        assert!(sanitize_relative_path("%2e%2e/%2e%2e/etc").is_none());

        // Backslash separators (Windows traversal).
        assert!(sanitize_relative_path("..\\..\\windows\\system32").is_none());

        // Drive letter.
        assert!(sanitize_relative_path("C:/Windows/x").is_none());

        // Null byte.
        assert!(sanitize_relative_path("good\0bad.txt").is_none());

        // Reserved Windows name.
        assert!(sanitize_relative_path("CON").is_none());
        assert!(sanitize_relative_path("Lpt1.txt").is_none());

        // Trailing dot/space-only component.
        assert!(sanitize_relative_path("foo/.").is_none());
        assert!(sanitize_relative_path("foo/...").is_none());
    }
}
