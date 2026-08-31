//! Path sanitization — the network trust boundary for listing paths.
//!
//! This is where a listing-supplied path is percent-decoded, and it is the
//! **only** place that is allowed to. The rules themselves live in
//! [`crate::safe_path`] because the final filesystem boundary has to apply the
//! same ones, and two copies would drift.
//!
//! (1) Decode exactly once, then validate every component. Reject:
//! - empty / overly long paths
//! - null bytes / control chars
//! - backslashes (Windows separator), absolute paths, drive letters, UNC
//! - any "." or ".." component (post-decode)
//! - Windows reserved device names
//! - components that are only dots/spaces (Windows quirks)
//!
//! ## Why the decode count matters
//!
//! Validating after one decode is only sound if nothing decodes again
//! afterwards. It used to: both `sync` and the queue ran the sanitised value
//! back through `percent_decode`, so `%252Fhome%252Fuser%252F.config%252Ftarget`
//! passed here as a single harmless component and became an absolute path
//! downstream. The value returned from here is decoded, and callers must treat
//! it as final.

use super::limits::{MAX_PATH_COMPONENT_LEN, MAX_RELATIVE_PATH_LEN};
use crate::engine;
use crate::safe_path::{self, SafeRelativePath};

// The caps are declared in `limits.rs` alongside the scraper's other budgets,
// but enforced in `safe_path`. Assert they agree so the two cannot drift apart
// silently.
const _: () = assert!(MAX_PATH_COMPONENT_LEN == safe_path::MAX_COMPONENT_LEN);
const _: () = assert!(MAX_RELATIVE_PATH_LEN == safe_path::MAX_RELATIVE_PATH_LEN);

/// Decodes a listing-supplied relative path once and validates it.
///
/// The returned string is already decoded. Do not decode it again.
pub(super) fn sanitize_relative_path(raw: &str) -> Option<String> {
    let decoded = engine::percent_decode(raw);
    SafeRelativePath::validate(&decoded).map(SafeRelativePath::into_string)
}

/// As [`sanitize_relative_path`], but the result must be a single component.
///
/// The separator check happens *after* decoding as well as before: a raw
/// `a%2Fb` decodes to `a/b`, which used to be accepted here and handed back as
/// a "component" containing a separator.
pub(super) fn sanitize_path_component(s: &str) -> Option<String> {
    if s.contains('/') {
        return None;
    }

    let decoded = engine::percent_decode(s);
    SafeRelativePath::validate_component(&decoded).map(SafeRelativePath::into_string)
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

    /// The double-decode regression. Each input is what a malicious listing
    /// actually sends; after the single permitted decode it must still be one
    /// inert component, and the encoded separator must survive *as text* so
    /// that nothing downstream can turn it back into a separator.
    #[test]
    fn double_encoded_paths_decode_once_and_stay_relative() {
        // The exact payload from the finding.
        let out = sanitize_relative_path("%252Fhome%252Fuser%252F.config%252Ftarget")
            .expect("one decode leaves a legal filename");
        assert_eq!(out, "%2Fhome%2Fuser%2F.config%2Ftarget");
        assert!(!out.starts_with('/'), "must not be absolute: {out}");
        assert!(!out.contains('/'), "must remain one component: {out}");

        // Double-encoded separators, dots and backslashes.
        for (raw, expected) in [
            ("%252F", "%2F"),
            ("%252f", "%2f"),
            ("%255C", "%5C"),
            ("%252e%252e", "%2e%2e"),
            ("%252E%252E%252Fescape", "%2E%2E%2Fescape"),
            ("a%252F..%252F..%252Fb", "a%2F..%2F..%2Fb"),
            ("%255C%255Cserver%255Cshare", "%5C%5Cserver%5Cshare"),
        ] {
            let out = sanitize_relative_path(raw)
                .unwrap_or_else(|| panic!("{raw:?} should decode to a legal name"));
            assert_eq!(out, expected, "{raw:?} decoded the wrong number of times");
            assert!(!out.contains('/'), "{raw:?} produced a separator: {out}");
            assert!(!out.contains('\\'), "{raw:?} produced a backslash: {out}");
        }
    }

    /// Triple encoding is the same attack one layer further out, and must be
    /// no more dangerous than double.
    #[test]
    fn triple_encoded_paths_are_no_worse() {
        let out = sanitize_relative_path("%25252Fetc%25252Fpasswd").unwrap();
        assert_eq!(out, "%252Fetc%252Fpasswd");
        assert!(!out.contains('/'));
    }

    /// Whatever this function returns must still be safe if the whole
    /// validation is applied to it a second time — i.e. it is a fixed point.
    /// A value that only passes because it was checked once is the bug.
    #[test]
    fn output_is_stable_under_revalidation() {
        for raw in [
            "file.mkv",
            "sub/file.mkv",
            "%252Fhome%252Fuser%252F.config%252Ftarget",
            "%252e%252e%252Fx",
            "100%2525.txt",
        ] {
            let Some(once) = sanitize_relative_path(raw) else {
                continue;
            };
            assert!(
                SafeRelativePath::validate(&once).is_some(),
                "{raw:?} -> {once:?} does not survive revalidation"
            );
            assert!(
                safe_path::resolve_under(std::path::Path::new("/dl"), &once).is_ok(),
                "{raw:?} -> {once:?} does not resolve under a root"
            );
        }
    }

    #[test]
    fn a_component_may_not_gain_a_separator_by_decoding() {
        // Raw separator: rejected before decoding, as before.
        assert!(sanitize_path_component("sub/file.mkv").is_none());
        // Encoded separator: used to be accepted and handed back as "a/b".
        assert!(sanitize_path_component("a%2Fb").is_none());
        assert!(sanitize_path_component("a%2fb").is_none());

        assert_eq!(
            sanitize_path_component("my%20folder").as_deref(),
            Some("my folder")
        );
    }
}
