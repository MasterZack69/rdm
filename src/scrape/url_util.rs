//! URL shaping, containment and naming.
//!
//! [`ensure_trailing_slash`] is what makes [`is_under_base`]'s prefix test a
//! statement about directories rather than about strings, so the two belong
//! together.

use reqwest::Url;

use crate::engine;
use crate::secret_url;
use crate::ui;
use super::path::sanitize_path_component;

pub(super) fn ensure_trailing_slash(mut url: Url) -> Url {
    url.set_fragment(None);
    if !url.path().ends_with('/') {
        let new_path = format!("{}/", url.path());
        url.set_path(&new_path);
    }
    url
}

pub(super) fn is_under_base(url: &Url, base: &Url) -> bool {
    if url.scheme() != base.scheme() {
        return false;
    }
    if url.host_str() != base.host_str() {
        return false;
    }
    if url.port_or_known_default() != base.port_or_known_default() {
        return false;
    }
    // base.path() ends with '/' by construction.
    url.path().starts_with(base.path())
}

pub(super) fn derive_folder_name(base: &Url) -> String {
    let path = base.path().trim_end_matches('/');
    let last = path.rsplit('/').next().unwrap_or("");
    // Handed over still encoded. `sanitize_path_component` decodes, and it is
    // the only thing that should: decoding here as well made this a
    // double-decode, and the result of it names a directory on disk. A segment
    // of `%252Fetc` would have arrived there as `%2Fetc` and left as `/etc`.
    let candidate = if last.is_empty() {
        base.host_str().unwrap_or("download").to_owned()
    } else {
        last.to_owned()
    };
    sanitize_path_component(&candidate).unwrap_or_else(|| "download".to_owned())
}

/// The name of `dir` as it should appear on screen.
///
/// Display only — nothing here reaches the filesystem. A listing chose every
/// byte of this string, so it is decoded exactly once and then made safe to
/// draw: an ESC in a directory name would otherwise be handed to the terminal
/// by the scan spinner.
pub(super) fn directory_label(base: &Url, dir: &Url) -> String {
    let rel = dir
        .as_str()
        .strip_prefix(base.as_str())
        .unwrap_or(dir.as_str());
    let decoded = engine::percent_decode(rel);
    let trimmed = decoded.trim_end_matches('/');
    let last = trimmed.rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        // A URL rather than a name, and a directory URL can carry a token.
        ui::terminal_safe(&secret_url::redact(dir.as_str()))
    } else {
        // A name made entirely of control characters would sanitise down to
        // nothing, and a blank label reads as a bug rather than as a refusal.
        ui::terminal_safe_or(last, "(unnamed)")
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn test_ensure_trailing_slash() {
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir")).as_str(),
            "http://x.com/dir/"
        );
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir/")).as_str(),
            "http://x.com/dir/"
        );
        // Query is preserved (unlike old behavior).
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir?q=1")).as_str(),
            "http://x.com/dir/?q=1"
        );
        // Fragment is dropped.
        assert_eq!(
            ensure_trailing_slash(u("http://x.com/dir#frag")).as_str(),
            "http://x.com/dir/"
        );
    }

    #[test]
    fn test_is_under_base() {
        let base = ensure_trailing_slash(u("http://x.com/files/"));
        assert!(is_under_base(&u("http://x.com/files/a.zip"), &base));
        assert!(is_under_base(&u("http://x.com/files/sub/a.zip"), &base));
        assert!(!is_under_base(&u("http://x.com/other/a.zip"), &base));
        assert!(!is_under_base(&u("https://x.com/files/a.zip"), &base)); // scheme differs
        assert!(!is_under_base(&u("http://y.com/files/a.zip"), &base)); // host differs
    }

    #[test]
    fn a_directory_label_is_the_last_component_decoded_once() {
        let base = ensure_trailing_slash(u("http://x.com/files/"));
        assert_eq!(
            directory_label(&base, &u("http://x.com/files/my%20photos/")),
            "my photos"
        );
    }

    /// The injection this label was the way in for. Encoded, because that is
    /// how it arrives in an HTML listing.
    #[test]
    fn an_escape_sequence_in_a_directory_name_never_reaches_the_terminal() {
        let base = ensure_trailing_slash(u("http://x.com/files/"));

        // CSI: clears the screen and moves the cursor.
        let label = directory_label(&base, &u("http://x.com/files/%1b%5b2J%1b%5bH-owned/"));
        assert!(!label.contains('\u{1b}'), "got: {:?}", label);
        assert_eq!(label, "[2J[H-owned");

        // OSC 0: retitles the window. OSC 52 would reach the clipboard.
        let label = directory_label(&base, &u("http://x.com/files/%1b%5d0;pwned%07/"));
        assert!(!label.contains('\u{1b}'), "got: {:?}", label);
        assert!(!label.contains('\u{7}'), "got: {:?}", label);

        // A name with nothing drawable left in it says so.
        assert_eq!(
            directory_label(&base, &u("http://x.com/files/%1b%07%08/")),
            "(unnamed)"
        );
    }

    /// `%252F` decodes once to `%2F`, which is not a separator. It used to be
    /// decoded twice, and the second decode made it one.
    #[test]
    fn a_double_encoded_separator_cannot_name_the_folder() {
        assert_eq!(derive_folder_name(&u("http://x.com/%252Fetc%252Fcron.d/")), "download");
        assert_eq!(derive_folder_name(&u("http://x.com/my%20files/")), "my files");
    }
}
