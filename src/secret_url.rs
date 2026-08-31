//! URLs that must never be printed in full.
//!
//! Several of the addresses rdm fetches are themselves credentials:
//!
//! - A Google Drive download URL carries `gdrive_api_key` in `key=`. The key
//!   is billable and persistent, so once it is in a log it stays valuable.
//! - A OneDrive direct link carries a short-lived `tempauth` signature.
//! - An anonymous Drive link, and most CDN links, carry a signature of their
//!   own.
//! - A MEGA link carries the file's decryption key in its fragment.
//!
//! The download engine used to report `Inspecting: <url>` through the progress
//! sink, and the single-download UI prints a detail line straight to stderr.
//! That put all of the above into terminal scrollback, CI job logs, support
//! captures and anything redirecting stderr to a file.
//!
//! So a URL that might carry a secret gets wrapped in [`SecretUrl`], whose
//! `Display` and `Debug` are both redacted. There is no way to print one in
//! full by accident; `as_str` exists for the request itself and is the only
//! way to get the real thing.

use std::fmt;

/// What replaces a query value, a fragment, or userinfo.
const PLACEHOLDER: &str = "REDACTED";

/// Query parameters whose *names* alone are worth hiding, because the name is
/// enough to tell an attacker reading a log which link is worth attacking.
/// Everything else keeps its name and loses its value.
const ALWAYS_HIDE_ENTIRELY: [&str; 0] = [];

/// A URL whose `Display` and `Debug` are always redacted.
///
/// Cheap to clone, and deliberately not `Deref<Target = str>`: reaching the
/// real value has to be a visible `as_str()` call at the point of use.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretUrl(String);

impl SecretUrl {
    pub fn new(url: impl Into<String>) -> Self {
        Self(url.into())
    }

    /// The real URL, for putting in a request. Never for printing.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The redacted form, when a caller needs it as an owned string.
    pub fn redacted(&self) -> String {
        redact(&self.0)
    }

    /// The scheme and authority only: `https://host:port`.
    ///
    /// The safest useful label for a progress line — it says where the bytes
    /// are coming from without saying anything about which bytes.
    pub fn origin(&self) -> String {
        origin(&self.0)
    }
}

impl fmt::Display for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&redact(&self.0))
    }
}

/// Redacted too. A `{:?}` in an error chain or a derived `Debug` is exactly
/// how one of these ends up in a log by accident.
impl fmt::Debug for SecretUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretUrl({})", redact(&self.0))
    }
}

impl From<&str> for SecretUrl {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for SecretUrl {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&reqwest::Url> for SecretUrl {
    fn from(u: &reqwest::Url) -> Self {
        Self::new(u.as_str())
    }
}

/// Strips userinfo, every query value and the fragment from `url`.
///
/// Parameter *names* are kept, because `?key=REDACTED&alt=media` is far more
/// useful when reading a bug report than `?REDACTED`, and a name is not a
/// secret. The path is kept for the same reason: it is usually the filename,
/// and it is what makes a log entry identifiable at all.
///
/// Falls back to a conservative textual redaction for anything that will not
/// parse as a URL, so a malformed address cannot leak by taking a different
/// code path.
pub fn redact(url: &str) -> String {
    let Ok(mut parsed) = reqwest::Url::parse(url) else {
        return redact_textually(url);
    };

    // Userinfo: `https://user:password@host/`.
    let has_userinfo = !parsed.username().is_empty() || parsed.password().is_some();
    if has_userinfo {
        let _ = parsed.set_username(PLACEHOLDER);
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some(PLACEHOLDER));
        }
    }

    // A MEGA link keeps its decryption key here.
    if parsed.fragment().is_some() {
        parsed.set_fragment(Some(PLACEHOLDER));
    }

    let redacted_query: Option<String> = parsed.query().map(|query| {
        if query.is_empty() {
            return String::new();
        }
        query
            .split('&')
            .map(|pair| match pair.split_once('=') {
                Some((name, _)) if ALWAYS_HIDE_ENTIRELY.contains(&name) => {
                    PLACEHOLDER.to_owned()
                }
                // A valueless flag such as `?download` reveals nothing.
                Some((name, "")) => format!("{}=", name),
                Some((name, _)) => format!("{}={}", name, PLACEHOLDER),
                None => pair.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("&")
    });

    if let Some(query) = redacted_query {
        parsed.set_query(Some(&query));
    }

    parsed.to_string()
}

/// `https://host:port` and nothing else.
pub fn origin(url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => match parsed.host_str() {
            Some(host) => match parsed.port() {
                Some(port) => format!("{}://{}:{}", parsed.scheme(), host, port),
                None => format!("{}://{}", parsed.scheme(), host),
            },
            None => format!("{}://", parsed.scheme()),
        },
        Err(_) => PLACEHOLDER.to_owned(),
    }
}

/// For strings that will not parse. Cuts at the first `?`, `#` or `@` so that
/// a half-formed URL cannot leak the part that carries the secret.
fn redact_textually(s: &str) -> String {
    let cut = s
        .find(['?', '#'])
        .into_iter()
        .chain(s.find('@'))
        .min();

    match cut {
        Some(at) => format!("{}{}", &s[..at], PLACEHOLDER),
        None => s.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The finding this module exists for: the Drive key travels in `key=`.
    #[test]
    fn a_drive_api_key_never_survives_redaction() {
        let url = "https://www.googleapis.com/drive/v3/files/1a2b3c?alt=media&key=AIzaSyRealSecretKey";
        let out = redact(url);

        assert!(!out.contains("AIzaSyRealSecretKey"), "{out}");
        // Still identifiable: the file id and the parameter names remain.
        assert!(out.contains("1a2b3c"), "{out}");
        assert!(out.contains("key=REDACTED"), "{out}");
        assert!(out.contains("alt=REDACTED"), "{out}");
    }

    #[test]
    fn a_onedrive_tempauth_signature_never_survives_redaction() {
        let url = "https://my.sharepoint.com/personal/x/_layouts/download.aspx?share=abc&tempauth=eyJ0eXAiOiJKV1QifQ.SIGNATURE";
        let out = redact(url);

        assert!(!out.contains("SIGNATURE"), "{out}");
        assert!(!out.contains("eyJ0eXAiOiJKV1QifQ"), "{out}");
        assert!(out.contains("tempauth=REDACTED"), "{out}");
    }

    /// A MEGA link's decryption key lives in the fragment, which is the part a
    /// query-only redaction would miss entirely.
    #[test]
    fn a_mega_fragment_key_never_survives_redaction() {
        let out = redact("https://mega.nz/file/AbCdEfGh#TheDecryptionKeyLivesHere");

        assert!(!out.contains("TheDecryptionKeyLivesHere"), "{out}");
        assert!(out.contains("AbCdEfGh"), "the handle is not the secret: {out}");
        assert!(out.contains("REDACTED"), "{out}");
    }

    #[test]
    fn userinfo_never_survives_redaction() {
        let out = redact("https://alice:hunter2@files.example.com/private.zip");

        assert!(!out.contains("hunter2"), "{out}");
        assert!(!out.contains("alice"), "{out}");
        assert!(out.contains("files.example.com"), "{out}");
    }

    #[test]
    fn an_ordinary_url_is_left_readable() {
        let url = "https://files.example.com/pub/holiday.mkv";
        assert_eq!(redact(url), url);
    }

    #[test]
    fn a_valueless_flag_keeps_its_shape() {
        let out = redact("https://x.com/f.bin?download&raw=1");
        assert!(out.contains("download"), "{out}");
        assert!(out.contains("raw=REDACTED"), "{out}");
    }

    /// A string that will not parse must not leak by falling through.
    #[test]
    fn an_unparseable_url_is_still_redacted() {
        let out = redact("not a url at all?key=AIzaSySecret");
        assert!(!out.contains("AIzaSySecret"), "{out}");

        let out = redact("garbage#TheKey");
        assert!(!out.contains("TheKey"), "{out}");
    }

    /// The whole point of the newtype: you cannot print one in full by
    /// accident, through `Display` or through `Debug`.
    #[test]
    fn neither_display_nor_debug_can_leak_the_secret() {
        let secret = SecretUrl::new(
            "https://www.googleapis.com/drive/v3/files/x?alt=media&key=AIzaSyRealSecretKey",
        );

        assert!(!format!("{}", secret).contains("AIzaSyRealSecretKey"));
        assert!(!format!("{:?}", secret).contains("AIzaSyRealSecretKey"));
        assert!(!format!("{:#?}", secret).contains("AIzaSyRealSecretKey"));
        // And an anyhow-style chained format, which is how these reach logs.
        assert!(!format!("failed to reach {}", secret).contains("AIzaSyRealSecretKey"));

        // The real value is still available where it is actually needed.
        assert!(secret.as_str().contains("AIzaSyRealSecretKey"));
    }

    #[test]
    fn origin_says_where_without_saying_what() {
        assert_eq!(
            origin("https://files.example.com/pub/x.bin?key=secret"),
            "https://files.example.com"
        );
        assert_eq!(
            origin("http://127.0.0.1:8080/x?key=secret"),
            "http://127.0.0.1:8080"
        );

        let secret = SecretUrl::new("https://cdn.example.com/f?tempauth=SIG");
        assert_eq!(secret.origin(), "https://cdn.example.com");
    }
}
