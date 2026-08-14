//! Hoster support: one module per file host.
//!
//! Every hoster needs the same four things, and they are the reason this
//! layer exists rather than a pile of `if url.contains("…")` in `main`:
//!
//!   1. **Recognise its own links** — without claiming links that merely look
//!      similar. `notmega.nz` is not MEGA.
//!   2. **Say what a link points at** — a single file or a whole folder. These
//!      are different code paths everywhere downstream (one progress bar
//!      versus many, one destination versus a tree), so it is worth knowing
//!      before any network call.
//!   3. **Declare what it can actually do**, so callers such as `rdm sync` can
//!      refuse politely up front instead of failing halfway through.
//!   4. **Download**, with whatever host-specific chunking, keys, tokens or
//!      backoff it needs.
//!
//! ## Adding a hoster
//!
//! Say we are adding pixeldrain:
//!
//!   1. `mkdir src/hoster/pixeldrain/`, write `mod.rs` with a `detect`-able
//!      URL test and a `download` entry point.
//!   2. Add `pub mod pixeldrain;` below.
//!   3. Add a `Pixeldrain` variant to [`Kind`].
//!   4. Run `cargo build`. Every `match` in this file is exhaustive on
//!      purpose, so the compiler now points at each place that needs an
//!      answer for the new host — detection, naming, capabilities. Nothing
//!      silently defaults.
//!
//! Step 4 is also where a host can turn out to need less than a full
//! downloader: Dropbox only needs its links rewritten, after which the generic
//! engine does the work.
//!
//! ## Why an enum and not a trait
//!
//! A `Box<dyn Hoster>` looks tidier right up until the first `async fn`:
//! native async functions in traits are not dyn-compatible, so a trait
//! object needs either `async_trait` (an extra dependency, plus a boxed
//! allocation per call) or hand-written `Pin<Box<dyn Future>>` signatures.
//! With a handful of hosters an enum costs nothing at runtime, keeps every
//! hoster's own function signatures honest (MEGA needs a `FileKey`;
//! pixeldrain will not), and turns "I forgot to wire up the new host"
//! from a silent fallthrough into a compile error.

/// Dropbox (dropbox.com): share links rewritten to `dl=1`, then downloaded by
/// the generic engine.
pub mod dropbox;

/// GoFile (gofile.io): API-resolved content trees, guest or account tokens.
pub mod gofile;

/// MEGA (mega.nz): AES-CTR chunked downloads, folder shares, MAC verification.
pub mod mega;

/// A supported file host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Mega,
    Gofile,
    Dropbox,
}

/// What a link points at.
///
/// Worth distinguishing before any request goes out, because a folder link
/// fans out into many downloads under one destination directory while a file
/// link resolves to exactly one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkKind {
    File,
    Folder,
}

/// What a hoster is able to do, so callers can refuse early and clearly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Folder or album links expand into a listing of files.
    pub folders: bool,
    /// Interrupted downloads can pick up where they stopped.
    pub resume: bool,
    /// A finished file can be checked against a host-provided digest, so
    /// corruption is caught rather than saved.
    pub integrity_check: bool,
    /// Ranged requests can run in parallel against one file.
    pub parallel_chunks: bool,
}

impl Kind {
    /// Identifies the hoster responsible for `url`, if any.
    ///
    /// Returns `None` for ordinary HTTP links, which the generic engine
    /// handles perfectly well.
    pub fn detect(url: &str) -> Option<Self> {
        if mega::is_mega_url(url) {
            return Some(Self::Mega);
        }
        if gofile::is_gofile_url(url) {
            return Some(Self::Gofile);
        }
        if dropbox::is_dropbox_url(url) {
            return Some(Self::Dropbox);
        }
        None
    }

    /// Lower-case name, for messages and config keys.
    pub fn name(self) -> &'static str {
        match self {
            Self::Mega => "mega",
            Self::Gofile => "gofile",
            Self::Dropbox => "dropbox",
        }
    }

    /// Name as people write it, for user-facing output.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Mega => "MEGA",
            Self::Gofile => "GoFile",
            Self::Dropbox => "Dropbox",
        }
    }

    pub fn capabilities(self) -> Capabilities {
        match self {
            Self::Mega => Capabilities {
                folders: true,
                resume: true,
                integrity_check: true,
                parallel_chunks: true,
            },
            // No integrity check: GoFile publishes no per-file digest, so a
            // finished download can only be checked against the advertised
            // length. No parallel chunks: its storage nodes rate-limit per
            // connection, and running several files at once beats splitting
            // one.
            Self::Gofile => Capabilities {
                folders: true,
                resume: true,
                integrity_check: false,
                parallel_chunks: false,
            },
            // No folders: a Dropbox folder share does not expand into a
            // listing, it is zipped and served as one response, so there is
            // nothing for a folder-aware caller to walk. No integrity check
            // either \u{2014} the response carries a length and no digest. Resume and
            // parallel chunks are inherited from the CDN, which honours
            // `Range` once the share link has been rewritten.
            Self::Dropbox => Capabilities {
                folders: false,
                resume: true,
                integrity_check: false,
                parallel_chunks: true,
            },
        }
    }

    /// Whether this link is a folder share or a single file.
    ///
    /// Purely syntactic — no network access — so it is safe to call while
    /// parsing arguments.
    pub fn link_kind(self, url: &str) -> LinkKind {
        match self {
            Self::Mega => {
                if mega::folder::is_folder_link(url) {
                    LinkKind::Folder
                } else {
                    LinkKind::File
                }
            }
            // Always a folder. A GoFile content id is opaque: the same
            // `/d/<id>` shape is used whether it holds one file or a tree of
            // them, and which it is only becomes known after the API call.
            // Treating every link as a folder means the one-file case lands
            // in a directory of its own, which is the harmless direction to
            // be wrong in.
            Self::Gofile => LinkKind::Folder,
            // Always a file, folder shares included: Dropbox zips a folder
            // and serves it as one response, so there is exactly one
            // destination path either way. `dropbox::is_folder_link` is still
            // available for callers that want to say which it was.
            Self::Dropbox => LinkKind::File,
        }
    }

    /// Convenience for the common "is this a folder link" question.
    pub fn is_folder_link(self, url: &str) -> bool {
        self.link_kind(url) == LinkKind::Folder
    }
}

/// Identifies the hoster responsible for `url`, if any.
pub fn detect(url: &str) -> Option<Kind> {
    Kind::detect(url)
}

/// Does any hoster module claim this link?
///
/// A `true` here means the generic HTTP engine must not be handed the URL as
/// it stands: MEGA and GoFile links are API handles plus decryption keys
/// rather than fetchable addresses, and a Dropbox share link serves an HTML
/// preview page until [`dropbox::resolve`] rewrites it into a direct one.
pub fn is_hoster_url(url: &str) -> bool {
    detect(url).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mega_links_are_routed_to_mega() {
        assert_eq!(
            Kind::detect("https://mega.nz/file/AbCdEfGh#key"),
            Some(Kind::Mega)
        );
        assert_eq!(
            Kind::detect("https://mega.co.nz/folder/AbCdEfGh#key"),
            Some(Kind::Mega)
        );
    }

    #[test]
    fn gofile_links_are_routed_to_gofile() {
        assert_eq!(
            Kind::detect("https://gofile.io/d/AbCdEf"),
            Some(Kind::Gofile)
        );
        assert_eq!(
            Kind::detect("https://www.gofile.io/d/AbCdEf"),
            Some(Kind::Gofile)
        );
        assert!(is_hoster_url("https://gofile.io/d/AbCdEf"));
    }

    #[test]
    fn dropbox_share_links_are_routed_to_dropbox() {
        assert_eq!(
            Kind::detect("https://www.dropbox.com/scl/fi/abc123/report.pdf?rlkey=k&dl=0"),
            Some(Kind::Dropbox)
        );
        assert_eq!(
            Kind::detect("https://dropbox.com/sh/abc123/AABBCC?dl=0"),
            Some(Kind::Dropbox)
        );
        assert!(is_hoster_url(
            "https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0"
        ));
    }

    /// Ordinary links must fall through to the generic engine, and lookalike
    /// hosts must not be claimed by anyone.
    #[test]
    fn everything_else_is_left_to_the_generic_engine() {
        assert_eq!(Kind::detect("https://example.com/file.zip"), None);
        assert_eq!(Kind::detect("https://notmega.nz/file/abc#key"), None);
        assert_eq!(Kind::detect("https://mega.nz.evil.com/file/abc#key"), None);
        assert_eq!(Kind::detect("https://notgofile.io/d/abc"), None);
        assert_eq!(Kind::detect("https://gofile.io.evil.com/d/abc"), None);
        assert_eq!(Kind::detect("https://notdropbox.com/scl/fi/abc/f.zip"), None);
        assert_eq!(
            Kind::detect("https://dropbox.com.evil.com/scl/fi/abc/f.zip"),
            None
        );
        // A Dropbox CDN link is already direct, so no hoster claims it: there
        // is nothing to rewrite.
        assert_eq!(
            Kind::detect("https://dl.dropboxusercontent.com/cd/0/get/abc/f.zip"),
            None
        );
        assert!(!is_hoster_url("https://example.com/f.zip"));
    }

    #[test]
    fn folder_and_file_links_are_told_apart_without_a_request() {
        let mega = Kind::Mega;
        assert_eq!(
            mega.link_kind("https://mega.nz/folder/s6lVFYbI#key"),
            LinkKind::Folder
        );
        assert_eq!(
            mega.link_kind("https://mega.nz/file/AbCdEfGh#key"),
            LinkKind::File
        );
        assert!(mega.is_folder_link("https://mega.nz/folder/s6lVFYbI#key"));
        assert!(!mega.is_folder_link("https://mega.nz/file/AbCdEfGh#key"));
    }

    /// One content id can be a single file or a whole tree, and the link does
    /// not say which, so every GoFile link is handled as a folder.
    #[test]
    fn every_gofile_link_is_treated_as_a_folder() {
        assert!(Kind::Gofile.is_folder_link("https://gofile.io/d/AbCdEf"));
        assert_eq!(
            Kind::Gofile.link_kind("https://gofile.io/d/AbCdEf"),
            LinkKind::Folder
        );
    }

    /// Dropbox is the other way round from GoFile: the link does say which it
    /// is, and it does not matter, because a folder share arrives as one zip.
    /// Both halves of that are easy to get wrong in the other direction.
    #[test]
    fn a_dropbox_folder_share_is_still_a_single_download() {
        let link = "https://www.dropbox.com/scl/fo/abc123/h?rlkey=k&dl=0";

        assert!(dropbox::is_folder_link(link));
        assert_eq!(Kind::Dropbox.link_kind(link), LinkKind::File);
        assert!(!Kind::Dropbox.is_folder_link(link));
        assert!(!Kind::Dropbox.capabilities().folders);
    }

    #[test]
    fn mega_advertises_what_it_implements() {
        let caps = Kind::Mega.capabilities();
        assert!(caps.folders);
        assert!(caps.resume);
        assert!(caps.integrity_check);
        assert!(caps.parallel_chunks);
        assert_eq!(Kind::Mega.name(), "mega");
        assert_eq!(Kind::Mega.display_name(), "MEGA");
    }

    /// The two `false`s are the point of this test: callers are meant to read
    /// them and not offer what GoFile cannot do.
    #[test]
    fn gofile_advertises_what_it_implements() {
        let caps = Kind::Gofile.capabilities();
        assert!(caps.folders);
        assert!(caps.resume);
        assert!(!caps.integrity_check);
        assert!(!caps.parallel_chunks);
        assert_eq!(Kind::Gofile.name(), "gofile");
        assert_eq!(Kind::Gofile.display_name(), "GoFile");
    }

    #[test]
    fn dropbox_advertises_what_it_implements() {
        let caps = Kind::Dropbox.capabilities();
        assert!(!caps.folders);
        assert!(caps.resume);
        assert!(!caps.integrity_check);
        assert!(caps.parallel_chunks);
        assert_eq!(Kind::Dropbox.name(), "dropbox");
        assert_eq!(Kind::Dropbox.display_name(), "Dropbox");
    }
}
