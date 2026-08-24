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
//! Say we are adding a host called `newhost`:
//!
//!   1. `mkdir src/hoster/newhost/`, write `mod.rs` with a `detect`-able
//!      URL test and a `download` entry point.
//!   2. Add `pub mod newhost;` below.
//!   3. Add a `Newhost` variant to [`Kind`].
//!   4. Run `cargo build`. Every `match` in this file is exhaustive on
//!      purpose, so the compiler now points at each place that needs an
//!      answer for the new host — detection, naming, capabilities. Nothing
//!      silently defaults.
//!
//! Step 4 is also where a host can turn out to need less than a full
//! downloader: Dropbox only needs its links rewritten, after which the generic
//! engine does the work.
//!
//! pixeldrain was this section's example while it was still hypothetical;
//! `src/hoster/pixeldrain/` is what those four steps look like once somebody
//! has actually followed them.
//!
//! ## Why an enum and not a trait
//!
//! A `Box<dyn Hoster>` looks tidier right up until the first `async fn`:
//! native async functions in traits are not dyn-compatible, so a trait
//! object needs either `async_trait` (an extra dependency, plus a boxed
//! allocation per call) or hand-written `Pin<Box<dyn Future>>` signatures.
//! With a handful of hosters an enum costs nothing at runtime, keeps every
//! hoster's own function signatures honest (MEGA needs a `FileKey`;
//! pixeldrain does not), and turns "I forgot to wire up the new host"
//! from a silent fallthrough into a compile error.

/// Dropbox (dropbox.com): share links rewritten to `dl=1`, then downloaded by
/// the generic engine.
pub mod dropbox;

/// Google Drive (drive.google.com, docs.google.com): the virus-scan warning
/// page followed for a file, export endpoints for a Google Doc, and the Drive
/// API for a folder listing.
pub mod gdrive;

/// GoFile (gofile.io): API-resolved content trees, guest or account tokens.
pub mod gofile;

/// MEGA (mega.nz): AES-CTR chunked downloads, folder shares, MAC verification.
pub mod mega;

/// OneDrive (1drv.ms, onedrive.live.com): anonymous share resolution, then the
/// generic engine for a file and a recursive walk for a folder.
pub mod onedrive;

/// pixeldrain (pixeldrain.com): a documented API turns a link into a stable,
/// unsigned, ranged URL, and the generic engine does the rest. `/l/<id>`
/// lists expand into a flat listing.
pub mod pixeldrain;

/// A supported file host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Mega,
    Gofile,
    Dropbox,
    OneDrive,
    Gdrive,
    Pixeldrain,
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
///
/// These are per-hoster best cases, not per-link promises: the engine still
/// probes each response and adapts, so a host that usually supports ranged
/// requests can still answer one particular URL without them.
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
        if onedrive::is_onedrive_url(url) {
            return Some(Self::OneDrive);
        }
        if gdrive::is_gdrive_url(url) {
            return Some(Self::Gdrive);
        }
        if pixeldrain::is_pixeldrain_url(url) {
            return Some(Self::Pixeldrain);
        }
        None
    }

    /// Lower-case name, for messages and config keys.
    pub fn name(self) -> &'static str {
        match self {
            Self::Mega => "mega",
            Self::Gofile => "gofile",
            Self::Dropbox => "dropbox",
            Self::OneDrive => "onedrive",
            Self::Gdrive => "gdrive",
            Self::Pixeldrain => "pixeldrain",
        }
    }

    /// Name as people write it, for user-facing output.
    pub fn display_name(self) -> &'static str {
        match self {
            Self::Mega => "MEGA",
            Self::Gofile => "GoFile",
            Self::Dropbox => "Dropbox",
            Self::OneDrive => "OneDrive",
            Self::Gdrive => "Google Drive",
            // Lower-case, and identical to `name`. pixeldrain writes its own
            // name that way, so this is the right answer rather than a
            // copy-paste slip; there is a test below saying so, to stop it
            // being helpfully "corrected" to `Pixeldrain` later.
            Self::Pixeldrain => "pixeldrain",
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
            // either — the response carries a length and no digest.
            //
            // Resume and parallel chunks belong to the CDN rather than to us,
            // and in practice only a file share gets them: a folder share's
            // zip is built while it is being sent, so Dropbox cannot advertise
            // a byte range into a file that does not exist yet and the
            // response arrives without `Accept-Ranges`. The engine notices and
            // drops to a single connection, which is why these stay `true` as
            // the best case rather than being pessimised for every link.
            Self::Dropbox => Capabilities {
                folders: false,
                resume: true,
                integrity_check: false,
                parallel_chunks: true,
            },
            // Folders, unlike Dropbox: a OneDrive folder share expands into a
            // real listing of items that each carry their own download URL, so
            // there is a tree to walk and files land under their own names.
            //
            // No integrity check. The API can hand over a `quickXorHash`, but
            // nothing in this crate can compute one, and a digest that cannot
            // be recomputed would only be decoration.
            //
            // Parallel chunks are the best case, as everywhere else: a file
            // share is handed to the engine whole and gets them, while inside
            // a folder walk each file takes one connection and the parallelism
            // is files at once.
            Self::OneDrive => Capabilities {
                folders: true,
                resume: true,
                integrity_check: false,
                parallel_chunks: true,
            },
            // No integrity check, for the OneDrive reason: the API publishes
            // an `md5Checksum` for an uploaded file, and nothing in this crate
            // can compute one to compare it against.
            Self::Gdrive => Capabilities {
                folders: true,
                resume: true,
                integrity_check: false,
                parallel_chunks: true,
            },
            // Folders, like OneDrive: a `/l/<id>` list expands into a real
            // listing, and it arrives whole in one response rather than
            // needing a walk.
            //
            // No integrity check, and here that is a decision rather than a
            // limitation, which is worth being straight about: `/info` does
            // publish a `hash_sha256`, and this crate does contain a SHA-256.
            // Verifying it would mean reading the finished file back off disk,
            // which the engine has no hook for, so a `true` here would be
            // advertising a check that nothing performs.
            //
            // Parallel chunks: pixeldrain serves ranged requests — it is how
            // its own in-browser player seeks — so a single file is handed to
            // the engine whole and gets them. Note that a file carrying a
            // `download_speed_limit` is capped whatever the connection count,
            // so on those the chunks buy latency rather than bandwidth.
            Self::Pixeldrain => Capabilities {
                folders: true,
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
            // Unknowable from the link, and unlike GoFile there is no harmless
            // direction to guess in: a OneDrive share id is opaque, and a file
            // and a folder differ in where the download lands rather than only
            // in how it is counted. So the syntactic answer is the shape a
            // caller can always act on — one link, one destination — and the
            // truth comes from `onedrive::resolve`, which has to ask the API
            // before anything can be fetched anyway. Guessing from the `/u/`
            // and `/f/` path hints in a `1drv.ms` link would be reading
            // undocumented tea leaves, and `link_kind` is contractually not
            // allowed to make a request.
            Self::OneDrive => LinkKind::File,
            // The one host here whose links answer this honestly: a folder is
            // spelled `/drive/folders/<id>` or `folderview?id=<id>`, and
            // everything else is a file or a document. No guessing and no
            // request, which is what the contract asks for.
            Self::Gdrive => {
                if gdrive::is_folder_link(url) {
                    LinkKind::Folder
                } else {
                    LinkKind::File
                }
            }
            // The one host here that can answer this honestly without asking
            // anybody: `/u/<id>` is a file, `/l/<id>` is a list, and the link
            // itself says which. No opaque id, no tea leaves, no request.
            //
            // A link that parses as neither is reported as a file, because
            // `pixeldrain::resolve` is where a malformed link gets a real
            // error message and this function is not allowed to produce one.
            Self::Pixeldrain => {
                if pixeldrain::is_list_link(url) {
                    LinkKind::Folder
                } else {
                    LinkKind::File
                }
            }
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
/// rather than fetchable addresses, a Dropbox share link serves an HTML
/// preview page until [`dropbox::resolve`] rewrites it into a direct one, a
/// OneDrive link is a preview page too until [`onedrive::resolve`] asks the API
/// what is behind it. A Google Drive link is a viewer page, a document with no
/// file behind it at all, or a folder id, and [`gdrive::resolve`] is what says
/// which, and a pixeldrain link is a page for a viewer until
/// [`pixeldrain::direct_url`] turns it into an API URL.
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

    #[test]
    fn onedrive_share_links_are_routed_to_onedrive() {
        assert_eq!(
            Kind::detect("https://1drv.ms/u/s!AbCdEfGh"),
            Some(Kind::OneDrive)
        );
        assert_eq!(
            Kind::detect("https://onedrive.live.com/?cid=ABC&id=ABC%21123"),
            Some(Kind::OneDrive)
        );
        assert!(is_hoster_url("https://1drv.ms/f/s!AbCdEfGh"));
    }

    #[test]
    fn google_drive_links_are_routed_to_gdrive() {
        assert_eq!(
            Kind::detect("https://drive.google.com/file/d/1A2b3C4d5E6f/view"),
            Some(Kind::Gdrive)
        );
        assert_eq!(
            Kind::detect("https://docs.google.com/spreadsheets/d/1A2b3C4d5E6f/edit#gid=0"),
            Some(Kind::Gdrive)
        );
        assert!(is_hoster_url(
            "https://drive.google.com/drive/folders/1A2b3C4d5E6f"
        ));

        assert_eq!(
            Kind::detect("https://notdrive.google.com/file/d/1A2b3C4d5E6f/view"),
            None
        );
        assert_eq!(
            Kind::detect("https://drive.google.com.evil.com/file/d/1A2b3C4d5E6f/view"),
            None
        );
        // Where a confirmed Drive download lands. Already a fetchable address,
        // so claiming it would send a resolved URL back through resolution.
        assert_eq!(
            Kind::detect("https://drive.usercontent.google.com/download?id=1A2b3C4d5E6f"),
            None
        );
    }

    #[test]
    fn pixeldrain_links_are_routed_to_pixeldrain() {
        assert_eq!(
            Kind::detect("https://pixeldrain.com/u/AbCdEf12"),
            Some(Kind::Pixeldrain)
        );
        assert_eq!(
            Kind::detect("https://www.pixeldrain.com/l/AbCdEf12"),
            Some(Kind::Pixeldrain)
        );
        // The API forms count too: anyone who has read pixeldrain's API docs
        // will eventually paste one of these instead of the share link.
        assert_eq!(
            Kind::detect("https://pixeldrain.com/api/file/AbCdEf12"),
            Some(Kind::Pixeldrain)
        );
        assert!(is_hoster_url("https://pixeldrain.com/u/AbCdEf12"));
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
        assert_eq!(Kind::detect("https://not1drv.ms/u/s!abc"), None);
        assert_eq!(Kind::detect("https://1drv.ms.evil.com/u/s!abc"), None);
        assert_eq!(Kind::detect("https://notpixeldrain.com/u/abc"), None);
        assert_eq!(Kind::detect("https://pixeldrain.com.evil.com/u/abc"), None);
        // The sneakiest of the lot: userinfo before the `@` reads like the
        // host until it is parsed. The host here is `pixeldrain.com.evil.net`.
        assert_eq!(
            Kind::detect("https://evil.com@pixeldrain.com.evil.net/u/abc"),
            None
        );
        // A Dropbox CDN link is already direct, so no hoster claims it: there
        // is nothing to rewrite.
        assert_eq!(
            Kind::detect("https://dl.dropboxusercontent.com/cd/0/get/abc/f.zip"),
            None
        );
        // OneDrive for Business lives on SharePoint and authenticates against
        // a tenant, which an anonymous badger token cannot do.
        assert_eq!(
            Kind::detect("https://contoso-my.sharepoint.com/:u:/g/personal/x"),
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

    /// A OneDrive link keeps the same shape whatever is behind it, so the
    /// syntactic answer is the one a caller can act on and the API settles the
    /// rest. Which is not the same as saying folders are unsupported.
    #[test]
    fn a_onedrive_link_does_not_say_whether_it_is_a_folder() {
        for link in ["https://1drv.ms/u/s!AbCdEfGh", "https://1drv.ms/f/s!AbCdEfGh"] {
            assert_eq!(Kind::OneDrive.link_kind(link), LinkKind::File);
            assert!(!Kind::OneDrive.is_folder_link(link));
        }

        assert!(Kind::OneDrive.capabilities().folders);
    }

    /// Google Drive is the exception to both of the cases above: the link says
    /// which it is, and it changes what happens — a folder becomes a tree of
    /// downloads rather than one destination.
    #[test]
    fn a_google_drive_folder_link_says_so() {
        let folder = "https://drive.google.com/drive/folders/1A2b3C4d5E6f";
        let file = "https://drive.google.com/file/d/1A2b3C4d5E6f/view";

        assert_eq!(Kind::Gdrive.link_kind(folder), LinkKind::Folder);
        assert!(Kind::Gdrive.is_folder_link(folder));
        assert_eq!(Kind::Gdrive.link_kind(file), LinkKind::File);
        assert!(!Kind::Gdrive.is_folder_link(file));
    }

    /// The exact opposite of the two tests above, and the reason `link_kind`
    /// is worth having at all: pixeldrain says which it is, up front, for free.
    #[test]
    fn a_pixeldrain_link_says_whether_it_is_a_list() {
        let file = "https://pixeldrain.com/u/AbCdEf12";
        let list = "https://pixeldrain.com/l/AbCdEf12";

        assert_eq!(Kind::Pixeldrain.link_kind(file), LinkKind::File);
        assert!(!Kind::Pixeldrain.is_folder_link(file));

        assert_eq!(Kind::Pixeldrain.link_kind(list), LinkKind::Folder);
        assert!(Kind::Pixeldrain.is_folder_link(list));

        // A link that parses as neither is not a list. `resolve` is what gives
        // it a real error; this function is not allowed to make a request and
        // so is not entitled to an opinion.
        assert_eq!(
            Kind::Pixeldrain.link_kind("https://pixeldrain.com/"),
            LinkKind::File
        );
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

    #[test]
    fn onedrive_advertises_what_it_implements() {
        let caps = Kind::OneDrive.capabilities();
        assert!(caps.folders);
        assert!(caps.resume);
        assert!(!caps.integrity_check);
        assert!(caps.parallel_chunks);
        assert_eq!(Kind::OneDrive.name(), "onedrive");
        assert_eq!(Kind::OneDrive.display_name(), "OneDrive");
    }

    #[test]
    fn gdrive_advertises_what_it_implements() {
        let caps = Kind::Gdrive.capabilities();
        assert!(caps.folders);
        assert!(caps.resume);
        assert!(!caps.integrity_check);
        assert!(caps.parallel_chunks);
        assert_eq!(Kind::Gdrive.name(), "gdrive");
        assert_eq!(Kind::Gdrive.display_name(), "Google Drive");
    }

    #[test]
    fn pixeldrain_advertises_what_it_implements() {
        let caps = Kind::Pixeldrain.capabilities();
        assert!(caps.folders);
        assert!(caps.resume);
        assert!(!caps.integrity_check);
        assert!(caps.parallel_chunks);
        assert_eq!(Kind::Pixeldrain.name(), "pixeldrain");
        // Lower-case on purpose, and identical to `name`: pixeldrain writes
        // its own name that way. This assertion exists to stop it being
        // helpfully "corrected" to `Pixeldrain` by someone tidying up.
        assert_eq!(Kind::Pixeldrain.display_name(), "pixeldrain");
    }
}
