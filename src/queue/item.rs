//! One queued item: what it points at, and what to call it.

use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

use crate::config::Config;
use crate::engine;
use crate::hoster::{gdrive, onedrive, pixeldrain};
use crate::mega;
use crate::safe_path;
use crate::ui;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Status {
    Pending,
    Downloading,
    Complete,
    Failed { reason: String, attempts: u32 },
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: u64,
    pub url: String,
    /// Where to write this item.
    ///
    /// Either a path the user typed after `-o`, or a listing-relative path the
    /// scraper already percent-decoded **exactly once** and validated. Nothing
    /// downstream may decode it again: that is what let a double-encoded
    /// listing entry turn into an absolute path. See [`Self::resolve_output`].
    pub output: Option<String>,
    pub connections: Option<usize>,
    pub status: Status,
    /// Bytes written once the item finished. `serde(default)` keeps queue.json
    /// files written by older versions loadable.
    #[serde(default)]
    pub size: Option<u64>,
    /// Carried from the command that queued this item, because the download
    /// happens in a different run than the flag.
    #[serde(default)]
    pub allow_private: bool,
}

impl Item {
    /// Does this item need the MEGA downloader rather than the engine?
    pub fn is_mega(&self) -> bool {
        mega::is_mega_url(&self.url)
    }

    /// Does this item need the OneDrive API before anything can be fetched?
    pub fn is_onedrive(&self) -> bool {
        onedrive::is_onedrive_url(&self.url)
    }

    /// Does this item need the Drive API, or the warning page, before
    /// anything can be fetched?
    pub fn is_gdrive(&self) -> bool {
        gdrive::is_gdrive_url(&self.url)
    }

    /// Does this item need the pixeldrain client rather than a bare engine
    /// download?
    ///
    /// The URL would fetch perfectly well without one — pixeldrain's is an
    /// ordinary ranged HTTPS address with no signature on it. What would go
    /// missing is the API key, which lives in the client's headers, so an
    /// account holder would be throttled to anonymous speed by a queue run and
    /// nowhere else.
    pub fn is_pixeldrain(&self) -> bool {
        pixeldrain::is_pixeldrain_url(&self.url)
    }

    /// Human-friendly name for progress lines: the output path if we have one,
    /// otherwise the last URL segment.
    ///
    /// Sanitised for the terminal, because for a directory download the name
    /// came from a remote listing and can carry ESC or OSC sequences.
    pub fn display_name(&self) -> String {
        if let Some(o) = &self.output {
            // Not decoded. `output` is already final: either the user typed it,
            // in which case a literal `%20` is part of the name they chose, or
            // the scraper decoded it once already.
            let raw = o.rsplit('/').next().unwrap_or(o);
            return ui::terminal_safe(raw);
        }

        // A MEGA link's last segment is `<handle>#<key>`, which is both
        // meaningless to the user and a secret we should not be printing. The
        // real filename only arrives once the API decrypts the attributes, so
        // until then the handle alone is the honest label.
        if self.is_mega() {
            return mega::parse_link(&self.url)
                .map(|link| format!("MEGA {}", link.handle))
                .unwrap_or_else(|_| "MEGA link".to_owned());
        }

        // A share link's last segment is an opaque token, and the real name
        // exists only once the API has been asked. Naming the host is the
        // honest label until then.
        if self.is_onedrive() {
            return "OneDrive link".to_owned();
        }

        // A Drive link's last segment is `view` or an id, and neither names
        // anything.
        if self.is_gdrive() {
            return "Google Drive link".to_owned();
        }

        // The id is not a secret the way a MEGA key is, so it can be shown —
        // but `/u/AbCdEf12` still names nothing a person recognises, and the
        // real filename only arrives with the API's answer.
        if self.is_pixeldrain() {
            return match pixeldrain::parse_link(&self.url) {
                Ok(pixeldrain::Link::File(id)) => format!("pixeldrain {id}"),
                Ok(pixeldrain::Link::List(id)) => format!("pixeldrain list {id}"),
                Err(_) => "pixeldrain link".to_owned(),
            };
        }

        let raw = self
            .url
            .split('?')
            .next()
            .unwrap_or(&self.url)
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.url);

        // The URL is the trust boundary for a bare link, so this is the single
        // permitted decode for that case.
        ui::terminal_safe(&engine::percent_decode(raw))
    }

    /// Where this item should be written, resolved against the config.
    ///
    /// ## Why there is no `percent_decode` here
    ///
    /// There used to be, and it was the second half of the double decode. The
    /// scraper hands over a path it has already decoded once and validated, so
    /// decoding again re-created the separators the validation had just proved
    /// absent: `%2Fhome%2Fuser%2F.config%2Ftarget` became
    /// `/home/user/.config/target`, and an absolute path is honoured verbatim.
    ///
    /// A relative output with no traversal component — exactly the shape the
    /// scraper produces — is additionally resolved through
    /// [`safe_path::resolve_under`], so it cannot leave the download
    /// directory. An absolute path, or a relative one containing `..`, can only
    /// have been typed by the user (the scraper rejects both), so those keep
    /// their existing behaviour and `-o /tmp/x` and `-o ../sibling/x` still
    /// work.
    pub fn resolve_output(&self, cfg: &Config) -> String {
        let Some(output) = &self.output else {
            let raw = self
                .url
                .split('?')
                .next()
                .and_then(|p| p.rsplit('/').next())
                .filter(|s| !s.is_empty())
                .unwrap_or("download.bin");

            // The URL is the trust boundary, so this decode is the only one.
            let decoded = engine::percent_decode(raw);
            let name = engine::safe_filename(&decoded).unwrap_or_else(|| "download.bin".to_owned());
            return cfg.resolve_output_path(&name);
        };

        if is_plain_relative(output) {
            if let Ok(path) = safe_path::resolve_under(Path::new(&cfg.download_dir), output) {
                return path.to_string_lossy().into_owned();
            }
        }

        cfg.resolve_output_path(output)
    }

    /// The `(explicit destination, fallback directory)` pair a share-link
    /// downloader takes.
    ///
    /// Unlike every other source, MEGA and OneDrive know the filename and we
    /// do not: MEGA has it encrypted in the file attributes, OneDrive has it
    /// behind an API call. So an item with no explicit output deliberately
    /// passes `None` and lets them name the file, instead of
    /// [`Self::resolve_output`] inventing one out of the link.
    pub fn share_destination(&self, cfg: &Config) -> (Option<String>, String) {
        match &self.output {
            // Not decoded, for the same reason as `resolve_output`.
            Some(_) => (Some(self.resolve_output(cfg)), cfg.download_dir.clone()),
            None => (None, cfg.download_dir.clone()),
        }
    }
}

/// True for a relative path made only of ordinary components.
///
/// This is the shape the scraper produces. Anything else — absolute, or
/// carrying `..` — can only have been typed by the user, because
/// `sanitize_relative_path` rejects both.
fn is_plain_relative(output: &str) -> bool {
    let path = Path::new(output);

    if path.is_absolute() {
        return false;
    }

    path.components()
        .all(|c| matches!(c, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queue;
    use crate::queue::test_support::{
        MEGA_LINK, ONEDRIVE_LINK, PIXELDRAIN_LINK, PIXELDRAIN_LIST, queue_with,
    };

    /// `output` is final by the time it reaches an item, so it is *not*
    /// decoded. This test previously asserted the opposite — that
    /// `album/my%20song.flac` displayed as `my song.flac` — which is the
    /// behaviour that let a double-encoded listing entry escape the download
    /// directory. A `-o` value is now also taken literally, which is
    /// independently more correct: a filename containing a real `%20` was
    /// being silently rewritten.
    #[test]
    fn display_name_prefers_output_and_does_not_decode_it() {
        let mut q = Queue::default();
        q.add(
            "https://x.com/dir/my%20song.flac".into(),
            Some("album/my%20song.flac".into()),
            None,
        );
        assert_eq!(q.items[0].display_name(), "my%20song.flac");

        // A path the scraper produced is already decoded, so it displays
        // as the user expects without a second decode.
        let mut q = Queue::default();
        q.add(
            "https://x.com/dir/my%20song.flac".into(),
            Some("album/my song.flac".into()),
            None,
        );
        assert_eq!(q.items[0].display_name(), "my song.flac");

        // With no output, the URL is the trust boundary and is decoded once.
        let mut q = queue_with(&["https://x.com/dir/track%2001.flac?token=1"]);
        assert_eq!(q.items[0].display_name(), "track 01.flac");
        assert!(q.remove(1));
    }

    /// A directory listing controls this string, so it must not be able to
    /// repaint the board.
    #[test]
    fn display_name_is_safe_for_the_terminal() {
        let mut q = Queue::default();
        q.add(
            "https://x.com/a.bin".into(),
            Some("dir/\u{1b}[2Kspoofed\u{7}.mkv".into()),
            None,
        );
        let name = q.items[0].display_name();
        assert!(!name.contains('\u{1b}'), "{name:?}");
        assert!(!name.contains('\u{7}'), "{name:?}");

        // And on the URL path too.
        let q = queue_with(&["https://x.com/dir/%1b%5b2Kfake.bin"]);
        assert!(!q.items[0].display_name().contains('\u{1b}'));
    }

    /// The critical regression, at the queue boundary. This is the value the
    /// scraper legitimately produces from a `%252F...` listing entry; it must
    /// stay a single filename beneath the download directory.
    #[test]
    fn a_singly_decoded_output_is_not_decoded_again() {
        let cfg = Config::default();
        let mut q = Queue::default();
        q.add(
            "https://x.com/evil".into(),
            Some("%2Fhome%2Fuser%2F.config%2Ftarget".into()),
            None,
        );

        let resolved = q.items[0].resolve_output(&cfg);
        let resolved = Path::new(&resolved);

        assert!(
            resolved.starts_with(&cfg.download_dir),
            "escaped the download dir: {}",
            resolved.display()
        );
        assert_eq!(
            resolved.file_name().and_then(|s| s.to_str()),
            Some("%2Fhome%2Fuser%2F.config%2Ftarget")
        );
        assert_ne!(resolved, Path::new("/home/user/.config/target"));
    }

    /// Defence in depth: even if something upstream regressed and handed the
    /// queue a traversal, a plain relative path is resolved under the download
    /// directory rather than being joined blindly.
    #[test]
    fn nested_relative_outputs_stay_under_the_download_dir() {
        let cfg = Config::default();
        let mut q = Queue::default();
        q.add(
            "https://x.com/a".into(),
            Some("show/s01/e01.mkv".into()),
            None,
        );

        let resolved = q.items[0].resolve_output(&cfg);
        assert!(Path::new(&resolved).starts_with(&cfg.download_dir), "{resolved}");
        assert!(resolved.ends_with("show/s01/e01.mkv"), "{resolved}");
    }

    /// A user's own `-o` is still honoured, absolute or relative, so this fix
    /// does not narrow what the CLI accepts.
    #[test]
    fn user_supplied_output_paths_still_work() {
        let cfg = Config::default();

        let mut q = Queue::default();
        q.add("https://x.com/a".into(), Some("/tmp/exact.bin".into()), None);
        assert_eq!(q.items[0].resolve_output(&cfg), "/tmp/exact.bin");

        let mut q = Queue::default();
        q.add(
            "https://x.com/a".into(),
            Some("../sibling/file.bin".into()),
            None,
        );
        let resolved = q.items[0].resolve_output(&cfg);
        assert!(resolved.contains(".."), "{resolved}");
    }

    #[test]
    fn is_plain_relative_recognises_only_ordinary_paths() {
        assert!(is_plain_relative("a.bin"));
        assert!(is_plain_relative("a/b/c.bin"));
        assert!(is_plain_relative("%2Fnot-a-path"));

        assert!(!is_plain_relative("/abs/path"));
        assert!(!is_plain_relative("../x"));
        assert!(!is_plain_relative("a/../b"));
        assert!(!is_plain_relative("./a"));
    }

    #[test]
    fn mega_items_are_recognised() {
        let q = queue_with(&[MEGA_LINK, "https://x.com/a.bin"]);
        assert!(q.items[0].is_mega());
        assert!(!q.items[1].is_mega());
        assert_eq!(q.pending_mega_count(), 1);
    }

    /// The link's last segment is `<handle>#<key>` — printing that on the
    /// progress line would leak the decryption key into logs and scrollback.
    #[test]
    fn mega_display_name_never_shows_the_key() {
        let q = queue_with(&[MEGA_LINK]);
        let name = q.items[0].display_name();
        assert_eq!(name, "MEGA AbCdEfGh");
        assert!(!name.contains("thekey"));

        // An explicit output still wins — that is a real filename.
        let mut q = Queue::default();
        q.add(MEGA_LINK.into(), Some("movies/holiday.mkv".into()), None);
        assert_eq!(q.items[0].display_name(), "holiday.mkv");
    }

    /// With no explicit output, MEGA must be left to name the file: only it
    /// can decrypt the real filename out of the attributes.
    ///
    /// The explicit-output half of this test used to pass
    /// `movies/my%20film.mkv` and expect `movies/my film.mkv`, i.e. it
    /// asserted the second decode. It now expects the path to be taken
    /// literally.
    #[test]
    fn mega_destination_defers_naming_when_it_can() {
        let cfg = Config::default();

        let q = queue_with(&[MEGA_LINK]);
        let (output, dir) = q.items[0].share_destination(&cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);

        // resolve_output would have invented this nonsense from the link.
        assert!(q.items[0].resolve_output(&cfg).contains("AbCdEfGh"));

        let mut q = Queue::default();
        q.add(MEGA_LINK.into(), Some("movies/my film.mkv".into()), None);
        let (output, _) = q.items[0].share_destination(&cfg);
        let output = output.expect("an explicit output must be honoured");
        assert!(output.ends_with("movies/my film.mkv"), "{output}");
        assert!(Path::new(&output).starts_with(&cfg.download_dir), "{output}");
    }

    #[test]
    fn onedrive_items_are_recognised() {
        let q = queue_with(&[ONEDRIVE_LINK, "https://x.com/a.bin"]);
        assert!(q.items[0].is_onedrive());
        assert!(!q.items[1].is_onedrive());
        assert!(
            !q.items[0].is_mega(),
            "the two dispatch paths must not overlap"
        );
    }

    /// The last segment of a share link is an opaque token, so it names
    /// nothing. Printing it would put a meaningless string on the board and
    /// leave it in `queue list` afterwards.
    #[test]
    fn onedrive_display_name_never_shows_the_share_token() {
        let q = queue_with(&[ONEDRIVE_LINK]);
        assert_eq!(q.items[0].display_name(), "OneDrive link");
        assert!(!q.items[0].display_name().contains("AbCdEfGh"));

        // An explicit output still wins — that is a real filename.
        let mut q = Queue::default();
        q.add(ONEDRIVE_LINK.into(), Some("share/holiday.mkv".into()), None);
        assert_eq!(q.items[0].display_name(), "holiday.mkv");
    }

    /// Same hazard as MEGA: resolve_output would carve a filename out of the
    /// share token, so naming has to wait for the API.
    #[test]
    fn onedrive_naming_waits_for_the_api_too() {
        let cfg = Config::default();
        let q = queue_with(&[ONEDRIVE_LINK]);

        let (output, dir) = q.items[0].share_destination(&cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);
        assert!(q.items[0].resolve_output(&cfg).contains("AbCdEfGh"));
    }

    #[test]
    fn pixeldrain_items_are_recognised() {
        let q = queue_with(&[PIXELDRAIN_LINK, "https://x.com/a.bin"]);
        assert!(q.items[0].is_pixeldrain());
        assert!(!q.items[1].is_pixeldrain());
        assert!(
            !q.items[0].is_mega() && !q.items[0].is_onedrive(),
            "the three dispatch paths must not overlap"
        );
    }

    /// The id is not a secret, but it names nothing, and the board would
    /// otherwise print it as though it were a filename.
    #[test]
    fn pixeldrain_display_name_says_what_the_link_is() {
        let q = queue_with(&[PIXELDRAIN_LINK, PIXELDRAIN_LIST]);
        assert_eq!(q.items[0].display_name(), "pixeldrain AbCdEf12");
        assert_eq!(q.items[1].display_name(), "pixeldrain list Zz9900");

        // An explicit output still wins — that is a real filename.
        let mut q = Queue::default();
        q.add(
            PIXELDRAIN_LINK.into(),
            Some("clips/holiday.mkv".into()),
            None,
        );
        assert_eq!(q.items[0].display_name(), "holiday.mkv");
    }

    /// Same hazard as MEGA and OneDrive: resolve_output would carve a filename
    /// out of the id, so naming has to wait for the API.
    #[test]
    fn pixeldrain_naming_waits_for_the_api_too() {
        let cfg = Config::default();
        let q = queue_with(&[PIXELDRAIN_LINK]);

        let (output, dir) = q.items[0].share_destination(&cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);
        assert!(q.items[0].resolve_output(&cfg).contains("AbCdEf12"));
    }
}
