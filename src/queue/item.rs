//! One queued item: what it points at, and what to call it.

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::engine;
use crate::hoster::{gdrive, onedrive, pixeldrain};
use crate::mega;

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
    pub output: Option<String>,
    pub connections: Option<usize>,
    pub status: Status,
    /// Bytes written once the item finished. `serde(default)` keeps queue.json
    /// files written by older versions loadable.
    #[serde(default)]
    pub size: Option<u64>,
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
    pub fn display_name(&self) -> String {
        if let Some(o) = &self.output {
            let raw = o.rsplit('/').next().unwrap_or(o);
            return engine::percent_decode(raw);
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
        engine::percent_decode(raw)
    }

    /// Where this item should be written, resolved against the config.
    pub fn resolve_output(&self, cfg: &Config) -> String {
        let raw_path = match &self.output {
            Some(o) => engine::percent_decode(o),
            None => {
                let raw = self
                    .url
                    .split('?')
                    .next()
                    .and_then(|p| p.rsplit('/').next())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("download.bin");
                engine::percent_decode(raw)
            }
        };
        cfg.resolve_output_path(&raw_path)
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
            Some(o) => (
                Some(cfg.resolve_output_path(&engine::percent_decode(o))),
                cfg.download_dir.clone(),
            ),
            None => (None, cfg.download_dir.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queue;
    use crate::queue::test_support::{
        MEGA_LINK, ONEDRIVE_LINK, PIXELDRAIN_LINK, PIXELDRAIN_LIST, queue_with,
    };

    #[test]
    fn display_name_prefers_output_and_decodes() {
        let mut q = Queue::default();
        q.add(
            "https://x.com/dir/my%20song.flac".into(),
            Some("album/my%20song.flac".into()),
            None,
        );
        assert_eq!(q.items[0].display_name(), "my song.flac");

        let mut q = queue_with(&["https://x.com/dir/track%2001.flac?token=1"]);
        assert_eq!(q.items[0].display_name(), "track 01.flac");
        assert!(q.remove(1));
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
        q.add(MEGA_LINK.into(), Some("movies/my%20film.mkv".into()), None);
        let (output, _) = q.items[0].share_destination(&cfg);
        let output = output.expect("an explicit output must be honoured");
        assert!(output.ends_with("movies/my film.mkv"), "{output}");
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
