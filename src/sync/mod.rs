//! `rdm sync` — mirror a remote directory listing into a local folder.
//!
//! Scanning, verification, and deletion all report through [`crate::ui`], and
//! the actual downloading is handed to [`crate::queue`], so a sync shows the
//! same live per-file board as `rdm queue start`.
//!
//! MEGA, OneDrive, Google Drive and pixeldrain shares take separate paths
//! through [`run()`]. Almost none of the HTTP machinery applies to them: there
//! is no HTML listing to scrape, no `HEAD` request to compare sizes with, and
//! no per-file URL that could become a queue item. What they do give is a
//! listing with sizes in it, which makes the whole verification phase
//! unnecessary.
//!
//! The module is laid out as:
//!
//! - `run`: the entry point, and the generic HTTP mirror.
//! - `mega`, `onedrive`, `gdrive`, `pixeldrain`: one share path each.
//! - `orphans`: what `--delete` may remove, and the empty-directory sweep.
//! - `report`: the plan sample, and the bulk-delete prompt.
//! - `paths`: turning listing entries into local paths.

mod gdrive;
mod mega;
mod onedrive;
mod orphans;
mod paths;
mod pixeldrain;
mod report;
mod run;

pub use run::run;
