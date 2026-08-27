//! Tunables.
//!
//! Collected in one place so the budgets a scan runs under can be read
//! without reading the crawl itself.

use std::time::Duration;

pub(super) const MAX_DEPTH: u32 = 10;
pub(super) const MAX_DIRS: usize = 500;
pub(super) const MAX_FILES: usize = 10_000;
pub(super) const CONCURRENCY: usize = 8;
pub(super) const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Redirect hops per request. Same budget reqwest was given before they were
/// followed by hand.
pub(super) const MAX_REDIRECTS: usize = 10;

/// (5) Cap how much HTML we'll buffer per directory.
pub(super) const MAX_HTML_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

// (1, 2) Filename / path sanitization caps.
pub(super) const MAX_PATH_COMPONENT_LEN: usize = 255;
pub(super) const MAX_RELATIVE_PATH_LEN: usize = 4096;
