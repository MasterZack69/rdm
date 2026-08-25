//! The reporting contract between the download engine and whatever happens to
//! be drawing.

use std::sync::Arc;

/// What a worker is currently doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Waiting,
    Inspecting,
    Downloading,
    Finishing,
}

impl SlotState {
    /// Short word shown instead of numbers while there are no bytes yet.
    pub fn label(self) -> &'static str {
        match self {
            SlotState::Waiting => "queued",
            SlotState::Inspecting => "connecting",
            SlotState::Downloading => "starting",
            SlotState::Finishing => "saving",
        }
    }
}

/// The download engine reports progress through this trait instead of printing
/// directly, so the same engine drives a solo bar, a queue lane, or nothing.
pub trait ProgressSink: Send + Sync {
    /// Total size, once known. `None` means the server never told us.
    fn total(&self, _bytes: Option<u64>) {}
    /// Absolute number of bytes on disk so far.
    fn progress(&self, _downloaded: u64) {}
    /// Phase change.
    fn state(&self, _state: SlotState) {}
    /// Chatty per-download detail. Hidden in multi-file runs.
    fn detail(&self, _msg: &str) {}
    /// Something the user must see even mid-queue (retries, server hiccups).
    fn note(&self, _msg: &str) {}
    /// Tear down any live rendering owned by this sink.
    fn finish(&self) {}
}

/// Prints nothing. Used by `--quiet`.
pub struct Silent;

impl ProgressSink for Silent {}

pub fn silent() -> Arc<dyn ProgressSink> {
    Arc::new(Silent)
}
