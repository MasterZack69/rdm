pub mod args;
pub mod chunk;
pub mod config;
pub mod engine;
pub mod hoster;
pub mod inspect;
pub mod parallel;
pub mod queue;
pub mod range_download;
pub mod resume;
pub mod retry;
pub mod scrape;
pub mod signal;
pub mod sync;
pub mod ui;

/// Transitional alias for `hoster::mega`.
///
/// MEGA used to be a top-level module and the callers still say
/// `crate::mega`. Keeping the name resolvable here means the move of the
/// crypto and folder code could be verified as byte-identical on its own,
/// rather than mixed into a rename sweep across three large files. New code
/// should reach for `crate::hoster::mega`, and this line goes away once the
/// existing call sites are updated.
pub use hoster::mega;
