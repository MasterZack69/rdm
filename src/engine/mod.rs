//! The download engine.
//!
//! Argument parsing lives in `args.rs` and command dispatch lives in
//! `main.rs`. This module knows how to turn a URL into a file on disk and
//! nothing else.
//!
//! It never prints. Callers hand it a [`ProgressSink`](crate::ui::ProgressSink)
//! and receive an [`Outcome`]; a single download passes a
//! [`SoloBar`](crate::ui::SoloBar), the queue passes a lane of its live board.
//! That is what makes per-file progress lines possible during parallel runs.
//!
//! The module is laid out as:
//!
//! - `request`: what a caller asks for, and how the download ended.
//! - `download`: the entry point that turns a request into a file.
//! - `run`: wrappers that own a progress bar and print the summary line.
//! - `output`: reconciling the requested path with what is already on disk.
//! - `streaming`: the fallback for servers that do not report a size.
//! - `url`: download-URL normalisation and filename extraction.
//! - `name`: reducing a server-supplied filename to one safe component.
//! - `client`: the shared HTTP client and configuration.

mod client;
mod download;
mod name;
mod output;
mod request;
mod run;
mod streaming;
mod url;

pub use download::download;
pub use name::safe_filename;
pub use output::resolve_existing_output;
pub use request::{DownloadRequest, ExistingPolicy, Outcome, OutputDecision};
pub use run::{run_download, run_download_with_client, run_download_with_identity};
pub use streaming::{build_streaming_request, resolve_resume_action, ResumeAction};
pub use url::{extract_filename_from_url, normalize_download_url, percent_decode};
