//! Single-download convenience wrappers.
//!
//! These own the progress bar and the summary line, which is what lets
//! [`download`] itself stay silent.

use anyhow::Result;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::ui::{self, ProgressSink};

use super::download::download;
use super::request::{DownloadRequest, Outcome};
use super::url::extract_filename_from_url;

/// Renders its own progress bar and prints a summary line. Used by
/// `rdm <url>` and `rdm download`.
pub async fn run_download(
    url: String,
    output: Option<String>,
    connections: usize,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(url, output, connections, None, None, cancel, quiet).await
}

/// The same, over a client the caller has already authenticated.
///
/// For a source whose authorisation is a session rather than part of the URL —
/// a password-protected Dropbox share, whose cookies live in that client's jar.
pub async fn run_download_with_client(
    url: String,
    output: Option<String>,
    connections: usize,
    client: reqwest::Client,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(url, output, connections, Some(client), None, cancel, quiet).await
}

/// The same, for a source whose URL is a credential rather than a name.
pub async fn run_download_with_identity(
    url: String,
    output: Option<String>,
    connections: usize,
    identity: String,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    run(
        url,
        output,
        connections,
        None,
        Some(identity),
        cancel,
        quiet,
    )
    .await
}

async fn run(
    url: String,
    output: Option<String>,
    connections: usize,
    client: Option<reqwest::Client>,
    identity: Option<String>,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    let name = output
        .clone()
        .map(|o| o.rsplit('/').next().unwrap_or(&o).to_owned())
        .or_else(|| extract_filename_from_url(&url))
        .unwrap_or_else(|| "download".to_owned());

    let bar = if quiet {
        None
    } else {
        Some(ui::SoloBar::new(&name))
    };
    let sink: Arc<dyn ProgressSink> = match &bar {
        Some(b) => Arc::clone(b) as Arc<dyn ProgressSink>,
        None => ui::silent(),
    };

    let request = DownloadRequest::new(url, output, connections);
    let request = match client {
        Some(authenticated) => request.with_client(authenticated),
        None => request,
    };
    let request = match identity {
        Some(identity) => request.with_resume_identity(identity),
        None => request,
    };

    let result = download(request, cancel, sink).await;
    let elapsed = bar.as_ref().map(|b| b.elapsed()).unwrap_or_default();

    match result {
        Ok(Outcome::Completed { path, bytes }) => {
            if !quiet {
                let secs = elapsed.as_secs_f64();
                let avg = if secs > 0.1 {
                    Some((bytes as f64 / secs) as u64)
                } else {
                    None
                };
                eprintln!("  \u{2705} Download complete: {}", path);
                eprintln!(
                    "  {} in {} ({})",
                    ui::format_size(bytes),
                    ui::format_duration(elapsed.as_secs()),
                    ui::format_speed(avg),
                );
            }
            Ok(())
        }
        Ok(Outcome::AlreadyPresent { path }) => {
            if !quiet {
                eprintln!("  \u{2713} Already downloaded: {}", path);
            }
            Ok(())
        }
        Ok(Outcome::Cancelled) => {
            if !quiet {
                eprintln!("  Download cancelled.");
            }
            Ok(())
        }
        Err(e) => {
            if !quiet {
                eprintln!("  \u{274d} Download failed.");
                eprintln!("  Progress saved. Resume by running the same command again.");
            }
            Err(e)
        }
    }
}
