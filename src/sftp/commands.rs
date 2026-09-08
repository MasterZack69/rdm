//! CLI routing uses remote metadata, never filename-extension heuristics.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::args::DownloadOpts;
use crate::config::Config;
use crate::engine::{DownloadRequest, ExistingPolicy, Outcome};
use crate::{queue, ui};
use super::batch::{Batch, download_files};
use super::local::path_text;
use super::{SftpOptions, SftpUrl, download, list};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandMode {
    Download,
    Enqueue,
}

pub async fn run(
    cfg: &Config,
    url: &str,
    opts: &DownloadOpts,
    parallel: Option<usize>,
    mode: CommandMode,
    cancel: CancellationToken,
) -> Result<()> {
    let target = SftpUrl::parse(url)?;
    let options = SftpOptions::from_config(cfg)?;
    let listing = list(target.as_str(), &options, opts.allow_private, cancel.clone()).await?;
    super::session::check_cancel(&cancel)?;
    if opts.connections.is_some_and(|count| count > 1) && !opts.quiet {
        eprintln!("  SFTP uses one stream per file; -p controls concurrent files.");
    }
    let Some(mut listing) = listing else {
        let name = target.path().file_name().and_then(|name| name.to_str())
            .context("SFTP file has no usable filename")?;
        let output = single_output(cfg, opts.output.as_deref(), name)?;
        if mode == CommandMode::Enqueue {
            let id = queue::Queue::locked(|queue| Ok(queue.add_with_scope(
                target.as_str().to_owned(), Some(output), Some(1), opts.allow_private,
            )))?;
            if !opts.quiet { eprintln!("  Added SFTP file #{id}"); }
            return Ok(());
        }
        if parallel.is_some() && !opts.quiet {
            eprintln!("  -p applies to directories; ignoring it for this SFTP file.");
        }
        let sink: Arc<dyn ui::ProgressSink> = if opts.quiet {
            ui::silent()
        } else {
            ui::SoloBar::new(&ui::terminal_safe(name))
        };
        let request = DownloadRequest::new(target.as_str().to_owned(), Some(output), 1)
            .with_allow_private(opts.allow_private);
        let outcome = download(request, options, cancel, sink).await?;
        if !opts.quiet {
            match outcome {
                Outcome::Completed { path, bytes } => eprintln!(
                    "  Downloaded {}: {}", ui::terminal_safe(&path), ui::format_size(bytes),
                ),
                Outcome::AlreadyPresent { path } => eprintln!("  Already present: {}", ui::terminal_safe(&path)),
                Outcome::Cancelled => eprintln!("  SFTP download cancelled; its checkpoint was retained."),
            }
        }
        return Ok(());
    };
    if listing.skipped > 0 && !opts.quiet {
        eprintln!("  Skipped {} remote symlink(s) or special file(s).", listing.skipped);
    }
    let root = match &opts.output {
        Some(output) => PathBuf::from(cfg.resolve_output_path(output)),
        None => {
            // The folder name came from the URL, so keep it BELOW the trusted
            // root rather than making that network-chosen name the root.
            for file in &mut listing.files {
                file.relative_path = format!("{}/{}", target.folder_name(), file.relative_path);
            }
            PathBuf::from(&cfg.download_dir)
        }
    };
    if listing.files.is_empty() {
        if !opts.quiet { eprintln!("  SFTP directory contains no downloadable regular files."); }
        return Ok(());
    }
    if mode == CommandMode::Enqueue {
        let root = std::path::absolute(root)?;
        let items: Vec<(String, String)> = listing.files.into_iter().map(|file| {
            let output = path_text(&root.join(&file.relative_path))?.to_owned();
            Ok((file.url, output))
        }).collect::<Result<_>>()?;
        let count = items.len();
        super::session::check_cancel(&cancel)?;
        queue::Queue::locked(|queue| {
            for (url, output) in items {
                queue.add_with_scope(url, Some(output), Some(1), opts.allow_private);
            }
            Ok(())
        })?;
        if !opts.quiet { eprintln!("  Queued {count} SFTP file(s)."); }
        return Ok(());
    }
    download_files(listing.files, options, Batch {
        root,
        parallel: parallel.unwrap_or(cfg.queue_parallel),
        allow_private: opts.allow_private,
        policy: ExistingPolicy::Reuse,
        quiet: opts.quiet,
    }, cancel).await
}

fn single_output(cfg: &Config, output: Option<&str>, name: &str) -> Result<String> {
    match output {
        None => Ok(cfg.resolve_output_path(name)),
        Some(output) => {
            let resolved = cfg.resolve_output_path(output);
            if output.ends_with('/') || Path::new(&resolved).is_dir() {
                Ok(path_text(&Path::new(&resolved).join(name))?.to_owned())
            } else {
                Ok(resolved)
            }
        }
    }
}
