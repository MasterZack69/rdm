// segv's mom

use std::future::Future;
use std::path::Path;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use rdm::args::{
    Cli, ClearTarget, Command, DownloadOpts, QueueCommand, RetryTarget, normalize_extensions,
};
use rdm::{cli, config, queue, scrape, signal, sync};

fn main() -> Result<()> {
    let args = Cli::parse();
    let cfg = config::Config::load();

    match args.command {
        None => {
            // `arg_required_else_help` guarantees a URL when no subcommand ran.
            let url = args
                .url
                .as_deref()
                .expect("clap guarantees a URL when no subcommand is given");
            quick_download(&cfg, url, &args.opts)
        }

        Some(Command::Download { url, opts }) => {
            let url = cli::normalize_download_url(&url);
            let connections = opts.connections.unwrap_or(cfg.connections);
            let output_path = resolve_output(opts.output.clone(), &url, &cfg);

            run_async(|cancel| async move {
                cli::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
            })
        }

        Some(Command::Sync { url, opts, parallel, delete, ext }) => {
            let connections = opts.connections.unwrap_or(cfg.connections);
            let parallel = parallel.unwrap_or(cfg.queue_parallel);
            let ext_filter = normalize_extensions(&ext);
            let allow_private = opts.allow_private;

            // `-o` names the destination directory for a sync, not a file.
            let mut cfg = cfg;
            if let Some(dir) = opts.output {
                cfg.download_dir = dir;
            }

            run_async(|cancel| async move {
                sync::run(
                    &cfg,
                    &url,
                    connections,
                    parallel,
                    delete,
                    ext_filter,
                    allow_private,
                    cancel,
                )
                .await
            })
        }

        Some(Command::Queue { command }) => run_queue(&cfg, command),

        Some(Command::Config) => {
            cfg.print();
            Ok(())
        }
    }
}

// \u{2500}\u{2500} Command handlers \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}

/// `rdm <URL>` \u{2014} download a file, or expand a directory listing into the queue
/// and immediately start working through it.
fn quick_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = cli::normalize_download_url(url);
    let connections = opts.connections.unwrap_or(cfg.connections);
    let scan_for_listing = opts.output.is_none() && looks_like_directory(&url);

    run_async(|cancel| async move {
        if scan_for_listing {
            // A failed scan is not fatal: fall through and treat the URL as a
            // single file, which is what it usually turns out to be.
            if let Ok(Some(files)) = scrape::discover_files(&url, true, opts.allow_private).await {
                if !files.is_empty() {
                    print_discovered(&files);

                    queue::Queue::locked(|q| {
                        for file in &files {
                            q.add(
                                file.url.clone(),
                                Some(file.relative_path.clone()),
                                Some(connections),
                            );
                        }
                        Ok(())
                    })?;

                    return queue::start(cfg, cancel, cfg.queue_parallel).await;
                }
            }
        }

        let output_path = resolve_output(opts.output.clone(), &url, cfg);
        cli::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
    })
}

fn run_queue(cfg: &config::Config, command: QueueCommand) -> Result<()> {
    match command {
        QueueCommand::Add { url, opts } => queue_add(cfg, &url, &opts),

        QueueCommand::List => {
            queue::Queue::load_readonly().print_list();
            Ok(())
        }

        QueueCommand::Start { parallel } => {
            let parallel = parallel.unwrap_or(cfg.queue_parallel);
            run_async(|cancel| async move { queue::start(cfg, cancel, parallel).await })
        }

        QueueCommand::Stop => {
            queue::send_signal("stop")?;
            eprintln!("  \u{23f9}  Stop signal sent. Queue will stop after current download.");
            Ok(())
        }

        QueueCommand::Skip => {
            queue::send_signal("skip")?;
            eprintln!("  \u{23ed}  Skip signal sent.");
            Ok(())
        }

        QueueCommand::Remove { id } => {
            if queue::Queue::locked(|q| Ok(q.remove(id)))? {
                eprintln!("  Removed #{id}");
            } else {
                eprintln!("  No item with ID #{id}");
            }
            Ok(())
        }

        QueueCommand::Retry { target } => {
            match target {
                Some(RetryTarget::Failed) => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_failed()))?;
                    eprintln!("  Requeued {n} failed item(s).");
                }
                Some(RetryTarget::Skipped) => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_skipped()))?;
                    eprintln!("  Requeued {n} skipped item(s).");
                }
                Some(RetryTarget::Id(id)) => {
                    if queue::Queue::locked(|q| Ok(q.retry_item(id)))? {
                        eprintln!("  \u{2705} #{id} requeued.");
                    } else {
                        eprintln!("  #{id} is not failed or skipped.");
                    }
                }
                None => {
                    let n = queue::Queue::locked(|q| Ok(q.retry_failed() + q.retry_skipped()))?;
                    eprintln!("  Requeued {n} item(s).");
                }
            }
            Ok(())
        }

        QueueCommand::Clear { target } => {
            match target {
                Some(ClearTarget::Pending) => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_pending()))?;
                    eprintln!("  Cleared {n} pending item(s).");
                }
                Some(ClearTarget::Done) => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_finished()))?;
                    eprintln!("  Cleared {n} finished item(s).");
                }
                None => {
                    let n = queue::Queue::locked(|q| Ok(q.clear_all()))?;
                    eprintln!("  Cleared {n} item(s). Queue is empty.");
                }
            }
            Ok(())
        }
    }
}

/// `rdm queue add` \u{2014} enqueue a single file, or every file behind a listing.
fn queue_add(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = cli::normalize_download_url(url);

    let discovered = if looks_like_directory(&url) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(scrape::discover_files(&url, true, opts.allow_private))
            .unwrap_or(None)
    } else {
        None
    };

    match discovered {
        Some(files) if !files.is_empty() => {
            queue::Queue::locked(|q| {
                for file in &files {
                    q.add(
                        file.url.clone(),
                        Some(file.relative_path.clone()),
                        opts.connections,
                    );
                }
                Ok(())
            })?;

            print_discovered(&files);
        }

        _ => {
            let output = opts
                .output
                .as_deref()
                .map(|o| resolve_relative_to_config(o, cfg));
            let id = queue::Queue::locked(|q| Ok(q.add(url.clone(), output, opts.connections)))?;
            eprintln!("  \u{2705} Added #{}: {}", id, cli::percent_decode(&url));
        }
    }

    eprintln!(
        "  {} item(s) pending.",
        queue::Queue::load_readonly().pending_count()
    );
    Ok(())
}

// \u{2500}\u{2500} Helpers \u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}\u{2500}

/// Builds a multi-threaded runtime, wires SIGINT/SIGTERM to a
/// [`CancellationToken`] so in-flight downloads can checkpoint their progress,
/// and always tears the handler down again.
fn run_async<F, Fut>(body: F) -> Result<()>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async move {
            let cancel = CancellationToken::new();
            let handler = signal::spawn_signal_handler(cancel.clone());
            let result = body(cancel).await;
            handler.abort();
            result
        })
}

fn print_discovered(files: &[scrape::RemoteFile]) {
    eprintln!("  \u{1f4c1} Found {} file(s):", files.len());
    eprintln!();
    for file in files {
        eprintln!("     + {}", cli::percent_decode(&file.relative_path));
    }
    eprintln!();
}

/// Resolves `-o` for a single-file download.
///
/// A trailing separator or an existing directory means "put the file in here
/// under its remote name"; anything else is taken as the filename itself.
/// Relative paths land under the configured download directory.
fn resolve_output(output: Option<String>, url: &str, cfg: &config::Config) -> String {
    let filename_from_url = || -> String {
        cli::extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_string())
    };

    match output {
        Some(o) => {
            let path = Path::new(&o);
            if o.ends_with('/') || o.ends_with('\\') || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches('\\');
                format!("{}/{}", dir, filename_from_url())
            } else if path.is_absolute() {
                o
            } else {
                cfg.resolve_output_path(&o)
            }
        }
        None => cfg.resolve_output_path(&filename_from_url()),
    }
}

fn resolve_relative_to_config(output: &str, cfg: &config::Config) -> String {
    if Path::new(output).is_absolute() {
        output.to_string()
    } else {
        cfg.resolve_output_path(output)
    }
}

/// Heuristic: does this URL point at a directory listing rather than a file?
///
/// A trailing slash or a last segment with no extension says "listing", except
/// for long hex-ish segments, which are almost always opaque file IDs.
fn looks_like_directory(url: &str) -> bool {
    if url.ends_with('/') {
        return true;
    }

    let last_segment = url
        .split('?')
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("");

    if last_segment.is_empty() {
        return true;
    }

    if last_segment.contains('.') {
        return false;
    }

    let is_hex_like = last_segment.len() > 16
        && last_segment
            .chars()
            .all(|c| c.is_ascii_hexdigit() || c == '-' || c == '_');

    !is_hex_like
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trailing_slash_is_a_directory() {
        assert!(looks_like_directory("https://example.com/music/"));
        assert!(looks_like_directory("https://example.com/"));
    }

    #[test]
    fn extension_means_file() {
        assert!(!looks_like_directory("https://example.com/song.flac"));
        assert!(!looks_like_directory("https://example.com/a/b/archive.tar.gz"));
    }

    #[test]
    fn bare_segment_is_a_directory() {
        assert!(looks_like_directory("https://example.com/music"));
    }

    #[test]
    fn long_hex_segment_is_a_file_id() {
        assert!(!looks_like_directory(
            "https://example.com/0123456789abcdef01234"
        ));
    }

    #[test]
    fn query_string_is_ignored() {
        assert!(!looks_like_directory("https://example.com/song.flac?token=1"));
        assert!(looks_like_directory("https://example.com/music?page=2"));
    }
}
