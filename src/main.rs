// segv's mom

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use rdm::args::{
    Cli, ClearTarget, Command, DownloadOpts, QueueCommand, RetryTarget, normalize_extensions,
};
use rdm::ui::{self, ProgressSink};
use rdm::{config, engine, mega, queue, scrape, signal, sync};

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
            quick_download(&cfg, url, &args.opts, args.parallel)
        }

        Some(Command::Download { url, opts }) => {
            // Before `normalize_download_url`: a MEGA link's `#key` fragment is
            // load-bearing and must reach the parser untouched.
            if mega::is_mega_url(&url) {
                return mega_download(&cfg, &url, &opts);
            }

            let url = engine::normalize_download_url(&url);
            let connections = opts.connections.unwrap_or(cfg.connections);
            let output_path = resolve_output(opts.output.clone(), &url, &cfg);

            run_async(|cancel| async move {
                engine::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
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

// -- Command handlers ------------------------------------------------

/// `rdm <URL>` — download a file, or expand a directory listing into the queue
/// and immediately start working through it.
///
/// `parallel` only takes effect on the listing path, where several files are
/// downloaded at once. On the single-file path it has nothing to act on, so we
/// say so rather than accepting the flag and quietly doing nothing with it.
fn quick_download(
    cfg: &config::Config,
    url: &str,
    opts: &DownloadOpts,
    parallel: Option<usize>,
) -> Result<()> {
    // MEGA first: `looks_like_directory` sees `/file/AbCdEfGh#key` as an
    // extensionless segment and would hand the link to the scraper, which
    // finds nothing there.
    if mega::is_mega_url(url) {
        if parallel.is_some() && !opts.quiet {
            eprintln!("  \u{26a0} -p applies to directory listings; MEGA uses mega_workers.");
        }
        return mega_download(cfg, url, opts);
    }

    let url = engine::normalize_download_url(url);
    let connections = opts.connections.unwrap_or(cfg.connections);
    let scan_for_listing = opts.output.is_none() && looks_like_directory(&url);

    run_async(|cancel| async move {
        if scan_for_listing {
            // A failed scan is not fatal: fall through and treat the URL as a
            // single file, which is what it usually turns out to be.
            if let Ok(Some(files)) = scrape::discover_files(&url, true, opts.allow_private).await
                && !files.is_empty()
            {
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

                let parallel = parallel.unwrap_or(cfg.queue_parallel);
                return queue::start(cfg, cancel, parallel).await;
            }
        }

        if parallel.is_some() && !opts.quiet {
            eprintln!(
                "  \u{26a0} -p applies to directory listings; ignoring it for a single file."
            );
        }

        let output_path = resolve_output(opts.output.clone(), &url, cfg);
        engine::run_download(url, Some(output_path), connections, cancel, opts.quiet).await
    })
}

/// `rdm <mega link>` — fetch, decrypt and verify a MEGA file.
///
/// Kept separate from the normal download path on purpose: MEGA needs its own
/// API round trip, its own chunk ladder and its own quota handling, and none
/// of that belongs in the generic engine.
fn mega_download(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    let url = url.trim().to_string();
    let (output, download_dir) = mega_destination(opts.output.clone(), cfg);
    let options = mega_options(cfg, opts);
    let quiet = opts.quiet;

    // The name is only known after the API call decrypts the attributes, so
    // the bar starts out labelled with the link's handle.
    let label = mega::parse_link(&url)
        .map(|link| link.handle)
        .unwrap_or_else(|_| "mega".to_string());

    run_async(|cancel| async move {
        let sink = progress_sink(quiet, &label);
        let client = reqwest::Client::new();
        let outcome = mega::download(
            client,
            &url,
            output,
            &download_dir,
            options,
            cancel,
            sink,
        )
        .await?;

        report_mega(&outcome, quiet);
        Ok(())
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

/// `rdm queue add` — enqueue a single file, or every file behind a listing.
fn queue_add(cfg: &config::Config, url: &str, opts: &DownloadOpts) -> Result<()> {
    // The queue worker downloads through the generic engine, which cannot
    // decrypt a MEGA stream. Refusing here beats enqueueing an item that is
    // guaranteed to fail later.
    if mega::is_mega_url(url) {
        anyhow::bail!("MEGA links cannot be queued yet — download it directly with `rdm <link>`");
    }

    let url = engine::normalize_download_url(url);

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
            eprintln!("  \u{2705} Added #{}: {}", id, engine::percent_decode(&url));
        }
    }

    eprintln!(
        "  {} item(s) pending.",
        queue::Queue::load_readonly().pending_count()
    );
    Ok(())
}

// -- Helpers ---------------------------------------------------------

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

/// A single-file progress bar, or nothing at all under `-q`.
fn progress_sink(quiet: bool, label: &str) -> Arc<dyn ProgressSink> {
    if quiet {
        ui::silent()
    } else {
        let bar: Arc<dyn ProgressSink> = ui::SoloBar::new(label);
        bar
    }
}

/// Splits `-o` into the (exact path, fallback directory) pair MEGA needs.
///
/// MEGA is the one source where we do not know the filename up front — it
/// arrives encrypted in the file attributes. So a directory-ish `-o` means
/// "use the real name, in here", and only a concrete path overrides it.
fn mega_destination(output: Option<String>, cfg: &config::Config) -> (Option<String>, String) {
    match output {
        Some(o) => {
            let path = Path::new(&o);
            if o.ends_with('/') || o.ends_with('\\') || path.is_dir() {
                let dir = o.trim_end_matches('/').trim_end_matches('\\').to_string();
                (None, dir)
            } else {
                (
                    Some(resolve_relative_to_config(&o, cfg)),
                    cfg.download_dir.clone(),
                )
            }
        }
        None => (None, cfg.download_dir.clone()),
    }
}

/// `-c` doubles as the MEGA worker count: to the user it is still "how many
/// connections do I want to this file".
fn mega_options(cfg: &config::Config, opts: &DownloadOpts) -> mega::MegaOptions {
    mega::MegaOptions {
        workers: opts.connections.unwrap_or(cfg.mega_workers),
        verify_mac: cfg.mega_verify_mac,
        resume_on_ip_change: cfg.mega_resume_on_ip_change,
        max_retries: u32::try_from(cfg.max_retries).unwrap_or(6),
        overwrite: false,
    }
}

fn report_mega(outcome: &mega::MegaOutcome, quiet: bool) {
    if quiet {
        return;
    }

    match outcome {
        mega::MegaOutcome::Completed { path, bytes } => {
            eprintln!(
                "  \u{2705} {} ({})",
                path.display(),
                ui::format_size(*bytes)
            );
        }
        mega::MegaOutcome::AlreadyPresent { path } => {
            eprintln!("  \u{2713} Already downloaded: {}", path.display());
        }
        mega::MegaOutcome::Cancelled { path } => {
            eprintln!(
                "  \u{23f8} Stopped \u{2014} partial file kept at {}, rerun to resume.",
                path.display()
            );
        }
    }
}

/// Shows what was found without burying the terminal: a listing of 4000 files
/// used to print 4000 lines before a single byte was downloaded.
fn print_discovered(files: &[scrape::DiscoveredFile]) {
    const SAMPLE: usize = 20;

    eprintln!("  \u{1f4c1} Found {} file(s):", files.len());
    eprintln!();
    for file in files.iter().take(SAMPLE) {
        eprintln!("     + {}", engine::percent_decode(&file.relative_path));
    }
    if files.len() > SAMPLE {
        eprintln!("     \u{2026} and {} more", files.len() - SAMPLE);
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
        engine::extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_string())
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

    // -- MEGA routing --

    /// The reason the MEGA check has to come first: the listing heuristic
    /// genuinely reads a MEGA link as a directory, so any code path that tests
    /// for a listing before testing for MEGA sends the link to the scraper.
    #[test]
    fn mega_links_would_be_mistaken_for_listings() {
        let link = "https://mega.nz/file/AbCdEfGh#thekey";
        assert!(mega::is_mega_url(link));
        assert!(
            looks_like_directory(link),
            "if this ever stops being true the ordering comment above is stale, not wrong"
        );
    }

    #[test]
    fn ordinary_links_are_not_sent_to_mega() {
        assert!(!mega::is_mega_url("https://example.com/mega.nz/file.zip"));
        assert!(!mega::is_mega_url("https://example.com/song.flac"));
    }

    #[test]
    fn mega_destination_prefers_the_real_filename() {
        let cfg = config::Config::default();

        // No -o: let MEGA name the file, inside the download dir.
        let (output, dir) = mega_destination(None, &cfg);
        assert_eq!(output, None);
        assert_eq!(dir, cfg.download_dir);

        // Directory-ish -o: same, but somewhere else.
        let (output, dir) = mega_destination(Some("/data/mega/".to_string()), &cfg);
        assert_eq!(output, None);
        assert_eq!(dir, "/data/mega");

        // Concrete -o: the user's name wins.
        let (output, _) = mega_destination(Some("/data/movie.mkv".to_string()), &cfg);
        assert_eq!(output.as_deref(), Some("/data/movie.mkv"));
    }

    #[test]
    fn mega_workers_come_from_connections_then_config() {
        let cfg = config::Config::default();

        let defaults = mega_options(&cfg, &DownloadOpts::default());
        assert_eq!(defaults.workers, cfg.mega_workers);
        assert_eq!(defaults.verify_mac, cfg.mega_verify_mac);
        assert!(!defaults.overwrite);

        let opts = DownloadOpts {
            connections: Some(3),
            ..DownloadOpts::default()
        };
        assert_eq!(mega_options(&cfg, &opts).workers, 3);
    }
}
