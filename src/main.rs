// segv's mom

use rdm::args::{self, Cli, Commands, QueueCommands, SyncArgs};
use rdm::cli;
use rdm::config;
use rdm::queue;
use rdm::scrape;
use rdm::signal;
use rdm::sync;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;

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

fn run_download_command(
    url: String,
    output: Option<String>,
    connections: Option<usize>,
    cfg: &config::Config,
) -> Result<()> {
    let url = cli::normalize_download_url(&url);
    let connections = connections.unwrap_or(cfg.connections);
    let output_path = args::resolve_output(output, &url, &cfg.download_dir);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let cancel = CancellationToken::new();
            let sh = signal::spawn_signal_handler(cancel.clone());
            let result = cli::run_download(url, Some(output_path), connections, cancel, false).await;
            sh.abort();
            result
        })
}

fn run_sync_command(
    args: SyncArgs,
    global_output: Option<String>,
    global_connections: Option<usize>,
    allow_private: bool,
    cfg: &config::Config,
) -> Result<()> {
    let mut cfg = cfg.clone();
    let url = cli::normalize_download_url(&args.url);
    let connections = global_connections.unwrap_or(cfg.connections);
    let parallel = args.parallel.unwrap_or(cfg.queue_parallel);
    let ext_filter = args::parse_ext_filter(args.ext);

    if let Some(dir) = global_output {
        cfg.download_dir = dir;
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let cancel = CancellationToken::new();
            let sh = signal::spawn_signal_handler(cancel.clone());
            let result = sync::run(
                &cfg,
                &url,
                connections,
                parallel,
                args.delete,
                ext_filter,
                allow_private,
                cancel,
            )
            .await;
            sh.abort();
            result
        })
}

fn run_queue_add(
    url: String,
    output: Option<String>,
    connections: Option<usize>,
    allow_private: bool,
    cfg: &config::Config,
) -> Result<()> {
    let url = cli::normalize_download_url(&url);
    let resolved = output.map(|o| {
        let path = std::path::Path::new(&o);
        if path.is_absolute() {
            o
        } else {
            cfg.resolve_output_path(&o)
        }
    });

    let files = if looks_like_directory(&url) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(scrape::discover_files(&url, true, allow_private))
    } else {
        Ok(None)
    };

    match files {
        Ok(Some(urls)) => {
            let count = urls.len();
            queue::Queue::locked(|q| {
                for f in &urls {
                    q.add(f.url.clone(), Some(f.relative_path.clone()), connections);
                }
                Ok(())
            })?;

            eprintln!("  📁 Found {} file(s):", count);
            eprintln!();
            for f in &urls {
                eprintln!("     + {}", cli::percent_decode(&f.relative_path));
            }
            let q = queue::Queue::load_readonly();
            eprintln!();
            eprintln!("  {} item(s) pending.", q.pending_count());
            Ok(())
        }
        Ok(None) if looks_like_directory(&url) => {
            let id = queue::Queue::locked(|q| Ok(q.add(url.clone(), resolved, connections)))?;
            let q = queue::Queue::load_readonly();
            eprintln!("  ✅ Added #{}: {}", id, cli::percent_decode(&url));
            eprintln!("  {} item(s) pending.", q.pending_count());
            Ok(())