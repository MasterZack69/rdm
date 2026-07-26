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
        }
        _ => {
            let id = queue::Queue::locked(|q| Ok(q.add(url.clone(), resolved, connections)))?;
            let q = queue::Queue::load_readonly();
            eprintln!("  ✅ Added #{}: {}", id, cli::percent_decode(&url));
            eprintln!("  {} item(s) pending.", q.pending_count());
            Ok(())
        }
    }
}

fn run_queue_start(cfg: &config::Config, parallel: Option<usize>) -> Result<()> {
    let parallel = parallel.unwrap_or(cfg.queue_parallel);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let cancel = CancellationToken::new();
            let sh = signal::spawn_signal_handler(cancel.clone());
            let result = queue::start(cfg, cancel, parallel).await;
            sh.abort();
            result
        })
}

fn run_queue_retry(target: Option<String>) -> Result<()> {
    let n = match target.as_deref() {
        None | Some("all") => queue::Queue::locked(|q| Ok(q.retry_failed() + q.retry_skipped()))?,
        Some("failed") | Some("f") => queue::Queue::locked(|q| Ok(q.retry_failed()))?,
        Some("skipped") | Some("s") => queue::Queue::locked(|q| Ok(q.retry_skipped()))?,
        Some(id_str) => {
            let id: u64 = id_str
                .parse()
                .map_err(|_| anyhow::anyhow!("Usage: rdm queue retry <ID|failed|skipped>"))?;
            let ok = queue::Queue::locked(|q| Ok(q.retry_item(id)))?;
            if ok {
                eprintln!("  ✅ #{} requeued.", id);
            } else {
                eprintln!("  #{} is not failed or skipped.", id);
            }
            return Ok(());
        }
    };
    eprintln!("  Requeued {} item(s).", n);
    Ok(())
}

fn run_queue_clear(filter: Option<String>) -> Result<()> {
    let n = match filter.as_deref() {
        Some("pending") | Some("p") => queue::Queue::locked(|q| Ok(q.clear_pending()))?,
        Some("done") | Some("finished") | Some("d") => queue::Queue::locked(|q| Ok(q.clear_finished()))?,
        _ => queue::Queue::locked(|q| Ok(q.clear_all()))?,
    };
    eprintln!("  Cleared {} item(s).", n);
    Ok(())
}

fn run_queue_command(
    args: QueueCommands,
    global_output: Option<String>,
    global_connections: Option<usize>,
    allow_private: bool,
    cfg: &config::Config,
) -> Result<()> {
    match args {
        QueueCommands::Add(download) => {
            run_queue_add(download.url, global_output, global_connections, allow_private, cfg)
        }
        QueueCommands::List => {
            queue::Queue::load_readonly().print_list();
            Ok(())
        }
        QueueCommands::Start { parallel } => run_queue_start(cfg, parallel),
        QueueCommands::Stop => {
            queue::send_signal("stop")?;
            eprintln!("  ⏹  Stop signal sent. Queue will stop after current download.");
            Ok(())
        }
        QueueCommands::Skip => {
            queue::send_signal("skip")?;
            eprintln!("  ⏭  Skip signal sent.");
            Ok(())
        }
        QueueCommands::Remove { id } => {
            let removed = queue::Queue::locked(|q| Ok(q.remove(id)))?;
            if removed {
                eprintln!("  Removed #{}", id);
            } else {
                eprintln!("  No item with ID #{}", id);
            }
            Ok(())
        }
        QueueCommands::Retry { target } => run_queue_retry(target),
        QueueCommands::Clear { filter } => run_queue_clear(filter),
    }
}

fn run_quick_download(url: &str, cli: &Cli, cfg: &config::Config) -> Result<()> {
    let url = cli::normalize_download_url(url);
    let connections = cli.connections.unwrap_or(cfg.connections);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let cancel = CancellationToken::new();
            let sh = signal::spawn_signal_handler(cancel.clone());

            if cli.output.is_none() && looks_like_directory(&url) {
                match scrape::discover_files(&url, true, cli.allow_private).await {
                    Ok(Some(files)) => {
                        eprintln!("  📁 Found {} file(s):", files.len());
                        eprintln!();
                        for f in &files {
                            eprintln!("     + {}", cli::percent_decode(&f.relative_path));
                        }
                        eprintln!();

                        queue::Queue::locked(|q| {
                            for f in &files {
                                q.add(f.url.clone(), Some(f.relative_path.clone()), Some(connections));
                            }
                            Ok(())
                        })?;

                        let result = queue::start(cfg, cancel, cfg.queue_parallel).await;
                        sh.abort();
                        return result;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }

            let output_path = args::resolve_output(cli.output.clone(), &url, &cfg.download_dir);
            let result = cli::run_download(url, Some(output_path), connections, cancel.clone(), false).await;
            sh.abort();
            result
        })
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = config::Config::load();

    match cli.command {
        Some(Commands::Download(args)) => {
            run_download_command(args.url, cli.output, cli.connections, &cfg)
        }
        Some(Commands::Sync(args)) => {
            run_sync_command(args, cli.output, cli.connections, cli.allow_private, &cfg)
        }
        Some(Commands::Config) => {
            cfg.print();
            Ok(())
        }
        Some(Commands::Queue(args)) => {
            run_queue_command(args.command, cli.output, cli.connections, cli.allow_private, &cfg)
        }
        None => {
            if let Some(ref url) = cli.url {
                run_quick_download(url, &cli, &cfg)
            } else {
                // clap's arg_required_else_help already handles the empty case,
                // but we keep this branch for completeness.
                Cli::parse_from(["rdm", "--help"]);
                Ok(())
            }
        }
    }
}
