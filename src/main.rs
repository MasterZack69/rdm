mod chunk;
mod cli;
mod config;
mod inspect;
mod parallel;
mod priority_queue;
mod queue;
mod queue_state;
mod range_download;
mod resume;
mod retry;
mod signal;

use anyhow::Result;
use config::Config;
use queue_state::Priority;
use std::env;
use tokio_util::sync::CancellationToken;

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let cfg = Config::load();

    match args.get(1).map(|s| s.as_str()) {
        Some("download") => {
            let url = args.get(2)
                .ok_or_else(|| anyhow::anyhow!("Missing URL\nUsage: rdm download <URL> [output] [-c N]"))?
                .clone();

            let (output, connections) = parse_download_args(&args[3..]);
            let connections = connections.unwrap_or(cfg.connections);

            let output = match output {
                Some(o) => Some(o),
                None => {
                    let filename = extract_auto_filename(&url);
                    Some(cfg.resolve_output_path(&filename))
                }
            };

            tokio::runtime::Builder::new_multi_thread().enable_all().build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());
                    let result = cli::run_download(url, output, connections, cancel).await;
                    sh.abort();
                    result
                })
        }

        Some("queue") => {
            let entries = parse_queue_args(&args[2..]);
            let connections = parse_flag_usize(&args[2..], &["-c", "--connections"])
                .unwrap_or(cfg.connections);
            let max_jobs = parse_flag_usize(&args[2..], &["-j", "--jobs"])
                .unwrap_or(cfg.max_jobs);
            let priority = parse_priority(&args[2..])
                .unwrap_or(cfg.default_priority);

            tokio::runtime::Builder::new_multi_thread().enable_all().build()?
                .block_on(run_queue(entries, connections, max_jobs, priority, &cfg))
        }

        Some("config") => {
            let path = config::config_path();
            eprintln!("RDM — Configuration");
            eprintln!();
            eprintln!("  Config file : {}", path.display());
            eprintln!("  Exists      : {}", if path.exists() { "yes" } else { "no" });
            eprintln!();
            eprintln!("  connections       : {}", cfg.connections);
            eprintln!("  max_jobs          : {}", cfg.max_jobs);
            eprintln!("  download_dir      : {}",
                cfg.download_dir.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "(not set)".into()));
            eprintln!("  default_priority  : {}", cfg.default_priority);
            Ok(())
        }

        Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            let url = url.to_string();
            let filename = extract_auto_filename(&url);
            let output = Some(cfg.resolve_output_path(&filename));

            tokio::runtime::Builder::new_multi_thread().enable_all().build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());
                    let result = cli::run_download(url, output, cfg.connections, cancel).await;
                    sh.abort();
                    result
                })
        }

        _ => {
            eprintln!("RDM — Rust Download Manager");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  rdm <URL>                              Quick download");
            eprintln!("  rdm download <URL> [output] [-c N]     Download with options");
            eprintln!("  rdm queue <URLs...> [options]           Download queue");
            eprintln!("  rdm queue                              Resume previous queue");
            eprintln!("  rdm config                             Show configuration");
            eprintln!();
            eprintln!("Options:");
            eprintln!("  -c, --connections N   Connections per file (default: {})", cfg.connections);
            eprintln!("  -o, --output FILE     Output filename");
            eprintln!("  -j, --jobs M          Max concurrent downloads (default: {})", cfg.max_jobs);
            eprintln!("  -p, --priority LEVEL  high, normal, low (default: {})", cfg.default_priority);
            eprintln!();
            eprintln!("Config: {}", config::config_path().display());
            std::process::exit(1);
        }
    }
}

async fn run_queue(
    urls: Vec<String>,
    connections: usize,
    max_concurrent: usize,
    priority: Priority,
    cfg: &Config,
) -> Result<()> {
    let mut q = queue::DownloadQueue::restore(max_concurrent).await;
    let has_new = !urls.is_empty();
    let had_restored = q.pending_count() + q.paused_count() > 0;

    for url in &urls {
    let filename = extract_auto_filename(url);
    let output = cfg.resolve_output_path(&filename);
    q.add_with_priority(url.clone(), output, connections, priority);
}

    if !has_new && !had_restored {
        eprintln!("RDM — Queue Mode");
        eprintln!("  No jobs in queue.");
        return Ok(());
    }

    eprintln!("RDM — Queue Mode");
    eprintln!("  Pending     : {}", q.pending_count());
    eprintln!("  Paused      : {}", q.paused_count());
    eprintln!("  Concurrent  : {}", max_concurrent);
    eprintln!("  Connections : {} per file", connections);
    if has_new { eprintln!("  Priority    : {}", priority); }
    if let Some(ref dir) = cfg.download_dir { eprintln!("  Download dir: {}", dir.display()); }
    eprintln!();

    let cancel = CancellationToken::new();
    let signal_handle = signal::spawn_signal_handler(cancel.clone());
    let results = q.process(cancel).await;
    signal_handle.abort();
    queue::print_summary(&results);

    if results.iter().any(|r| matches!(r.outcome, queue::JobOutcome::Failed(_))) {
        anyhow::bail!("Some downloads failed");
    }
    Ok(())
}

fn extract_auto_filename(url: &str) -> String {
    url.split('?').next()
        .and_then(|p| p.rsplit('/').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("download.bin")
        .to_string()
}

fn parse_download_args(args: &[String]) -> (Option<String>, Option<usize>) {
    let mut output: Option<String> = None;
    let mut connections: Option<usize> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--connections" => {
                if let Some(v) = args.get(i + 1) { connections = v.parse().ok(); i += 2; }
                else { i += 1; }
            }
            "-o" | "--output" => {
                if let Some(v) = args.get(i + 1) { output = Some(v.clone()); i += 2; }
                else { i += 1; }
            }
            other if !other.starts_with('-') && output.is_none() => { output = Some(other.to_string()); i += 1; }
            _ => { i += 1; }
        }
    }
    (output, connections)
}

fn parse_queue_args(args: &[String]) -> Vec<String> {
    let mut urls = Vec::new();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--connections" | "-j" | "--jobs" | "-p" | "--priority" => { i += 2; }
            s if s.starts_with("http://") || s.starts_with("https://") => { urls.push(s.to_string()); i += 1; }
            _ => { i += 1; }
        }
    }
    urls
}

fn parse_flag_usize(args: &[String], flags: &[&str]) -> Option<usize> {
    let mut i = 0;
    while i < args.len() {
        if flags.contains(&args[i].as_str()) { return args.get(i + 1).and_then(|v| v.parse().ok()); }
        i += 1;
    }
    None
}

fn parse_priority(args: &[String]) -> Option<Priority> {
    let mut i = 0;
    while i < args.len() {
        if args[i] == "-p" || args[i] == "--priority" {
            return args.get(i + 1).and_then(|v| match v.to_lowercase().as_str() {
                "high" | "h" => Some(Priority::High),
                "normal" | "n" => Some(Priority::Normal),
                "low" | "l" => Some(Priority::Low),
                _ => None,
            });
        }
        i += 1;
    }
    None
}
