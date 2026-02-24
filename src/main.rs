mod chunk;
mod cli;
mod config;
mod inspect;
mod parallel;
mod range_download;
mod resume;
mod retry;
mod signal;

use anyhow::Result;
use config::Config;
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

        Some("config") => {
            let path = config::config_path();
            eprintln!("RDM — Configuration");
            eprintln!();
            eprintln!("  Config file : {}", path.display());
            eprintln!("  Exists      : {}", if path.exists() { "yes" } else { "no" });
            eprintln!();
            eprintln!("  connections       : {}", cfg.connections);
            eprintln!("  download_dir      : {}",
                cfg.download_dir.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "(not set)".into()));
            Ok(())
        }

        Some(url) if url.starts_with("http://") || url.starts_with("https://") =>
        {
            let url = url.to_string();
            let (output, connections) = parse_download_args(&args[2..]);
            let connections = connections.unwrap_or(cfg.connections);

            let output = {
                let filename = match output {
    Some(o) => o,
    None => extract_auto_filename(&url),
};
                Some(cfg.resolve_output_path(&filename))
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


        _ => {
            eprintln!("RDM — Rust Download Manager");
            eprintln!();
            eprintln!("Usage:");
            eprintln!("  rdm <URL>                              Quick download");
            eprintln!("  rdm download <URL> [output] [-c N]     Download with options");
            eprintln!("  rdm config                             Show configuration");
            eprintln!();
            eprintln!("Options:");
            eprintln!("  -c, --connections N   Connections per file (default: {})", cfg.connections);
            eprintln!("  -o, --output FILE     Output filename");
            eprintln!();
            eprintln!("Config: {}", config::config_path().display());
            std::process::exit(1);
        }
    }
}

fn extract_auto_filename(url: &str) -> String {
    let raw = url.split('?').next()
        .and_then(|p| p.rsplit('/').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("download.bin");
    cli::percent_decode(raw)
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
