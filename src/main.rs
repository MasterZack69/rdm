mod chunk;
mod cli;
mod config;
mod inspect;
mod parallel;
mod range_download;
mod resume;
mod retry;
mod scrape;
mod signal;
mod queue;

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

                Some("queue") | Some("q") => {
            match args.get(2).map(|s| s.as_str()) {

                                Some("add") | Some("a") => {
                    let url = args.get(3)
                        .ok_or_else(|| anyhow::anyhow!("Usage: rdm queue add <URL> [-o name] [-c N]"))?
                        .clone();
                    let (output, connections) = parse_download_args(&args[4..]);

                    let files = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?
                        .block_on(scrape::discover_files(&url));

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
                        _ => {
                            let id = queue::Queue::locked(|q| {
                                Ok(q.add(url.clone(), output, connections))
                            })?;
                            let q = queue::Queue::load_readonly();
                            eprintln!("  ✅ Added #{}: {}", id, cli::percent_decode(&url));
                            eprintln!("  {} item(s) pending.", q.pending_count());
                            Ok(())
                        }
                    }
                }

                Some("list") | Some("ls") | Some("l") => {
                    queue::Queue::load_readonly().print_list();
                    Ok(())
                }

                Some("start") | Some("run") | Some("s") => {
                    tokio::runtime::Builder::new_multi_thread().enable_all().build()?
                        .block_on(async {
                            let cancel = CancellationToken::new();
                            let sh = signal::spawn_signal_handler(cancel.clone());
                            let result = queue::start(&cfg, cancel).await;
                            sh.abort();
                            result
                        })
                }

                Some("stop") => {
                    queue::send_signal("stop")?;
                    eprintln!("  ⏹  Stop signal sent. Queue will stop after current download.");
                    Ok(())
                }

                Some("skip") | Some("next") | Some("n") => {
                    queue::send_signal("skip")?;
                    eprintln!("  ⏭  Skip signal sent.");
                    Ok(())
                }

                Some("remove") | Some("rm") => {
                    let id: u64 = args.get(3)
                        .ok_or_else(|| anyhow::anyhow!("Usage: rdm queue remove <ID>"))?
                        .parse()
                        .map_err(|_| anyhow::anyhow!("Invalid ID — must be a number"))?;
                    let removed = queue::Queue::locked(|q| Ok(q.remove(id)))?;
                    if removed {
                        eprintln!("  Removed #{}", id);
                    } else {
                        eprintln!("  No item with ID #{}", id);
                    }
                    Ok(())
                }

                Some("retry") | Some("r") => {
                    match args.get(3).map(|s| s.as_str()) {
                        Some("failed") | Some("f") => {
                            let n = queue::Queue::locked(|q| Ok(q.retry_failed()))?;
                            eprintln!("  Requeued {} failed item(s).", n);
                            Ok(())
                        }
                        Some("skipped") | Some("s") => {
                            let n = queue::Queue::locked(|q| Ok(q.retry_skipped()))?;
                            eprintln!("  Requeued {} skipped item(s).", n);
                            Ok(())
                        }
                        Some(id_str) => {
                            let id: u64 = id_str.parse()
                                .map_err(|_| anyhow::anyhow!("Usage: rdm queue retry <ID|failed|skipped>"))?;
                            let ok = queue::Queue::locked(|q| Ok(q.retry_item(id)))?;
                            if ok {
                                eprintln!("  ✅ #{} requeued.", id);
                            } else {
                                eprintln!("  #{} is not failed or skipped.", id);
                            }
                            Ok(())
                        }
                        None => {
                            // Retry all failed + skipped
                            let n = queue::Queue::locked(|q| {
                                Ok(q.retry_failed() + q.retry_skipped())
                            })?;
                            eprintln!("  Requeued {} item(s).", n);
                            Ok(())
                        }
                    }
                }

                                Some("clear") | Some("c") => {
                    match args.get(3).map(|s| s.as_str()) {
                        Some("pending") | Some("p") => {
                            let n = queue::Queue::locked(|q| Ok(q.clear_pending()))?;
                            eprintln!("  Cleared {} pending item(s).", n);
                            Ok(())
                        }
                        Some("done") | Some("finished") | Some("d") => {
                            let n = queue::Queue::locked(|q| Ok(q.clear_finished()))?;
                            eprintln!("  Cleared {} finished item(s).", n);
                            Ok(())
                        }
                        _ => {
                            let n = queue::Queue::locked(|q| Ok(q.clear_all()))?;
                            eprintln!("  Cleared {} item(s). Queue is empty.", n);
                            Ok(())
                        }
                    }
                }

                _ => {
                    eprintln!("RDM — Queue");
                    eprintln!();
                    eprintln!("Usage:");
                    eprintln!("  rdm queue add <URL> [-o name] [-c N]   Add download");
                    eprintln!("  rdm queue list                         Show queue");
                    eprintln!("  rdm queue start                        Start processing");
                    eprintln!("  rdm queue stop                         Stop after current");
                    eprintln!("  rdm queue skip                         Skip current download");
                    eprintln!("  rdm queue remove <ID>                  Remove item");
                    eprintln!("  rdm queue retry [ID|failed|skipped]    Requeue items");
                    eprintln!("  rdm queue clear [pending|done]                 Clear pending or finished");
                    eprintln!();
                    eprintln!("Shortcuts: q, a, ls, s, n, rm, r, c");
                    Ok(())
                }
            }
        }

                Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
            let url = url.to_string();
            let (output, connections) = parse_download_args(&args[2..]);
            let connections = connections.unwrap_or(cfg.connections);

            tokio::runtime::Builder::new_multi_thread().enable_all().build()?
                .block_on(async {
                    let cancel = CancellationToken::new();
                    let sh = signal::spawn_signal_handler(cancel.clone());

                                        // Check if URL is a directory listing
                    if output.is_none() {
                        if let Ok(Some(files)) = scrape::discover_files(&url).await {
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

                            let result = queue::start(&cfg, cancel).await;
                            sh.abort();
                            return result;
                        }
                    }

                    let output_filename = output.unwrap_or_else(|| {
                        let raw = url.split('?').next()
                            .and_then(|p| p.rsplit('/').next())
                            .filter(|s| !s.is_empty())
                            .unwrap_or("download.bin");
                        cli::percent_decode(raw)
                    });
                    let output_path = cfg.resolve_output_path(&output_filename);

                    let result = cli::run_download(
                        url, Some(output_path), connections, cancel.clone()
                    ).await;
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
            eprintln!("  rdm queue <command>                     Manage download queue");
            eprintln!("  rdm config                             Show configuration");
            eprintln!();
            eprintln!("Queue commands:");
            eprintln!("  rdm queue add <URL> [-o name] [-c N]   Add to queue");
            eprintln!("  rdm queue list                         Show queue");
            eprintln!("  rdm queue start                        Start processing");
            eprintln!("  rdm queue stop / skip                  Live control");
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
