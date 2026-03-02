use anyhow::{Context, Result};
use futures_util::StreamExt;
use std::io::{self, BufRead, Write};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::inspect;
use crate::parallel;
use crate::resume::ResumeMetadata;
use crate::retry::RetryConfig;

static SHARED_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn shared_client() -> Result<&'static reqwest::Client> {
    if let Some(c) = SHARED_CLIENT.get() {
        return Ok(c);
    }
    let client = reqwest::Client::builder()
        .user_agent("rdm")
        .connect_timeout(Duration::from_secs(10))
        .build()
        .context("Failed to build HTTP client")?;
    Ok(SHARED_CLIENT.get_or_init(|| client))
}

static SHARED_CONFIG: OnceLock<crate::config::Config> = OnceLock::new();

fn shared_config() -> &'static crate::config::Config {
    SHARED_CONFIG.get_or_init(crate::config::Config::load)
}


pub async fn run_download(
    url: String,
    output: Option<String>,
    connections: usize,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<()> {
    let output_path = resolve_output_path(&url, output.as_deref());
    let output_path = match resolve_existing_output(&output_path, &url).await? {
        Some(p) => p,
        None => {
            if !quiet {
                eprintln!("  Download cancelled.");
            }
            return Ok(());
        }
    };
    let connections = connections.max(1);

    let client = shared_client()?;

    if !quiet {
        eprintln!("  Inspecting: {}", url);
    }

    let info = inspect::inspect_url(client, &url).await?;

    // Use server-suggested filename when URL has no extension
    let output_path = if let Some(ref name) = info.suggested_filename {
        let path = std::path::Path::new(&output_path);
        if path.extension().is_none() {
            let dir = path.parent().unwrap_or(std::path::Path::new("."));
            dir.join(name).to_string_lossy().to_string()
        } else {
            output_path
        }
    } else {
        output_path
    };

    // ── Fix #1: Unknown file size → streaming fallback ──
    let file_size = match info.size {
        Some(0) => anyhow::bail!("Cannot download empty file (Content-Length: 0)"),
        Some(s) => s,
        None => {
            if !quiet {
                eprintln!("  File size : unknown (streaming)");
                eprintln!("  Output    : {}", output_path);
                eprintln!();
            }
            let start_time = Instant::now();
            let result = download_streaming(client, &url, &output_path, cancel, quiet).await;
            if !quiet { eprint!("\r\x1b[2K"); }
            return match result {
                Ok(bytes) => {
                    if !quiet {
                        let secs = start_time.elapsed().as_secs_f64();
                        let avg = if secs > 0.1 { (bytes as f64 / secs) as u64 } else { 0 };
                        eprintln!("  ✅ Download complete: {}", output_path);
                        eprintln!("  {} in {:.1}s ({})", format_bytes(bytes), secs, format_speed(avg));
                    }
                    Ok(())
                }
                Err(e) => {
                    if !quiet { eprintln!("  ❌ Download failed."); }
                    Err(e)
                }
            };
        }
    };

    // Fix #5: lower single-connection threshold from 32 MiB → 4 MiB
    let connections = if file_size < 4 * 1024 * 1024 { 1 } else { connections };

    if !quiet {
        eprintln!("  File size : {}", format_bytes(file_size));
        eprintln!("  Range     : {}", if info.supports_range { "supported" } else { "not supported" });
        eprintln!("  Output    : {}", output_path);
    }

    let chunks = if info.supports_range && connections > 1 {
        plan_chunks_with_count(file_size, connections as u32)
    } else {
        vec![Chunk { id: 1, start: 0, end: file_size - 1 }]
    };

    if !info.supports_range {
        let meta_path = crate::resume::ResumeMetadata::meta_path(&output_path);
        let part_path = format!("{}.part", &output_path);
        let _ = std::fs::remove_file(&meta_path);
        let _ = std::fs::remove_file(&part_path);
    }

    if !quiet {
        eprintln!("  Chunks    : {}", chunks.len());
        eprintln!();
    }

    let start_time = Instant::now();
    let last_print = std::sync::Mutex::new(Instant::now() - Duration::from_secs(1));
    let speed_samples: std::sync::Mutex<std::collections::VecDeque<(u64, u64)>> =
        std::sync::Mutex::new(std::collections::VecDeque::new());

    let progress_callback = move |downloaded: u64, total: u64| {
        if quiet { return; }

        let now = Instant::now();
        let is_complete = downloaded >= total;

        {
            let mut lp = last_print.lock().unwrap();
            if !is_complete && now.duration_since(*lp) < Duration::from_millis(100) {
                return;
            }
            *lp = now;
        }

        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        let mut samples = speed_samples.lock().unwrap();
        samples.push_back((elapsed_ms, downloaded));

        while samples.len() > 1 && elapsed_ms - samples.front().unwrap().0 > 3000 {
            samples.pop_front();
        }

        let speed_bps = if samples.len() >= 2 {
            let oldest = samples.front().unwrap();
            let dt = (elapsed_ms - oldest.0) as f64 / 1000.0;
            let db = downloaded.saturating_sub(oldest.1) as f64;
            if dt > 0.1 { (db / dt) as u64 } else { 0 }
        } else {
            0
        };

        drop(samples);
        print_progress_bar(downloaded, total, speed_bps);
    };

    let retry_config = RetryConfig {
    max_retries: shared_config().max_retries,
    ..RetryConfig::default()
    };

    let download_result = parallel::download_parallel(
        client, &url, &output_path, file_size, &chunks,
        &retry_config, Some(progress_callback), cancel,
    ).await;

    if !quiet {
        eprint!("\r\x1b[2K");
    }

    match download_result {
        Ok(bytes) => {
            if !quiet {
                let secs = start_time.elapsed().as_secs_f64();
                let avg = if secs > 0.1 { (bytes as f64 / secs) as u64 } else { 0 };
                eprintln!("  ✅ Download complete: {}", output_path);
                eprintln!("  {} in {:.1}s ({})",
                    format_bytes(bytes), secs, format_speed(avg),
                );
            }
            Ok(())
        }

        Err(e) => {
            if !quiet {
                eprintln!("  ❌ Download failed.");
                eprintln!("  Progress saved. Resume by running the same command again.");
            }
            Err(e)
        }
    }
}

async fn download_streaming(
    client: &reqwest::Client,
    url: &str,
    output_path: &str,
    cancel: CancellationToken,
    quiet: bool,
) -> Result<u64> {
    let temp_path = format!("{}.part", output_path);

    // Resume: check existing .part file size
    let existing_bytes = tokio::fs::metadata(&temp_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    let mut req = client.get(url); 

    let resp = req.send().await.context("GET request failed")?;
    let status = resp.status();

    let (resume_offset, append) = if existing_bytes > 0 && status == reqwest::StatusCode::PARTIAL_CONTENT {
        if !quiet {
            eprintln!("  Resuming from {}", format_bytes_compact(existing_bytes));
        }
        (existing_bytes, true)
    } else if status.is_success() {
        if existing_bytes > 0 && !quiet {
            eprintln!("  Server ignored range request, restarting from zero");
        }
        (0, false)
    } else {
        anyhow::bail!(
            "Server returned {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown")
        );
    };

    let file = if append {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&temp_path)
            .await
            .context("Failed to open .part for append")?
    } else {
        tokio::fs::File::create(&temp_path)
            .await
            .context("Failed to create .part file")?
    };

    let mut writer = tokio::io::BufWriter::with_capacity(512 * 1024, file);
    let mut stream = resp.bytes_stream();
    let mut downloaded: u64 = resume_offset;
    let mut bytes_since_flush: u64 = 0;
    let start_time = Instant::now();
    let mut last_print = Instant::now() - Duration::from_secs(1);

    loop {
        let chunk = tokio::select! {
            c = stream.next() => c,
            _ = cancel.cancelled() => {
                writer.flush().await.ok();
                anyhow::bail!("Download cancelled at {} bytes", downloaded);
            }
        };

        match chunk {
            Some(Ok(data)) => {
                let len = data.len() as u64;
                writer.write_all(&data).await.context("Write failed")?;
                downloaded += len;
                bytes_since_flush += len;

                if bytes_since_flush >= 4 * 1024 * 1024 {
                    writer.flush().await?;
                    bytes_since_flush = 0;
                }

                if !quiet {
                    let now = Instant::now();
                    if now.duration_since(last_print) >= Duration::from_millis(100) {
                        last_print = now;
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let new_bytes = downloaded - resume_offset;
                        let speed = if elapsed > 0.1 { (new_bytes as f64 / elapsed) as u64 } else { 0 };
                        eprint!("\r\x1b[2K  {} | {}", format_bytes_compact(downloaded), format_speed(speed));
                    }
                }
            }
            Some(Err(e)) => {
                writer.flush().await.ok();
                return Err(e).context(format!("Stream error at byte {}", downloaded));
            }
            None => break,
        }
    }

    writer.flush().await?;
    drop(writer);

    tokio::fs::rename(&temp_path, output_path)
        .await
        .with_context(|| format!("Failed to rename '{}' to '{}'", temp_path, output_path))?;

    Ok(downloaded)
}

pub async fn resolve_existing_output(path: &str, url: &str) -> Result<Option<String>> {
    use std::io::{BufRead, IsTerminal, Write};

    if !std::path::Path::new(path).exists() {
        return Ok(Some(path.to_string()));
    }

    let part_path = format!("{}.part", path);
    if std::path::Path::new(&part_path).exists() {
        return Ok(Some(path.to_string()));
    }

    let meta_path = crate::resume::ResumeMetadata::meta_path(path);
    if let Ok(meta) = crate::resume::load(&meta_path).await {
        let chunks: Vec<crate::chunk::Chunk> = meta.chunks.iter().map(|c| {
            crate::chunk::Chunk { id: c.id, start: c.start, end: c.end }
        }).collect();
        if crate::resume::validate_against(&meta, url, meta.file_size, &chunks) {
            return Ok(Some(path.to_string()));
        }
    }

    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "File already exists: {}\n  Use -o to specify a different output path.",
            path
        );
    }

    let parent = std::path::Path::new(path)
        .parent()
        .unwrap_or(std::path::Path::new(""));

    eprintln!("  ⚠ File already exists: {}", path);
    eprintln!();
    eprintln!("  1) Overwrite");
    eprintln!("  2) Rename");
    eprintln!("  3) Cancel");

    loop {
        eprint!("  Choice [1/2/3]: ");
        std::io::stderr().flush()?;

        let mut input = String::new();
        std::io::stdin().lock().read_line(&mut input)?;

        match input.trim() {
            "1" => {
                let _ = std::fs::remove_file(path);
                let _ = std::fs::remove_file(&part_path);
                let _ = std::fs::remove_file(&meta_path);
                return Ok(Some(path.to_string()));
            }
            "2" => {
                loop {
                    eprint!("  New filename: ");
                    std::io::stderr().flush()?;
                    let mut name = String::new();
                    std::io::stdin().lock().read_line(&mut name)?;
                    let trimmed = name.trim();
                    if trimmed.is_empty() {
                        eprintln!("  Filename cannot be empty.");
                        continue;
                    }
                    let new_path = if parent.as_os_str().is_empty() {
                        trimmed.to_string()
                    } else {
                        parent.join(trimmed).to_string_lossy().to_string()
                    };
                    return Ok(Some(new_path));
                }
            }
            "3" => return Ok(None),
            _ => eprintln!("  Invalid choice. Enter 1, 2, or 3."),
        }
    }
}

fn resolve_output_path(url: &str, output: Option<&str>) -> String {
    if let Some(provided) = output { return provided.to_string(); }
    extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_string())
}

fn extract_filename_from_url(url: &str) -> Option<String> {
    let path = url.split('?').next()?;
    let segment = path.rsplit('/').next()?;
    let decoded = percent_decode(segment);
    let trimmed = decoded.trim();
    if trimmed.is_empty() || trimmed == "/" { return None; }
    Some(trimmed.to_string())
}

pub fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(h), Some(l)) = (hi, lo) {
                if let Ok(s) = std::str::from_utf8(&[h, l]) {
                    if let Ok(decoded) = u8::from_str_radix(s, 16) {
                        bytes.push(decoded);
                        continue;
                    }
                }
            }
            bytes.push(b'%');
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_string())
}

fn plan_chunks_with_count(file_size: u64, count: u32) -> Vec<Chunk> {
    let count = count.max(1);
    let chunk_size = file_size / count as u64;
    let remainder = file_size % count as u64;
    let mut chunks = Vec::with_capacity(count as usize);
    let mut offset: u64 = 0;
    for i in 0..count {
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let size = chunk_size + extra;
        let start = offset;
        let end = start + size - 1;
        chunks.push(Chunk { id: i + 1, start, end });
        offset = end + 1;
    }
    chunks
}

// ── Progress Display ───────────────────────────────────────────────

fn print_progress_bar(downloaded: u64, total: u64, speed_bps: u64) {
    if total == 0 { return; }
    let pct = (downloaded as f64 / total as f64 * 100.0).min(100.0);
    let remaining = total.saturating_sub(downloaded);
    let speed = format_speed(speed_bps);
    let eta = format_eta(speed_bps, remaining);
    let bar_width = 25;
    let filled = (pct / 100.0 * bar_width as f64) as usize;
    let empty = bar_width - filled;
    eprint!("\r\x1b[2K  {:>5.1}% [{}{}] {} / {} | {} | {}",
        pct, "█".repeat(filled), "░".repeat(empty),
        format_bytes_compact(downloaded), format_bytes_compact(total), speed, eta,
    );
}

fn format_speed(bytes_per_sec: u64) -> String {
    if bytes_per_sec == 0 { return "-- MB/s".to_string(); }
    format!("{}/s", format_bytes_compact(bytes_per_sec))
}

fn format_eta(speed_bps: u64, remaining: u64) -> String {
    if speed_bps == 0 {
        return "ETA --:--".to_string();
    }
    let eta_secs = remaining / speed_bps;
    if eta_secs >= 3600 {
        format!("ETA {}h {:02}m", eta_secs / 3600, (eta_secs % 3600) / 60)
    } else if eta_secs >= 60 {
        format!("ETA {}m {:02}s", eta_secs / 60, eta_secs % 60)
    } else {
        format!("ETA {}s", eta_secs)
    }
}

fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024; const MIB: u64 = KIB * 1024; const GIB: u64 = MIB * 1024;
    if bytes >= GIB { format!("{:.2} GiB ({} bytes)", bytes as f64 / GIB as f64, bytes) }
    else if bytes >= MIB { format!("{:.2} MiB ({} bytes)", bytes as f64 / MIB as f64, bytes) }
    else if bytes >= KIB { format!("{:.2} KiB ({} bytes)", bytes as f64 / KIB as f64, bytes) }
    else { format!("{} bytes", bytes) }
}

fn format_bytes_compact(bytes: u64) -> String {
    const KIB: f64 = 1024.0; const MIB: f64 = KIB * 1024.0; const GIB: f64 = MIB * 1024.0;
    let b = bytes as f64;
    if b >= GIB { format!("{:.2} GiB", b / GIB) }
    else if b >= MIB { format!("{:.1} MiB", b / MIB) }
    else if b >= KIB { format!("{:.1} KiB", b / KIB) }
    else { format!("{} B", bytes) }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_extract_filename_simple() { assert_eq!(extract_filename_from_url("https://example.com/path/file.zip"), Some("file.zip".into())); }
    #[test] fn test_extract_filename_query() { assert_eq!(extract_filename_from_url("https://example.com/file.tar.gz?t=1"), Some("file.tar.gz".into())); }
    #[test] fn test_extract_filename_percent() { assert_eq!(extract_filename_from_url("https://example.com/my%20file.zip"), Some("my file.zip".into())); }
    #[test] fn test_extract_filename_trailing() { assert_eq!(extract_filename_from_url("https://example.com/"), None); }
    #[test] fn test_resolve_explicit() { assert_eq!(resolve_output_path("https://example.com/f.zip", Some("out.zip")), "out.zip"); }
    #[test] fn test_resolve_from_url() { assert_eq!(resolve_output_path("https://example.com/data.tar.gz", None), "data.tar.gz"); }
    #[test] fn test_resolve_fallback() { assert_eq!(resolve_output_path("https://example.com/", None), "download.bin"); }

    #[test]
    fn test_plan_chunks_even() {
        let chunks = plan_chunks_with_count(1000, 4);
        assert_eq!(chunks.len(), 4);
        let total: u64 = chunks.iter().map(|c| c.end - c.start + 1).sum();
        assert_eq!(total, 1000);
    }

    #[test]
    fn test_plan_chunks_remainder() {
        let chunks = plan_chunks_with_count(1003, 4);
        let total: u64 = chunks.iter().map(|c| c.end - c.start + 1).sum();
        assert_eq!(total, 1003);
        for i in 1..chunks.len() { assert_eq!(chunks[i].start, chunks[i-1].end + 1); }
    }
}
