use anyhow::{Context, Result};
use std::io::{self, BufRead, IsTerminal, Write};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::chunk::Chunk;
use crate::inspect;
use crate::parallel;
use crate::resume::ResumeMetadata;
use crate::retry::RetryConfig;

pub async fn run_download(
    url: String,
    output: Option<String>,
    connections: usize,
    cancel: CancellationToken,
) -> Result<()> {
    let output_path = resolve_output_path(&url, output.as_deref());
    let output_path = match resolve_existing_output(&output_path, &url).await? {
        Some(p) => p,
        None => {
            eprintln!("  Download cancelled.");
            return Ok(());
        }
    };
    let connections = connections.max(1);

    let client = reqwest::Client::builder()
        .user_agent("rdm/0.1.3")
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(30))
        .build()
        .context("Failed to build HTTP client")?;

    eprintln!("  Inspecting: {}", url);
    let info = inspect::inspect_url(&client, &url).await?;

    let file_size = info.size.context("Server did not report file size. Cannot use parallel download.")?;
    if file_size == 0 { anyhow::bail!("Cannot download empty file (Content-Length: 0)"); }

    eprintln!("  File size : {}", format_bytes(file_size));
    eprintln!("  Range     : {}", if info.supports_range { "supported" } else { "not supported" });
    eprintln!("  Output    : {}", output_path);

    let chunks = if info.supports_range && connections > 1 {
        plan_chunks_with_count(file_size, connections as u32)
    } else {
        vec![Chunk { id: 1, start: 0, end: file_size - 1 }]
    };

    eprintln!("  Chunks    : {}", chunks.len());
    eprintln!();

    let start_time = Instant::now();
    let speed_samples: std::sync::Mutex<std::collections::VecDeque<(u64, u64)>> =
        std::sync::Mutex::new(std::collections::VecDeque::new());

    let progress_callback = move |downloaded: u64, total: u64| {
        let elapsed_ms = start_time.elapsed().as_millis() as u64;
        let mut samples = speed_samples.lock().unwrap();
        samples.push_back((elapsed_ms, downloaded));

        // Keep 3 second window
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

    let retry_config = RetryConfig::default();

    let download_result = parallel::download_parallel(
        &client, &url, &output_path, file_size, &chunks,
        &retry_config, Some(progress_callback), cancel,
    ).await;

    eprint!("\r\x1b[2K");

    match download_result {
                Ok(bytes) => {
            let secs = start_time.elapsed().as_secs_f64();
            let avg = if secs > 0.1 { (bytes as f64 / secs) as u64 } else { 0 };
            eprintln!("  ✅ Download complete: {}", output_path);
            eprintln!("  {} in {:.1}s ({})",
                format_bytes(bytes), secs, format_speed(avg),
            );
            Ok(())
        }

        Err(e) => {
            eprintln!("  ❌ Download failed.");
            eprintln!("  Progress saved. Resume by running the same command again.");
            Err(e)
        }
    }
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

    // Resume metadata exists → validate using resume module APIs
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


/// Prompt user for a new filename. Returns None if input was empty.
fn prompt_rename(original_path: &str) -> Result<Option<String>> {
    use std::path::Path;

    eprint!("  Enter new filename: ");
    io::stderr().flush().ok();

    let mut input = String::new();
    io::stdin().lock().read_line(&mut input)
        .context("Failed to read input")?;

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    // If user gave just a filename (no directory), preserve original directory
    let new_path = if Path::new(trimmed).parent().map(|p| p.as_os_str().is_empty()).unwrap_or(true) {
        match Path::new(original_path).parent() {
            Some(dir) if !dir.as_os_str().is_empty() => {
                dir.join(trimmed).to_string_lossy().to_string()
            }
            _ => trimmed.to_string(),
        }
    } else {
        trimmed.to_string()
    };

    eprintln!("  Output changed to: {}", new_path);
    Ok(Some(new_path))
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
