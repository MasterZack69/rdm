use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};

/// Downloads a file from `url` into `output_path` using streaming async I/O
/// with a safe temporary file strategy.
///
/// The data is first written to `<output_path>.part`. Only after the entire
/// download succeeds is the file atomically renamed to the final path.
/// If the download fails at any point, the `.part` file is cleaned up.
///
/// # Arguments
///
/// * `client`            — A reusable `reqwest::Client` (with timeouts configured).
/// * `url`               — The remote file URL.
/// * `output_path`       — Final local destination path.
/// * `progress_callback` — Optional callback invoked after every chunk write.
///                         Receives `(bytes_downloaded_so_far, total_size_option)`.
///
/// # Errors
///
/// Returns an `anyhow::Error` on network failures, HTTP errors, or file I/O problems.
/// The `.part` file is removed on failure to prevent partial/corrupt files.
pub async fn download_stream<F>(
    client: &Client,
    url: &str,
    output_path: &str,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(u64, Option<u64>),
{
    let temp_path = format!("{}.part", output_path);

    // Run the inner download; clean up the temp file on any error.
    match download_inner(client, url, &temp_path, progress_callback).await {
        Ok(bytes_downloaded) => {
            // ── Atomic rename: .part → final path ───────────────────
            fs::rename(&temp_path, output_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to rename temp file '{}' to '{}'",
                        temp_path, output_path
                    )
                })?;

            Ok(bytes_downloaded)
        }
        Err(e) => {
            // Best-effort cleanup — ignore errors from remove since
            // the file might not have been created yet.
            let _ = fs::remove_file(&temp_path).await;
            Err(e)
        }
    }
}

/// Inner download logic, separated so the caller can handle temp file
/// cleanup uniformly regardless of where the error occurs.
async fn download_inner<F>(
    client: &Client,
    url: &str,
    temp_path: &str,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(u64, Option<u64>),
{
    // ── Send GET request ────────────────────────────────────────────────
    let response = client
        .get(url)
        .send()
        .await
        .context("GET request failed — check the URL and your network connection")?;

    let status = response.status();
    if !status.is_success() {
        anyhow::bail!(
            "Server returned non-success status: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown")
        );
    }

    let total_size = response.content_length();

    // ── Open temporary output file ──────────────────────────────────────
    let file = fs::File::create(temp_path)
        .await
        .with_context(|| format!("Failed to create temp file: {}", temp_path))?;

    let mut writer = BufWriter::with_capacity(64 * 1024, file);

    // ── Stream response body to disk ────────────────────────────────────
    let mut stream = response.bytes_stream();
    let mut downloaded: u64 = 0;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.context("Error while reading response stream")?;

        writer
            .write_all(&chunk)
            .await
            .context("Error writing chunk to temp file")?;

        downloaded += chunk.len() as u64;

        if let Some(ref cb) = progress_callback {
            cb(downloaded, total_size);
        }
    }

    // ── Flush remaining buffered data ───────────────────────────────────
    writer
        .flush()
        .await
        .context("Error flushing temp file")?;

    // Explicitly shut down the writer to ensure all data hits disk
    writer
        .shutdown()
        .await
        .context("Error closing temp file")?;

    Ok(downloaded)
}
