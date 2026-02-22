use anyhow::{Context, Result};
use reqwest::{header, Client, StatusCode};

/// Metadata obtained from inspecting the remote file URL.
#[derive(Debug, Clone)]
pub struct FileInfo {
    /// The file size in bytes, if the server reported it.
    /// `None` means Content-Length was absent — chunk splitting cannot be used.
    pub size: Option<u64>,

    /// Whether the server supports HTTP range requests (resumable downloads).
    pub supports_range: bool,
}

impl FileInfo {
    /// Returns `true` if we have enough information to split into multiple chunks.
    pub fn can_chunk(&self) -> bool {
        self.size.is_some() && self.supports_range
    }
}

/// Inspect a remote URL by sending a HEAD request, then optionally probing
/// range support with a targeted GET request.
///
/// The `Client` is accepted as a parameter so it can be reused across calls,
/// benefiting from connection pooling and shared configuration.
pub async fn inspect_url(client: &Client, url: &str) -> Result<FileInfo> {
    // ── Step 1a: HEAD request ───────────────────────────────────────────
    let head_resp = client
        .head(url)
        .send()
        .await
        .context("HEAD request failed — check the URL and your network connection")?;

    let status = head_resp.status();
    if !status.is_success() {
        anyhow::bail!(
            "Server returned non-success status: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Unknown")
        );
    }

    let headers = head_resp.headers();

    // ── Extract Content-Length (optional) ────────────────────────────────
    let size = extract_content_length(headers);

    // ── Detect range support from HEAD ──────────────────────────────────
    let head_says_ranges = headers
        .get(header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("bytes"))
        .unwrap_or(false);

    // ── Step 1b: Range probing fallback ─────────────────────────────────
    // Many servers omit Accept-Ranges in HEAD responses but still support
    // byte ranges. We send a small GET with `Range: bytes=0-0` to confirm.
    let supports_range = if head_says_ranges {
        true
    } else {
        probe_range_support(client, url).await?
    };

    Ok(FileInfo {
        size,
        supports_range,
    })
}

/// Try to parse `Content-Length` from response headers.
/// Returns `None` if the header is missing, empty, or unparseable.
fn extract_content_length(headers: &header::HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&len| len > 0)
}

/// Send a `GET` request with `Range: bytes=0-0` to verify that the server
/// actually supports byte-range requests.
///
/// Detection criteria:
///   1. HTTP 206 Partial Content status code
///   2. Presence of a `Content-Range` response header
///
/// If both conditions are met, the server reliably supports ranges.
async fn probe_range_support(client: &Client, url: &str) -> Result<bool> {
    let probe_resp = client
        .get(url)
        .header(header::RANGE, "bytes=0-0")
        .send()
        .await
        .context("Range probe GET request failed")?;

    let status = probe_resp.status();
    let has_content_range = probe_resp.headers().get(header::CONTENT_RANGE).is_some();

    // Some servers return 200 OK and ignore the Range header entirely.
    // We only trust an explicit 206 + Content-Range combination.
    let supported = status == StatusCode::PARTIAL_CONTENT && has_content_range;

    Ok(supported)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_extract_content_length_present() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("12345"));
        assert_eq!(extract_content_length(&headers), Some(12345));
    }

    #[test]
    fn test_extract_content_length_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_extract_content_length_zero() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_extract_content_length_invalid() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("abc"));
        assert_eq!(extract_content_length(&headers), None);
    }

    #[test]
    fn test_file_info_can_chunk() {
        let info = FileInfo { size: Some(1024), supports_range: true };
        assert!(info.can_chunk());

        let info = FileInfo { size: None, supports_range: true };
        assert!(!info.can_chunk());

        let info = FileInfo { size: Some(1024), supports_range: false };
        assert!(!info.can_chunk());
    }
}
