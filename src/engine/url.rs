//! Download-URL normalisation and filename extraction.

use super::name::safe_filename;

/// The filename a URL implies, if any.
///
/// The last path segment is chosen by whoever published the URL, and decoding
/// it can reveal separators that were encoded specifically to survive the split
/// below. So the decoded result is reduced to a single component rather than
/// trusted as one.
pub fn extract_filename_from_url(url: &str) -> Option<String> {
    let normalized = normalize_download_url(url);
    let without_fragment = normalized.split('#').next()?;
    let path = without_fragment.split('?').next()?;
    let segment = path.rsplit('/').next()?;
    safe_filename(&percent_decode(segment))
}

pub fn normalize_download_url(url: &str) -> String {
    let Ok(mut parsed) = reqwest::Url::parse(url) else {
        return url.to_owned();
    };

    let Some(fragment) = parsed.fragment() else {
        return url.to_owned();
    };

    let route = fragment.split('?').next().unwrap_or(fragment);
    let route = route.trim_start_matches('/').to_owned();
    let mut route_segments = route.split('/');
    if route_segments.next() != Some("download") {
        return url.to_owned();
    }

    let rest: Vec<String> = route_segments
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    let Some(last) = rest.last() else {
        return url.to_owned();
    };
    if !last.contains('.') {
        return url.to_owned();
    }

    parsed.set_fragment(None);
    parsed.set_query(None);

    let mut base_dir = parsed.path().to_owned();
    if !base_dir.ends_with('/') {
        if let Some(pos) = base_dir.rfind('/') {
            base_dir.truncate(pos + 1);
        } else {
            base_dir.clear();
            base_dir.push('/');
        }
    }

    let new_path = format!("{}download/{}", base_dir, rest.join("/"));
    parsed.set_path(&new_path);
    parsed.to_string()
}

pub fn percent_decode(input: &str) -> String {
    let mut bytes = Vec::with_capacity(input.len());
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next();
            let lo = chars.next();
            if let (Some(h), Some(l)) = (hi, lo) {
                if let Ok(s) = std::str::from_utf8(&[h, l])
                    && let Ok(decoded) = u8::from_str_radix(s, 16)
                {
                    bytes.push(decoded);
                    continue;
                }
                // Failed decode \u{2014} push all three bytes back
                bytes.push(b'%');
                bytes.push(h);
                bytes.push(l);
            } else {
                // Incomplete sequence \u{2014} push what we have
                bytes.push(b'%');
                if let Some(h) = hi {
                    bytes.push(h);
                }
            }
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| input.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_filename_simple() {
        assert_eq!(
            extract_filename_from_url("https://example.com/path/file.zip"),
            Some("file.zip".into())
        );
    }

    #[test]
    fn test_extract_filename_query() {
        assert_eq!(
            extract_filename_from_url("https://example.com/file.tar.gz?t=1"),
            Some("file.tar.gz".into())
        );
    }

    #[test]
    fn test_extract_filename_hash_download_route() {
        assert_eq!(
            extract_filename_from_url(
                "https://mobdisc.com/dwbfc3e38e/download.html?lang=en#/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
            ),
            Some("8189-DOOM-3-v1-1-0-22-cache1.zip".into())
        );
    }

    #[test]
    fn test_normalize_hash_download_route() {
        assert_eq!(
            normalize_download_url(
                "https://mobdisc.com/dwbfc3e38e/download.html?lang=en#/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
            ),
            "https://mobdisc.com/dwbfc3e38e/download/8189-DOOM-3-v1-1-0-22-cache1.zip"
        );
    }

    #[test]
    fn test_normalize_ignores_regular_fragment() {
        let url = "https://example.com/file.zip#section";
        assert_eq!(normalize_download_url(url), url);
    }

    #[test]
    fn test_extract_filename_percent() {
        assert_eq!(
            extract_filename_from_url("https://example.com/my%20file.zip"),
            Some("my file.zip".into())
        );
    }

    #[test]
    fn test_extract_filename_trailing() {
        assert_eq!(extract_filename_from_url("https://example.com/"), None);
    }

    /// The separators are encoded, so they survive the `rsplit('/')` above and
    /// only become separators once decoded. Reducing to a component afterwards
    /// is what closes that gap.
    #[test]
    fn a_url_basename_cannot_climb_out_of_the_download_directory() {
        assert_eq!(
            extract_filename_from_url("https://example.com/d/..%2f..%2f.ssh%2fauthorized_keys"),
            Some("authorized_keys".into())
        );
        assert_eq!(
            extract_filename_from_url("https://example.com/d/%2fetc%2fpasswd"),
            Some("passwd".into())
        );
        // Nothing usable left is None, so the caller falls back to its own
        // default rather than to a name the server chose.
        assert_eq!(
            extract_filename_from_url("https://example.com/d/%2e%2e"),
            None
        );
    }

    #[test]
    fn a_url_basename_cannot_carry_terminal_escapes() {
        assert_eq!(
            extract_filename_from_url("https://example.com/%1b%5b2Kdone.mp4"),
            Some("[2Kdone.mp4".into())
        );
    }

    #[test]
    fn test_percent_decode_valid() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
    }

    #[test]
    fn test_percent_decode_invalid_hex() {
        // %GH is not valid hex \u{2014} all three bytes should be preserved
        assert_eq!(percent_decode("test%GHvalue"), "test%GHvalue");
    }

    #[test]
    fn test_percent_decode_truncated_at_end() {
        // trailing %2 with no second hex char
        assert_eq!(percent_decode("test%2"), "test%2");
    }

    #[test]
    fn test_percent_decode_bare_percent() {
        assert_eq!(percent_decode("test%"), "test%");
    }
}
