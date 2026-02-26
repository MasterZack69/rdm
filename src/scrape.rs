use anyhow::{Context, Result};
use std::collections::{HashSet, VecDeque};

use crate::cli;

pub struct DiscoveredFile {
    pub url: String,
    pub relative_path: String,
}

pub async fn discover_files(url: &str) -> Result<Option<Vec<DiscoveredFile>>> {
    const MAX_DEPTH: u32 = 10;
    const MAX_DIRS: usize = 500;
    const MAX_FILES: usize = 10_000;

    let client = reqwest::Client::builder()
        .user_agent("rdm")
        .timeout(std::time::Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("Failed to build HTTP client")?;

    let base_url = ensure_trailing_slash(url);

    let folder_name = base_url.trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("download");
    let folder_name = cli::percent_decode(folder_name);

    let mut files: Vec<DiscoveredFile> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, u32)> = VecDeque::new();

    queue.push_back((base_url.clone(), 0));

    while let Some((dir_url, depth)) = queue.pop_front() {
        if depth > MAX_DEPTH || !visited.insert(dir_url.clone()) {
            continue;
        }

        eprintln!("  📂 Scanning {}",
            cli::percent_decode(dir_url.strip_prefix(&base_url).unwrap_or(&dir_url))
                .trim_end_matches('/')
                .split('/')
                .last()
                .map(|s| if s.is_empty() { &dir_url } else { s })
                .unwrap_or(&dir_url),
        );

        let (found_files, found_dirs) = match fetch_directory(&client, &dir_url).await {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(_) => continue,
        };

        for file_url in found_files {
            let relative = file_url
                .strip_prefix(&base_url)
                .unwrap_or(&file_url)
                .to_string();

            if !is_safe_relative_path(&relative) {
                continue;
            }

            let relative = format!("{}/{}", folder_name, relative);

            files.push(DiscoveredFile { url: file_url, relative_path: relative });

            if files.len() >= MAX_FILES {
                eprintln!("   ⚠ File limit reached ({}), stopping scan", MAX_FILES);
                return Ok(Some(files));
            }
        }

        if visited.len() > MAX_DIRS {
            eprintln!("   ⚠ Directory limit reached ({}), stopping scan", MAX_DIRS);
            break;
        }

        for sub_dir in found_dirs {
            if !visited.contains(&sub_dir) {
                queue.push_back((sub_dir, depth + 1));
            }
        }
    }

    if files.is_empty() {
        return Ok(None);
    }

    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(Some(files))
}

fn is_safe_relative_path(path: &str) -> bool {
    let decoded = crate::cli::percent_decode(path);

    if decoded.is_empty() {
        return false;
    }

    if decoded.starts_with('/') {
        return false;
    }

    if decoded.split('/').any(|c| c == "..") {
        return false;
    }

    true
}

async fn fetch_directory(
    client: &reqwest::Client,
    url: &str,
) -> Result<Option<(Vec<String>, Vec<String>)>> {
    let response = client.get(url).send().await
        .context("Failed to fetch URL")?;

    let content_type = response.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !content_type.contains("text/html") {
        return Ok(None);
    }

    let final_url = ensure_trailing_slash(&response.url().to_string());

    let body = response.text().await
        .context("Failed to read response body")?;

    Ok(Some(parse_links(&body, &final_url)))
}

fn ensure_trailing_slash(url: &str) -> String {
    let base = url.split('?').next().unwrap_or(url);
    if base.ends_with('/') {
        base.to_string()
    } else {
        format!("{}/", base)
    }
}

fn extract_origin(url: &str) -> Option<&str> {
    let after_scheme = url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let scheme_len = url.len() - after_scheme.len();
    match after_scheme.find('/') {
        Some(pos) => Some(&url[..scheme_len + pos]),
        None => Some(url),
    }
}

fn extract_hrefs(html: &str) -> Vec<String> {
    let mut hrefs = Vec::new();
    let bytes = html.as_bytes();
    let len = bytes.len();
    let mut pos = 0;

    while pos + 5 < len {
        if !(bytes[pos] == b'h' || bytes[pos] == b'H') {
            pos += 1;
            continue;
        }
        if !bytes[pos..pos + 5].eq_ignore_ascii_case(b"href=") {
            pos += 1;
            continue;
        }
        pos += 5;

        while pos < len && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= len { break; }

        let quote = bytes[pos];
        if quote != b'"' && quote != b'\'' {
            continue;
        }
        pos += 1;

        let start = pos;
        while pos < len && bytes[pos] != quote {
            pos += 1;
        }
        if pos >= len { break; }

        if let Ok(href) = std::str::from_utf8(&bytes[start..pos]) {
            hrefs.push(href.to_string());
        }

        pos += 1;
    }

    hrefs
}

fn parse_links(html: &str, base_url: &str) -> (Vec<String>, Vec<String>) {
    let mut files = Vec::new();
    let mut dirs = Vec::new();
    let origin = extract_origin(base_url);

    for href in extract_hrefs(html) {
        if href.is_empty()
            || href == "../"
            || href == "./"
            || href == "/"
            || href.starts_with('#')
            || href.starts_with('?')
            || href.starts_with("mailto:")
            || href.starts_with("javascript:")
        {
            continue;
        }

        let absolute = if href.starts_with("http://") || href.starts_with("https://") {
            if href.starts_with(base_url) {
                href
            } else {
                continue;
            }
        } else if href.starts_with('/') {
            match origin {
                Some(o) => {
                    let absolute = format!("{}{}", o, href);
                    if !absolute.starts_with(base_url) {
                        continue;
                    }
                    absolute
                }
                None => continue,
            }
        } else {
            format!("{}{}", base_url, href)
        };

        if absolute.ends_with('/') {
            dirs.push(absolute);
        } else {
            files.push(absolute);
        }
    }

    files.sort();
    files.dedup();
    dirs.sort();
    dirs.dedup();

    (files, dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trailing_slash() {
        assert_eq!(ensure_trailing_slash("http://x.com/dir"), "http://x.com/dir/");
        assert_eq!(ensure_trailing_slash("http://x.com/dir/"), "http://x.com/dir/");
        assert_eq!(ensure_trailing_slash("http://x.com/dir?q=1"), "http://x.com/dir/");
    }

    #[test]
    fn test_extract_origin() {
        assert_eq!(extract_origin("http://x.com/path/"), Some("http://x.com"));
        assert_eq!(extract_origin("https://x.com:8080/a"), Some("https://x.com:8080"));
        assert_eq!(extract_origin("http://x.com"), Some("http://x.com"));
    }

    #[test]
    fn test_parse_files_and_dirs() {
        let html = r#"
        <a href="../">../</a>
        <a href="movie.mkv">movie.mkv</a>
        <a href="subdir/">subdir/</a>
        <a href="photo.png">photo.png</a>
        "#;
        let (files, dirs) = parse_links(html, "http://x.com/root/");
        assert_eq!(files, vec![
            "http://x.com/root/movie.mkv",
            "http://x.com/root/photo.png",
        ]);
        assert_eq!(dirs, vec![
            "http://x.com/root/subdir/",
        ]);
    }

    #[test]
    fn test_parse_nested_dirs() {
        let html = r#"
        <a href="../">../</a>
        <a href="deep/">deep/</a>
        <a href="file.mp4">file.mp4</a>
        "#;
        let (files, dirs) = parse_links(html, "http://x.com/a/b/");
        assert_eq!(files, vec!["http://x.com/a/b/file.mp4"]);
        assert_eq!(dirs, vec!["http://x.com/a/b/deep/"]);
    }

    #[test]
    fn test_skips_parent_dir() {
        let html = r#"<a href="../">Parent</a><a href="f.zip">f.zip</a>"#;
        let (files, dirs) = parse_links(html, "http://x.com/d/");
        assert_eq!(files, vec!["http://x.com/d/f.zip"]);
        assert!(dirs.is_empty());
    }

    #[test]
    fn test_skips_external() {
        let html = r#"
        <a href="http://evil.com/malware.exe">bad</a>
        <a href="good.zip">good</a>
        "#;
        let (files, _) = parse_links(html, "http://safe.com/d/");
        assert_eq!(files, vec!["http://safe.com/d/good.zip"]);
    }

    #[test]
    fn test_absolute_path_under_base() {
        let html = r#"<a href="/files/sub/">sub</a><a href="/files/a.mkv">a</a>"#;
        let (files, dirs) = parse_links(html, "http://x.com/files/");
        assert_eq!(files, vec!["http://x.com/files/a.mkv"]);
        assert_eq!(dirs, vec!["http://x.com/files/sub/"]);
    }

    #[test]
    fn test_absolute_path_outside_base_rejected() {
        let html = r#"<a href="/other/secret.zip">bad</a><a href="good.zip">good</a>"#;
        let (files, _) = parse_links(html, "http://x.com/files/");
        assert_eq!(files, vec!["http://x.com/files/good.zip"]);
    }

    #[test]
    fn test_percent_encoded_names() {
        let html = r#"<a href="file%201.mp4">file 1</a><a href="sub%20dir/">sub dir</a>"#;
        let (files, dirs) = parse_links(html, "http://x.com/d/");
        assert_eq!(files, vec!["http://x.com/d/file%201.mp4"]);
        assert_eq!(dirs, vec!["http://x.com/d/sub%20dir/"]);
    }

    #[test]
    fn test_single_quotes() {
        let html = "<a href='video.mp4'>video</a>";
        let (files, _) = parse_links(html, "http://x.com/d/");
        assert_eq!(files, vec!["http://x.com/d/video.mp4"]);
    }

    #[test]
    fn test_empty_html_returns_empty() {
        let (files, dirs) = parse_links("<html></html>", "http://x.com/");
        assert!(files.is_empty());
        assert!(dirs.is_empty());
    }

    #[test]
    fn test_safe_relative_path() {
        assert!(is_safe_relative_path("file.mkv"));
        assert!(is_safe_relative_path("sub/file.mkv"));
        assert!(is_safe_relative_path("a/b/c/d.mp4"));
        assert!(!is_safe_relative_path(""));
        assert!(!is_safe_relative_path("/etc/passwd"));
        assert!(!is_safe_relative_path("../../.ssh/keys"));
        assert!(!is_safe_relative_path("sub/../../etc/passwd"));
        assert!(!is_safe_relative_path(".."));
    }

    #[test]
    fn test_path_traversal_in_href() {
        let html = r#"
        <a href="../../etc/passwd">sneaky</a>
        <a href="legit.mp4">legit</a>
        "#;
        let (files, _) = parse_links(html, "http://x.com/files/sub/");
        assert_eq!(files.len(), 2);
    }
}
