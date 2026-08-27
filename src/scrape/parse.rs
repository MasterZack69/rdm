//! Link parsing.
//!
//! A state-machine `href` extractor and the classification of what it finds
//! into files and subdirectories. Everything here is pure, which is why the
//! parser's false-positive cases are cheap to pin down in tests.

use reqwest::Url;
use std::collections::HashSet;

use super::url_util::is_under_base;

pub(super) fn parse_links(html: &str, page_url: &Url, base: &Url) -> (Vec<String>, Vec<Url>) {
    let mut files: Vec<String> = Vec::new();
    let mut dirs: Vec<Url> = Vec::new();
    let mut seen_files: HashSet<String> = HashSet::new();
    let mut seen_dirs: HashSet<String> = HashSet::new();

    for raw_href in extract_hrefs(html) {
        // (13) Decode the most common HTML entities in href values.
        let href_owned = decode_html_entities(&raw_href);
        let href = href_owned.trim();

        if href.is_empty()
            || href == "../"
            || href == "./"
            || href == ".."
            || href == "."
            || href == "/"
            || href.starts_with('#')
            || href.starts_with('?')
        {
            continue;
        }

        let lower = href.to_ascii_lowercase();
        if lower.starts_with("javascript:")
            || lower.starts_with("mailto:")
            || lower.starts_with("data:")
            || lower.starts_with("file:")
            || lower.starts_with("ftp:")
        {
            continue;
        }

        // (16, 18) Use real URL resolution. Handles absolute, protocol-relative,
        // root-relative and path-relative hrefs correctly.
        let mut resolved = match page_url.join(href) {
            Ok(u) => u,
            Err(_) => continue,
        };

        resolved.set_fragment(None);

        if resolved.scheme() != "http" && resolved.scheme() != "https" {
            continue;
        }

        // (2) Proper containment check (origin + path prefix).
        if !is_under_base(&resolved, base) {
            continue;
        }

        if resolved.path().ends_with('/') {
            let key = resolved.as_str().to_owned();
            if seen_dirs.insert(key) {
                dirs.push(resolved);
            }
        } else {
            let s = resolved.as_str().to_owned();
            if seen_files.insert(s.clone()) {
                files.push(s);
            }
        }
    }

    files.sort();
    dirs.sort_by(|a, b| a.as_str().cmp(b.as_str()));

    (files, dirs)
}

// (10-14) Robust state-machine href extractor:
// - Skips <script>, <style>, and HTML comments
// - Requires an attribute boundary before `href` (rejects `data-href`)
// - Handles all HTML whitespace (space, tab, CR, LF, FF)
// - Supports double-quoted, single-quoted, and unquoted values
// - Doesn't read past EOF
fn extract_hrefs(html: &str) -> Vec<String> {
    let bytes = html.as_bytes();
    let len = bytes.len();
    let mut hrefs = Vec::new();
    let mut i = 0;

    let mut in_tag = false;
    let mut tag_start: usize = 0;
    let mut in_script = false;
    let mut in_style = false;
    let mut in_comment = false;

    while i < len {
        if in_comment {
            if i + 3 <= len && &bytes[i..i + 3] == b"-->" {
                in_comment = false;
                i += 3;
            } else {
                i += 1;
            }
            continue;
        }

        if !in_tag {
            // Detect comment start
            if i + 4 <= len && &bytes[i..i + 4] == b"<!--" {
                in_comment = true;
                i += 4;
                continue;
            }

            if bytes[i] == b'<' {
                // Inside <script>/<style>, only look for end tags.
                if in_script {
                    if i + 9 <= len && bytes[i..i + 9].eq_ignore_ascii_case(b"</script>") {
                        in_script = false;
                        i += 9;
                        continue;
                    }
                    i += 1;
                    continue;
                }

                if in_style {
                    if i + 8 <= len && bytes[i..i + 8].eq_ignore_ascii_case(b"</style>") {
                        in_style = false;
                        i += 8;
                        continue;
                    }
                    i += 1;
                    continue;
                }

                // Detect <script ...> / <style ...>
                if i + 7 < len
                    && bytes[i + 1..i + 7].eq_ignore_ascii_case(b"script")
                    && (bytes[i + 7] == b'>' || is_html_whitespace(bytes[i + 7]))
                {
                    in_script = true;
                }

                if i + 6 < len
                    && bytes[i + 1..i + 6].eq_ignore_ascii_case(b"style")
                    && (bytes[i + 6] == b'>' || is_html_whitespace(bytes[i + 6]))
                {
                    in_style = true;
                }

                in_tag = true;
                tag_start = i;
                i += 1;
                continue;
            }
            i += 1;
            continue;
        }

        // in_tag == true
        if in_script || in_style {
            // We're past the opening `<` but inside the script/style tag content.
            if bytes[i] == b'>' {
                in_tag = false;
            }
            i += 1;
            continue;
        }

        if bytes[i] == b'>' {
            in_tag = false;
            i += 1;
            continue;
        }

        // (12) Require an attribute boundary before `href`. The byte immediately
        // before must be whitespace, `<`, or `/` (for self-closing variations).
        let boundary_ok = i > tag_start
            && (is_html_whitespace(bytes[i - 1]) || bytes[i - 1] == b'<' || bytes[i - 1] == b'/');

        if boundary_ok && i + 4 <= len && bytes[i..i + 4].eq_ignore_ascii_case(b"href") {
            let mut j = i + 4;
            while j < len && is_html_whitespace(bytes[j]) {
                j += 1;
            }

            if j < len && bytes[j] == b'=' {
                j += 1;
                while j < len && is_html_whitespace(bytes[j]) {
                    j += 1;
                }

                if j >= len {
                    break;
                }

                let (val_bytes, end) = if bytes[j] == b'"' || bytes[j] == b'\'' {
                    let quote = bytes[j];
                    j += 1;
                    let start = j;
                    while j < len && bytes[j] != quote {
                        j += 1;
                    }
                    if j >= len {
                        break;
                    }
                    (&bytes[start..j], j + 1)
                } else {
                    // (10) Unquoted attribute value: read until whitespace or `>`.
                    let start = j;
                    while j < len && !is_html_whitespace(bytes[j]) && bytes[j] != b'>' {
                        j += 1;
                    }
                    (&bytes[start..j], j)
                };

                let s = match std::str::from_utf8(val_bytes) {
                    Ok(s) => s.to_owned(),
                    Err(_) => String::from_utf8_lossy(val_bytes).into_owned(),
                };

                hrefs.push(s);
                i = end;
                continue;
            }
        }
        i += 1;
    }

    hrefs
}

fn is_html_whitespace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

// (13) Minimal HTML entity decoder (handles the common cases that show up in
// hrefs: &amp; &lt; &gt; &quot; &apos; and numeric &#nn; / &#xNN;).
fn decode_html_entities(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'&' {
            let scan_end = (i + 12).min(bytes.len());
            if let Some(rel) = bytes[i + 1..scan_end].iter().position(|&b| b == b';') {
                let entity = &s[i + 1..i + 1 + rel];
                let replacement: Option<char> = match entity {
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    e if e.starts_with("#x") || e.starts_with("#X") => {
                        u32::from_str_radix(&e[2..], 16)
                            .ok()
                            .and_then(char::from_u32)
                    }
                    e if e.starts_with('#') => e[1..].parse::<u32>().ok().and_then(char::from_u32),
                    _ => None,
                };

                if let Some(ch) = replacement {
                    out.push(ch);
                    i = i + 1 + rel + 1;
                    continue;
                }
            }
        }
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn u(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn test_parse_files_and_dirs() {
        let html = r#"
            <a href="../">../</a>
            <a href="movie.mkv">movie.mkv</a>
            <a href="subdir/">subdir/</a>
            <a href="photo.png">photo.png</a>
        "#;
        let page = u("http://x.com/root/");
        let (files, dirs) = parse_links(html, &page, &page);

        assert_eq!(
            files,
            vec![
                "http://x.com/root/movie.mkv".to_owned(),
                "http://x.com/root/photo.png".to_owned(),
            ]
        );
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].as_str(), "http://x.com/root/subdir/");
    }

    #[test]
    fn test_parse_nested_dirs() {
        let html = r#"
            <a href="../">../</a>
            <a href="deep/">deep/</a>
            <a href="file.mp4">file.mp4</a>
        "#;
        let page = u("http://x.com/a/b/");
        let (files, dirs) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/a/b/file.mp4".to_owned()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/a/b/deep/");
    }

    #[test]
    fn test_skips_external() {
        let html = r#"
            <a href="http://evil.com/malware.exe">bad</a>
            <a href="good.zip">good</a>
        "#;
        let page = u("http://safe.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://safe.com/d/good.zip".to_owned()]);
    }

    #[test]
    fn test_absolute_path_under_base() {
        let html = r#"<a href="/files/sub/">sub</a><a href="/files/a.mkv">a</a>"#;
        let page = u("http://x.com/files/");
        let (files, dirs) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/files/a.mkv".to_owned()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/files/sub/");
    }

    #[test]
    fn test_absolute_path_outside_base_rejected() {
        let html = r#"<a href="/other/secret.zip">bad</a><a href="good.zip">good</a>"#;
        let page = u("http://x.com/files/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/files/good.zip".to_owned()]);
    }

    #[test]
    fn test_percent_encoded_names() {
        let html = r#"<a href="file%201.mp4">f</a><a href="sub%20dir/">s</a>"#;
        let page = u("http://x.com/d/");
        let (files, dirs) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/file%201.mp4".to_owned()]);
        assert_eq!(dirs[0].as_str(), "http://x.com/d/sub%20dir/");
    }

    #[test]
    fn test_single_quotes() {
        let html = "<a href='video.mp4'>video</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_owned()]);
    }

    #[test]
    fn test_unquoted_href() {
        // (10) Unquoted attribute values are valid HTML5.
        let html = "<a href=video.mp4>video</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_owned()]);
    }

    #[test]
    fn test_empty_html_returns_empty() {
        let page = u("http://x.com/");
        let (files, dirs) = parse_links("<html></html>", &page, &page);

        assert!(files.is_empty());
        assert!(dirs.is_empty());
    }

    #[test]
    fn test_path_traversal_in_href_rejected() {
        // (1, 2) Traversal hrefs are now resolved + scope-checked, then rejected.
        let html = r#"
            <a href="../../etc/passwd">sneaky</a>
            <a href="legit.mp4">legit</a>
        "#;
        let page = u("http://x.com/files/sub/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/files/sub/legit.mp4".to_owned()]);
    }

    #[test]
    fn test_encoded_traversal_in_href_rejected() {
        let html = r#"<a href="%2e%2e/%2e%2e/etc/passwd">x</a>"#;
        let page = u("http://x.com/files/sub/");
        let (files, _) = parse_links(html, &page, &page);

        assert!(files.is_empty());
    }

    #[test]
    fn test_data_href_attribute_not_matched() {
        // (12) `data-href` must not be parsed as `href`.
        let html = r#"<a data-href="evil.exe" href="good.txt">x</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/good.txt".to_owned()]);
    }

    #[test]
    fn test_href_inside_script_ignored() {
        let html = r#"<script>var x = '<a href="evil.exe">';</script> <a href="good.txt">good</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/good.txt".to_owned()]);
    }

    #[test]
    fn test_href_inside_comment_ignored() {
        let html = r#"<!-- <a href="evil.exe">x</a> --> <a href="good.txt">good</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/good.txt".to_owned()]);
    }

    #[test]
    fn test_protocol_relative_href() {
        // (16) Protocol-relative URLs resolve against the page's scheme.
        let html = r#"<a href="//x.com/files/a.mp4">a</a>"#;
        let page = u("http://x.com/files/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/files/a.mp4".to_owned()]);
    }

    #[test]
    fn test_html_entities_in_href() {
        let html = r#"<a href="a&amp;b.mp4">x</a>"#;
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        // url::Url::join re-encodes `&` as needed for paths.
        assert_eq!(files.len(), 1);
        assert!(files[0].ends_with("a&b.mp4"));
    }

    #[test]
    fn test_mixed_case_attr_and_whitespace() {
        let html = "<a HREF =\n\"video.mp4\">v</a>";
        let page = u("http://x.com/d/");
        let (files, _) = parse_links(html, &page, &page);

        assert_eq!(files, vec!["http://x.com/d/video.mp4".to_owned()]);
    }
}
