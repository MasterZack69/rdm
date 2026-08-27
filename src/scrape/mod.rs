//! Directory scraper.
//!
//! Discovers files served by a "directory listing" style HTTP endpoint
//! (Apache/nginx autoindex, S3 web index, etc.) by recursively parsing
//! anchor links. Hardened against:
//! - Path traversal (decoded *and* encoded)
//! - SSRF via private/loopback/link-local addresses, whether the URL names
//!   one outright or resolves to one
//! - Redirects that escape the base scope, checked before the hop is made
//! - Oversized response bodies
//! - HTML parser false positives (script/style/comments, attribute boundaries)
//! - Cross-directory duplicates
//! - Windows-specific filename quirks (drive letters, reserved names, trailing
//!   dots)
//!
//! Progress is reported through a single `ui::ScanSpinner` line rather than
//! one `Scanning ...` line per directory.
//!
//! ## Module layout
//!
//! - `limits` — the budgets a scan runs under.
//! - `scope` — which addresses may be contacted, name resolution included.
//! - `url_util` — base-scope containment and URL-derived naming.
//! - `path` — turning a URL-relative path into a safe on-disk path.
//! - `fetch` — GETs, hand-followed redirects, body caps, content types.
//! - `parse` — anchor extraction and link classification.
//!
//! ## Where a request is allowed to go
//!
//! Two rules, and the order of both matters more than their content.
//!
//! Checking the host used to mean checking it only when it was already a
//! literal IP, which is the one case an attacker never needs. A name is not a
//! literal IP, so a domain with an A record for 127.0.0.1, for RFC1918 space,
//! or for 169.254.169.254 was waved through. `ScopeGuard` resolves the name
//! and requires *every* address it answers with to be external.
//!
//! Redirects used to be followed by reqwest, with the escape noticed from
//! `response.url()` once the response was already in hand. That is too late:
//! the internal service has been contacted and has answered, and refusing to
//! parse the body does not un-send the request. Redirects are now followed by
//! hand and every hop is checked before it is issued.
//!
//! Public API preserved:
//! - `pub struct DiscoveredFile { pub url: String, pub relative_path: String }`
//! - `pub async fn discover_files(url: &str, wrap_in_folder: bool, allow_private: bool)
//! -> Result<Option<Vec<DiscoveredFile>>>`

mod fetch;
mod limits;
mod parse;
mod path;
mod scope;
mod url_util;

use anyhow::{Context, Result};
use reqwest::Url;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::ui;
use fetch::fetch_and_parse;
use limits::{CONCURRENCY, MAX_DEPTH, MAX_DIRS, MAX_FILES, REQUEST_TIMEOUT};
use path::sanitize_relative_path;
use scope::{parse_and_validate_url, parse_host_as_ip, ScopeGuard};
use url_util::{derive_folder_name, directory_label, ensure_trailing_slash};

// ---------- Public types ----------

pub struct DiscoveredFile {
    pub url: String,
    pub relative_path: String,
}

// ---------- Entry point ----------

pub async fn discover_files(
    url: &str,
    wrap_in_folder: bool,
    allow_private: bool,
) -> Result<Option<Vec<DiscoveredFile>>> {
    let base_url = parse_and_validate_url(url, allow_private).context("Invalid base URL")?;
    let base_url = ensure_trailing_slash(base_url);
    let guard = ScopeGuard::new(allow_private);

    // Resolve once, up front, and refuse the whole scan if the name points
    // anywhere internal.
    let base_addrs = guard
        .resolve(&base_url)
        .await
        .context("Invalid base URL")?;

    let mut builder = reqwest::Client::builder()
        .user_agent("rdm")
        .timeout(REQUEST_TIMEOUT)
        // Followed by hand in `fetch_following_redirects` so that a hop is
        // checked before it is taken rather than after it has been answered.
        .redirect(reqwest::redirect::Policy::none());

    // Pin the name to the addresses that were just checked. Without this the
    // connection performs its own lookup, and a record with a short TTL can
    // answer publicly for the check and privately a moment later for the
    // connect. Every request this scan makes goes to the base host —
    // `is_under_base` refuses a redirect anywhere else before it is followed
    // — so pinning the one host covers the entire crawl.
    if let Some(host) = base_url.host_str()
        && parse_host_as_ip(host).is_none()
    {
        builder = builder.resolve_to_addrs(host, &base_addrs);
    }

    let client = builder.build().context("Failed to build HTTP client")?;
    let folder_name = derive_folder_name(&base_url);
    let base_str = base_url.as_str().to_owned();

    let mut files: Vec<DiscoveredFile> = Vec::new();
    let mut seen_files: HashSet<String> = HashSet::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut current_level: Vec<(Url, u32)> = vec![(base_url.clone(), 0)];

    let sem = Arc::new(Semaphore::new(CONCURRENCY));
    // One live line for the whole scan instead of a line per directory.
    let spinner = ui::ScanSpinner::new();
    let mut hard_stopped = false;

    while !current_level.is_empty() && !hard_stopped {
        let mut tasks = tokio::task::JoinSet::new();

        for (dir_url, depth) in current_level.drain(..) {
            if depth > MAX_DEPTH {
                continue;
            }

            let key = dir_url.as_str().to_owned();
            if !visited.insert(key) {
                continue;
            }

            if visited.len() > MAX_DIRS {
                spinner.note(&format!(
                    " ⚠ Directory limit reached ({}), stopping scan",
                    MAX_DIRS
                ));
                hard_stopped = true;
                break;
            }

            spinner.dir(&directory_label(&base_url, &dir_url));

            let client_c = client.clone();
            let sem_c = sem.clone();
            let base_c = base_url.clone();
            let guard_c = guard;

            tasks.spawn(async move {
                let _permit = match sem_c.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => {
                        return (
                            Err(anyhow::anyhow!("semaphore closed")) as Result<_>,
                            depth,
                            dir_url,
                        );
                    }
                };
                let res = fetch_and_parse(&client_c, &dir_url, &base_c, &guard_c).await;
                (res, depth, dir_url)
            });
        }

        let mut next_level: Vec<(Url, u32)> = Vec::new();
        let mut next_seen: HashSet<String> = HashSet::new();

        while let Some(joined) = tasks.join_next().await {
            let (result, depth, dir_url) = match joined {
                Ok(v) => v,
                Err(e) => {
                    spinner.note(&format!(" ⚠ task join error: {}", e));
                    continue;
                }
            };

            let (found_files, found_dirs) = match result {
                Ok(Some(r)) => r,
                Ok(None) => continue,
                Err(e) => {
                    spinner.note(&format!(" ⚠ failed to scan {}: {:#}", dir_url, e));
                    continue;
                }
            };

            for file_url in found_files {
                if !seen_files.insert(file_url.clone()) {
                    continue;
                }

                let raw_relative = match file_url.strip_prefix(&base_str) {
                    Some(s) => s.to_owned(),
                    None => continue,
                };

                let safe = match sanitize_relative_path(&raw_relative) {
                    Some(s) => s,
                    None => continue,
                };

                let final_rel = if wrap_in_folder {
                    format!("{}/{}", folder_name, safe)
                } else {
                    safe
                };

                files.push(DiscoveredFile {
                    url: file_url,
                    relative_path: final_rel,
                });

                spinner.add_files(1);

                if files.len() >= MAX_FILES {
                    spinner.note(&format!(
                        " ⚠ File limit reached ({}), stopping scan",
                        MAX_FILES
                    ));
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    hard_stopped = true;
                    break;
                }
            }

            if hard_stopped {
                break;
            }

            for sub in found_dirs {
                let k = sub.as_str().to_owned();
                if !visited.contains(&k) && next_seen.insert(k) {
                    next_level.push((sub, depth + 1));
                }
            }
        }

        current_level = next_level;
    }

    spinner.finish();

    if files.is_empty() {
        return Ok(None);
    }

    files.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(Some(files))
}
