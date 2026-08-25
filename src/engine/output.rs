//! Reconciling the requested output path with what is already on disk.

use anyhow::Result;

use super::request::{ExistingPolicy, OutputDecision};
use super::url::extract_filename_from_url;

/// Decides what to do about an already-existing output file.
///
/// A resumable download (a `.part` file, or valid `.rdm` metadata) always wins
/// over the policy — there is nothing to ask about, we just continue.
pub async fn resolve_existing_output(
    path: &str,
    url: &str,
    identity: Option<&str>,
    policy: ExistingPolicy,
) -> Result<OutputDecision> {
    use std::io::{BufRead, IsTerminal, Write};

    if !std::path::Path::new(path).exists() {
        return Ok(OutputDecision::Use(path.to_owned()));
    }

    let part_path = format!("{}.part", path);
    if std::path::Path::new(&part_path).exists() {
        return Ok(OutputDecision::Use(path.to_owned()));
    }

    let meta_path = crate::resume::ResumeMetadata::meta_path(path);
    if let Ok(meta) = crate::resume::load(&meta_path).await {
        let chunks: Vec<crate::chunk::Chunk> = meta
            .chunks
            .iter()
            .map(|c| crate::chunk::Chunk {
                id: c.id,
                start: c.start,
                end: c.end,
            })
            .collect();
        if crate::resume::validate_against(&meta, url, identity, meta.file_size, &chunks) {
            return Ok(OutputDecision::Use(path.to_owned()));
        }
    }

    match policy {
        // Batch runs must never block on stdin.
        ExistingPolicy::Reuse => return Ok(OutputDecision::AlreadyPresent),
        ExistingPolicy::Overwrite => {
            let _ = std::fs::remove_file(path);
            let _ = std::fs::remove_file(&part_path);
            let _ = std::fs::remove_file(&meta_path);
            return Ok(OutputDecision::Use(path.to_owned()));
        }
        ExistingPolicy::Ask => {}
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

    eprintln!("  \u{26a0} File already exists: {}", path);
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
                return Ok(OutputDecision::Use(path.to_owned()));
            }
            "2" => loop {
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
                    trimmed.to_owned()
                } else {
                    parent.join(trimmed).to_string_lossy().to_string()
                };
                return Ok(OutputDecision::Use(new_path));
            },
            "3" => return Ok(OutputDecision::Cancelled),
            _ => eprintln!("  Invalid choice. Enter 1, 2, or 3."),
        }
    }
}

pub(super) fn resolve_output_path(url: &str, output: Option<&str>) -> String {
    if let Some(provided) = output {
        return provided.to_owned();
    }
    extract_filename_from_url(url).unwrap_or_else(|| "download.bin".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_explicit() {
        assert_eq!(
            resolve_output_path("https://example.com/f.zip", Some("out.zip")),
            "out.zip"
        );
    }

    #[test]
    fn test_resolve_from_url() {
        assert_eq!(
            resolve_output_path("https://example.com/data.tar.gz", None),
            "data.tar.gz"
        );
    }

    #[test]
    fn test_resolve_fallback() {
        assert_eq!(
            resolve_output_path("https://example.com/", None),
            "download.bin"
        );
    }

    #[tokio::test]
    async fn missing_file_is_always_used() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.bin").to_string_lossy().to_string();
        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/nope.bin",
                None,
                ExistingPolicy::Ask
            )
            .await
            .unwrap(),
            OutputDecision::Use(path),
        );
    }

    #[tokio::test]
    async fn reuse_policy_reports_existing_file_instead_of_prompting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("done.bin");
        std::fs::write(&path, b"payload").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/done.bin",
                None,
                ExistingPolicy::Reuse
            )
            .await
            .unwrap(),
            OutputDecision::AlreadyPresent,
        );
    }

    #[tokio::test]
    async fn overwrite_policy_clears_the_file_and_its_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stale.bin");
        std::fs::write(&path, b"old").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/stale.bin",
                None,
                ExistingPolicy::Overwrite
            )
            .await
            .unwrap(),
            OutputDecision::Use(path.clone()),
        );
        assert!(!std::path::Path::new(&path).exists());
    }

    #[tokio::test]
    async fn a_partial_download_resumes_regardless_of_policy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("half.bin");
        std::fs::write(&path, b"old").unwrap();
        std::fs::write(dir.path().join("half.bin.part"), b"partial").unwrap();
        let path = path.to_string_lossy().to_string();

        assert_eq!(
            resolve_existing_output(
                &path,
                "https://example.com/half.bin",
                None,
                ExistingPolicy::Reuse
            )
            .await
            .unwrap(),
            OutputDecision::Use(path),
        );
    }
}
