use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

use crate::cli;
use crate::config::Config;
use crate::queue;
use crate::scrape;

pub async fn run(
    cfg: &Config,
    url: &str,
    connections: usize,
    parallel: usize,
    delete: bool,
    cancel: CancellationToken,
) -> Result<()> {
    {
        let q = queue::Queue::load_readonly();
        if q.pending_count() > 0 {
            anyhow::bail!(
                "Queue has {} pending item(s).\n  \
                 Run 'rdm queue start' to finish them, or 'rdm queue clear' to reset.",
                q.pending_count()
            );
        }
    }

    let files = scrape::discover_files(url)
        .await
        .context("Failed to scan remote directory")?;

    let files = match files {
        Some(f) if !f.is_empty() => f,
        _ => {
            eprintln!("  ❌ No files found at {}", url);
            return Ok(());
        }
    };

    if cancel.is_cancelled() {
        eprintln!("  ⚠ Cancelled during scan.");
        return Ok(());
    }

    let remote_decoded: HashSet<String> = files
        .iter()
        .map(|f| cli::percent_decode(&f.relative_path))
        .collect();

    let sync_root_result = derive_sync_root(cfg, &files);

    if delete {
        match &sync_root_result {
            SyncRoot::MixedRoots => {
                eprintln!("  ⚠ Cannot use --delete: files have mixed folder roots.");
                eprintln!("    Delete must be performed manually.");
            }
            SyncRoot::Empty => {
                eprintln!("  ⚠ Cannot use --delete: unable to determine sync root.");
            }
            SyncRoot::Ok(_) => {}
        }
    }

    eprintln!("  🔍 Checking {} file(s)...", files.len());

    let mut to_download: Vec<(String, String, Option<PathBuf>)> = Vec::new();
    let mut up_to_date = 0u64;

    for f in &files {
        let paths = candidate_paths(cfg, &f.relative_path);
        match find_existing(&paths) {
            Some((_, size)) if size > 0 => {
                up_to_date += 1;
            }
            Some((path, _)) => {
                to_download.push((f.url.clone(), f.relative_path.clone(), Some(path)));
            }
            None => {
                to_download.push((f.url.clone(), f.relative_path.clone(), None));
            }
        }
    }

    to_download.sort_by(|a, b| a.1.cmp(&b.1));

    let mut to_delete: Vec<String> = Vec::new();
    if delete {
        if let SyncRoot::Ok(ref root) = sync_root_result {
            let root_path = Path::new(root);
            if root_path.is_dir() {
                collect_orphan_files(root_path, root_path, &remote_decoded, &mut to_delete);
                to_delete.sort();
            }
        }
    }

    eprintln!();
    eprintln!("  Remote     : {} file(s)", files.len());
    eprintln!("  Up to date : {}", up_to_date);
    eprintln!("  To download: {}", to_download.len());
    if delete {
        eprintln!("  To delete  : {}", to_delete.len());
    }

    if to_download.is_empty() && to_delete.is_empty() {
        eprintln!();
        eprintln!("  ✅ Everything is up to date!");
        return Ok(());
    }

    if !to_download.is_empty() {
        eprintln!();
        for (_, relative, _) in &to_download {
            eprintln!("     + {}", cli::percent_decode(relative));
        }
    }

    if !to_delete.is_empty() {
        eprintln!();
        for path in &to_delete {
            eprintln!("     - {}", path);
        }
    }

    eprintln!();

    if !to_download.is_empty() {
        for (_, _, local_path) in &to_download {
            if let Some(p) = local_path {
                let _ = std::fs::remove_file(p);
                let _ = std::fs::remove_file(format!("{}.part", p.display()));
                let meta = crate::resume::ResumeMetadata::meta_path(&p.to_string_lossy());
                let _ = std::fs::remove_file(&meta);
            }
        }

        for (_, relative, _) in &to_download {
            let decoded = cli::percent_decode(relative);
            let local_path = cfg.resolve_output_path(&decoded);
            if let Some(parent) = Path::new(&local_path).parent() {
                std::fs::create_dir_all(parent).ok();
            }
        }

        queue::Queue::locked(|q| {
            for (file_url, relative, _) in &to_download {
                q.add(file_url.clone(), Some(relative.clone()), Some(connections));
            }
            Ok(())
        })?;

        eprintln!(
            "  📥 Starting download ({} file(s), {} parallel)...",
            to_download.len(),
            parallel,
        );
        eprintln!();

        let result = queue::start(cfg, cancel.clone(), parallel).await;
        let _ = queue::Queue::locked(|q| Ok(q.clear_finished()));

        if let Err(e) = result {
            eprintln!("  ⚠ Some downloads failed — skipping delete phase.");
            return Err(e);
        }

        let q = queue::Queue::load_readonly();
        if q.failed_count() > 0 {
            eprintln!("  ⚠ {} download(s) failed — skipping delete phase.", q.failed_count());
            return Ok(());
        }
    }

    if !to_delete.is_empty() {
        if cancel.is_cancelled() {
            eprintln!("  ⚠ Cancelled before delete phase.");
            return Ok(());
        }

        let total_local = up_to_date as usize + to_delete.len();
        if total_local > 0 {
            let pct = (to_delete.len() as f64 / total_local as f64) * 100.0;
            if to_delete.len() > 10 && pct > 50.0 {
                eprintln!(
                    "  ⚠ Warning: about to delete {} of {} local files ({:.0}%)",
                    to_delete.len(), total_local, pct,
                );
                eprintln!("    This usually means the remote listing is incomplete.");
                eprint!("    Continue? [y/N]: ");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input).ok();
                if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
                    eprintln!("  ⛔ Aborted.");
                    return Ok(());
                }
            }
        }

        let mut deleted = 0u64;
        let mut delete_failed = 0u64;

        if let SyncRoot::Ok(ref root) = sync_root_result {
            for relative in &to_delete {
                let full_path = Path::new(root).join(relative);
                match std::fs::remove_file(&full_path) {
                    Ok(_) => {
                        deleted += 1;
                        let _ = std::fs::remove_file(format!("{}.part", full_path.display()));
                        let meta = crate::resume::ResumeMetadata::meta_path(
                            &full_path.to_string_lossy(),
                        );
                        let _ = std::fs::remove_file(&meta);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        delete_failed += 1;
                        eprintln!("  ⚠ Failed to delete {}: {}", relative, e);
                    }
                }
            }
            remove_empty_dirs(Path::new(root));
        }

        eprintln!("  🗑  Deleted {} file(s)", deleted);
        if delete_failed > 0 {
            eprintln!("  ⚠  Failed to delete {} file(s)", delete_failed);
        }
    }

    eprintln!();
    eprintln!("  ✅ Sync complete!");
    Ok(())
}

enum SyncRoot {
    Ok(String),
    Empty,
    MixedRoots,
}

fn candidate_paths(cfg: &Config, relative: &str) -> Vec<PathBuf> {
    let decoded_relative = cli::percent_decode(relative);
    let filename_encoded = extract_filename(relative);
    let filename_decoded = cli::percent_decode(&filename_encoded);

    let mut paths: Vec<PathBuf> = vec![
        PathBuf::from(cfg.resolve_output_path(&decoded_relative)),
        PathBuf::from(cfg.resolve_output_path(&filename_decoded)),
        PathBuf::from(cfg.resolve_output_path(relative)),
        PathBuf::from(cfg.resolve_output_path(&filename_encoded)),
    ];
    paths.sort();
    paths.dedup();
    paths
}

fn extract_filename(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

fn find_existing(paths: &[PathBuf]) -> Option<(PathBuf, u64)> {
    for path in paths {
        if let Ok(m) = std::fs::metadata(path) {
            if m.is_file() {
                return Some((path.clone(), m.len()));
            }
        }
    }
    None
}

fn derive_sync_root(cfg: &Config, files: &[scrape::DiscoveredFile]) -> SyncRoot {
    let first = match files.first() {
        Some(f) => f,
        None => return SyncRoot::Empty,
    };
    let prefix = match first.relative_path.split('/').next() {
        Some(p) if !p.is_empty() => p,
        _ => return SyncRoot::Empty,
    };
    if files.iter().any(|f| f.relative_path.split('/').next() != Some(prefix)) {
        return SyncRoot::MixedRoots;
    }
    let decoded_prefix = cli::percent_decode(prefix);
    SyncRoot::Ok(cfg.resolve_output_path(&decoded_prefix))
}

fn collect_orphan_files(
    dir: &Path,
    base: &Path,
    remote_decoded: &HashSet<String>,
    out: &mut Vec<String>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".part") || name.ends_with(".rdm") {
                continue;
            }
        }
        if path.is_dir() {
            collect_orphan_files(&path, base, remote_decoded, out);
        } else if path.is_file() {
            let relative = match path.strip_prefix(base) {
                Ok(r) => r.to_string_lossy().to_string().replace('\\', "/"),
                Err(_) => continue,
            };
            if relative.is_empty() {
                continue;
            }
            let folder = match base.file_name().and_then(|n| n.to_str()) {
                Some(n) => n,
                None => return,
            };
            let full = format!("{}/{}", folder, relative);
            if !remote_decoded.contains(&full) {
                out.push(relative);
            }
        }
    }
}

fn remove_empty_dirs(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            remove_empty_dirs(&path);
            let _ = std::fs::remove_dir(&path);
        }
    }
}
