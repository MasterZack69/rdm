use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::chunk::Chunk;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResumeMetadata {
    pub url: String,
    pub file_size: u64,
    pub chunks: Vec<ChunkState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChunkState {
    pub id: u32,
    pub start: u64,
    pub end: u64,
    pub completed: u64,
}

impl ChunkState {
    pub fn total_bytes(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn is_complete(&self) -> bool {
        self.completed >= self.total_bytes()
    }
}

impl ResumeMetadata {
    pub fn total_completed(&self) -> u64 {
        self.chunks.iter().map(|c| c.completed).sum()
    }

    pub fn is_complete(&self) -> bool {
        self.chunks.iter().all(|c| c.is_complete())
    }

    pub fn meta_path(output_path: &str) -> String {
        format!("{}.rdm", output_path)
    }
}

pub fn create_new(url: String, file_size: u64, chunks: &[Chunk]) -> ResumeMetadata {
    let chunk_states = chunks
        .iter()
        .map(|c| ChunkState {
            id: c.id,
            start: c.start,
            end: c.end,
            completed: 0,
        })
        .collect();

    ResumeMetadata {
        url,
        file_size,
        chunks: chunk_states,
    }
}

pub async fn save_atomic(path: &str, meta: &ResumeMetadata) -> Result<()> {
    let json = serde_json::to_string_pretty(meta)
        .context("Failed to serialize resume metadata")?;

    let tmp_path = format!("{}.tmp", path);

    let write_result = async {
        let mut file = fs::File::create(&tmp_path)
            .await
            .with_context(|| format!("Failed to create temp file: {}", tmp_path))?;

        file.write_all(json.as_bytes())
            .await
            .context("Failed to write metadata to temp file")?;

        file.flush()
            .await
            .context("Failed to flush metadata temp file")?;

        file.sync_all()
            .await
            .context("Failed to sync metadata temp file to disk")?;

        drop(file);

        fs::rename(&tmp_path, path)
            .await
            .with_context(|| format!("Failed to rename '{}' to '{}'", tmp_path, path))?;

        let parent = std::path::Path::new(path)
            .parent()
            .unwrap_or(std::path::Path::new("."));

        let dir = fs::File::open(parent)
            .await
            .with_context(|| format!("Failed to open parent dir: {}", parent.display()))?;

        dir.sync_all()
            .await
            .context("Failed to sync parent directory")?;

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = &write_result {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(anyhow::anyhow!("{:#}", e));
    }

    Ok(())
}

pub async fn load(path: &str) -> Result<ResumeMetadata> {
    let data = fs::read_to_string(path)
        .await
        .with_context(|| format!("Failed to read metadata file: {}", path))?;

    let meta: ResumeMetadata = serde_json::from_str(&data)
        .with_context(|| format!("Failed to parse metadata file: {}", path))?;

    Ok(meta)
}

pub async fn delete(path: &str) -> Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("Failed to delete metadata: {}", path)),
    }
}

pub fn update_progress(meta: &mut ResumeMetadata, chunk_id: u32, completed: u64) {
    if let Some(chunk) = meta.chunks.iter_mut().find(|c| c.id == chunk_id) {
        chunk.completed = chunk.completed.max(completed);
    }
}

pub fn validate_against(
    meta: &ResumeMetadata,
    url: &str,
    file_size: u64,
    chunks: &[Chunk],
) -> bool {
    if meta.url != url || meta.file_size != file_size {
        return false;
    }

    if meta.chunks.len() != chunks.len() {
        return false;
    }

    if meta.total_completed() > file_size {
        return false;
    }

    meta.chunks.iter().zip(chunks.iter()).all(|(state, chunk)| {
        state.id == chunk.id
            && state.start == chunk.start
            && state.end == chunk.end
            && state.completed <= state.total_bytes()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::Chunk;

    fn sample_chunks() -> Vec<Chunk> {
        vec![
            Chunk { id: 1, start: 0, end: 499 },
            Chunk { id: 2, start: 500, end: 999 },
            Chunk { id: 3, start: 1000, end: 1499 },
            Chunk { id: 4, start: 1500, end: 1999 },
        ]
    }

    #[test]
    fn test_create_new() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        assert_eq!(meta.url, "https://example.com/file.bin");
        assert_eq!(meta.file_size, 2000);
        assert_eq!(meta.chunks.len(), 4);

        for (state, chunk) in meta.chunks.iter().zip(chunks.iter()) {
            assert_eq!(state.id, chunk.id);
            assert_eq!(state.start, chunk.start);
            assert_eq!(state.end, chunk.end);
            assert_eq!(state.completed, 0);
        }
    }

    #[test]
    fn test_serialization_roundtrip() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        let json = serde_json::to_string_pretty(&meta).expect("serialize failed");
        let deserialized: ResumeMetadata =
            serde_json::from_str(&json).expect("deserialize failed");

        assert_eq!(meta, deserialized);
    }

    #[test]
    fn test_update_progress() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        update_progress(&mut meta, 1, 250);
        assert_eq!(meta.chunks[0].completed, 250);

        update_progress(&mut meta, 3, 500);
        assert_eq!(meta.chunks[2].completed, 500);
        assert!(meta.chunks[2].is_complete());
    }

    #[test]
    fn test_update_progress_no_rollback() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        update_progress(&mut meta, 1, 400);
        assert_eq!(meta.chunks[0].completed, 400);

        update_progress(&mut meta, 1, 200);
        assert_eq!(meta.chunks[0].completed, 400, "must not roll back");

        update_progress(&mut meta, 1, 0);
        assert_eq!(meta.chunks[0].completed, 400, "must not roll back to zero");

        update_progress(&mut meta, 1, 500);
        assert_eq!(meta.chunks[0].completed, 500, "must advance forward");
    }

    #[test]
    fn test_update_progress_nonexistent_chunk() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        let original = meta.clone();

        update_progress(&mut meta, 99, 100);
        assert_eq!(meta, original);
    }

    #[test]
    fn test_total_completed() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        update_progress(&mut meta, 1, 500);
        update_progress(&mut meta, 2, 300);
        assert_eq!(meta.total_completed(), 800);
    }

    #[test]
    fn test_is_complete() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);

        assert!(!meta.is_complete());

        for c in &chunks {
            update_progress(&mut meta, c.id, c.end - c.start + 1);
        }
        assert!(meta.is_complete());
    }

    #[test]
    fn test_chunk_state_total_bytes() {
        let state = ChunkState { id: 1, start: 100, end: 599, completed: 0 };
        assert_eq!(state.total_bytes(), 500);
    }

    #[test]
    fn test_validate_against_matching() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        assert!(validate_against(&meta, "https://example.com/file.bin", 2000, &chunks));
    }

    #[test]
    fn test_validate_against_url_mismatch() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        assert!(!validate_against(&meta, "https://other.com/file.bin", 2000, &chunks));
    }

    #[test]
    fn test_validate_against_size_mismatch() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        assert!(!validate_against(&meta, "https://example.com/file.bin", 9999, &chunks));
    }

    #[test]
    fn test_validate_against_chunk_count_mismatch() {
        let chunks = sample_chunks();
        let meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        let fewer = &chunks[..2];
        assert!(!validate_against(&meta, "https://example.com/file.bin", 2000, fewer));
    }

    #[test]
    fn test_validate_against_invalid_progress() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        meta.chunks[0].completed = 99999;
        assert!(!validate_against(&meta, "https://example.com/file.bin", 2000, &chunks));
    }

    #[test]
    fn test_validate_against_total_exceeds_file_size() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        meta.chunks[0].completed = 500;
        meta.chunks[1].completed = 500;
        meta.chunks[2].completed = 500;
        meta.chunks[3].completed = 501;
        assert!(
            !validate_against(&meta, "https://example.com/file.bin", 2000, &chunks),
            "total_completed (2001) exceeds file_size (2000)"
        );
    }

    #[test]
    fn test_validate_against_total_equals_file_size() {
        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        meta.chunks[0].completed = 500;
        meta.chunks[1].completed = 500;
        meta.chunks[2].completed = 500;
        meta.chunks[3].completed = 500;
        assert!(validate_against(&meta, "https://example.com/file.bin", 2000, &chunks));
    }

    #[tokio::test]
    async fn test_save_atomic_and_load() {
        let path = "/tmp/rdm_test_resume_meta.json";
        let _ = fs::remove_file(path).await;
        let _ = fs::remove_file(&format!("{}.tmp", path)).await;

        let chunks = sample_chunks();
        let mut meta = create_new("https://example.com/file.bin".into(), 2000, &chunks);
        update_progress(&mut meta, 1, 500);
        update_progress(&mut meta, 2, 250);

        save_atomic(path, &meta).await.expect("save failed");

        let loaded = load(path).await.expect("load failed");
        assert_eq!(meta, loaded);

        let tmp_exists = fs::metadata(&format!("{}.tmp", path)).await.is_ok();
        assert!(!tmp_exists, "temp file should not remain after atomic save");

        fs::remove_file(path).await.expect("cleanup failed");
    }

    #[tokio::test]
    async fn test_save_overwrite_preserves_valid_data() {
        let path = "/tmp/rdm_test_resume_overwrite_valid.json";
        let _ = fs::remove_file(path).await;

        let chunks = sample_chunks();

        let meta_v1 = create_new("https://example.com/v1".into(), 1000, &chunks);
        save_atomic(path, &meta_v1).await.expect("first save failed");

        let loaded_v1 = load(path).await.expect("load v1 failed");
        assert_eq!(loaded_v1.url, "https://example.com/v1");

        let mut meta_v2 = create_new("https://example.com/v2".into(), 2000, &chunks);
        update_progress(&mut meta_v2, 1, 500);
        update_progress(&mut meta_v2, 3, 250);
        save_atomic(path, &meta_v2).await.expect("second save failed");

        let loaded_v2 = load(path).await.expect("load v2 failed");
        assert_eq!(loaded_v2.url, "https://example.com/v2");
        assert_eq!(loaded_v2.file_size, 2000);
        assert_eq!(loaded_v2.chunks[0].completed, 500);
        assert_eq!(loaded_v2.chunks[2].completed, 250);

        let content = fs::read_to_string(path).await.expect("read failed");
        let parsed: ResumeMetadata =
            serde_json::from_str(&content).expect("JSON must be valid after overwrite");
        assert_eq!(parsed, meta_v2);

        fs::remove_file(path).await.expect("cleanup failed");
    }

    #[tokio::test]
    async fn test_save_atomic_no_leftover_tmp() {
        let path = "/tmp/rdm_test_no_leftover.json";
        let tmp = format!("{}.tmp", path);
        let _ = fs::remove_file(path).await;
        let _ = fs::remove_file(&tmp).await;

        let chunks = sample_chunks();
        let meta = create_new("https://example.com/clean".into(), 500, &chunks);

        save_atomic(path, &meta).await.expect("save failed");

        assert!(fs::metadata(path).await.is_ok());
        assert!(fs::metadata(&tmp).await.is_err());

        fs::remove_file(path).await.expect("cleanup failed");
    }

    #[tokio::test]
    async fn test_load_nonexistent_fails() {
        let result = load("/tmp/rdm_this_file_does_not_exist.json").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_existing() {
        let path = "/tmp/rdm_test_resume_delete.json";
        fs::write(path, b"{}").await.expect("setup failed");

        delete(path).await.expect("delete failed");
        assert!(fs::metadata(path).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_nonexistent_ok() {
        let result = delete("/tmp/rdm_this_does_not_exist_delete.json").await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_meta_path() {
        assert_eq!(
            ResumeMetadata::meta_path("/home/user/file.zip"),
            "/home/user/file.zip.rdm"
        );
    }
}
