//! Bounded, batched positional writes for one range worker.
//!
//! A worker owns a separately opened, guarded file handle. Only completed
//! writes advance its resume counter; buffered bytes can always be fetched
//! again. A completed write reaches the OS, not necessarily stable storage.
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const BATCH_BYTES: usize = 1024 * 1024;

pub(crate) struct RangeWriter {
    file: Arc<std::fs::File>,
    buffer: Vec<u8>,
    start: u64,
    resume_from: u64,
    committed: u64,
    progress: Arc<AtomicU64>,
}

impl RangeWriter {
    pub(crate) fn new(
        file: std::fs::File,
        start: u64,
        resume_from: u64,
        progress: Arc<AtomicU64>,
    ) -> Self {
        Self {
            file: Arc::new(file),
            buffer: Vec::with_capacity(BATCH_BYTES),
            start,
            resume_from,
            committed: 0,
            progress,
        }
    }

    pub(crate) fn has_buffered(&self) -> bool {
        !self.buffer.is_empty()
    }

    pub(crate) async fn write_all(&mut self, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let n = (BATCH_BYTES - self.buffer.len()).min(data.len());
            self.buffer.extend_from_slice(&data[..n]);
            data = &data[n..];
            if self.buffer.len() == BATCH_BYTES {
                self.flush().await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let data = std::mem::take(&mut self.buffer);
        let len = data.len() as u64;
        let offset = self
            .start
            .checked_add(self.committed)
            .ok_or_else(|| io::Error::other("range offset overflow"))?;
        let file = Arc::clone(&self.file);
        let (result, mut data) = tokio::task::spawn_blocking(move || {
            let result = write_fully_at(&file, &data, offset);
            (result, data)
        })
        .await
        .map_err(io::Error::other)?;
        result?;
        self.committed = self
            .committed
            .checked_add(len)
            .ok_or_else(|| io::Error::other("range progress overflow"))?;
        let progress = self
            .resume_from
            .checked_add(self.committed)
            .ok_or_else(|| io::Error::other("range progress overflow"))?;
        self.progress.store(progress, Ordering::SeqCst);
        data.clear();
        self.buffer = data;
        Ok(())
    }
}

fn write_fully_at(file: &std::fs::File, mut data: &[u8], mut offset: u64) -> io::Result<()> {
    while !data.is_empty() {
        #[cfg(unix)]
        let result = {
            use std::os::unix::fs::FileExt;
            file.write_at(data, offset)
        };
        #[cfg(windows)]
        let result = {
            use std::os::windows::fs::FileExt;
            file.seek_write(data, offset)
        };
        #[cfg(not(any(unix, windows)))]
        let result = {
            use std::io::{Seek, SeekFrom, Write};
            let mut handle = file.try_clone()?;
            handle.seek(SeekFrom::Start(offset))?;
            handle.write(data)
        };
        let n = match result {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positional write returned zero",
                ));
            }
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        offset = offset
            .checked_add(n as u64)
            .ok_or_else(|| io::Error::other("range offset overflow"))?;
        data = &data[n..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn reports_only_written_bytes_and_preserves_other_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, vec![0x55; 3 * BATCH_BYTES]).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let progress = Arc::new(AtomicU64::new(20));
        let mut writer = RangeWriter::new(file, 100, 20, Arc::clone(&progress));
        writer
            .write_all(&vec![0x77; BATCH_BYTES + 123])
            .await
            .unwrap();
        assert_eq!(progress.load(Ordering::SeqCst), 20 + BATCH_BYTES as u64);
        assert!(writer.has_buffered());
        writer.flush().await.unwrap();
        assert_eq!(
            progress.load(Ordering::SeqCst),
            20 + BATCH_BYTES as u64 + 123
        );
        let data = std::fs::read(path).unwrap();
        assert!(data[..100].iter().all(|x| *x == 0x55));
        assert!(
            data[100..100 + BATCH_BYTES + 123]
                .iter()
                .all(|x| *x == 0x77)
        );
        assert!(data[100 + BATCH_BYTES + 123..].iter().all(|x| *x == 0x55));
    }

    #[tokio::test]
    async fn failed_write_does_not_advance_resume_progress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"unchanged").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let progress = Arc::new(AtomicU64::new(3));
        let mut writer = RangeWriter::new(file, 3, 3, Arc::clone(&progress));
        writer.write_all(b"data").await.unwrap();
        assert!(writer.flush().await.is_err());
        assert_eq!(progress.load(Ordering::SeqCst), 3);
        assert_eq!(std::fs::read(path).unwrap(), b"unchanged");
    }
}
