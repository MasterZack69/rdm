/// Represents a single download chunk with a byte range.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub id: u32,
    pub start: u64,
    pub end: u64,
}

const NUM_CHUNKS: u32 = 8;

pub fn plan_chunks(size: Option<u64>, supports_range: bool) -> Option<Vec<Chunk>> {
    let file_size = size?;

    if file_size == 0 {
        return None;
    }

    if !supports_range {
        return Some(vec![Chunk {
            id: 1,
            start: 0,
            end: file_size - 1,
        }]);
    }

    let chunk_size = file_size / NUM_CHUNKS as u64;
    let remainder = file_size % NUM_CHUNKS as u64;

    let mut chunks = Vec::with_capacity(NUM_CHUNKS as usize);
    let mut offset: u64 = 0;

    for i in 0..NUM_CHUNKS {
        let extra = if (i as u64) < remainder { 1 } else { 0 };
        let current_chunk_size = chunk_size + extra;

        let start = offset;
        let end = start + current_chunk_size - 1;

        chunks.push(Chunk { id: i + 1, start, end });

        offset = end + 1;
    }

    Some(chunks)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_size_returns_none() {
        assert!(plan_chunks(None, true).is_none());
    }

    #[test]
    fn test_zero_size_returns_none() {
        assert!(plan_chunks(Some(0), true).is_none());
    }

    #[test]
    fn test_single_chunk_when_no_range_support() {
        let chunks = plan_chunks(Some(1000), false).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 999);
    }

    #[test]
    fn test_eight_chunks_when_range_supported() {
        let chunks = plan_chunks(Some(1024), true).unwrap();
        assert_eq!(chunks.len(), 8);
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[7].end, 1023);

        for i in 1..chunks.len() {
            assert_eq!(chunks[i].start, chunks[i - 1].end + 1);
        }
    }

    #[test]
    fn test_total_bytes_match_with_remainder() {
        let file_size: u64 = 1000;
        let chunks = plan_chunks(Some(file_size), true).unwrap();
        let total: u64 = chunks.iter().map(|c| c.end - c.start + 1).sum();
        assert_eq!(total, file_size);
    }
}

