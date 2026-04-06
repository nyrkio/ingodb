mod writer;
mod reader;
mod bloom;
mod error;

pub use writer::SSTableWriter;
pub use reader::SSTableReader;
pub use bloom::BloomFilter;
pub use error::SSTableError;

/// SSTable file magic: "ISST"
pub const SSTABLE_MAGIC: [u8; 4] = *b"ISST";

/// SSTable format version
pub const SSTABLE_VERSION: u16 = 2;

/// Default data block size (4 KB, aligned for direct I/O)
pub const DEFAULT_BLOCK_SIZE: usize = 4096;

/// Footer size: index_offset(8) + index_count(4) + bloom_offset(8) + bloom_size(4) + magic(4) + version(2) = 30
pub const FOOTER_SIZE: usize = 30;

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::{DocumentId, IBlob, Value};

    fn make_entries(n: usize) -> Vec<(DocumentId, IBlob)> {
        let mut entries: Vec<_> = (0..n)
            .map(|i| {
                let blob = IBlob::from_pairs(vec![
                    ("id", Value::U64(i as u64)),
                    ("name", Value::String(format!("entry-{i}"))),
                ]);
                (*blob.id(), blob)
            })
            .collect();
        entries.sort_by_key(|(id, _)| *id);
        entries
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sst");

        let entries = make_entries(100);

        SSTableWriter::new().write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        // Verify all entries can be looked up
        for (id, blob) in &entries {
            let found = reader.get(id).unwrap().expect("entry not found");
            assert_eq!(found.id(), blob.id());
            assert_eq!(found.fields(), blob.fields());
        }

        // Verify a missing key returns None
        let missing_id = DocumentId::from_bytes([0xFF; 16]);
        assert!(reader.get(&missing_id).unwrap().is_none());
    }

    #[test]
    fn test_iter_all_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("iter.sst");

        let entries = make_entries(50);
        SSTableWriter::new().write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        let all = reader.iter().unwrap();
        assert_eq!(all.len(), 50);

        // Verify sorted order
        for i in 1..all.len() {
            assert!(all[i - 1].0 <= all[i].0);
        }
    }

    #[test]
    fn test_small_block_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small_blocks.sst");

        let entries = make_entries(20);

        // Very small blocks to force multiple blocks
        SSTableWriter::with_block_size(128).write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        assert!(reader.block_count() > 1, "expected multiple blocks");

        for (id, blob) in &entries {
            let found = reader.get(id).unwrap().expect("entry not found");
            assert_eq!(found.id(), blob.id());
        }
    }

    #[test]
    fn test_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.sst");

        let entries = make_entries(1);
        SSTableWriter::new().write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        let found = reader.get(&entries[0].0).unwrap().unwrap();
        assert_eq!(found.id(), entries[0].1.id());
    }

    #[test]
    fn test_min_max_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("minmax.sst");

        let entries = make_entries(50);
        SSTableWriter::new().write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        // Entries are sorted by ID, so first is min and last is max
        assert_eq!(reader.min_id(), &entries.first().unwrap().0);
        assert_eq!(reader.max_id(), &entries.last().unwrap().0);
        assert!(reader.file_size() > 0);
    }

    #[test]
    fn test_min_max_single_entry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("minmax1.sst");

        let entries = make_entries(1);
        SSTableWriter::new().write(&path, &entries).unwrap();
        let reader = SSTableReader::open(&path).unwrap();

        assert_eq!(reader.min_id(), reader.max_id());
        assert_eq!(reader.min_id(), &entries[0].0);
    }
}
