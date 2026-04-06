use crate::bloom::BloomFilter;
use crate::error::SSTableError;
use crate::{FOOTER_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION};
use ingodb_blob::{DocumentId, IBlob};
use std::fs;
use std::path::{Path, PathBuf};

/// Index entry loaded from an SSTable.
#[derive(Debug, Clone)]
struct BlockIndex {
    last_id: DocumentId,
    offset: u64,
    size: u32,
}

/// Reads and queries an SSTable file.
pub struct SSTableReader {
    path: PathBuf,
    data: Vec<u8>,
    block_indices: Vec<BlockIndex>,
    bloom: BloomFilter,
}

impl SSTableReader {
    /// Open an SSTable file and load its index and bloom filter into memory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SSTableError> {
        let path = path.as_ref().to_path_buf();
        let data = fs::read(&path)?;

        if data.len() < FOOTER_SIZE {
            return Err(SSTableError::Corrupted("file too small for footer".into()));
        }

        // Parse footer (last FOOTER_SIZE bytes)
        let footer_start = data.len() - FOOTER_SIZE;
        let footer = &data[footer_start..];

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_count = u32::from_le_bytes(footer[8..12].try_into().unwrap()) as usize;
        let bloom_offset = u64::from_le_bytes(footer[12..20].try_into().unwrap()) as usize;
        let bloom_size = u32::from_le_bytes(footer[20..24].try_into().unwrap()) as usize;

        // Verify magic
        if footer[24..28] != SSTABLE_MAGIC {
            return Err(SSTableError::InvalidMagic);
        }
        let version = u16::from_le_bytes(footer[28..30].try_into().unwrap());
        if version != SSTABLE_VERSION {
            return Err(SSTableError::UnsupportedVersion(version));
        }

        // Parse bloom filter
        if bloom_offset + bloom_size > data.len() {
            return Err(SSTableError::Corrupted("bloom filter out of bounds".into()));
        }
        let bloom_data = &data[bloom_offset..bloom_offset + bloom_size];
        let bloom_num_bits = u32::from_le_bytes(bloom_data[0..4].try_into().unwrap()) as usize;
        let bloom_num_hashes = u32::from_le_bytes(bloom_data[4..8].try_into().unwrap());
        let bloom = BloomFilter::from_bytes(&bloom_data[8..], bloom_num_bits, bloom_num_hashes);

        // Parse block index — each entry: [id:16][offset:8][size:4] = 28 bytes
        let index_entry_size = 28;
        if index_offset + index_count * index_entry_size > data.len() {
            return Err(SSTableError::Corrupted("index out of bounds".into()));
        }

        let mut block_indices = Vec::with_capacity(index_count);
        for i in 0..index_count {
            let base = index_offset + i * index_entry_size;
            let mut id_bytes = [0u8; 16];
            id_bytes.copy_from_slice(&data[base..base + 16]);
            let offset = u64::from_le_bytes(data[base + 16..base + 24].try_into().unwrap());
            let size = u32::from_le_bytes(data[base + 24..base + 28].try_into().unwrap());
            block_indices.push(BlockIndex {
                last_id: DocumentId::from_bytes(id_bytes),
                offset,
                size,
            });
        }

        Ok(SSTableReader {
            path,
            data,
            block_indices,
            bloom,
        })
    }

    /// Point lookup: find a blob by its document ID.
    /// Returns None if the ID is not in this SSTable.
    pub fn get(&self, id: &DocumentId) -> Result<Option<IBlob>, SSTableError> {
        // Check bloom filter first
        if !self.bloom.may_contain(id.as_bytes()) {
            return Ok(None);
        }

        // Binary search the block index to find the right block
        let block_idx = match self
            .block_indices
            .binary_search_by_key(id, |idx| idx.last_id)
        {
            Ok(i) => i,
            Err(i) => {
                if i >= self.block_indices.len() {
                    return Ok(None);
                }
                i
            }
        };

        // Read and decompress the block
        let block = &self.block_indices[block_idx];
        let entries = self.read_block(block)?;

        // Binary search within the block
        match entries.binary_search_by_key(id, |(entry_id, _)| *entry_id) {
            Ok(i) => Ok(Some(entries[i].1.clone())),
            Err(_) => Ok(None),
        }
    }

    /// Iterate over all entries in the SSTable in ID-sorted order.
    pub fn iter(&self) -> Result<Vec<(DocumentId, IBlob)>, SSTableError> {
        let mut all_entries = Vec::new();
        for block in &self.block_indices {
            let entries = self.read_block(block)?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }

    /// Number of data blocks in this SSTable.
    pub fn block_count(&self) -> usize {
        self.block_indices.len()
    }

    /// Path to the SSTable file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_block(&self, block: &BlockIndex) -> Result<Vec<(DocumentId, IBlob)>, SSTableError> {
        let start = block.offset as usize;
        let end = start + block.size as usize;

        if end > self.data.len() {
            return Err(SSTableError::Corrupted(format!(
                "block at offset {} extends past end of file",
                block.offset
            )));
        }

        let block_data = &self.data[start..end];

        // Verify CRC
        let stored_crc = u32::from_le_bytes(block_data[0..4].try_into().unwrap());
        let compressed = &block_data[4..];
        let computed_crc = crc32fast::hash(compressed);

        if stored_crc != computed_crc {
            return Err(SSTableError::CrcMismatch {
                offset: block.offset,
            });
        }

        // Decompress
        let decompressed = lz4_flex::decompress_size_prepended(compressed)
            .map_err(|e| SSTableError::Corrupted(format!("LZ4 decompress failed: {e}")))?;

        // Parse entries: [entry_count:4][id:16][blob_len:4][blob_bytes]...
        let entry_count =
            u32::from_le_bytes(decompressed[0..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(entry_count);
        let mut pos = 4;

        for _ in 0..entry_count {
            if pos + 20 > decompressed.len() {
                return Err(SSTableError::Corrupted("truncated entry in block".into()));
            }

            let mut id_bytes = [0u8; 16];
            id_bytes.copy_from_slice(&decompressed[pos..pos + 16]);
            let blob_len =
                u32::from_le_bytes(decompressed[pos + 16..pos + 20].try_into().unwrap()) as usize;
            pos += 20;

            if pos + blob_len > decompressed.len() {
                return Err(SSTableError::Corrupted("truncated blob in block".into()));
            }

            let blob = IBlob::decode(&decompressed[pos..pos + blob_len])?;
            entries.push((DocumentId::from_bytes(id_bytes), blob));
            pos += blob_len;
        }

        Ok(entries)
    }
}
