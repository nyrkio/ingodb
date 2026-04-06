use crate::bloom::BloomFilter;
use crate::error::SSTableError;
use crate::{DEFAULT_BLOCK_SIZE, SSTABLE_MAGIC, SSTABLE_VERSION};
use ingodb_blob::{DocumentId, IBlob};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

/// Index entry: points to a data block by the last document ID it contains.
#[derive(Debug, Clone)]
struct BlockIndex {
    /// Last (largest) document ID in this block
    last_id: DocumentId,
    /// Byte offset of the block in the file
    offset: u64,
    /// Compressed size of the block in bytes
    size: u32,
}

/// Writes an SSTable file from sorted (id, blob) pairs.
///
/// File layout:
/// ```text
/// [data block 0]  — each block: [crc32:4][compressed_data]
/// [data block 1]
/// ...
/// [data block N]
/// [bloom filter bytes]
/// [index entries]  — each: [id:16][offset:8][size:4] = 28 bytes
/// [footer: 30B]
///   [index_offset: 8B]
///   [index_count: 4B]
///   [bloom_offset: 8B]
///   [bloom_size: 4B]
///   [magic: 4B "ISST"]
///   [version: 2B]
/// ```
///
/// Within each data block (after decompression):
/// ```text
/// [entry_count: 4B]
/// [id:16B][blob_len:4B][blob_bytes] ...repeated
/// ```
pub struct SSTableWriter {
    block_size: usize,
}

impl SSTableWriter {
    pub fn new() -> Self {
        SSTableWriter {
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    pub fn with_block_size(block_size: usize) -> Self {
        SSTableWriter { block_size }
    }

    /// Write an SSTable from sorted entries.
    /// Entries MUST be sorted by document ID.
    pub fn write(
        &self,
        path: impl AsRef<Path>,
        entries: &mut [(DocumentId, IBlob)],
    ) -> Result<(), SSTableError> {
        if entries.is_empty() {
            return Err(SSTableError::Empty);
        }

        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let mut block_indices: Vec<BlockIndex> = Vec::new();
        let mut bloom = BloomFilter::new(entries.len());

        // Build data blocks
        let mut current_block = Vec::new();
        let mut block_entry_count = 0u32;
        let mut file_offset = 0u64;

        // Reserve space for entry count at start of block
        current_block.extend_from_slice(&0u32.to_le_bytes());

        for (id, blob) in entries.iter_mut() {
            bloom.insert(id.as_bytes());

            let blob_bytes = blob.encode();
            let entry_size = 16 + 4 + blob_bytes.len();

            // If adding this entry would exceed block size, flush current block
            if block_entry_count > 0 && current_block.len() + entry_size > self.block_size {
                file_offset =
                    self.flush_block(&mut writer, &current_block, block_entry_count, file_offset, &mut block_indices)?;

                // Reset block
                current_block.clear();
                current_block.extend_from_slice(&0u32.to_le_bytes());
                block_entry_count = 0;
            }

            // Write entry into current block buffer
            current_block.extend_from_slice(id.as_bytes());
            current_block.extend_from_slice(&(blob_bytes.len() as u32).to_le_bytes());
            current_block.extend_from_slice(&blob_bytes);
            block_entry_count += 1;
        }

        // Flush final block
        if block_entry_count > 0 {
            self.flush_block(&mut writer, &current_block, block_entry_count, file_offset, &mut block_indices)?;
        }

        // Write bloom filter
        let bloom_offset = block_indices
            .last()
            .map(|b| b.offset + b.size as u64)
            .unwrap_or(0);
        let bloom_bytes = bloom.as_bytes();
        let bloom_size = bloom_bytes.len() as u32 + 4 + 4; // data + num_bits + num_hashes
        writer.write_all(&(bloom.num_bits() as u32).to_le_bytes())?;
        writer.write_all(&bloom.num_hashes().to_le_bytes())?;
        writer.write_all(bloom_bytes)?;

        // Write index entries
        let index_offset = bloom_offset + bloom_size as u64;
        let index_count = block_indices.len() as u32;
        for idx in &block_indices {
            writer.write_all(idx.last_id.as_bytes())?;
            writer.write_all(&idx.offset.to_le_bytes())?;
            writer.write_all(&idx.size.to_le_bytes())?;
        }

        // Write footer
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&index_count.to_le_bytes())?;
        writer.write_all(&bloom_offset.to_le_bytes())?;
        writer.write_all(&bloom_size.to_le_bytes())?;
        writer.write_all(&SSTABLE_MAGIC)?;
        writer.write_all(&SSTABLE_VERSION.to_le_bytes())?;

        writer.flush()?;
        Ok(())
    }

    fn flush_block(
        &self,
        writer: &mut BufWriter<File>,
        block_data: &[u8],
        entry_count: u32,
        file_offset: u64,
        block_indices: &mut Vec<BlockIndex>,
    ) -> Result<u64, SSTableError> {
        // Patch entry count at the start of block data
        let mut data = block_data.to_vec();
        data[..4].copy_from_slice(&entry_count.to_le_bytes());

        // Compress with LZ4
        let compressed = lz4_flex::compress_prepend_size(&data);

        // CRC of compressed data
        let crc = crc32fast::hash(&compressed);

        // Write [crc:4][compressed_data]
        writer.write_all(&crc.to_le_bytes())?;
        writer.write_all(&compressed)?;

        let block_size = 4 + compressed.len();

        // Extract last ID from the block for the index
        let last_id = self.extract_last_id(&data, entry_count);

        block_indices.push(BlockIndex {
            last_id,
            offset: file_offset,
            size: block_size as u32,
        });

        Ok(file_offset + block_size as u64)
    }

    fn extract_last_id(&self, block_data: &[u8], entry_count: u32) -> DocumentId {
        // Walk through entries to find the last ID
        let mut pos = 4; // skip entry_count
        let mut last_id = [0u8; 16];
        for _ in 0..entry_count {
            last_id.copy_from_slice(&block_data[pos..pos + 16]);
            let blob_len =
                u32::from_le_bytes(block_data[pos + 16..pos + 20].try_into().unwrap()) as usize;
            pos += 20 + blob_len;
        }
        DocumentId::from_bytes(last_id)
    }
}

impl Default for SSTableWriter {
    fn default() -> Self {
        Self::new()
    }
}
