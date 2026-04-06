mod compaction;

use ingodb_blob::{DocumentId, IBlob};
use ingodb_memtable::MemTable;
use ingodb_sstable::{SSTableReader, SSTableWriter};
use ingodb_wal::Wal;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

pub use compaction::{
    CompactionAction, CompactionFilter, CompactionPick, SizeTieredCompaction, SstMeta,
    TombstoneFilter, UcsCompaction,
};

#[derive(Debug, Error)]
pub enum LsmError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WAL error: {0}")]
    Wal(#[from] ingodb_wal::WalError),

    #[error("SSTable error: {0}")]
    SSTable(#[from] ingodb_sstable::SSTableError),

    #[error("blob error: {0}")]
    Blob(#[from] ingodb_blob::BlobError),
}

/// Configuration for the LSM storage engine.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Directory for all data files
    pub data_dir: PathBuf,
    /// MemTable flush threshold in bytes (default 64 MB)
    pub memtable_size: usize,
    /// SSTable data block size (default 4096)
    pub block_size: usize,
    /// Compaction trigger: number of SSTables at a size tier before merging (default 4)
    pub compaction_threshold: usize,
    /// UCS scaling parameter W: <0 leveled, 0 balanced, >0 tiered (default 0)
    pub scaling_parameter: i32,
}

impl Default for LsmConfig {
    fn default() -> Self {
        LsmConfig {
            data_dir: PathBuf::from("ingodb_data"),
            memtable_size: 64 * 1024 * 1024,
            block_size: 4096,
            compaction_threshold: 4,
            scaling_parameter: 0,
        }
    }
}

/// The LSM storage engine. Ties together WAL, MemTable, and SSTables.
pub struct LsmEngine {
    config: LsmConfig,
    /// Active memtable receiving writes
    memtable: MemTable,
    /// WAL for the active memtable
    wal: Mutex<Wal>,
    /// SSTables on disk, ordered for reads: L0 first → L1 → ..., within each level newest first
    sstables: Mutex<Vec<SSTableReader>>,
    /// Counter for generating SSTable file names
    next_sst_id: AtomicU64,
}

impl LsmEngine {
    /// Open or create an LSM engine at the given directory.
    pub fn open(config: LsmConfig) -> Result<Self, LsmError> {
        std::fs::create_dir_all(&config.data_dir)?;
        let wal_path = config.data_dir.join("wal.log");

        // Recover existing WAL
        let recovered = Wal::recover(&wal_path)?;

        // Open WAL for new writes
        let wal = if recovered.is_empty() {
            Wal::open(&wal_path)?
        } else {
            // Truncate and reopen — recovered data goes into memtable
            std::fs::remove_file(&wal_path).ok();
            Wal::open(&wal_path)?
        };

        let memtable = MemTable::new(config.memtable_size);

        // Replay recovered blobs into memtable (versions already stamped)
        for blob in recovered {
            memtable.insert(blob);
        }

        // Load existing SSTables
        let mut sstables = Vec::new();
        let mut max_id = 0u64;
        let sst_dir = config.data_dir.clone();
        if sst_dir.exists() {
            let mut sst_files: Vec<_> = std::fs::read_dir(&sst_dir)?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path()
                        .extension()
                        .is_some_and(|ext| ext == "sst")
                })
                .collect();

            // Sort by name (which encodes creation order)
            sst_files.sort_by_key(|e| e.file_name());

            for entry in sst_files {
                let path = entry.path();
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = stem.parse::<u64>() {
                        max_id = max_id.max(id);
                    }
                }
                match SSTableReader::open(&path) {
                    Ok(reader) => sstables.push(reader),
                    Err(e) => {
                        eprintln!("warning: skipping corrupt SSTable {}: {e}", path.display());
                    }
                }
            }
        }

        let ucs = UcsCompaction::new(config.scaling_parameter, config.memtable_size as u64);
        sort_sstables_by_level(&mut sstables, &ucs);

        Ok(LsmEngine {
            config,
            memtable,
            wal: Mutex::new(wal),
            sstables: Mutex::new(sstables),
            next_sst_id: AtomicU64::new(max_id + 1),
        })
    }

    /// Insert a document into the engine.
    /// Stamps a server-assigned `_version` before writing.
    pub fn put(&self, mut blob: IBlob) -> Result<(), LsmError> {
        // Server stamps the version — this is the single point of version assignment
        blob.set_version(DocumentId::new());

        // Write to WAL first for durability (version is now embedded in the blob)
        {
            let mut wal = self.wal.lock();
            wal.append(&mut blob)?;
        }

        // Insert into memtable (keyed by _id; upsert replaces old version)
        let should_flush = self.memtable.insert(blob);

        if should_flush {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Delete a document by writing a tombstone.
    /// Stamps a server-assigned `_version` on the tombstone.
    pub fn delete(&self, id: &DocumentId) -> Result<(), LsmError> {
        let mut tombstone = IBlob::tombstone(*id);
        tombstone.set_version(DocumentId::new());

        {
            let mut wal = self.wal.lock();
            wal.append(&mut tombstone)?;
        }

        let should_flush = self.memtable.insert(tombstone);

        if should_flush {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Look up a document by its stable document ID.
    /// Returns None if the document doesn't exist or has been deleted.
    ///
    /// Read order: memtable → SSTables (L0 first → L1 → ..., within each level newest first).
    /// The first match is guaranteed to be the current version because:
    /// (a) data enters at L0 via flush,
    /// (b) compaction merges within a level and outputs to L(x+1) keeping only the highest _version,
    /// so higher levels never have newer versions than lower levels.
    pub fn get(&self, id: &DocumentId) -> Result<Option<IBlob>, LsmError> {
        // Check memtable first (fastest, always has latest version for a given _id)
        if let Some(blob) = self.memtable.get(id) {
            return Ok(if blob.is_deleted() { None } else { Some(blob) });
        }

        // Check SSTables (level-ordered: L0 first, within level newest first)
        let sstables = self.sstables.lock();
        for sst in sstables.iter() {
            if let Some(blob) = sst.get(id)? {
                return Ok(if blob.is_deleted() { None } else { Some(blob) });
            }
        }

        Ok(None)
    }

    /// Check if a document ID exists in the engine (not deleted).
    pub fn contains(&self, id: &DocumentId) -> Result<bool, LsmError> {
        Ok(self.get(id)?.is_some())
    }

    /// Flush the current memtable to a new SSTable.
    pub fn flush_memtable(&self) -> Result<(), LsmError> {
        let mut entries = self.memtable.drain();
        if entries.is_empty() {
            return Ok(());
        }

        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.config.data_dir.join(format!("{sst_id:012}.sst"));

        SSTableWriter::with_block_size(self.config.block_size).write(&sst_path, &mut entries)?;

        let reader = SSTableReader::open(&sst_path)?;

        {
            let mut sstables = self.sstables.lock();
            sstables.insert(0, reader); // L0, newest — goes to front
            // Re-sort to maintain level ordering
            let ucs = self.ucs();
            sort_sstables_by_level(&mut sstables, &ucs);
        }

        // Reset WAL
        {
            let wal = self.wal.lock();
            let wal_path = wal.path().to_path_buf();
            drop(wal);
            std::fs::remove_file(&wal_path).ok();
            *self.wal.lock() = Wal::open(&wal_path)?;
        }

        // Check if compaction is needed
        self.maybe_compact()?;

        Ok(())
    }

    /// Run UCS compaction if needed.
    fn maybe_compact(&self) -> Result<(), LsmError> {
        let ucs = self.ucs();
        let sstables = self.sstables.lock();

        let metas: Vec<SstMeta> = sstables
            .iter()
            .enumerate()
            .map(|(i, s)| SstMeta {
                path: s.path().to_path_buf(),
                min_id: *s.min_id(),
                max_id: *s.max_id(),
                file_size: s.file_size(),
                seq: i as u64,
            })
            .collect();
        drop(sstables);

        if let Some(pick) = ucs.pick_compaction(&metas) {
            let mut tombstone_filter = TombstoneFilter::new(pick.output_level, pick.max_level);
            self.run_compaction(&pick.inputs, Some(&mut tombstone_filter))?;
        }

        Ok(())
    }

    fn ucs(&self) -> UcsCompaction {
        UcsCompaction::new(self.config.scaling_parameter, self.config.memtable_size as u64)
    }

    /// Run compaction on the given SSTable files, optionally applying a filter.
    /// For duplicate `_id`s, the entry with the highest `_version` wins.
    fn run_compaction(
        &self,
        inputs: &[PathBuf],
        filter: Option<&mut dyn CompactionFilter>,
    ) -> Result<(), LsmError> {
        // Merge all input SSTables
        let mut merged: Vec<(DocumentId, IBlob)> = Vec::new();
        for path in inputs {
            let reader = SSTableReader::open(path)?;
            merged.extend(reader.iter()?);
        }

        // Sort by _id, then dedup: for same _id, keep the one with highest _version
        merged.sort_by(|(id_a, blob_a), (id_b, blob_b)| {
            id_a.cmp(id_b).then_with(|| blob_b.version().cmp(blob_a.version()))
        });
        merged.dedup_by_key(|(id, _)| *id);

        // Apply compaction filter (tombstone purge + any user filter)
        if let Some(filter) = filter {
            merged.retain_mut(|(id, blob)| match filter.filter(id, blob) {
                CompactionAction::Keep => true,
                CompactionAction::Drop => false,
                CompactionAction::Transform(new_blob) => {
                    *blob = new_blob;
                    true
                }
            });
        }

        if merged.is_empty() {
            // All entries were dropped — just remove input files
            let mut sstables = self.sstables.lock();
            for path in inputs {
                sstables.retain(|s| s.path() != path);
                std::fs::remove_file(path).ok();
            }
            return Ok(());
        }

        // Write merged SSTable
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let output_path = self.config.data_dir.join(format!("{sst_id:012}.sst"));
        SSTableWriter::with_block_size(self.config.block_size).write(&output_path, &mut merged)?;
        let new_reader = SSTableReader::open(&output_path)?;

        // Swap old SSTables for new one
        let mut sstables = self.sstables.lock();
        for path in inputs {
            sstables.retain(|s| s.path() != path);
            std::fs::remove_file(path).ok();
        }
        sstables.push(new_reader);

        // Re-sort by level for correct read ordering
        let ucs = self.ucs();
        sort_sstables_by_level(&mut sstables, &ucs);

        Ok(())
    }

    /// Number of SSTables on disk.
    pub fn sstable_count(&self) -> usize {
        self.sstables.lock().len()
    }

    /// Number of entries in the active memtable.
    pub fn memtable_size(&self) -> usize {
        self.memtable.len()
    }

    /// Force a sync of the WAL to disk.
    pub fn sync(&self) -> Result<(), LsmError> {
        self.wal.lock().sync()?;
        Ok(())
    }
}

/// Sort SSTables for correct read ordering:
/// L0 first → L1 → L2 → ..., within each level newest first (by filename/creation order).
///
/// This guarantees that for any `_id`, the first match is the current version.
fn sort_sstables_by_level(sstables: &mut Vec<SSTableReader>, ucs: &UcsCompaction) {
    sstables.sort_by(|a, b| {
        let level_a = ucs.level_for_size(a.file_size());
        let level_b = ucs.level_for_size(b.file_size());
        level_a.cmp(&level_b).then_with(|| {
            // Within same level, newest first (higher path = newer)
            b.path().cmp(a.path())
        })
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::Value;

    fn make_blob(n: u64) -> IBlob {
        IBlob::from_pairs(vec![
            ("n", Value::U64(n)),
            ("data", Value::String(format!("document-{n}"))),
        ])
    }

    fn test_engine() -> (LsmEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 4096, // small for testing
            block_size: 256,
            compaction_threshold: 4,
            scaling_parameter: 0,
        };
        let engine = LsmEngine::open(config).unwrap();
        (engine, dir)
    }

    #[test]
    fn test_put_and_get() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(1);
        let id = *blob.id();

        engine.put(blob.clone()).unwrap();
        let found = engine.get(&id).unwrap().unwrap();
        assert_eq!(found.id(), &id);
        assert_eq!(found.fields(), blob.fields());
        assert!(!found.version().is_nil(), "version should be stamped by engine");
    }

    #[test]
    fn test_get_missing() {
        let (engine, _dir) = test_engine();
        let missing = DocumentId::from_bytes([0xFF; 16]);
        assert!(engine.get(&missing).unwrap().is_none());
    }

    #[test]
    fn test_flush_and_read_from_sstable() {
        let (engine, _dir) = test_engine();

        let blobs: Vec<_> = (0..10).map(|i| make_blob(i)).collect();
        let ids: Vec<_> = blobs.iter().map(|b| *b.id()).collect();
        for b in &blobs {
            engine.put(b.clone()).unwrap();
        }

        // Force flush
        engine.flush_memtable().unwrap();
        assert_eq!(engine.memtable_size(), 0);
        assert!(engine.sstable_count() >= 1);

        // All blobs still retrievable from SSTable
        for (i, id) in ids.iter().enumerate() {
            let found = engine.get(id).unwrap().unwrap();
            assert_eq!(found.get("n"), Some(&Value::U64(i as u64)));
        }
    }

    #[test]
    fn test_recovery_from_wal() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024, // large enough to not auto-flush
            block_size: 256,
            compaction_threshold: 4,
            scaling_parameter: 0,
        };

        let blob = make_blob(42);
        let id = *blob.id();

        // Write and sync, then drop (simulating crash)
        {
            let engine = LsmEngine::open(config.clone()).unwrap();
            engine.put(blob).unwrap();
            engine.sync().unwrap();
        }

        // Reopen — should recover from WAL
        {
            let engine = LsmEngine::open(config).unwrap();
            let found = engine.get(&id).unwrap().unwrap();
            assert_eq!(found.id(), &id);
            assert!(!found.version().is_nil(), "recovered blob should have version");
        }
    }

    #[test]
    fn test_many_writes_trigger_flushes() {
        let (engine, _dir) = test_engine();

        let mut ids = Vec::new();
        for i in 0..100 {
            let blob = make_blob(i);
            ids.push(*blob.id());
            engine.put(blob).unwrap();
        }

        assert!(engine.sstable_count() >= 1, "expected at least one SSTable");

        // Verify all documents are retrievable
        for (i, id) in ids.iter().enumerate() {
            let found = engine.get(id).unwrap();
            assert!(found.is_some(), "blob {i} not found");
        }
    }

    #[test]
    fn test_upsert_version_advances() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        let blob1 = IBlob::with_id(id, [("x".into(), Value::U64(1))].into());
        engine.put(blob1).unwrap();
        let v1 = *engine.get(&id).unwrap().unwrap().version();

        let blob2 = IBlob::with_id(id, [("x".into(), Value::U64(2))].into());
        engine.put(blob2).unwrap();
        let found = engine.get(&id).unwrap().unwrap();
        let v2 = *found.version();

        assert!(v2 > v1, "version should advance on update");
        assert_eq!(found.get("x"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_delete_from_memtable() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(1);
        let id = *blob.id();

        engine.put(blob).unwrap();
        assert!(engine.get(&id).unwrap().is_some());

        engine.delete(&id).unwrap();
        assert!(engine.get(&id).unwrap().is_none());
        assert!(!engine.contains(&id).unwrap());
    }

    #[test]
    fn test_delete_from_sstable() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(1);
        let id = *blob.id();

        engine.put(blob).unwrap();
        engine.flush_memtable().unwrap();
        assert!(engine.get(&id).unwrap().is_some());

        engine.delete(&id).unwrap();
        assert!(engine.get(&id).unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        engine.delete(&id).unwrap();
        assert!(engine.get(&id).unwrap().is_none());
    }

    #[test]
    fn test_delete_and_reinsert() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        let blob1 = IBlob::with_id(id, [("x".into(), Value::U64(1))].into());
        engine.put(blob1).unwrap();
        engine.delete(&id).unwrap();
        assert!(engine.get(&id).unwrap().is_none());

        // Re-insert with same _id
        let blob2 = IBlob::with_id(id, [("x".into(), Value::U64(2))].into());
        engine.put(blob2).unwrap();
        let found = engine.get(&id).unwrap().unwrap();
        assert_eq!(found.get("x"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_level_ordering_invariant() {
        // Verify that after flush + update, we always get the latest version
        // even when the old version is in a higher-level (larger) SSTable
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024, // large enough to control flush manually
            block_size: 256,
            compaction_threshold: 100, // prevent auto-compaction
            scaling_parameter: 0,
        };
        let engine = LsmEngine::open(config).unwrap();
        let id = DocumentId::new();

        // Write v1, flush to SSTable (will be L0)
        let blob1 = IBlob::with_id(id, [("v".into(), Value::U64(1))].into());
        engine.put(blob1).unwrap();
        engine.flush_memtable().unwrap();

        // Write v2, flush to a second SSTable (also L0, but newer)
        let blob2 = IBlob::with_id(id, [("v".into(), Value::U64(2))].into());
        engine.put(blob2).unwrap();
        engine.flush_memtable().unwrap();

        // Should get v2, not v1
        let found = engine.get(&id).unwrap().unwrap();
        assert_eq!(found.get("v"), Some(&Value::U64(2)),
            "level-aware read should return newest version");
    }
}
