mod compaction;
mod database;
mod secondary;
pub mod stats;

pub use database::Database;

use ingodb_blob::{DocumentId, IBlob, Value};
use ingodb_memtable::MemTable;
use ingodb_query::{compare_values, Filter, Query, SortDirection, SortField};
use ingodb_sstable::{IdKeyExtractor, SSTableReader, SSTableWriter};
use ingodb_wal::Wal;
use stats::{extract_filter_fields, QueryPattern, QueryStats, QueryTimer};
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

    #[error("not implemented: {0}")]
    NotImplemented(String),
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
    /// Query statistics collector
    query_stats: QueryStats,
    /// Secondary indexes (sorted by non-_id fields)
    secondary_indexes: Mutex<Vec<secondary::SecondaryIndex>>,
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
            query_stats: QueryStats::new(),
            secondary_indexes: Mutex::new(Vec::new()),
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
        let mut timer = QueryTimer::start(QueryPattern {
            query_type: "get".into(),
            filter_fields: vec![],
            sort_fields: vec![],
            join_edge: None,
        });
        timer.set_docs_scanned(1);

        // Check memtable first (fastest, always has latest version for a given _id)
        if let Some(blob) = self.memtable.get(id) {
            let found = if blob.is_deleted() { None } else { Some(blob) };
            self.query_stats.record(timer.finish(if found.is_some() { 1 } else { 0 }));
            return Ok(found);
        }

        // Check SSTables (level-ordered: L0 first, within level newest first)
        let sstables = self.sstables.lock();
        for sst in sstables.iter() {
            if let Some(blob) = sst.get_by_id(id)? {
                let found = if blob.is_deleted() { None } else { Some(blob) };
                drop(sstables);
                self.query_stats.record(timer.finish(if found.is_some() { 1 } else { 0 }));
                return Ok(found);
            }
        }
        drop(sstables);

        self.query_stats.record(timer.finish(0));
        Ok(None)
    }

    /// Check if a document ID exists in the engine (not deleted).
    pub fn contains(&self, id: &DocumentId) -> Result<bool, LsmError> {
        Ok(self.get(id)?.is_some())
    }

    /// Flush the current memtable to a new SSTable.
    pub fn flush_memtable(&self) -> Result<(), LsmError> {
        let entries = self.memtable.drain();
        if entries.is_empty() {
            return Ok(());
        }

        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.config.data_dir.join(format!("{sst_id:012}.sst"));

        let mut blobs: Vec<IBlob> = entries.into_iter().map(|(_, blob)| blob).collect();
        SSTableWriter::with_block_size(self.config.block_size)
            .write(&sst_path, &mut blobs, &IdKeyExtractor)?;

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
                min_id: s.min_id(),
                max_id: s.max_id(),
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
        let mut merged: Vec<IBlob> = Vec::new();
        for path in inputs {
            let reader = SSTableReader::open(path)?;
            let entries = reader.iter()?;
            merged.extend(entries.into_iter().map(|(_, blob)| blob));
        }

        // Sort by _id, then dedup: for same _id, keep the one with highest _version
        merged.sort_by(|a, b| {
            a.id().cmp(b.id()).then_with(|| b.version().cmp(a.version()))
        });
        merged.dedup_by(|a, b| a.id() == b.id());

        // Apply compaction filter (tombstone purge + any user filter)
        if let Some(filter) = filter {
            merged.retain_mut(|blob| match filter.filter(blob.id(), blob) {
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
        SSTableWriter::with_block_size(self.config.block_size)
            .write(&output_path, &mut merged, &IdKeyExtractor)?;
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

    /// Access the query statistics collector.
    pub fn query_stats(&self) -> &QueryStats {
        &self.query_stats
    }

    /// Check if a secondary index exists for the given sort fields.
    fn has_secondary_index(&self, sort_fields: &[String]) -> bool {
        self.secondary_indexes.lock().iter().any(|idx| idx.matches_sort(sort_fields))
    }

    /// Build a secondary index for the given sort fields from current SSTables.
    fn build_secondary_index(&self, sort_fields: &[String]) -> Result<(), LsmError> {
        let sstables = self.sstables.lock();
        let sst_refs: Vec<&SSTableReader> = sstables.iter().collect();

        if sst_refs.is_empty() {
            return Ok(());
        }

        let idx_name = sort_fields.join("_");
        let idx_path = self.config.data_dir.join(format!("idx_{idx_name}.sst"));

        let index = secondary::SecondaryIndex::build(
            sort_fields,
            &sst_refs,
            &idx_path,
            self.config.block_size,
        )?;
        drop(sstables);

        self.secondary_indexes.lock().push(index);
        Ok(())
    }

    /// Try to use a secondary index for a sorted scan.
    /// Returns None if no matching index exists.
    fn scan_with_secondary_index(
        &self,
        sort_fields: &[String],
        filter: Option<&Filter>,
        limit: Option<usize>,
    ) -> Option<Result<Vec<IBlob>, LsmError>> {
        let indexes = self.secondary_indexes.lock();
        let index = indexes.iter().find(|idx| idx.matches_sort(sort_fields))?;

        // Read sorted entries from the index
        let sorted_entries = match index.iter_sorted() {
            Ok(entries) => entries,
            Err(e) => return Some(Err(e)),
        };
        drop(indexes);

        // For each entry, look up the full document by _id.
        // This handles stale entries: if the doc was deleted or updated,
        // get() returns the current version (or None).
        let mut results = Vec::new();
        for (id, _projected) in sorted_entries {
            match self.get(&id) {
                Ok(Some(blob)) => {
                    // Apply filter on the full document
                    if let Some(f) = filter {
                        if !f.matches(&|field| blob.get_field(field)) {
                            continue;
                        }
                    }
                    results.push(blob);
                    if let Some(lim) = limit {
                        if results.len() >= lim {
                            break;
                        }
                    }
                }
                Ok(None) => continue, // stale/deleted — skip
                Err(e) => return Some(Err(e)),
            }
        }

        // Merge with memtable (always fresh, may have docs not in the index)
        let memtable_docs: Vec<IBlob> = self.memtable.iter()
            .map(|(_, blob)| blob)
            .filter(|blob| !blob.is_deleted())
            .filter(|blob| {
                filter.map_or(true, |f| f.matches(&|field| blob.get_field(field)))
            })
            .collect();

        if !memtable_docs.is_empty() {
            // Merge memtable results into the sorted results
            // Deduplicate: memtable version wins (newer)
            let mut seen: std::collections::HashSet<DocumentId> = results.iter().map(|b| *b.id()).collect();

            // Remove entries from results that are superseded by memtable
            let memtable_ids: std::collections::HashSet<DocumentId> = memtable_docs.iter().map(|b| *b.id()).collect();
            results.retain(|b| !memtable_ids.contains(b.id()));

            // Add memtable docs and re-sort
            for doc in memtable_docs {
                if seen.insert(*doc.id()) {
                    results.push(doc);
                }
            }

            // Re-sort by the indexed fields
            let sort_field_list: Vec<SortField> = sort_fields.iter()
                .map(|f| SortField { field: f.clone(), direction: SortDirection::Ascending })
                .collect();
            results.sort_by(|a, b| {
                for sf in &sort_field_list {
                    let va = a.get_field(&sf.field);
                    let vb = b.get_field(&sf.field);
                    let ord = match (&va, &vb) {
                        (Some(va), Some(vb)) => compare_values(va, vb).unwrap_or(std::cmp::Ordering::Equal),
                        (Some(_), None) => std::cmp::Ordering::Less,
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });

            if let Some(lim) = limit {
                results.truncate(lim);
            }
        }

        Some(Ok(results))
    }

    /// Number of secondary indexes.
    pub fn secondary_index_count(&self) -> usize {
        self.secondary_indexes.lock().len()
    }

    /// Full scan: merge all live documents, apply filter/sort/projection/limit.
    pub fn scan(
        &self,
        filter: Option<&Filter>,
        sort: Option<&[SortField]>,
        project: Option<&[String]>,
        limit: Option<usize>,
    ) -> Result<Vec<IBlob>, LsmError> {
        let mut timer = QueryTimer::start(QueryPattern {
            query_type: "scan".into(),
            filter_fields: filter.map(extract_filter_fields).unwrap_or_default(),
            sort_fields: sort.map(|s| s.iter().map(|sf| sf.field.clone()).collect()).unwrap_or_default(),
            join_edge: None,
        });

        // Try secondary index for sorted scans (ascending only for now)
        if let Some(sort_fields) = sort {
            let ascending_fields: Vec<String> = sort_fields.iter()
                .filter(|sf| sf.direction == SortDirection::Ascending)
                .map(|sf| sf.field.clone())
                .collect();
            if ascending_fields.len() == sort_fields.len() {
                // All ascending — check for matching index
                if let Some(result) = self.scan_with_secondary_index(&ascending_fields, filter, limit) {
                    let mut results = result?;
                    // Apply projection
                    if let Some(fields) = project {
                        results = results.into_iter().map(|blob| blob.project(fields)).collect();
                    }
                    let docs_returned = results.len() as u64;
                    timer.set_docs_scanned(docs_returned); // index scan only touches returned docs
                    self.query_stats.record(timer.finish(docs_returned));
                    return Ok(results);
                }
            }
        }

        // Collect all IBlobs from memtable + SSTables
        let mut all: Vec<IBlob> = Vec::new();

        // Memtable entries (newest versions)
        all.extend(self.memtable.iter().map(|(_, blob)| blob));

        // SSTable entries
        {
            let sstables = self.sstables.lock();
            for sst in sstables.iter() {
                all.extend(sst.iter()?.into_iter().map(|(_, blob)| blob));
            }
        }

        // Merge: sort by _id, dedup keeping highest _version
        all.sort_by(|a, b| {
            a.id().cmp(b.id()).then_with(|| b.version().cmp(a.version()))
        });
        all.dedup_by(|a, b| a.id() == b.id());

        // Count live documents (docs scanned = after dedup, excluding tombstones)
        let docs_scanned = all.iter().filter(|b| !b.is_deleted()).count() as u64;
        timer.set_docs_scanned(docs_scanned);

        // Filter out tombstones and apply query filter
        let mut results: Vec<IBlob> = Vec::new();
        for blob in all {
            if blob.is_deleted() {
                continue;
            }
            if let Some(f) = filter {
                if !f.matches(&|field| blob.get_field(field)) {
                    continue;
                }
            }
            results.push(blob);
        }

        // Sort (before projection — sort fields may not be in the projection)
        if let Some(sort_fields) = sort {
            results.sort_by(|a, b| {
                for sf in sort_fields {
                    let va = a.get_field(&sf.field);
                    let vb = b.get_field(&sf.field);
                    let ord = match (&va, &vb) {
                        (Some(va), Some(vb)) => {
                            compare_values(va, vb).unwrap_or(std::cmp::Ordering::Equal)
                        }
                        (Some(_), None) => std::cmp::Ordering::Less,   // non-null first
                        (None, Some(_)) => std::cmp::Ordering::Greater,
                        (None, None) => std::cmp::Ordering::Equal,
                    };
                    let ord = match sf.direction {
                        SortDirection::Ascending => ord,
                        SortDirection::Descending => ord.reverse(),
                    };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply limit (after sort)
        if let Some(lim) = limit {
            results.truncate(lim);
        }

        // Apply projection (last — sort fields may not be projected)
        if let Some(fields) = project {
            results = results.into_iter().map(|blob| blob.project(fields)).collect();
        }

        // Record stats
        let docs_returned = results.len() as u64;
        self.query_stats.record(timer.finish(docs_returned));

        // Reactive: consider building a secondary index for this sort pattern
        if let Some(sort_fields) = sort {
            let ascending_fields: Vec<String> = sort_fields.iter()
                .filter(|sf| sf.direction == SortDirection::Ascending)
                .map(|sf| sf.field.clone())
                .collect();
            if ascending_fields.len() == sort_fields.len()
                && !self.has_secondary_index(&ascending_fields)
            {
                // Check if this pattern has been executed enough times
                let pattern = QueryPattern {
                    query_type: "scan".into(),
                    filter_fields: filter.map(extract_filter_fields).unwrap_or_default(),
                    sort_fields: ascending_fields.clone(),
                    join_edge: None,
                };
                if let Some(stats) = self.query_stats.get_pattern(&pattern) {
                    if stats.count >= secondary::DEFAULT_INDEX_THRESHOLD {
                        // Build reactively — ignore errors (best effort)
                        let _ = self.build_secondary_index(&ascending_fields);
                    }
                }
            }
        }

        Ok(results)
    }

    /// Graph traversal as join-by-value.
    ///
    /// Starting from documents matching `start` filter, follows edges by joining
    /// `from_field` values against `to_field` values. Returns the discovered
    /// documents (not the starting set). Deduplicates by `_id`.
    pub fn traverse(
        &self,
        start: Option<&Filter>,
        from_field: &str,
        to_field: &str,
        depth: usize,
    ) -> Result<Vec<IBlob>, LsmError> {
        let timer = QueryTimer::start(QueryPattern {
            query_type: "traverse".into(),
            filter_fields: start.map(extract_filter_fields).unwrap_or_default(),
            sort_fields: vec![],
            join_edge: Some((from_field.into(), to_field.into())),
        });

        if depth == 0 {
            self.query_stats.record(timer.finish(0));
            return Ok(Vec::new());
        }

        // Get starting documents (inner scan records its own stats)
        let mut current = self.scan(start, None, None, None)?;
        let mut all_results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for _ in 0..depth {
            // Collect join keys from current documents
            let join_values: Vec<Value> = current
                .iter()
                .filter_map(|blob| blob.get_field(from_field))
                .collect();

            if join_values.is_empty() {
                break;
            }

            // Find target documents where to_field matches any join value
            let mut next = Vec::new();
            let candidates = self.scan(None, None, None, None)?;
            for doc in candidates {
                if let Some(target_val) = doc.get_field(to_field) {
                    if join_values.contains(&target_val) && seen.insert(*doc.id()) {
                        next.push(doc);
                    }
                }
            }

            all_results.extend(next.iter().cloned());
            current = next;

            if current.is_empty() {
                break;
            }
        }

        let docs_returned = all_results.len() as u64;
        self.query_stats.record(timer.finish(docs_returned));
        Ok(all_results)
    }

    /// Execute a Liquid AST query.
    pub fn execute(&self, query: &Query) -> Result<Vec<IBlob>, LsmError> {
        match query {
            Query::Get { id } => {
                Ok(self.get(id)?.into_iter().collect())
            }
            Query::Scan { filter, sort, project, limit } => {
                self.scan(
                    filter.as_ref(),
                    sort.as_deref(),
                    project.as_deref(),
                    *limit,
                )
            }
            Query::Traverse { start, from_field, to_field, depth } => {
                self.traverse(start.as_ref(), from_field, to_field, *depth)
            }
        }
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

    #[test]
    fn test_scan_all() {
        let (engine, _dir) = test_engine();
        for i in 0..5 {
            engine.put(make_blob(i)).unwrap();
        }
        let results = engine.scan(None, None, None, None).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_scan_with_filter() {
        let (engine, _dir) = test_engine();
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        let filter = Filter::Gt {
            field: "n".into(),
            value: Value::U64(6),
        };
        let results = engine.scan(Some(&filter), None, None, None).unwrap();
        assert_eq!(results.len(), 3); // n=7,8,9
        for r in &results {
            if let Some(Value::U64(n)) = r.get("n") {
                assert!(*n > 6);
            }
        }
    }

    #[test]
    fn test_scan_with_limit() {
        let (engine, _dir) = test_engine();
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        let results = engine.scan(None, None, None, Some(3)).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_scan_with_projection() {
        let (engine, _dir) = test_engine();
        engine.put(make_blob(1)).unwrap();

        let results = engine.scan(None, None, Some(&["n".into()]), None).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_projection());
        assert_eq!(results[0].field_count(), 1);
        assert_eq!(results[0].get("n"), Some(&Value::U64(1)));
        assert!(results[0].get("data").is_none());
    }

    #[test]
    fn test_scan_skips_tombstones() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(1);
        let id = *blob.id();
        engine.put(blob).unwrap();
        engine.put(make_blob(2)).unwrap();

        engine.delete(&id).unwrap();

        let results = engine.scan(None, None, None, None).unwrap();
        assert_eq!(results.len(), 1, "deleted doc not in scan results");
    }

    #[test]
    fn test_execute_get() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(42);
        let id = *blob.id();
        engine.put(blob).unwrap();

        let results = engine.execute(&Query::Get { id }).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("n"), Some(&Value::U64(42)));
    }

    #[test]
    fn test_execute_scan() {
        let (engine, _dir) = test_engine();
        for i in 0..5 {
            engine.put(make_blob(i)).unwrap();
        }
        let results = engine.execute(&Query::Scan {
            filter: Some(Filter::Lt { field: "n".into(), value: Value::U64(3) }),
            sort: None,
            project: None,
            limit: None,
        }).unwrap();
        assert_eq!(results.len(), 3); // n=0,1,2
    }

    #[test]
    fn test_traverse_simple_join() {
        let (engine, _dir) = test_engine();

        // Create users
        let user1 = IBlob::from_pairs(vec![
            ("type", Value::String("user".into())),
            ("name", Value::String("Henrik".into())),
        ]);
        let user1_id = *user1.id();
        engine.put(user1).unwrap();

        let user2 = IBlob::from_pairs(vec![
            ("type", Value::String("user".into())),
            ("name", Value::String("Alice".into())),
        ]);
        engine.put(user2).unwrap();

        // Create orders referencing users by _id
        engine.put(IBlob::from_pairs(vec![
            ("type", Value::String("order".into())),
            ("user_id", Value::Uuid(user1_id)),
            ("amount", Value::U64(100)),
        ])).unwrap();

        // Traverse: from orders, join user_id -> _id to find referenced users
        let results = engine.traverse(
            Some(&Filter::Eq { field: "type".into(), value: Value::String("order".into()) }),
            "user_id",
            "_id",
            1,
        ).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("name"), Some(&Value::String("Henrik".into())));
    }

    #[test]
    fn test_traverse_non_unique_join() {
        let (engine, _dir) = test_engine();

        // Two users named Henrik
        for i in 0..2 {
            engine.put(IBlob::from_pairs(vec![
                ("type", Value::String("user".into())),
                ("name", Value::String("Henrik".into())),
                ("seq", Value::U64(i)),
            ])).unwrap();
        }

        // An order referencing "Henrik" by name
        engine.put(IBlob::from_pairs(vec![
            ("type", Value::String("order".into())),
            ("user_name", Value::String("Henrik".into())),
        ])).unwrap();

        // Traverse: orders.user_name -> users.name (non-unique — should find both)
        let results = engine.traverse(
            Some(&Filter::Eq { field: "type".into(), value: Value::String("order".into()) }),
            "user_name",
            "name",
            1,
        ).unwrap();
        assert_eq!(results.len(), 2, "non-unique join should find all matches");
    }

    #[test]
    fn test_traverse_depth_2() {
        let (engine, _dir) = test_engine();

        // company -> department -> employee chain
        let company = IBlob::from_pairs(vec![
            ("type", Value::String("company".into())),
            ("name", Value::String("Nyrkio".into())),
        ]);
        let company_id = *company.id();
        engine.put(company).unwrap();

        let dept = IBlob::from_pairs(vec![
            ("type", Value::String("dept".into())),
            ("company_id", Value::Uuid(company_id)),
            ("name", Value::String("Engineering".into())),
        ]);
        let dept_id = *dept.id();
        engine.put(dept).unwrap();

        let emp = IBlob::from_pairs(vec![
            ("type", Value::String("employee".into())),
            ("dept_id", Value::Uuid(dept_id)),
            ("name", Value::String("Henrik".into())),
        ]);
        engine.put(emp).unwrap();

        // Depth 1: company -> departments (join company _id -> dept.company_id)
        let depts = engine.traverse(
            Some(&Filter::Eq { field: "type".into(), value: Value::String("company".into()) }),
            "_id",
            "company_id",
            1,
        ).unwrap();
        assert_eq!(depts.len(), 1);
        assert_eq!(depts[0].get("name"), Some(&Value::String("Engineering".into())));

        // Depth 2: company -> dept -> employees (same edge pattern repeated)
        // For depth>1 with the same edge, we need the same from/to fields to chain.
        // Here the chain is: company._id -> dept.company_id at hop 1,
        // then dept._id -> emp.dept_id at hop 2... but that's DIFFERENT edges.
        // Depth>1 with same edge only works for self-referential graphs.
        // For now, depth>1 repeats the same edge. So let's test that:

        // Self-referential: manager chain
        let ceo = IBlob::from_pairs(vec![
            ("role", Value::String("CEO".into())),
            ("name", Value::String("Boss".into())),
        ]);
        let ceo_id = *ceo.id();
        engine.put(ceo).unwrap();

        let vp = IBlob::from_pairs(vec![
            ("role", Value::String("VP".into())),
            ("name", Value::String("Manager".into())),
            ("reports_to", Value::Uuid(ceo_id)),
        ]);
        let vp_id = *vp.id();
        engine.put(vp).unwrap();

        let dev = IBlob::from_pairs(vec![
            ("role", Value::String("Dev".into())),
            ("name", Value::String("Coder".into())),
            ("reports_to", Value::Uuid(vp_id)),
        ]);
        engine.put(dev).unwrap();

        // From dev, follow reports_to -> _id, depth 2
        let chain = engine.traverse(
            Some(&Filter::Eq { field: "role".into(), value: Value::String("Dev".into()) }),
            "reports_to",
            "_id",
            2,
        ).unwrap();
        assert_eq!(chain.len(), 2, "depth 2 should find VP and CEO");
    }

    #[test]
    fn test_traverse_no_matches() {
        let (engine, _dir) = test_engine();
        engine.put(make_blob(1)).unwrap();

        let results = engine.traverse(
            Some(&Filter::Eq { field: "n".into(), value: Value::U64(1) }),
            "nonexistent_field",
            "_id",
            1,
        ).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_traverse_depth_zero() {
        let (engine, _dir) = test_engine();
        engine.put(make_blob(1)).unwrap();

        let results = engine.traverse(None, "_id", "_id", 0).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_filter_on_id() {
        let (engine, _dir) = test_engine();
        let blob = make_blob(42);
        let id = *blob.id();
        engine.put(blob).unwrap();
        engine.put(make_blob(1)).unwrap();

        // Scan filtering on _id
        let results = engine.scan(
            Some(&Filter::Eq { field: "_id".into(), value: Value::Uuid(id) }),
            None,
            None,
            None,
        ).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].get("n"), Some(&Value::U64(42)));
    }

    #[test]
    fn test_scan_sort_ascending() {
        let (engine, _dir) = test_engine();
        engine.put(make_blob(30)).unwrap();
        engine.put(make_blob(10)).unwrap();
        engine.put(make_blob(20)).unwrap();

        let results = engine.scan(
            None,
            Some(&[SortField { field: "n".into(), direction: SortDirection::Ascending }]),
            None,
            None,
        ).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![10, 20, 30]);
    }

    #[test]
    fn test_scan_sort_descending() {
        let (engine, _dir) = test_engine();
        engine.put(make_blob(30)).unwrap();
        engine.put(make_blob(10)).unwrap();
        engine.put(make_blob(20)).unwrap();

        let results = engine.scan(
            None,
            Some(&[SortField { field: "n".into(), direction: SortDirection::Descending }]),
            None,
            None,
        ).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![30, 20, 10]);
    }

    #[test]
    fn test_scan_sort_with_limit() {
        let (engine, _dir) = test_engine();
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        // Sort descending, take top 3
        let results = engine.scan(
            None,
            Some(&[SortField { field: "n".into(), direction: SortDirection::Descending }]),
            None,
            Some(3),
        ).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![9, 8, 7]);
    }

    #[test]
    fn test_scan_sort_with_filter_and_projection() {
        let (engine, _dir) = test_engine();
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        // Filter n > 5, sort ascending, project only "n"
        let filter = Filter::Gt { field: "n".into(), value: Value::U64(5) };
        let results = engine.scan(
            Some(&filter),
            Some(&[SortField { field: "n".into(), direction: SortDirection::Ascending }]),
            Some(&["n".into()]),
            None,
        ).unwrap();
        assert_eq!(results.len(), 4); // 6,7,8,9
        assert!(results[0].is_projection());
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![6, 7, 8, 9]);
    }

    #[test]
    fn test_reactive_index_creation() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
        };
        let engine = LsmEngine::open(config).unwrap();

        // Insert docs and flush to SSTable (index only covers on-disk data)
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        assert_eq!(engine.secondary_index_count(), 0, "no index yet");

        // Run sorted scan DEFAULT_INDEX_THRESHOLD times to trigger reactive index
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        for _ in 0..secondary::DEFAULT_INDEX_THRESHOLD {
            engine.scan(None, Some(&sort), None, None).unwrap();
        }

        assert_eq!(engine.secondary_index_count(), 1, "index should be created reactively");

        // Next scan should use the index (and produce correct results)
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, (0..10).collect::<Vec<u64>>());
    }

    #[test]
    fn test_no_index_below_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
        };
        let engine = LsmEngine::open(config).unwrap();

        for i in 0..5 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Run sorted scan fewer than threshold times
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        for _ in 0..secondary::DEFAULT_INDEX_THRESHOLD - 1 {
            engine.scan(None, Some(&sort), None, None).unwrap();
        }

        assert_eq!(engine.secondary_index_count(), 0, "should not build index below threshold");
    }

    #[test]
    fn test_index_handles_stale_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
        };
        let engine = LsmEngine::open(config).unwrap();

        let blob1 = make_blob(1);
        let blob2 = make_blob(2);
        let id_to_delete = *blob1.id();
        engine.put(blob1).unwrap();
        engine.put(blob2).unwrap();
        engine.flush_memtable().unwrap();

        // Build index
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        for _ in 0..secondary::DEFAULT_INDEX_THRESHOLD {
            engine.scan(None, Some(&sort), None, None).unwrap();
        }
        assert_eq!(engine.secondary_index_count(), 1);

        // Delete one doc after index was built
        engine.delete(&id_to_delete).unwrap();

        // Scan with index should skip the deleted doc
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(results.len(), 1, "stale entry should be skipped");
        assert_eq!(results[0].get("n"), Some(&Value::U64(2)));
    }
}
