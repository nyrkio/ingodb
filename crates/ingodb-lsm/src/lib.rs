mod compaction;
mod database;
mod equality;
mod secondary;
pub mod unsorted;
pub mod stats;

pub use database::Database;

use ingodb_blob::{DocumentId, IBlob, Value};
use ingodb_consistency::{Consistency, ConsistencyModel, Scope};
use ingodb_memtable::MemTable;
use ingodb_query::{compare_values, Filter, Query, SortDirection, SortField};
use ingodb_sstable::{MvccKeyExtractor, SSTableReader, SSTableWriter};
use ingodb_wal::Wal;
use stats::{extract_filter_fields, QueryPattern, QueryStats, QueryTimer};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
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

    #[error("requested consistency {0:?} cannot be provided by this engine: {1}")]
    UnsupportedConsistency(Consistency, &'static str),
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
    /// Max materialized ranges kept per field — sorted partials and unsorted
    /// blocks share this one unified LRU budget (default 500).
    pub max_ranges_per_field: usize,
    /// Number of background compaction threads (default 4)
    pub compaction_threads: usize,
    /// Enable adaptive W (auto-tune scaling parameter from read/write ratio)
    pub adaptive_w: bool,
    /// Minimum seconds between W adjustments (default 900 = 15 minutes)
    pub adaptive_w_cooldown_secs: u64,
    /// Maximum W change per adjustment (default 2)
    pub adaptive_w_max_step: i32,
    /// Minimum W value (default -8)
    pub adaptive_w_min: i32,
    /// Maximum W value (default 8)
    pub adaptive_w_max: i32,
    /// Minimum consistency level provided by `put()`. Default: `Consistency::default()`
    /// (single-node, no guarantees) — matches historical optimistic behavior.
    ///
    /// To get read-your-writes (and fix the small RYW race in optimistic mode),
    /// use `Consistency::single_node(ConsistencyModel::LINEARIZABLE)`.
    /// To additionally fsync per batch, use `STRICT_LINEARIZABLE`.
    /// Cluster scope is currently unsupported and errors at `open()`.
    pub min_consistency: Consistency,
    /// Group-commit batch-growing knob: in Durable mode, if a freshly-drained
    /// batch is smaller than `commit_wait_count`, the leader holds leadership
    /// and waits up to this many microseconds to gather more arrivals into
    /// the same fsync. Trades per-op latency for higher batch utilization.
    /// `0` disables (default); typical values 100–1000.
    pub commit_wait_usec: u64,
    /// Above this many entries in the batch, skip the wait and fsync now.
    /// `0` means "always wait the full `commit_wait_usec`". Effective only
    /// when `commit_wait_usec > 0`.
    pub commit_wait_count: usize,
    /// Group-commit "busy mode" (default: `true`). When enabled, the
    /// Durable leader starts with wait=0 (quiet mode). On the first
    /// fsync that durabilizes >= `num_cpus × 8` ops, the booster trips
    /// on permanently and the leader uses a 100 µs wait per batch.
    /// One-way switch.
    ///
    /// If `commit_wait_usec > 0` is explicitly configured, that static
    /// value overrides busy mode entirely.
    ///
    /// Intent: near-free default that captures peak throughput under
    /// real concurrency without paying the wait cost at low concurrency.
    pub commit_busy_mode: bool,
}

impl Default for LsmConfig {
    fn default() -> Self {
        LsmConfig {
            data_dir: PathBuf::from("ingodb_data"),
            memtable_size: 64 * 1024 * 1024,
            block_size: 4096,
            compaction_threshold: 4,
            scaling_parameter: 0,
            max_ranges_per_field: secondary::MAX_RANGES_PER_FIELD,
            compaction_threads: 4,
            adaptive_w: false,
            adaptive_w_cooldown_secs: 900, // 15 minutes
            adaptive_w_max_step: 2,
            adaptive_w_min: -8,
            adaptive_w_max: 8,
            min_consistency: Consistency::default(),
            commit_wait_usec: 0,
            commit_wait_count: 0,
            commit_busy_mode: true,
        }
    }
}

/// A single pending write in the group-commit queue. Used for Visible and
/// Durable modes — Optimistic mode uses a simpler `Vec<IBlob>` instead.
///
/// `blob` is consumed by the leader (taken via `Option::take`) before being
/// inserted into the memtable. `done` is signaled by the leader once the
/// caller's contract is met (memtable-visible, fsync'd if Durable).
struct CommitEntry {
    blob: Option<IBlob>,
    done: Arc<(Mutex<bool>, Condvar)>,
}

/// The group-commit queue. `leader_active` is set atomically with a push:
/// the thread that flips it from false to true becomes the batch leader,
/// later arrivals are followers. The leader resets it to false only while
/// holding this mutex AND observing the entries vec is empty — that atomic
/// check-and-release is what prevents follower entries from being orphaned.
struct CommitQueue {
    entries: Vec<CommitEntry>,
    leader_active: bool,
}

impl CommitQueue {
    fn new() -> Self {
        CommitQueue {
            entries: Vec::new(),
            leader_active: false,
        }
    }
}

/// How `put()` returns to the caller. Derived from [`LsmConfig::min_consistency`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitMode {
    /// Followers return immediately after pushing their blob to the batch.
    /// Used when the requested consistency model is empty.
    /// Smallest latency; small RYW race possible if a concurrent thread reads
    /// just after a follower returns.
    Optimistic,
    /// Followers wait until the leader has fsync'd AND inserted into the memtable.
    /// Provides `single_node(STRICT_LINEARIZABLE)`.
    Durable,
    /// Followers wait until the leader has inserted into the memtable
    /// (no fsync). Provides `single_node(LINEARIZABLE)`.
    Visible,
}

impl CommitMode {
    /// Map a requested consistency level to the cheapest commit mode that satisfies it.
    /// Returns an error if the requested level can't be provided on a single node.
    fn select(min: Consistency) -> Result<CommitMode, LsmError> {
        if min.scope == Scope::Cluster && !min.model.is_empty() {
            return Err(LsmError::UnsupportedConsistency(
                min,
                "cluster scope requires replication, not implemented",
            ));
        }
        Ok(if min.model.is_empty() {
            CommitMode::Optimistic
        } else if min.model.contains(ConsistencyModel::DURABLE) {
            CommitMode::Durable
        } else {
            CommitMode::Visible
        })
    }
}

/// Shared state for background compaction signaling.
struct CompactionSignal {
    /// True when compaction work may be available
    pending: Mutex<bool>,
    /// Notifies the background thread
    notify: Condvar,
    /// True when a compaction is currently running
    running: AtomicBool,
    /// Notifies waiters that compaction finished
    done: Condvar,
    done_mutex: Mutex<()>,
    /// Signal background thread to stop
    stop: AtomicBool,
}

/// Compaction statistics — tracks what compaction has done.
pub struct CompactionStats {
    /// Number of compaction runs completed
    pub runs: AtomicU64,
    /// Total input bytes read during compaction
    pub bytes_read: AtomicU64,
    /// Total output bytes written during compaction
    pub bytes_written: AtomicU64,
    /// Total input SSTables consumed
    pub sstables_read: AtomicU64,
    /// Total output SSTables produced
    pub sstables_written: AtomicU64,
}

impl CompactionStats {
    fn new() -> Self {
        CompactionStats {
            runs: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            sstables_read: AtomicU64::new(0),
            sstables_written: AtomicU64::new(0),
        }
    }

    /// Write amplification: bytes_written / bytes_read. Higher = more rewriting.
    pub fn write_amplification(&self) -> f64 {
        let r = self.bytes_read.load(Ordering::Relaxed);
        let w = self.bytes_written.load(Ordering::Relaxed);
        if r == 0 { 0.0 } else { w as f64 / r as f64 }
    }
}

/// The LSM storage engine. Ties together WAL, MemTable, and SSTables.
pub struct LsmEngine {
    config: LsmConfig,
    /// Active memtable receiving writes (swapped out when full)
    memtable: RwLock<MemTable>,
    /// Immutable memtables queued for flush (newest first)
    immutable_memtables: Mutex<Vec<MemTable>>,
    /// WAL for the active memtable
    wal: Mutex<Wal>,
    /// SSTables on disk, ordered for reads: L0 first → L1 → ..., within each level newest first.
    /// RwLock: multiple concurrent readers, exclusive access for flush/compaction.
    sstables: RwLock<Vec<SSTableReader>>,
    /// Counter for generating SSTable file names
    next_sst_id: AtomicU64,
    /// Query statistics collector
    query_stats: QueryStats,
    /// Secondary indexes (sorted by non-_id fields)
    secondary_indexes: Mutex<Vec<secondary::SecondaryIndex>>,
    /// Unsorted (materialized-subset) indexes, keyed by field. A reactive cache
    /// of range-scan results; cold after restart in v1.
    unsorted_indexes: Mutex<HashMap<String, unsorted::UnsortedIndex>>,
    /// Directory holding unsorted block SSTables (kept out of the primary
    /// SSTable load path, which only scans the top-level data dir).
    unsorted_dir: PathBuf,
    /// Count of scans served from an unsorted block (observability / tests).
    unsorted_hits: AtomicU64,
    /// Count of scans served from a sorted (secondary/partial) index.
    sorted_hits: AtomicU64,
    /// Lazy equality (Eq/In) index: per-SSTable inverted postings of `_id`s,
    /// built on read, verified against the main collection. Dropped per SSTable
    /// at compaction. See `docs/equality-index.md`.
    equality_postings: Mutex<equality::EqualityPostings>,
    /// Count of Eq/In scans served warm (entirely from cached `Exact` postings,
    /// no SSTable rescan) — observability / tests.
    equality_hits: AtomicU64,
    /// Newly built indexes awaiting persistence (collection_name not known here — Database handles it)
    pending_index_metadata: Mutex<Vec<IndexMetadata>>,
    /// Active snapshot versions — compaction preserves versions >= oldest snapshot
    active_snapshots: Mutex<BTreeSet<DocumentId>>,
    /// Group commit: queue of pending writes from concurrent put() calls.
    /// Used by Visible and Durable modes; Optimistic mode uses the legacy
    /// fast path with `optimistic_batch`.
    commit_queue: Mutex<CommitQueue>,
    /// Optimistic mode: pending blobs without follower-wait state.
    optimistic_batch: Mutex<Vec<IBlob>>,
    /// Derived from `config.min_consistency` at open() time.
    commit_mode: CommitMode,
    /// Group-commit busy-mode flag. When set, the Durable leader uses a
    /// 100 µs wait per batch. Toggled by `maybe_toggle_busy` after each
    /// fsync — trips on when ops/fsync ≥ `commit_busy_threshold` (upward
    /// hysteresis), off after 3 consecutive fsyncs with ops/fsync below
    /// `commit_busy_threshold / 4` (downward hysteresis).
    commit_busy_active: AtomicBool,
    /// `num_cpus × 8` cached at `open()`. Upward threshold for busy mode.
    /// Downward threshold is `commit_busy_threshold / 4` (= num_cpus × 2).
    commit_busy_threshold: usize,
    /// Consecutive count of fsyncs with ops below the downward threshold.
    /// Reset whenever a fsync covers ≥ down-threshold ops. Busy mode
    /// turns off when this reaches 3.
    consecutive_quiet_fsyncs: AtomicUsize,
    /// Monotonic counter of ops appended to the WAL. Bumped under the WAL
    /// lock right after each append. Used by busy mode to compute
    /// "ops made durable per fsync" — which is larger than entries.len()
    /// of a single leader's batch because fsyncs piggyback on other
    /// concurrent leaders' appends.
    ops_appended: AtomicU64,
    /// Monotonic high-water mark of ops known durably committed. Updated
    /// after each fsync via fetch_max with the appended snapshot.
    ops_durable: AtomicU64,
    /// Background compaction signaling
    compaction_signal: Arc<CompactionSignal>,
    /// Background compaction thread handle
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Compaction statistics
    compaction_stats: CompactionStats,
    /// Current effective W (may differ from config if adaptive_w is on)
    effective_w: std::sync::atomic::AtomicI32,
    /// Target W without step limiting (what W would be if we could jump instantly)
    target_w: std::sync::atomic::AtomicI32,
    /// Read operation counter (for adaptive W)
    read_count: AtomicU64,
    /// Write operation counter (for adaptive W)
    write_count: AtomicU64,
    /// Last time W was adjusted
    last_w_adjustment: Mutex<Instant>,
}

/// A consistent point-in-time view of the database.
///
/// All reads through a Snapshot see only documents with `_version <= self.version`.
/// Old versions are retained by compaction while any Snapshot references them.
pub struct Snapshot<'a> {
    engine: &'a LsmEngine,
    version: DocumentId,
}

impl<'a> Snapshot<'a> {
    /// Point lookup at this snapshot's point in time.
    pub fn get(&self, id: &DocumentId) -> Result<Option<IBlob>, LsmError> {
        self.engine.get_at(id, &self.version)
    }

    /// Scan at this snapshot's point in time.
    pub fn scan(
        &self,
        filter: Option<&Filter>,
        sort: Option<&[SortField]>,
        project: Option<&[String]>,
        limit: Option<usize>,
    ) -> Result<Vec<IBlob>, LsmError> {
        self.engine.scan_at(filter, sort, project, limit, &self.version)
    }

    /// The snapshot version.
    pub fn version(&self) -> &DocumentId {
        &self.version
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        self.engine.active_snapshots.lock().remove(&self.version);
    }
}

/// Metadata about a secondary index, for persistence in the system collection.
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// Fields the index covers
    pub fields: Vec<String>,
    /// Path to the index SSTable file
    pub path: PathBuf,
    /// The range the index covers (`None` = full range). Persisted so partial
    /// coverage survives restart.
    pub range: Option<Filter>,
}

impl LsmEngine {
    /// Open or create an LSM engine at the given directory.
    pub fn open(config: LsmConfig) -> Result<Self, LsmError> {
        let commit_mode = CommitMode::select(config.min_consistency)?;
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

        let initial_w = config.scaling_parameter;
        let ucs = UcsCompaction::new(initial_w, config.memtable_size as u64);
        sort_sstables_by_level(&mut sstables, &ucs);

        // Unsorted-index block files live in a subdirectory so they are never
        // picked up by the (non-recursive) primary SSTable loader above.
        // v1 treats unsorted blocks as a cold cache: clear on open.
        let unsorted_dir = config.data_dir.join("unsorted");
        std::fs::remove_dir_all(&unsorted_dir).ok();
        std::fs::create_dir_all(&unsorted_dir)?;

        Ok(LsmEngine {
            config,
            memtable: RwLock::new(memtable),
            immutable_memtables: Mutex::new(Vec::new()),
            wal: Mutex::new(wal),
            sstables: RwLock::new(sstables),
            next_sst_id: AtomicU64::new(max_id + 1),
            query_stats: QueryStats::new(),
            secondary_indexes: Mutex::new(Vec::new()),
            unsorted_indexes: Mutex::new(HashMap::new()),
            unsorted_dir,
            unsorted_hits: AtomicU64::new(0),
            sorted_hits: AtomicU64::new(0),
            equality_postings: Mutex::new(equality::EqualityPostings::new(
                equality::EQUALITY_RAM_BUDGET_BYTES,
            )),
            equality_hits: AtomicU64::new(0),
            pending_index_metadata: Mutex::new(Vec::new()),
            active_snapshots: Mutex::new(BTreeSet::new()),
            commit_queue: Mutex::new(CommitQueue::new()),
            optimistic_batch: Mutex::new(Vec::new()),
            commit_mode,
            commit_busy_active: AtomicBool::new(false),
            commit_busy_threshold: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .saturating_mul(8),
            consecutive_quiet_fsyncs: AtomicUsize::new(0),
            ops_appended: AtomicU64::new(0),
            ops_durable: AtomicU64::new(0),
            compaction_signal: Arc::new(CompactionSignal {
                pending: Mutex::new(false),
                notify: Condvar::new(),
                running: AtomicBool::new(false),
                done: Condvar::new(),
                done_mutex: Mutex::new(()),
                stop: AtomicBool::new(false),
            }),
            compaction_thread: Mutex::new(None),
            compaction_stats: CompactionStats::new(),
            effective_w: std::sync::atomic::AtomicI32::new(initial_w),
            target_w: std::sync::atomic::AtomicI32::new(initial_w),
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            last_w_adjustment: Mutex::new(Instant::now()),
        })
    }

    /// Insert a document into the engine.
    ///
    /// Concurrent `put()` calls batch their WAL writes via group commit.
    /// The exact return semantics depend on [`LsmConfig::min_consistency`]:
    ///
    /// - `Optimistic` (default, empty consistency): returns once the blob is
    ///   queued; the leader writes the WAL asynchronously. A small RYW race
    ///   exists for concurrent readers.
    /// - `Visible` (`LINEARIZABLE`): returns only after the leader has
    ///   inserted into the memtable.
    /// - `Durable` (`STRICT_LINEARIZABLE`): returns only after the leader has
    ///   fsync'd the WAL AND inserted into the memtable.
    pub fn put(&self, mut blob: IBlob) -> Result<(), LsmError> {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        blob.set_version(DocumentId::new());

        match self.commit_mode {
            CommitMode::Optimistic => self.put_optimistic(blob),
            CommitMode::Visible | CommitMode::Durable => self.put_with_wait(blob),
        }
    }

    /// Optimistic group-commit path: followers return as soon as the WAL lock
    /// is free, regardless of whether their blob has reached the memtable yet.
    /// Matches historical behavior; preserved for throughput when callers
    /// don't need RYW.
    fn put_optimistic(&self, blob: IBlob) -> Result<(), LsmError> {
        self.optimistic_batch.lock().push(blob);
        let mut wal = self.wal.lock();

        let mut to_write: Vec<IBlob> = std::mem::take(&mut *self.optimistic_batch.lock());
        if to_write.is_empty() {
            return Ok(()); // another leader already wrote our entry
        }

        wal.append_batch(&mut to_write)?;
        drop(wal);

        let indexes = self.secondary_indexes.lock();
        let memtable = self.memtable.read();
        for blob in to_write {
            if !indexes.is_empty() {
                for idx in indexes.iter() {
                    idx.notify_put(&blob);
                }
            }
            self.notify_unsorted_put(&blob);
            memtable.insert(blob);
        }
        let should_flush = memtable.should_flush();
        drop(memtable);
        drop(indexes);

        if should_flush {
            self.rotate_memtable()?;
        }
        Ok(())
    }

    /// MariaDB-style group commit with explicit follower wait. The leader's
    /// loop differs between Visible and Durable modes (see dispatch below).
    fn put_with_wait(&self, blob: IBlob) -> Result<(), LsmError> {
        let done = Arc::new((Mutex::new(false), Condvar::new()));
        let entry = CommitEntry {
            blob: Some(blob),
            done: done.clone(),
        };

        let became_leader = {
            let mut q = self.commit_queue.lock();
            q.entries.push(entry);
            if q.leader_active {
                false
            } else {
                q.leader_active = true;
                true
            }
        };

        if became_leader {
            match self.commit_mode {
                CommitMode::Durable => self.run_leader_pipelined()?,
                CommitMode::Visible => self.run_leader_visible()?,
                CommitMode::Optimistic => unreachable!("optimistic uses put_optimistic"),
            }
        } else {
            // Follower: wait until the leader signals us.
            let (lock, cv) = &*done;
            let mut g = lock.lock();
            while !*g {
                cv.wait(&mut g);
            }
        }
        Ok(())
    }

    /// Visible-mode leader: no slow stage to pipeline, so we keep leadership
    /// throughout the loop and process every batch that accumulates while
    /// holding the WAL writer mutex. Handoff overhead dominates parallelism
    /// gains here.
    fn run_leader_visible(&self) -> Result<(), LsmError> {
        let mut any_processed = false;
        let mut wal = self.wal.lock();
        loop {
            let mut entries = {
                let mut q = self.commit_queue.lock();
                if q.entries.is_empty() {
                    q.leader_active = false;
                    break;
                }
                std::mem::take(&mut q.entries)
            };

            let mut blobs: Vec<IBlob> = entries
                .iter_mut()
                .map(|e| e.blob.take().expect("entry blob not yet taken"))
                .collect();

            wal.append_batch(&mut blobs)?;

            {
                let indexes = self.secondary_indexes.lock();
                let memtable = self.memtable.read();
                for blob in blobs {
                    for idx in indexes.iter() {
                        idx.notify_put(&blob);
                    }
                    self.notify_unsorted_put(&blob);
                    memtable.insert(blob);
                }
            }

            for entry in &entries {
                let (lock, cv) = &*entry.done;
                *lock.lock() = true;
                cv.notify_one();
            }
            any_processed = true;
        }
        drop(wal);

        if any_processed {
            let should_flush = self.memtable.read().should_flush();
            if should_flush {
                self.rotate_memtable()?;
            }
        }
        Ok(())
    }

    /// Durable-mode leader with pipelined fsync.
    ///
    /// Phases per batch:
    /// 1. **Drain** the queue (under queue lock). Leadership is *not* released
    ///    yet — new arrivals during the optional wait become followers of THIS
    ///    batch instead of competing leaders.
    /// 2. **Optional batch-growing wait.** If `commit_wait_usec > 0` and the
    ///    batch is below `commit_wait_count` (or `commit_wait_count == 0`
    ///    meaning "always wait"), sleep for up to `wait_usec` and drain again.
    ///    This converts many small fsyncs into fewer larger ones at the cost
    ///    of per-op latency.
    /// 3. **Release leadership** so the next leader can begin appending while
    ///    we fsync (pipelining).
    /// 4. **Append + OS flush + fsync + memtable insert + signal followers.**
    /// 5. **Self-regulate**: if more work is queued AND no other leader
    ///    stepped in, take leadership back and loop.
    fn run_leader_pipelined(&self) -> Result<(), LsmError> {
        let static_usec = self.config.commit_wait_usec;
        let static_count = self.config.commit_wait_count;

        loop {
            // Resolve wait_usec/count per iteration. Busy mode can toggle
            // mid-stream (process_batch_durable updates `commit_busy_active`
            // after each fsync), so we re-read it every loop.
            //
            // Priority:
            //   1. Explicit static wait (commit_wait_usec > 0): use it verbatim.
            //   2. Busy mode (default): wait=0 quiet, wait=100 when active.
            //   3. Off: no wait.
            let (wait_usec, wait_count) = if static_usec > 0 {
                (static_usec, static_count)
            } else if self.config.commit_busy_mode
                && self.commit_busy_active.load(Ordering::Relaxed)
            {
                (100, 0)
            } else {
                (0, 0)
            };

            // Phase 1: drain queue. Hold leadership so the wait below
            // captures new arrivals as followers of this batch.
            let mut entries = {
                let mut q = self.commit_queue.lock();
                let entries = std::mem::take(&mut q.entries);
                if entries.is_empty() {
                    // Nothing queued — release leadership and exit.
                    q.leader_active = false;
                    break;
                }
                entries
            };

            // Note: the busy-mode trigger is evaluated *after* fsync, inside
            // process_batch_durable, because the relevant signal is "ops made
            // durable by this fsync" — which includes piggybacked appends
            // from concurrent leaders, not just our own entries.len().

            // Phase 2: optional batch-growing wait. Skip if batch already
            // meets the count threshold.
            if wait_usec > 0 && (wait_count == 0 || entries.len() < wait_count) {
                std::thread::sleep(std::time::Duration::from_micros(wait_usec));
                let extras = {
                    let mut q = self.commit_queue.lock();
                    std::mem::take(&mut q.entries)
                };
                entries.extend(extras);
            }

            // Phase 3: release leadership atomically. New arrivals can now
            // form batch N+1 and pipeline against our fsync below.
            {
                let mut q = self.commit_queue.lock();
                q.leader_active = false;
            }

            // Phase 4: WAL append + pipelined fsync + memtable + signal.
            self.process_batch_durable(&mut entries)?;

            // Phase 5: self-regulating continue.
            let continue_as_leader = {
                let mut q = self.commit_queue.lock();
                if q.entries.is_empty() || q.leader_active {
                    false
                } else {
                    q.leader_active = true;
                    true
                }
            };
            if !continue_as_leader {
                break;
            }
        }

        let should_flush = self.memtable.read().should_flush();
        if should_flush {
            self.rotate_memtable()?;
        }
        Ok(())
    }

    /// Busy-mode toggle logic, called after each Durable fsync.
    ///
    /// Hysteresis:
    /// - **Up**: a single fsync covering ≥ `commit_busy_threshold` ops
    ///   (= `num_cpus × 8`) flips busy mode on. Also resets the quiet
    ///   counter so subsequent partial activity doesn't immediately
    ///   start counting toward off.
    /// - **Down**: 3 consecutive fsyncs each covering fewer than
    ///   `commit_busy_threshold / 4` ops (= `num_cpus × 2`) flip it off.
    /// - **Middle band** (between down-threshold and up-threshold):
    ///   resets the quiet counter (these aren't quiet enough to count),
    ///   doesn't change `commit_busy_active`.
    ///
    /// No-op when `config.commit_busy_mode` is false.
    fn maybe_toggle_busy(&self, ops_in_fsync: usize) {
        if !self.config.commit_busy_mode {
            return;
        }
        let up = self.commit_busy_threshold;
        let down = up / 4;
        if ops_in_fsync >= up {
            // Up trigger: trip on and reset the quiet counter.
            self.consecutive_quiet_fsyncs.store(0, Ordering::Relaxed);
            if !self.commit_busy_active.load(Ordering::Relaxed) {
                self.commit_busy_active.store(true, Ordering::Relaxed);
            }
        } else if ops_in_fsync < down {
            // Quiet fsync: count it. Turn off after 3 in a row.
            let n = self
                .consecutive_quiet_fsyncs
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if n >= 3 && self.commit_busy_active.load(Ordering::Relaxed) {
                self.commit_busy_active.store(false, Ordering::Relaxed);
                self.consecutive_quiet_fsyncs.store(0, Ordering::Relaxed);
            }
        } else {
            // Middle band — not quiet, but not enough to trip on.
            self.consecutive_quiet_fsyncs.store(0, Ordering::Relaxed);
        }
    }

    /// One Durable batch with pipelined fsync. WAL writer mutex held only
    /// during append + OS flush; fsync runs without it so other leaders can
    /// append batch N+1 in parallel.
    fn process_batch_durable(&self, entries: &mut [CommitEntry]) -> Result<(), LsmError> {
        let mut blobs: Vec<IBlob> = entries
            .iter_mut()
            .map(|e| e.blob.take().expect("entry blob not yet taken"))
            .collect();
        let n = blobs.len();

        // Phase 1: append + push BufWriter to OS page cache (WAL lock).
        // Snapshot the global ops-appended counter while holding the lock —
        // this is the file's high-water mark our upcoming fsync is guaranteed
        // to cover. Other leaders may push it higher before our fsync, in
        // which case our fsync will durabilize more than we counted.
        let (sync_handle, ops_at_fsync_start) = {
            let mut wal = self.wal.lock();
            wal.append_batch(&mut blobs)?;
            wal.flush_buf()?;
            // Bump appended counter atomically with the append (still under WAL lock).
            let after_append = self.ops_appended.fetch_add(n as u64, Ordering::Relaxed)
                + n as u64;
            (wal.sync_handle(), after_append)
        };

        // Phase 2: fsync without WAL lock — pipelines with other leaders.
        sync_handle.sync_all()?;

        // Compute "ops made durable by this fsync" = the rise in the
        // ops_durable high-water mark. fetch_max returns the *previous*
        // value; if our snapshot is higher, we durabilized the delta.
        let prev_durable = self.ops_durable.fetch_max(ops_at_fsync_start, Ordering::Relaxed);
        let ops_in_fsync = if ops_at_fsync_start > prev_durable {
            (ops_at_fsync_start - prev_durable) as usize
        } else {
            0 // another leader's later fsync already covered our data
        };

        // Busy-mode hysteresis: turn on above up-threshold, off after
        // 3 consecutive fsyncs below down-threshold.
        self.maybe_toggle_busy(ops_in_fsync);

        // Phase 3: memtable + indexes. Must precede signaling.
        {
            let indexes = self.secondary_indexes.lock();
            let memtable = self.memtable.read();
            for blob in blobs {
                for idx in indexes.iter() {
                    idx.notify_put(&blob);
                }
                self.notify_unsorted_put(&blob);
                memtable.insert(blob);
            }
        }

        // Phase 4: signal followers — RYW + durability now satisfied.
        for entry in entries.iter() {
            let (lock, cv) = &*entry.done;
            *lock.lock() = true;
            cv.notify_one();
        }
        Ok(())
    }

    /// Insert a batch of documents. More efficient than individual put() calls
    /// because locks are acquired once for the entire batch.
    pub fn put_batch(&self, mut blobs: Vec<IBlob>) -> Result<(), LsmError> {
        if blobs.is_empty() {
            return Ok(());
        }
        self.write_count.fetch_add(blobs.len() as u64, Ordering::Relaxed);

        // Stamp versions
        for blob in blobs.iter_mut() {
            blob.set_version(DocumentId::new());
        }

        // WAL: one lock, all appends. fsync at the end if Durable mode.
        {
            let mut wal = self.wal.lock();
            for blob in blobs.iter_mut() {
                wal.append(blob)?;
            }
            if self.commit_mode == CommitMode::Durable {
                wal.sync()?;
            }
        }

        // Secondary indexes: one lock, all notifications
        {
            let indexes = self.secondary_indexes.lock();
            if !indexes.is_empty() {
                for blob in blobs.iter() {
                    for idx in indexes.iter() {
                        idx.notify_put(blob);
                    }
                }
            }
        }
        for blob in blobs.iter() {
            self.notify_unsorted_put(blob);
        }

        // Memtable: one lock, all inserts
        {
            let memtable = self.memtable.read();
            for blob in blobs {
                memtable.insert(blob);
            }
            if memtable.should_flush() {
                drop(memtable);
                self.rotate_memtable()?;
            }
        }

        Ok(())
    }

    /// Delete a document by writing a tombstone.
    /// Stamps a server-assigned `_version` on the tombstone.
    pub fn delete(&self, id: &DocumentId) -> Result<(), LsmError> {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        let mut tombstone = IBlob::tombstone(*id);
        tombstone.set_version(DocumentId::new());

        {
            let mut wal = self.wal.lock();
            wal.append(&mut tombstone)?;
        }

        // Notify secondary indexes of the deletion
        {
            let indexes = self.secondary_indexes.lock();
            for idx in indexes.iter() {
                idx.notify_delete(id);
            }
        }

        let should_flush = self.memtable.read().insert(tombstone);

        if should_flush {
            self.rotate_memtable()?;
        }

        Ok(())
    }

    /// Look up the latest version of a document by its stable document ID.
    /// Returns None if the document doesn't exist or has been deleted.
    pub fn get(&self, id: &DocumentId) -> Result<Option<IBlob>, LsmError> {
        self.get_at(id, &DocumentId::max())
    }

    /// Look up a document at a specific snapshot version.
    /// Returns the highest version <= snapshot for the given _id.
    fn get_at(&self, id: &DocumentId, snapshot: &DocumentId) -> Result<Option<IBlob>, LsmError> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        let mut timer = QueryTimer::start(QueryPattern {
            query_type: "get".into(),
            filter_fields: vec![],
            sort_fields: vec![],
            join_edge: None,
        });
        timer.set_docs_scanned(1);

        // Check active memtable first
        if let Some(blob) = self.memtable.read().get(id, snapshot) {
            let found = if blob.is_deleted() { None } else { Some(blob) };
            self.query_stats.record(timer.finish(if found.is_some() { 1 } else { 0 }));
            return Ok(found);
        }

        // Check immutable memtables (newest first)
        {
            let immutables = self.immutable_memtables.lock();
            for mt in immutables.iter().rev() {
                if let Some(blob) = mt.get(id, snapshot) {
                    let found = if blob.is_deleted() { None } else { Some(blob) };
                    self.query_stats.record(timer.finish(if found.is_some() { 1 } else { 0 }));
                    return Ok(found);
                }
            }
        }

        // Check SSTables — use snapshot-aware lookup
        let sstables = self.sstables.read();
        for sst in sstables.iter() {
            if let Some(blob) = sst.get_by_id_at(id, snapshot)? {
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
    ///
    /// Atomically writes:
    /// Rotate the active memtable: swap in a fresh one, queue the old for flush.
    /// Writers are only blocked for the brief swap, not during the flush I/O.
    fn rotate_memtable(&self) -> Result<(), LsmError> {
        let old_memtable;
        {
            let mut active = self.memtable.write();
            let new_memtable = MemTable::new(self.config.memtable_size);
            old_memtable = std::mem::replace(&mut *active, new_memtable);
        }
        // Old memtable is now immutable — queue it for flush
        self.immutable_memtables.lock().push(old_memtable);

        // Signal background flush, or flush inline
        if self.compaction_thread.lock().is_some() {
            self.signal_compaction();
        } else {
            self.flush_immutable_memtables()?;
        }
        Ok(())
    }

    /// Flush all queued immutable memtables to SSTables.
    fn flush_immutable_memtables(&self) -> Result<(), LsmError> {
        loop {
            let memtable = self.immutable_memtables.lock().pop();
            let Some(memtable) = memtable else { break };
            self.flush_single_memtable(memtable)?;
        }
        Ok(())
    }

    /// Flush one memtable to an SSTable + update indexes + reset WAL.
    /// 1. Primary SSTable from memtable data
    /// 2. Secondary index entries for the flushed data (merge buffer to disk)
    /// Only after both succeed: update metadata (SSTable list), reset WAL.
    fn flush_single_memtable(&self, memtable: MemTable) -> Result<(), LsmError> {
        let mut blobs = memtable.drain();
        if blobs.is_empty() {
            return Ok(());
        }

        // Step 1: Write primary SSTable
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.config.data_dir.join(format!("{sst_id:012}.sst"));

        SSTableWriter::with_block_size(self.config.block_size)
            .write(&sst_path, &mut blobs, &MvccKeyExtractor)?;

        // Step 2: Flush secondary index buffers to disk
        // Only if there are secondary indexes to flush.
        {
            let mut indexes = self.secondary_indexes.lock();
            if !indexes.is_empty() {
                let sstables = self.sstables.read();
                let sst_refs: Vec<&SSTableReader> = sstables.iter().collect();
                // Estimate doc count from SSTable count * avg docs per table
                // (avoid iterating all SSTables which is O(total data))
                let estimated_doc_count = sst_refs.len() as u64
                    * (self.config.memtable_size as u64 / 400) // ~400 bytes per doc estimate
                    + blobs.len() as u64;

                for index in indexes.iter_mut() {
                    let _ = index.compact(&sst_refs, estimated_doc_count, self.config.block_size);
                }
            }
        }

        // Step 3: Update metadata — now safe to make the new SSTable visible
        let reader = SSTableReader::open(&sst_path)?;

        {
            let mut sstables = self.sstables.write();
            sstables.insert(0, reader);
            let ucs = self.ucs();
            sort_sstables_by_level(&mut sstables, &ucs);
        }

        // Step 4: Reset WAL — data is safely on disk in SSTable + index
        {
            let wal = self.wal.lock();
            let wal_path = wal.path().to_path_buf();
            drop(wal);
            std::fs::remove_file(&wal_path).ok();
            *self.wal.lock() = Wal::open(&wal_path)?;
        }

        // Persist any read-built equality postings (debounced sidecar write).
        self.flush_equality_sidecars();

        // Trigger compaction (inline only — background coordinator handles its own)
        if self.compaction_thread.lock().is_none() {
            self.maybe_compact()?;
        }

        Ok(())
    }

    /// Flush the active memtable (public API for tests and wait_for_compaction).
    pub fn flush_memtable(&self) -> Result<(), LsmError> {
        self.rotate_memtable()
    }

    /// Run UCS compaction if needed.
    fn maybe_compact(&self) -> Result<(), LsmError> {
        self.maybe_adjust_w();
        let ucs = self.ucs();
        let sstables = self.sstables.read();

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
            let has_snapshots = self.oldest_snapshot().is_some();
            let mut tombstone_filter = TombstoneFilter::new(pick.output_level, pick.max_level, has_snapshots);
            self.run_compaction(&pick.inputs, Some(&mut tombstone_filter))?;
        }

        // Compact secondary indexes if needed
        self.maybe_compact_indexes()?;

        // Drop equality postings made redundant by a sorted full-range index.
        self.drop_redundant_equality_postings();

        Ok(())
    }

    /// Drop a field's equality postings once a sorted full-range index covers it:
    /// that index answers `Eq(field=v)` directly (and, routed first, intercepts
    /// the query before the equality index), so the postings are dead weight and
    /// would never be rebuilt. The top of the promotion ladder.
    fn drop_redundant_equality_postings(&self) {
        let fields = self.equality_postings.lock().fields();
        if fields.is_empty() {
            return;
        }
        let redundant: Vec<String> = {
            let indexes = self.secondary_indexes.lock();
            fields
                .into_iter()
                .filter(|f| {
                    indexes
                        .iter()
                        .any(|i| i.range.is_none() && i.fields == [f.clone()])
                })
                .collect()
        };
        if redundant.is_empty() {
            return;
        }
        let mut postings = self.equality_postings.lock();
        for f in redundant {
            postings.drop_field(&f);
        }
    }

    /// Promote unsorted blocks into sorted partial indexes at compaction:
    /// field-sort each non-empty block (the SSTable writer sorts by field key)
    /// and hand it to the secondary-index pool, which then merges partial ranges
    /// and extends to full coverage as usual. Empty negative-cache blocks stay.
    fn promote_unsorted_blocks(&self) -> Result<(), LsmError> {
        let drained: Vec<(String, Filter, Vec<IBlob>)> = {
            let mut uindexes = self.unsorted_indexes.lock();
            if uindexes.is_empty() {
                return Ok(());
            }
            let mut all = Vec::new();
            for u in uindexes.values_mut() {
                let field = u.field.clone();
                for (range, entries) in u.drain_all()? {
                    all.push((field.clone(), range, entries));
                }
            }
            uindexes.retain(|_, u| u.block_count() > 0);
            all
        };
        for (field, range, mut entries) in drained {
            // Skip only if an existing sorted index actually *covers* this range
            // (full-range, or a partial range that contains it). An index keyed
            // by the same field but covering an unrelated range must not block us.
            if self.has_covering_index(&field, &range) {
                continue;
            }
            let _ = self.spill_to_partial_index(&[field], Some(range), &mut entries);
        }
        Ok(())
    }

    /// Compact secondary indexes: drop unused, merge partial ranges, rebuild.
    fn maybe_compact_indexes(&self) -> Result<(), LsmError> {
        self.promote_unsorted_blocks()?;

        let mut indexes = self.secondary_indexes.lock();
        if indexes.is_empty() {
            return Ok(());
        }

        let sstables = self.sstables.read();
        let sst_refs: Vec<&SSTableReader> = sstables.iter().collect();
        // Estimate doc count without iterating all SSTables (which is O(total data))
        let estimated_doc_count = sst_refs.len() as u64
            * (self.config.memtable_size as u64 / 400)
            + self.memtable.read().len() as u64;

        // Consolidate multiple partial indexes for the same fields, tracking
        // coverage honestly: a full-range index supersedes the partials it
        // covers; partials whose ranges fold into a single interval merge into
        // one partial over that union; disjoint partials stay separate (we never
        // claim coverage we don't have).
        {
            let mut groups: HashMap<Vec<String>, Vec<secondary::SecondaryIndex>> = HashMap::new();
            for idx in std::mem::take(&mut *indexes) {
                groups.entry(idx.fields.clone()).or_default().push(idx);
            }
            let mut result: Vec<secondary::SecondaryIndex> = Vec::new();
            for (fields, group) in groups {
                if group.len() <= 1 {
                    result.extend(group);
                    continue;
                }
                if group.iter().any(|i| i.range.is_none()) {
                    // A full-range index covers all partials → keep one full, drop the rest.
                    let mut kept_full = false;
                    for idx in group {
                        if idx.range.is_none() && !kept_full {
                            kept_full = true;
                            result.push(idx);
                        } else {
                            std::fs::remove_file(&idx.path).ok();
                        }
                    }
                    continue;
                }
                // All partial: try to fold every range into one interval.
                let mut union = group[0].range.clone();
                for idx in &group[1..] {
                    union = match (union.as_ref(), idx.range.as_ref()) {
                        (Some(a), Some(b)) => unsorted::union_filter(a, b),
                        _ => None,
                    };
                    if union.is_none() {
                        break;
                    }
                }
                let union = match union {
                    Some(u) => u,
                    None => {
                        // Disjoint / not foldable — leave separate (correct coverage).
                        result.extend(group);
                        continue;
                    }
                };
                let mut all_entries: Vec<IBlob> = Vec::new();
                for idx in &group {
                    if let Ok(entries) = idx.iter_sorted() {
                        all_entries.extend(entries.into_iter().map(|(_, blob)| blob));
                    }
                }
                if all_entries.is_empty() {
                    result.extend(group);
                    continue;
                }
                let idx_name = fields.join("_");
                let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
                let merged_path = self.config.data_dir.join(format!("idx_{idx_name}_{sst_id:012}.sst"));
                match secondary::SecondaryIndex::build_partial(
                    &fields,
                    Some(union.clone()),
                    &mut all_entries,
                    &merged_path,
                    self.config.block_size,
                ) {
                    Ok(merged) => {
                        for idx in &group {
                            std::fs::remove_file(&idx.path).ok();
                        }
                        self.pending_index_metadata.lock().push(IndexMetadata {
                            fields: fields.clone(),
                            path: merged_path,
                            range: Some(union),
                        });
                        result.push(merged);
                    }
                    Err(_) => result.extend(group),
                }
            }
            *indexes = result;
        }

        // LRU eviction: cap the number of partial indexes per field set, evicting
        // the least-recently-used. (Promotion above drained all unsorted blocks
        // into this tier, so this is the unified cross-tier cap at compaction
        // time; the read path enforces the combined cap between compactions.)
        // Full-range indexes are never evicted.
        {
            let mut by_field: HashMap<Vec<String>, Vec<secondary::SecondaryIndex>> = HashMap::new();
            for idx in std::mem::take(&mut *indexes) {
                by_field.entry(idx.fields.clone()).or_default().push(idx);
            }
            let mut result: Vec<secondary::SecondaryIndex> = Vec::new();
            for (_fields, group) in by_field {
                let (full, mut partial): (Vec<_>, Vec<_>) =
                    group.into_iter().partition(|i| i.range.is_none());
                let keep_partial = self.config.max_ranges_per_field.saturating_sub(full.len());
                if partial.len() > keep_partial {
                    partial.sort_by_key(|i| i.last_used()); // oldest first
                    let drop_n = partial.len() - keep_partial;
                    for idx in partial.drain(..drop_n) {
                        std::fs::remove_file(&idx.path).ok();
                    }
                }
                result.extend(full);
                result.extend(partial);
            }
            *indexes = result;
        }

        // Compact individual indexes (merge buffer or full rebuild)
        for index in indexes.iter_mut() {
            let _ = index.compact(&sst_refs, estimated_doc_count, self.config.block_size);
        }

        // Note: `promote_unsorted_blocks` above already drained every block into
        // sorted partials (empty blocks → empty partials, absorbed into covering
        // partials by the union merge), so no separate block pruning is needed.

        Ok(())
    }

    fn ucs(&self) -> UcsCompaction {
        let w = self.effective_w.load(std::sync::atomic::Ordering::Relaxed);
        UcsCompaction::new(w, self.config.memtable_size as u64)
    }

    /// Current effective W parameter (may differ from config if adaptive).
    pub fn effective_w(&self) -> i32 {
        self.effective_w.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Target W without step limiting (what the workload ratio suggests).
    pub fn target_w(&self) -> i32 {
        self.target_w.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Adjust W based on observed read/write ratio.
    /// Called periodically by the compaction coordinator.
    fn maybe_adjust_w(&self) {
        if !self.config.adaptive_w {
            return;
        }

        let cooldown = std::time::Duration::from_secs(self.config.adaptive_w_cooldown_secs);
        let mut last = self.last_w_adjustment.lock();
        if last.elapsed() < cooldown {
            return;
        }

        let reads = self.read_count.swap(0, Ordering::Relaxed);
        let writes = self.write_count.swap(0, Ordering::Relaxed);
        let total = reads + writes;

        if total < 10 {
            return; // not enough data to decide
        }

        // Read ratio: 0.0 = all writes, 1.0 = all reads
        let read_ratio = reads as f64 / total as f64;

        // Target W: read-heavy → negative (leveled), write-heavy → positive (tiered)
        // Linear mapping: ratio 0.0 → max_w, ratio 1.0 → min_w
        let target_w = (self.config.adaptive_w_max as f64
            - (self.config.adaptive_w_max - self.config.adaptive_w_min) as f64 * read_ratio)
            as i32;

        self.target_w.store(target_w, std::sync::atomic::Ordering::Relaxed);

        let current_w = self.effective_w.load(std::sync::atomic::Ordering::Relaxed);
        let step = self.config.adaptive_w_max_step;

        // Clamp change to ±step
        let new_w = if target_w > current_w {
            (current_w + step).min(target_w).min(self.config.adaptive_w_max)
        } else if target_w < current_w {
            (current_w - step).max(target_w).max(self.config.adaptive_w_min)
        } else {
            current_w
        };

        if new_w != current_w {
            self.effective_w.store(new_w, std::sync::atomic::Ordering::Relaxed);
            eprintln!("[ingodb] adaptive W: {} → {} (read_ratio={:.2}, target={}, step-limited)",
                current_w, new_w, read_ratio, target_w);
        }

        *last = Instant::now();
    }

    /// Run compaction on the given SSTable files, optionally applying a filter.
    /// For duplicate `_id`s, the entry with the highest `_version` wins.
    fn run_compaction(
        &self,
        inputs: &[PathBuf],
        filter: Option<&mut dyn CompactionFilter>,
    ) -> Result<(), LsmError> {
        let num_inputs = inputs.len() as u64;
        let mut input_bytes = 0u64;

        // Merge all input SSTables
        let mut merged: Vec<IBlob> = Vec::new();
        for path in inputs {
            let reader = SSTableReader::open(path)?;
            input_bytes += reader.file_size();
            let entries = reader.iter()?;
            merged.extend(entries.into_iter().map(|(_, blob)| blob));
        }

        // Sort by _id, then _version desc
        merged.sort_by(|a, b| {
            a.id().cmp(b.id()).then_with(|| b.version().cmp(a.version()))
        });

        // MVCC-aware dedup: keep versions referenced by active snapshots
        let oldest_snap = self.oldest_snapshot();
        if let Some(oldest) = oldest_snap {
            // Keep latest version per _id PLUS any version >= oldest snapshot
            let mut kept = Vec::new();
            let mut i = 0;
            while i < merged.len() {
                let id = *merged[i].id();
                // Always keep the latest version (first in group)
                kept.push(merged[i].clone());
                i += 1;
                // Keep older versions if >= oldest snapshot
                while i < merged.len() && merged[i].id() == &id {
                    if merged[i].version() >= &oldest {
                        kept.push(merged[i].clone());
                    }
                    i += 1;
                }
            }
            merged = kept;
        } else {
            // No active snapshots — normal dedup, keep only latest per _id
            merged.dedup_by(|a, b| a.id() == b.id());
        }

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
            let mut sstables = self.sstables.write();
            for path in inputs {
                sstables.retain(|s| s.path() != path);
                self.equality_postings.lock().drop_sstable(path);
                std::fs::remove_file(Self::eq_sidecar_path(path)).ok();
                std::fs::remove_file(path).ok();
            }
            return Ok(());
        }

        // Write merged SSTable
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let output_path = self.config.data_dir.join(format!("{sst_id:012}.sst"));
        SSTableWriter::with_block_size(self.config.block_size)
            .write(&output_path, &mut merged, &MvccKeyExtractor)?;
        let new_reader = SSTableReader::open(&output_path)?;
        let output_bytes = new_reader.file_size();

        // Record compaction stats
        self.compaction_stats.runs.fetch_add(1, Ordering::Relaxed);
        self.compaction_stats.bytes_read.fetch_add(input_bytes, Ordering::Relaxed);
        self.compaction_stats.bytes_written.fetch_add(output_bytes, Ordering::Relaxed);
        self.compaction_stats.sstables_read.fetch_add(num_inputs, Ordering::Relaxed);
        self.compaction_stats.sstables_written.fetch_add(1, Ordering::Relaxed);

        // Snapshot the inputs' tracked equality keys before the swap drops them,
        // so we can carry their postings forward onto the merged output.
        let tracked_eq = self.equality_postings.lock().tracked_keys(inputs);

        // Swap old SSTables for new one.
        let mut sstables = self.sstables.write();
        for path in inputs {
            sstables.retain(|s| s.path() != path);
            self.equality_postings.lock().drop_sstable(path);
            std::fs::remove_file(path).ok();
        }
        sstables.push(new_reader);

        // Re-sort by level for correct read ordering
        let ucs = self.ucs();
        sort_sstables_by_level(&mut sstables, &ucs);
        drop(sstables);

        // Carry equality postings forward onto the merged output, rebuilt exactly
        // from the merged rows (free — we already have them). Keeps warm indexes
        // warm across compaction instead of cold-starting them.
        self.rebuild_equality_postings(&output_path, &merged, tracked_eq);

        // Persist the output's (and any other dirty) sidecars now, then keep the
        // resident set within its RAM budget.
        self.flush_equality_sidecars();
        self.enforce_equality_budget();

        Ok(())
    }

    /// Number of SSTables on disk.
    pub fn sstable_count(&self) -> usize {
        self.sstables.read().len()
    }

    /// Number of entries in the active memtable.
    pub fn memtable_size(&self) -> usize {
        self.memtable.read().len()
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

    /// Access compaction statistics.
    pub fn compaction_stats(&self) -> &CompactionStats {
        &self.compaction_stats
    }

    /// Start background compaction threads. Requires the engine to be in an Arc.
    /// If not called, compaction runs inline during flush (blocking the writer).
    pub fn start_background_compaction(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        let signal = Arc::clone(&self.compaction_signal);
        let num_threads = self.config.compaction_threads;

        let adaptive = self.config.adaptive_w;
        let cooldown = std::time::Duration::from_secs(self.config.adaptive_w_cooldown_secs);

        // Coordinator thread: wakes up, picks all compaction jobs, dispatches to workers
        let handle = std::thread::Builder::new()
            .name("ingodb-compaction-coordinator".into())
            .spawn(move || {
                loop {
                    // Wait for work, stop signal, or periodic timeout (for adaptive W)
                    {
                        let mut pending = signal.pending.lock();
                        while !*pending && !signal.stop.load(Ordering::Relaxed) {
                            if adaptive {
                                // Wake periodically to check read/write ratio
                                signal.notify.wait_for(&mut pending, cooldown);
                                break; // check regardless of whether signaled
                            } else {
                                signal.notify.wait(&mut pending);
                            }
                        }
                        if signal.stop.load(Ordering::Relaxed) {
                            break;
                        }
                        *pending = false;
                    }

                    signal.running.store(true, Ordering::SeqCst);

                    // Flush any immutable memtables first
                    let _ = engine.flush_immutable_memtables();

                    // Adaptive W: adjust based on recent read/write ratio
                    engine.maybe_adjust_w();

                    // Pick all eligible compaction jobs
                    let ucs = engine.ucs();
                    let sstables = engine.sstables.read();
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

                    let picks = ucs.pick_all_compactions(&metas);

                    if picks.is_empty() {
                        // No compaction work — also compact indexes
                        let _ = engine.maybe_compact_indexes();
                    } else {
                        // Dispatch compaction jobs to worker threads
                        let mut handles = Vec::new();
                        for pick in picks.into_iter().take(num_threads) {
                            let engine = Arc::clone(&engine);
                            handles.push(std::thread::Builder::new()
                                .name("ingodb-compaction-worker".into())
                                .spawn(move || {
                                    let has_snapshots = engine.oldest_snapshot().is_some();
                                    let mut tombstone_filter = TombstoneFilter::new(
                                        pick.output_level, pick.max_level, has_snapshots,
                                    );
                                    let _ = engine.run_compaction(
                                        &pick.inputs, Some(&mut tombstone_filter),
                                    );
                                })
                                .expect("failed to spawn compaction worker"));
                        }
                        // Wait for all workers to finish
                        for h in handles {
                            let _ = h.join();
                        }
                        // Compact secondary indexes after primary compaction
                        let _ = engine.maybe_compact_indexes();

                        // Signal again in case more work was created by compaction output
                        let mut pending = signal.pending.lock();
                        *pending = true;
                    }

                    signal.running.store(false, Ordering::SeqCst);
                    signal.done.notify_all();
                }
            })
            .expect("failed to spawn compaction coordinator");

        *self.compaction_thread.lock() = Some(handle);
    }

    /// Signal the background compaction thread to check for work.
    fn signal_compaction(&self) {
        let mut pending = self.compaction_signal.pending.lock();
        *pending = true;
        self.compaction_signal.notify.notify_one();
    }

    /// Wait until all pending compaction work is finished.
    /// Also flushes the memtable if it has data.
    pub fn wait_for_compaction(&self) -> Result<(), LsmError> {
        // Rotate active memtable if it has data, then flush all immutables
        if self.memtable.read().len() > 0 {
            self.rotate_memtable()?;
        }
        self.flush_immutable_memtables()?;

        // If background compaction is active, wait for it
        if self.compaction_thread.lock().is_some() {
            let mut guard = self.compaction_signal.done_mutex.lock();
            while self.compaction_signal.running.load(Ordering::SeqCst)
                || *self.compaction_signal.pending.lock()
            {
                self.compaction_signal.done.wait(&mut guard);
            }
        }
        Ok(())
    }

    /// Create a snapshot for consistent point-in-time reads.
    /// All reads through the snapshot see only versions <= the snapshot's version.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let version = DocumentId::new();
        self.active_snapshots.lock().insert(version);
        Snapshot { engine: self, version }
    }

    /// Oldest active snapshot version, or None if no snapshots active.
    fn oldest_snapshot(&self) -> Option<DocumentId> {
        self.active_snapshots.lock().iter().next().copied()
    }

    /// Check if a secondary index exists for the given sort fields.
    fn has_secondary_index(&self, sort_fields: &[String]) -> bool {
        self.secondary_indexes.lock().iter().any(|idx| idx.matches_sort(sort_fields))
    }

    /// Fan a write out to any unsorted-index blocks whose range it falls in.
    /// Cheap no-op (early return) when no unsorted indexes exist.
    fn notify_unsorted_put(&self, blob: &IBlob) {
        let indexes = self.unsorted_indexes.lock();
        if indexes.is_empty() {
            return;
        }
        for idx in indexes.values() {
            idx.notify_put(blob);
        }
    }

    /// Materialize a scan's matching subset as a new unsorted block on `field`,
    /// then enforce the unified per-field range budget.
    fn materialize_unsorted_block(
        &self,
        field: &str,
        range: Filter,
        matching: &[IBlob],
    ) {
        {
            let mut indexes = self.unsorted_indexes.lock();
            let idx = indexes
                .entry(field.to_string())
                .or_insert_with(|| unsorted::UnsortedIndex::new(field.to_string(), self.unsorted_dir.clone()));
            let _ = idx.materialize(range, matching, self.config.block_size);
        }
        self.enforce_combined_range_budget(field);
    }

    /// Whether a single-field sorted index on `field` already *covers* `range`
    /// — i.e. it is full-range, or its partial range contains `range`. Used to
    /// skip promoting a block that is genuinely redundant. (Range-aware: an index
    /// keyed by `field` but covering an unrelated range, e.g. a sort-spill keyed
    /// by the sort field with a filter on another column, does NOT count.)
    fn has_covering_index(&self, field: &str, range: &Filter) -> bool {
        self.secondary_indexes.lock().iter().any(|idx| {
            idx.fields.len() == 1
                && idx.fields[0] == field
                && idx.range.as_ref().map_or(true, |r| unsorted::range_contains(r, range))
        })
    }

    /// Enforce the unified LRU budget for a field: sorted partial indexes and
    /// unsorted blocks share one cap of `MAX_RANGES_PER_FIELD`. When over, the
    /// globally-least-recently-used range is dropped — whichever tier it's in
    /// (full-range sorted indexes are never evicted). A range's `last_used` is
    /// updated by every query that touches it, regardless of tier, so this is a
    /// true cross-tier LRU.
    fn enforce_combined_range_budget(&self, field: &str) {
        let mut sec = self.secondary_indexes.lock();
        let mut uns = self.unsorted_indexes.lock();

        // Candidate evictable ranges (exclude full-range sorted indexes).
        enum Ref {
            Sorted(usize),
            Block(usize),
        }
        let mut cands: Vec<(std::time::Instant, Ref)> = Vec::new();
        for (i, idx) in sec.iter().enumerate() {
            if idx.fields.len() == 1 && idx.fields[0] == field && idx.range.is_some() {
                cands.push((idx.last_used(), Ref::Sorted(i)));
            }
        }
        if let Some(u) = uns.get(field) {
            for (j, b) in u.blocks.iter().enumerate() {
                cands.push((b.last_used(), Ref::Block(j)));
            }
        }
        if cands.len() <= self.config.max_ranges_per_field {
            return;
        }
        let victims = cands.len() - self.config.max_ranges_per_field;
        cands.sort_by_key(|(t, _)| *t); // oldest first

        let mut drop_sorted: Vec<usize> = Vec::new();
        let mut drop_blocks: Vec<usize> = Vec::new();
        for (_, r) in cands.into_iter().take(victims) {
            match r {
                Ref::Sorted(i) => drop_sorted.push(i),
                Ref::Block(j) => drop_blocks.push(j),
            }
        }
        drop_sorted.sort_unstable_by(|a, b| b.cmp(a));
        for i in drop_sorted {
            let old = sec.remove(i);
            std::fs::remove_file(&old.path).ok();
        }
        if let Some(u) = uns.get_mut(field) {
            drop_blocks.sort_unstable_by(|a, b| b.cmp(a));
            for j in drop_blocks {
                u.blocks.remove(j).delete_file();
            }
            if u.block_count() == 0 {
                uns.remove(field);
            }
        }
    }

    /// Build a secondary index for the given sort fields from current SSTables.
    fn build_secondary_index(&self, sort_fields: &[String]) -> Result<(), LsmError> {
        let sstables = self.sstables.read();
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

        let meta = IndexMetadata {
            fields: sort_fields.to_vec(),
            path: idx_path,
            range: None,
        };
        self.pending_index_metadata.lock().push(meta);
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
        let index = indexes.iter().find(|idx| idx.matches_query(sort_fields, filter))?;
        index.mark_used();
        self.sorted_hits.fetch_add(1, Ordering::Relaxed);

        // Read sorted entries and clone fields before dropping the lock
        let sorted_entries = match index.iter_sorted() {
            Ok(entries) => entries,
            Err(e) => return Some(Err(e)),
        };
        let index_fields = index.fields.clone();
        drop(indexes);

        // For each entry, look up the full document by _id.
        // Verify the indexed field values still match (stale check).
        let mut results = Vec::new();
        for (id, projected) in sorted_entries {
            // Skip tombstones in the index buffer
            if projected.is_deleted() {
                continue;
            }
            match self.get(&id) {
                Ok(Some(blob)) => {
                    // Stale check: verify indexed field values match the primary
                    let is_current = index_fields.iter().all(|f| {
                        blob.get_field(f) == projected.get_field(f)
                    });
                    if !is_current {
                        continue;
                    }
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
        let memtable_docs: Vec<IBlob> = self.memtable.read().iter()
            .map(|(_, blob)| blob)
            .filter(|blob| !blob.is_deleted())
            .filter(|blob| {
                filter.map_or(true, |f| f.matches(&|field| blob.get_field(field)))
            })
            .collect();

        if !memtable_docs.is_empty() {
            // Merge: memtable version wins (newer), replace any matching index results
            let memtable_ids: std::collections::HashSet<DocumentId> = memtable_docs.iter().map(|b| *b.id()).collect();
            results.retain(|b| !memtable_ids.contains(b.id()));

            // Add memtable docs (dedup within memtable docs by id)
            let mut seen: std::collections::HashSet<DocumentId> = results.iter().map(|b| *b.id()).collect();
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

    /// Spill sorted scan results to disk as a partial secondary index.
    /// Replaces any existing index for the same sort fields.
    fn spill_to_partial_index(
        &self,
        sort_fields: &[String],
        range: Option<Filter>,
        sorted_results: &mut [IBlob],
    ) -> Result<(), LsmError> {
        let idx_name = sort_fields.join("_");
        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let idx_path = self.config.data_dir.join(format!("idx_{idx_name}_{sst_id:012}.sst"));

        let index = secondary::SecondaryIndex::build_partial(
            sort_fields,
            range.clone(),
            sorted_results,
            &idx_path,
            self.config.block_size,
        )?;

        let meta = IndexMetadata {
            fields: sort_fields.to_vec(),
            path: idx_path,
            range: index.range.clone(),
        };

        // Add alongside existing indexes for the same fields (compaction will merge)
        // Only replace if the exact same range already exists
        let mut indexes = self.secondary_indexes.lock();
        if let Some(pos) = indexes.iter().position(|idx| {
            idx.matches_sort(sort_fields) && idx.range == range
        }) {
            let old = indexes.remove(pos);
            std::fs::remove_file(&old.path).ok();
        }
        indexes.push(index);
        drop(indexes);

        self.pending_index_metadata.lock().push(meta);
        Ok(())
    }

    /// Try to use a secondary index to accelerate a filter-only scan.
    /// Works for simple Eq/Gt/Lt/Range filters on a single field that has an index.
    fn scan_with_filter_index(
        &self,
        filter: &Filter,
        limit: Option<usize>,
        snapshot: &DocumentId,
    ) -> Option<Result<Vec<IBlob>, LsmError>> {
        // Extract range boundaries from the filter for binary search on the index
        let (field, start_val, end_val) = match filter {
            Filter::Eq { field, value } => {
                (field.clone(), Some(value.clone()), Some(value.clone()))
            }
            Filter::Gt { field, value } => {
                (field.clone(), Some(value.clone()), None) // open upper bound
            }
            Filter::Lt { field, value } => {
                (field.clone(), None, Some(value.clone())) // open lower bound
            }
            Filter::Range { field, low, high } => {
                (field.clone(), Some(low.clone()), Some(high.clone()))
            }
            _ => return None,
        };

        // Find a single-field index on this field whose range *covers* the
        // query (full-range, or a partial range that contains it). The coverage
        // check is required for completeness: a partial index that does not
        // contain the query would miss rows outside its stored range.
        let indexes = self.secondary_indexes.lock();
        let index = indexes.iter().find(|idx| {
            idx.fields.len() == 1
                && idx.fields[0] == field
                && idx.range.as_ref().map_or(true, |r| unsorted::range_contains(r, filter))
        })?;
        index.mark_used();
        self.sorted_hits.fetch_add(1, Ordering::Relaxed);

        // Range scan on the index — O(log N + R) instead of O(N)
        let range_entries = match index.range_scan(
            start_val.as_ref(),
            end_val.as_ref(),
        ) {
            Ok(entries) => entries,
            Err(e) => return Some(Err(e)),
        };
        drop(indexes);


        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for (id, projected) in range_entries {
            if projected.is_deleted() {
                continue;
            }
            // Fetch the full document from primary
            match self.get_at(&id, snapshot) {
                Ok(Some(blob)) => {
                    // Verify the filter still holds on the full document (stale check)
                    if !filter.matches(&|f| blob.get_field(f)) {
                        continue;
                    }
                    if seen.insert(*blob.id()) {
                        results.push(blob);
                        if let Some(lim) = limit {
                            if results.len() >= lim {
                                break;
                            }
                        }
                    }
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }

        // Also check memtable for docs not in the index
        for (_, blob) in self.memtable.read().iter() {
            if blob.is_deleted() || blob.version() > snapshot {
                continue;
            }
            if filter.matches(&|f| blob.get_field(f)) && seen.insert(*blob.id()) {
                results.push(blob);
            }
        }

        Some(Ok(results))
    }

    /// Serve an `Eq` / `In` filter from the per-SSTable equality postings,
    /// building them lazily on read.
    ///
    /// Returns `None` to decline (→ the caller full-scans) either when the filter
    /// isn't an equality term, or when a value is **non-selective** in some
    /// SSTable (an `Overflow` posting): verifying its many candidates one-by-one
    /// would be slower than a sequential scan, so we leave it to the full-scan
    /// path. Selective values are served from `Exact` postings: contribute their
    /// ids, live-scan the memtable for un-indexed writes, then verify every
    /// candidate via `get_at` (resolving cross-level MVCC + staleness).
    fn scan_with_equality_index(
        &self,
        filter: &Filter,
        limit: Option<usize>,
        snapshot: &DocumentId,
    ) -> Option<Result<Vec<IBlob>, LsmError>> {
        let (field, values) = equality::equality_terms(filter)?;
        self.equality_postings.lock().touch_field(&field); // field-LRU recency

        // Phase 1: gather candidate ids from per-SSTable postings (build on read).
        // `warm` stays true only if every (sstable, value) was a cached hit.
        let mut candidate_ids: Vec<DocumentId> = Vec::new();
        let mut warm = true;
        // Set when any consulted posting is a non-exhaustive sample
        // (`Overflow`/`Partial`): the answer is then only trustworthy up to a LIMIT
        // that the verified candidates already satisfy.
        let mut incomplete = false;
        {
            let sstables = self.sstables.read();
            for sst in sstables.iter() {
                let path = sst.path();
                for v in &values {
                    let mut cached = self.equality_postings.lock().get(path, &field, v).cloned();
                    if cached.is_none() {
                        // Maybe persisted from a prior session but not yet loaded.
                        self.ensure_equality_loaded(path);
                        cached = self.equality_postings.lock().get(path, &field, v).cloned();
                    }
                    let posting = match cached {
                        Some(p) => p,
                        None => {
                            warm = false;
                            let posting = match self.scan_sstable_for_value(sst, &field, v) {
                                Ok(p) => p,
                                Err(e) => return Some(Err(e)),
                            };
                            self.equality_postings.lock().insert(path, &field, v, posting.clone());
                            posting
                        }
                    };
                    match posting {
                        // Selective: absence is authoritative, candidates are few.
                        equality::Posting::Exact(ids) => candidate_ids.extend(ids),
                        // Non-selective / incomplete in this SSTable. Without a
                        // LIMIT, a sequential full scan beats verifying this many
                        // candidates — decline. With a LIMIT we can try to satisfy
                        // it from the K-sized sample (checked after verification).
                        equality::Posting::Overflow(sample) | equality::Posting::Partial(sample) => {
                            if limit.is_none() {
                                return None;
                            }
                            incomplete = true;
                            candidate_ids.extend(sample);
                        }
                    }
                }
            }
        }

        if !warm {
            // Built new postings this query — keep the RAM working set bounded.
            self.enforce_equality_budget();
        }

        // Phase 2: verify candidates against the primary (resolves cross-level
        // MVCC + staleness), then live-scan the memtables for un-indexed writes.
        // `enough` lets a LIMIT query stop as soon as it has its rows — which is
        // what makes serving a non-selective value's LIMIT cheap (verify ~n ids,
        // not the whole sample).
        let enough = |results: &Vec<IBlob>| limit.is_some_and(|n| results.len() >= n);

        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for id in candidate_ids {
            if enough(&results) {
                break;
            }
            match self.get_at(&id, snapshot) {
                Ok(Some(blob)) => {
                    if blob.is_deleted() || !filter.matches(&|f| blob.get_field(f)) {
                        continue;
                    }
                    if seen.insert(*blob.id()) {
                        results.push(blob);
                    }
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }

        // Memtable + immutable memtables hold writes not yet flushed to any
        // SSTable (so not in any posting); scan them live. Bounded by memtable size.
        if !enough(&results) {
            let mut live: Vec<IBlob> = self.memtable.read().iter().map(|(_, b)| b).collect();
            for mt in self.immutable_memtables.lock().iter() {
                live.extend(mt.iter().map(|(_, b)| b));
            }
            for blob in live {
                if enough(&results) {
                    break;
                }
                if blob.is_deleted() || blob.version() > snapshot {
                    continue;
                }
                if filter.matches(&|f| blob.get_field(f)) && seen.insert(*blob.id()) {
                    results.push(blob);
                }
            }
        }

        if incomplete && !enough(&results) {
            // We only saw K-sized samples of a non-selective value and couldn't
            // confirm enough matches for the LIMIT — decline so the full scan
            // finds the rest. (A complete/Exact answer never reaches here.)
            return None;
        }
        if let Some(lim) = limit {
            results.truncate(lim);
        }
        if warm {
            self.equality_hits.fetch_add(1, Ordering::Relaxed);
        }
        Some(Ok(results))
    }

    /// Exhaustively scan one immutable SSTable for `field == value`, returning the
    /// `Posting` to cache: `Exact` (its ids are the complete match set for this
    /// SSTable) or `Overflow` when the value is non-selective per R/K. Tombstones
    /// and non-matching versions are naturally excluded.
    fn scan_sstable_for_value(
        &self,
        sst: &SSTableReader,
        field: &str,
        value: &Value,
    ) -> Result<equality::Posting, LsmError> {
        let entries = sst.iter()?;
        let rows = entries.len();
        let mut ids = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (_, blob) in entries {
            if blob.get_field(field).as_ref() == Some(value) && seen.insert(*blob.id()) {
                ids.push(*blob.id());
            }
        }
        Ok(equality::Posting::from_exhaustive(ids, rows))
    }

    /// Rebuild the equality postings for a freshly-compacted `output` SSTable from
    /// its merged rows, for exactly the `(field, value)` keys that the input
    /// SSTables tracked (`tracked`). Each key is recomputed exhaustively, so the
    /// result is `Exact`/`Overflow`/negative just as a fresh read would produce —
    /// but with no read needed. Keys that match nothing become `Exact([])`
    /// (negative cache carried forward).
    fn rebuild_equality_postings(
        &self,
        output: &Path,
        merged: &[IBlob],
        tracked: HashMap<String, std::collections::HashSet<Vec<u8>>>,
    ) {
        if tracked.is_empty() {
            return;
        }
        let rows = merged.len();

        // field → vkey → (ids, dedup set). Seed every tracked key so a zero-match
        // key still produces a negative posting.
        let mut buckets: HashMap<&str, HashMap<Vec<u8>, (Vec<DocumentId>, std::collections::HashSet<DocumentId>)>> =
            HashMap::new();
        for (field, vkeys) in &tracked {
            let m = buckets.entry(field.as_str()).or_default();
            for vk in vkeys {
                m.entry(vk.clone()).or_default();
            }
        }

        for blob in merged {
            if blob.is_deleted() {
                continue;
            }
            for field in tracked.keys() {
                if let Some(val) = blob.get_field(field) {
                    let vk = equality::value_key(&val);
                    if let Some(slot) = buckets.get_mut(field.as_str()).and_then(|m| m.get_mut(&vk)) {
                        if slot.1.insert(*blob.id()) {
                            slot.0.push(*blob.id());
                        }
                    }
                }
            }
        }

        let mut postings = self.equality_postings.lock();
        for (field, m) in buckets {
            for (vk, (ids, _)) in m {
                postings.insert_raw(output, field, vk, equality::Posting::from_exhaustive(ids, rows));
            }
        }
        // Budget enforcement happens after the post-compaction sidecar flush
        // (it needs to flush dirty first), not while holding this lock.
    }

    /// Sidecar path for an SSTable's equality postings (`<id>.eq` beside `<id>.sst`).
    fn eq_sidecar_path(sst: &Path) -> PathBuf {
        sst.with_extension("eq")
    }

    /// Persist every SSTable with un-flushed equality postings to its sidecar.
    /// Debounced: called at flush / compaction / shutdown, never on the read path.
    /// Written via temp-file + rename so a crash mid-write can't corrupt a sidecar.
    fn flush_equality_sidecars(&self) {
        let dirty = self.equality_postings.lock().take_dirty();
        for sst in dirty {
            let bytes = self.equality_postings.lock().serialize_sstable(&sst);
            let path = Self::eq_sidecar_path(&sst);
            match bytes {
                Some(b) => {
                    let tmp = path.with_extension("eqtmp");
                    if std::fs::write(&tmp, &b).is_ok() {
                        std::fs::rename(&tmp, &path).ok();
                    }
                }
                // No postings left for this SSTable — remove a stale sidecar.
                None => {
                    std::fs::remove_file(&path).ok();
                }
            }
        }
    }

    /// Lazily read an SSTable's sidecar into RAM the first time it's needed this
    /// session (warm restart). A miss after loading means "never built → scan".
    fn ensure_equality_loaded(&self, sst: &Path) {
        if self.equality_postings.lock().is_loaded(sst) {
            return;
        }
        let decoded = std::fs::read(Self::eq_sidecar_path(sst))
            .ok()
            .and_then(|b| equality::decode_postings(&b));
        let mut postings = self.equality_postings.lock();
        if postings.is_loaded(sst) {
            return; // another reader won the race
        }
        match decoded {
            Some(map) => postings.load_into(sst, map),
            None => postings.mark_loaded(sst),
        }
    }

    /// Bound the resident postings to their RAM budget. A rare backstop: only when
    /// over budget, flush dirty sidecars first (so eviction can't lose un-persisted
    /// postings), then evict the coldest fields from RAM — they reload from their
    /// sidecars on next access.
    fn enforce_equality_budget(&self) {
        if !self.equality_postings.lock().needs_eviction() {
            return;
        }
        self.flush_equality_sidecars();
        self.equality_postings.lock().enforce_budget();
    }

    /// Try to serve a filter-only scan from a single covering unsorted block.
    /// Returns `None` if no block fully contains the query range (→ full scan,
    /// which may then re-materialize a block over the whole range).
    fn scan_with_unsorted_index(
        &self,
        filter: &Filter,
        limit: Option<usize>,
        snapshot: &DocumentId,
    ) -> Option<Result<Vec<IBlob>, LsmError>> {
        // Only single-field interval filters can be covered by a block.
        let field = match filter {
            Filter::Eq { field, .. }
            | Filter::Gt { field, .. }
            | Filter::Lt { field, .. }
            | Filter::Range { field, .. } => field.clone(),
            _ => return None,
        };

        // Collect candidate (id, _) pairs from the covering block, then drop the
        // lock before resolving each against primary.
        let candidates = {
            let indexes = self.unsorted_indexes.lock();
            let idx = indexes.get(&field)?;
            let block = idx.find_covering(filter)?;
            block.mark_used();
            match block.scan(filter) {
                Ok(c) => c,
                Err(e) => return Some(Err(e)),
            }
        };

        self.unsorted_hits.fetch_add(1, Ordering::Relaxed);

        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (id, _projected) in candidates {
            match self.get_at(&id, snapshot) {
                Ok(Some(blob)) => {
                    if blob.is_deleted() {
                        continue;
                    }
                    // Stale check: re-verify the filter on the current document.
                    if !filter.matches(&|f| blob.get_field(f)) {
                        continue;
                    }
                    if seen.insert(*blob.id()) {
                        results.push(blob);
                    }
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }

        // Merge fresh memtable docs (in case any in-range write isn't buffered).
        for (_, blob) in self.memtable.read().iter() {
            if blob.is_deleted() || blob.version() > snapshot {
                continue;
            }
            if filter.matches(&|f| blob.get_field(f)) && seen.insert(*blob.id()) {
                results.push(blob);
            }
        }

        if let Some(lim) = limit {
            results.truncate(lim);
        }
        Some(Ok(results))
    }

    /// Serve a *sorted* scan whose single-field range filter is covered by an
    /// unsorted block: scan the block, sort, expand the sorted partial index
    /// with the result, then drop the now-redundant block range. Returns
    /// ascending-sorted results (the caller reverses for descending), or `None`
    /// if no block covers the filter. A sort query is demonstrated demand for a
    /// sorted index, so this promotes the block instead of just reading it.
    fn scan_sort_with_unsorted_block(
        &self,
        sort_fields: &[SortField],
        field_names: &[String],
        filter: &Filter,
        limit: Option<usize>,
    ) -> Option<Result<Vec<IBlob>, LsmError>> {
        let field = match filter {
            Filter::Eq { field, .. }
            | Filter::Gt { field, .. }
            | Filter::Lt { field, .. }
            | Filter::Range { field, .. } => field.clone(),
            _ => return None,
        };

        let candidates = {
            let indexes = self.unsorted_indexes.lock();
            let idx = indexes.get(&field)?;
            let block = idx.find_covering(filter)?;
            block.mark_used();
            match block.scan(filter) {
                Ok(c) => c,
                Err(e) => return Some(Err(e)),
            }
        };
        self.unsorted_hits.fetch_add(1, Ordering::Relaxed);

        // Resolve full docs (stale-checked) plus any fresh memtable docs.
        let mut results = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (id, _proj) in candidates {
            match self.get(&id) {
                Ok(Some(blob)) => {
                    if blob.is_deleted() || !filter.matches(&|f| blob.get_field(f)) {
                        continue;
                    }
                    if seen.insert(*blob.id()) {
                        results.push(blob);
                    }
                }
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
        for (_, blob) in self.memtable.read().iter() {
            if blob.is_deleted() {
                continue;
            }
            if filter.matches(&|f| blob.get_field(f)) && seen.insert(*blob.id()) {
                results.push(blob);
            }
        }

        // Sort ascending by the sort fields.
        results.sort_by(|a, b| {
            for sf in sort_fields {
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

        // Expand the sorted partial index with the full (un-limited) result, then
        // drop the now-redundant block range.
        let mut to_spill = results.clone();
        if self
            .spill_to_partial_index(field_names, Some(filter.clone()), &mut to_spill)
            .is_ok()
        {
            let mut indexes = self.unsorted_indexes.lock();
            if let Some(idx) = indexes.get_mut(&field) {
                idx.remove_covering(filter);
                if idx.block_count() == 0 {
                    indexes.remove(&field);
                }
            }
        }

        if let Some(lim) = limit {
            results.truncate(lim);
        }
        Some(Ok(results))
    }

    /// Number of unsorted blocks served from (observability / tests).
    pub fn unsorted_hits(&self) -> u64 {
        self.unsorted_hits.load(Ordering::Relaxed)
    }

    /// Number of scans served from a sorted (secondary/partial) index.
    pub fn sorted_hits(&self) -> u64 {
        self.sorted_hits.load(Ordering::Relaxed)
    }

    /// Number of scans served from the equality (Eq/In) index.
    pub fn equality_hits(&self) -> u64 {
        self.equality_hits.load(Ordering::Relaxed)
    }

    /// Number of distinct fields with at least one equality posting.
    pub fn equality_field_count(&self) -> usize {
        self.equality_postings.lock().field_count()
    }

    /// Total equality postings across all SSTables (observability / tests).
    pub fn equality_posting_count(&self) -> usize {
        self.equality_postings.lock().posting_count()
    }

    /// Total number of unsorted blocks across all columns.
    pub fn unsorted_block_count(&self) -> usize {
        self.unsorted_indexes.lock().values().map(|u| u.block_count()).sum()
    }

    /// Number of secondary indexes.
    pub fn secondary_index_count(&self) -> usize {
        self.secondary_indexes.lock().len()
    }

    /// Drain newly built index metadata (for persistence by Database).
    pub fn drain_pending_index_metadata(&self) -> Vec<IndexMetadata> {
        std::mem::take(&mut *self.pending_index_metadata.lock())
    }

    /// Load an existing secondary index from disk.
    pub fn load_secondary_index(&self, fields: Vec<String>, range: Option<Filter>, path: &Path) -> Result<(), LsmError> {
        let index = secondary::SecondaryIndex::open(fields, range, path)?;
        self.secondary_indexes.lock().push(index);
        Ok(())
    }

    /// Replay memtable entries into secondary indexes.
    /// Called by Database::open() after loading indexes, so that WAL-recovered
    /// documents that were written after the last index compaction get re-buffered.
    pub fn replay_memtable_to_indexes(&self) {
        let indexes = self.secondary_indexes.lock();
        if indexes.is_empty() {
            return;
        }
        for (_, blob) in self.memtable.read().iter() {
            for idx in indexes.iter() {
                if blob.is_deleted() {
                    idx.notify_delete(blob.id());
                } else {
                    idx.notify_put(&blob);
                }
            }
        }
    }

    /// Full scan: merge all live documents, apply filter/sort/projection/limit.
    pub fn scan(
        &self,
        filter: Option<&Filter>,
        sort: Option<&[SortField]>,
        project: Option<&[String]>,
        limit: Option<usize>,
    ) -> Result<Vec<IBlob>, LsmError> {
        self.scan_at(filter, sort, project, limit, &DocumentId::max())
    }

    /// Scan at a specific snapshot version.
    fn scan_at(
        &self,
        filter: Option<&Filter>,
        sort: Option<&[SortField]>,
        project: Option<&[String]>,
        limit: Option<usize>,
        snapshot: &DocumentId,
    ) -> Result<Vec<IBlob>, LsmError> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        let mut timer = QueryTimer::start(QueryPattern {
            query_type: "scan".into(),
            filter_fields: filter.map(extract_filter_fields).unwrap_or_default(),
            sort_fields: sort.map(|s| s.iter().map(|sf| sf.field.clone()).collect()).unwrap_or_default(),
            join_edge: None,
        });

        // Try secondary index for sorted scans (only for latest snapshot)
        if *snapshot == DocumentId::max() {
        if let Some(sort_fields) = sort {
            let field_names: Vec<String> = sort_fields.iter().map(|sf| sf.field.clone()).collect();
            let all_ascending = sort_fields.iter().all(|sf| sf.direction == SortDirection::Ascending);
            let all_descending = sort_fields.iter().all(|sf| sf.direction == SortDirection::Descending);

            if all_ascending || all_descending {
                // Prefer an existing sorted index; else promote a covering
                // unsorted block into a sorted partial index and serve from it.
                let via = self
                    .scan_with_secondary_index(&field_names, filter, limit)
                    .or_else(|| {
                        filter.and_then(|f| {
                            self.scan_sort_with_unsorted_block(sort_fields, &field_names, f, limit)
                        })
                    });
                if let Some(result) = via {
                    let mut results = result?;
                    if all_descending {
                        results.reverse();
                        // Re-apply limit after reverse (index scan may have applied limit from the wrong end)
                        if let Some(lim) = limit {
                            results.truncate(lim);
                        }
                    }
                    if let Some(fields) = project {
                        results = results.into_iter().map(|blob| blob.project(fields)).collect();
                    }
                    let docs_returned = results.len() as u64;
                    timer.set_docs_scanned(docs_returned);
                    self.query_stats.record(timer.finish(docs_returned));
                    return Ok(results);
                }
            }
        }
        // Try indexes for filter-only scans (no sort required). Range indexes
        // first — an `Eq` reads an existing covering range index but never
        // populates one — then the equality index builds/serves Eq/In on read.
        if sort.is_none() {
            if let Some(filter) = filter {
                let via = self
                    .scan_with_filter_index(filter, limit, snapshot)
                    .or_else(|| self.scan_with_unsorted_index(filter, limit, snapshot))
                    .or_else(|| self.scan_with_equality_index(filter, limit, snapshot));
                if let Some(result) = via {
                    let mut results = result?;
                    if let Some(fields) = project {
                        results = results.into_iter().map(|blob| blob.project(fields)).collect();
                    }
                    let docs_returned = results.len() as u64;
                    timer.set_docs_scanned(docs_returned);
                    self.query_stats.record(timer.finish(docs_returned));
                    return Ok(results);
                }
            }
        }

        } // end secondary index check (latest snapshot only)

        // Collect all IBlobs from memtable + SSTables
        let mut all: Vec<IBlob> = Vec::new();

        // Memtable entries (active + immutable)
        all.extend(self.memtable.read().iter().map(|(_, blob)| blob));
        for mt in self.immutable_memtables.lock().iter() {
            all.extend(mt.iter().map(|(_, blob)| blob));
        }

        // SSTable entries
        {
            let sstables = self.sstables.read();
            for sst in sstables.iter() {
                all.extend(sst.iter()?.into_iter().map(|(_, blob)| blob));
            }
        }

        // MVCC: filter to versions visible at snapshot
        all.retain(|b| b.version() <= snapshot);

        // Merge: sort by _id, dedup keeping highest _version (within snapshot)
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

        // Reactive: materialize this *range* filter's result subset. We are here
        // only because no index served the query, so `results` is the full
        // matching subset over `filter`. `Eq`/`In` are handled by the equality
        // index (built on read above) and deliberately do not appear here — they
        // never populate range indexes.
        if *snapshot == DocumentId::max() && sort.is_none() {
            if let Some(f) = filter {
                let field = match f {
                    Filter::Gt { field, .. }
                    | Filter::Lt { field, .. }
                    | Filter::Range { field, .. } => Some(field.clone()),
                    _ => None,
                };
                if let Some(field) = field {
                    let matching = results.len() as u64;
                    // Materialize when the result is at most half the collection
                    // (which already implies it's a proper subset).
                    let small_enough = (matching as f64)
                        <= unsorted::UNSORTED_MATERIALIZE_MAX_FRACTION * docs_scanned as f64;
                    // Trivially-sorted results (empty → negative cache, or a
                    // single row) go straight to a sorted partial index;
                    // everything else becomes an unsorted block whose field-sort
                    // is deferred to compaction.
                    let already_sorted = matching <= 1;
                    if small_enough {
                        if already_sorted {
                            let mut to_spill = results.clone();
                            let _ = self.spill_to_partial_index(&[field.clone()], Some(f.clone()), &mut to_spill);
                            self.enforce_combined_range_budget(&field);
                        } else {
                            self.materialize_unsorted_block(&field, f.clone(), &results);
                        }
                    }
                }
            }
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

        // A sort scan that fell to a full scan materializes its sorted result as
        // a partial index — always (the scan is the cost; the sort is cheap, so
        // there is no size threshold). Stored ascending; descending reads reverse.
        if let Some(sort_fields) = sort {
            let field_names: Vec<String> = sort_fields.iter().map(|sf| sf.field.clone()).collect();
            let all_same_direction = sort_fields.iter().all(|sf| sf.direction == sort_fields[0].direction);
            if all_same_direction {
                let mut to_spill = results.clone();
                if sort_fields[0].direction == SortDirection::Descending {
                    to_spill.reverse();
                }
                let _ = self.spill_to_partial_index(
                    &field_names,
                    filter.cloned(),
                    &mut to_spill,
                );
            }
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

        // Filter-only scans are served by unsorted blocks (materialized above)
        // and promoted to sorted partial indexes at compaction — there is no
        // separate "reactive full-range filter index" trigger.

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
        self.traverse_at(start, from_field, to_field, depth, &DocumentId::max())
    }

    fn traverse_at(
        &self,
        start: Option<&Filter>,
        from_field: &str,
        to_field: &str,
        depth: usize,
        snapshot: &DocumentId,
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
        let mut current = self.scan_at(start, None, None, None, snapshot)?;
        let mut all_results = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for _ in 0..depth {
            let join_values: Vec<Value> = current
                .iter()
                .filter_map(|blob| blob.get_field(from_field))
                .collect();

            if join_values.is_empty() {
                break;
            }

            let mut next = Vec::new();
            let candidates = self.scan_at(None, None, None, None, snapshot)?;
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

impl Drop for LsmEngine {
    fn drop(&mut self) {
        // Persist any read-built equality postings before shutting down, so a
        // clean restart is warm even for a read-only session.
        self.flush_equality_sidecars();

        // Signal the background compaction thread to stop
        self.compaction_signal.stop.store(true, Ordering::SeqCst);
        self.compaction_signal.notify.notify_one();
        if let Some(handle) = self.compaction_thread.lock().take() {
            let _ = handle.join();
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

    fn deterministic_id(i: u64) -> DocumentId {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&i.to_be_bytes());
        let hash = i.wrapping_mul(0x517cc1b727220a95);
        bytes[8..16].copy_from_slice(&hash.to_be_bytes());
        DocumentId::from_bytes(bytes)
    }

    fn make_product_with_id(id: DocumentId, i: u64) -> IBlob {
        let categories = ["electronics", "books", "clothing", "home", "sports"];
        let category = categories[(i % categories.len() as u64) as usize];
        let price = (i % 1000) as f64 + 0.99;
        IBlob::with_id(id, [
            ("type".into(), Value::String("product".into())),
            ("name".into(), Value::String(format!("Product #{i}"))),
            ("category".into(), Value::String(category.into())),
            ("price".into(), Value::F64(price)),
            ("rating".into(), Value::F64((i % 50) as f64 / 10.0)),
            ("stock".into(), Value::U64(i % 500)),
            ("description".into(), Value::String(format!("Desc {i}"))),
        ].into())
    }

    fn test_engine() -> (LsmEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 4096, // small for testing
            block_size: 256,
            compaction_threshold: 4,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
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
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
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
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
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
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
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
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        for i in 0..3 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // A sort scan always materializes its sorted result as an index — there
        // is no size threshold (the scan is the cost, not the sort).
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();

        assert_eq!(engine.secondary_index_count(), 1, "even a small sort materializes a sorted index");
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
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        let blob1 = make_blob(1);
        let id_to_delete = *blob1.id();
        engine.put(blob1).unwrap();
        for i in 2..10 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Sorted scan creates index via spill
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1);

        // Delete one doc after index was built
        engine.delete(&id_to_delete).unwrap();

        // Scan with index should skip the deleted doc
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(results.len(), 8, "stale entry should be skipped");
        assert!(results.iter().all(|b| b.get("n") != Some(&Value::U64(1))));
    }

    #[test]
    fn test_index_maintained_on_update() {
        // Henrik's scenario: index on field1, update field1 from 5 to 9.
        // Scan for field1 < 7 should NOT find the old value.
        // Scan for field1 > 7 should find the new value.
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        let id = DocumentId::new();
        let blob = IBlob::with_id(id, [("field1".into(), Value::U64(5))].into());
        engine.put(blob).unwrap();

        // Add enough docs to exceed spill threshold
        for i in [1u64, 2, 3, 4, 6, 8, 10, 11, 12] {
            engine.put(IBlob::from_pairs(vec![("field1", Value::U64(i))])).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Sorted scan creates index via spill (>5 results)
        let sort = [SortField { field: "field1".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1);

        // Update field1 from 5 to 9
        let updated = IBlob::with_id(id, [("field1".into(), Value::U64(9))].into());
        engine.put(updated).unwrap();

        // Scan for field1 < 7 — should NOT find the old value (5)
        let results = engine.scan(
            Some(&Filter::Lt { field: "field1".into(), value: Value::U64(7) }),
            Some(&sort),
            None,
            None,
        ).unwrap();
        let vals: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("field1") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(vals, vec![1, 2, 3, 4, 6], "old value 5 should not appear (stale, updated to 9)");

        // Scan for field1 > 7 — should find the new value (9) plus 8, 10, 11, 12
        let results = engine.scan(
            Some(&Filter::Gt { field: "field1".into(), value: Value::U64(7) }),
            Some(&sort),
            None,
            None,
        ).unwrap();
        let vals: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("field1") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(vals, vec![8, 9, 10, 11, 12], "new value 9 should appear among results > 7");
    }

    #[test]
    fn test_index_maintained_on_put_new_doc() {
        // New document inserted after index built should appear in sorted scan
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        for i in 0..8u64 {
            engine.put(IBlob::from_pairs(vec![("val", Value::U64(i * 10))])).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Sorted scan creates index via spill (8 > 5)
        let sort = [SortField { field: "val".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1);

        // Insert new doc after index built
        engine.put(IBlob::from_pairs(vec![("val", Value::U64(25))])).unwrap();

        // Sorted scan should include the new doc in correct position
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        let vals: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("val") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(vals, vec![0, 10, 20, 25, 30, 40, 50, 60, 70], "new doc should appear in sorted position");
    }

    #[test]
    fn test_sort_spills_to_disk() {
        let (engine, _dir) = test_engine();

        // Insert >5 docs (spill threshold)
        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        assert_eq!(engine.secondary_index_count(), 0);

        // Sorted scan should spill to disk and create an index
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(results.len(), 10);

        assert_eq!(engine.secondary_index_count(), 1, "should spill to disk as partial index");
    }

    #[test]
    fn test_small_sort_spills_to_index() {
        let (engine, _dir) = test_engine();

        for i in 0..5 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        let results = engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(results.len(), 5);

        assert_eq!(engine.secondary_index_count(), 1, "a sort always materializes a sorted index");
    }

    #[test]
    fn test_new_scan_replaces_old_index() {
        let (engine, _dir) = test_engine();

        for i in 0..20 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // First sorted scan creates an index (full range, no filter)
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1);

        // Second sorted scan with a filter creates a new partial index, replacing the old
        let filter = Filter::Lt { field: "n".into(), value: Value::U64(10) };
        engine.scan(Some(&filter), Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1, "should replace, not accumulate");
    }

    #[test]
    fn test_descending_sort_uses_index() {
        let (engine, _dir) = test_engine();

        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // First, ascending scan to create the index
        let asc_sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&asc_sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 1);

        // Now descending scan should reuse the same index (reversed)
        let desc_sort = [SortField { field: "n".into(), direction: SortDirection::Descending }];
        let results = engine.scan(None, Some(&desc_sort), None, None).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);

        // Should still be just 1 index (not 2)
        assert_eq!(engine.secondary_index_count(), 1);
    }

    #[test]
    fn test_descending_sort_spills_and_creates_index() {
        let (engine, _dir) = test_engine();

        for i in 0..10 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Descending scan should spill to disk and create an ascending index
        let desc_sort = [SortField { field: "n".into(), direction: SortDirection::Descending }];
        let results = engine.scan(None, Some(&desc_sort), None, None).unwrap();
        let ns: Vec<u64> = results.iter()
            .filter_map(|b| match b.get("n") { Some(Value::U64(n)) => Some(*n), _ => None })
            .collect();
        assert_eq!(ns, vec![9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
        assert_eq!(engine.secondary_index_count(), 1, "descending scan should create ascending index");
    }

    #[test]
    fn test_partial_ranges_accumulated() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        for i in 0..20 {
            engine.put(make_blob(i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Two different filtered scans produce two partial indexes
        let sort = [SortField { field: "n".into(), direction: SortDirection::Ascending }];

        let filter1 = Filter::Lt { field: "n".into(), value: Value::U64(10) };
        engine.scan(Some(&filter1), Some(&sort), None, None).unwrap();

        let filter2 = Filter::Gt { field: "n".into(), value: Value::U64(5) };
        engine.scan(Some(&filter2), Some(&sort), None, None).unwrap();

        assert_eq!(engine.secondary_index_count(), 2, "two different ranges should accumulate");
    }

    // ---- MVCC Snapshot Tests ----

    #[test]
    fn test_snapshot_get_sees_old_version() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(1))].into())).unwrap();

        let snap = engine.snapshot();

        let blob2 = IBlob::with_id(id, [("x".into(), Value::U64(2))].into());
        engine.put(blob2).unwrap();

        // Regular get sees latest
        let latest = engine.get(&id).unwrap().unwrap();
        assert_eq!(latest.get("x"), Some(&Value::U64(2)));

        // Snapshot get sees old version
        let old = snap.get(&id).unwrap().unwrap();
        assert_eq!(old.get("x"), Some(&Value::U64(1)));
    }

    #[test]
    fn test_snapshot_scan_consistent() {
        let (engine, _dir) = test_engine();

        for i in 0..5 {
            engine.put(make_blob(i)).unwrap();
        }

        let snap = engine.snapshot();

        // Insert more after snapshot
        for i in 100..105 {
            engine.put(make_blob(i)).unwrap();
        }

        // Regular scan sees all 10
        let all = engine.scan(None, None, None, None).unwrap();
        assert_eq!(all.len(), 10);

        // Snapshot scan sees only the first 5
        let snapped = snap.scan(None, None, None, None).unwrap();
        assert_eq!(snapped.len(), 5);
        for blob in &snapped {
            if let Some(Value::U64(n)) = blob.get("n") {
                assert!(*n < 100, "snapshot should not see docs inserted after snapshot");
            }
        }
    }

    #[test]
    fn test_snapshot_survives_flush() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 256,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        let id = DocumentId::new();
        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(1))].into())).unwrap();

        let snap = engine.snapshot();

        // Update and flush
        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(2))].into())).unwrap();
        engine.flush_memtable().unwrap();

        // Snapshot still sees old version
        let old = snap.get(&id).unwrap().unwrap();
        assert_eq!(old.get("x"), Some(&Value::U64(1)));

        // Regular get sees new version
        let new = engine.get(&id).unwrap().unwrap();
        assert_eq!(new.get("x"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_snapshot_delete_visibility() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(1))].into())).unwrap();

        let snap = engine.snapshot();

        engine.delete(&id).unwrap();

        // Regular get: deleted
        assert!(engine.get(&id).unwrap().is_none());

        // Snapshot: still sees the document
        let found = snap.get(&id).unwrap().unwrap();
        assert_eq!(found.get("x"), Some(&Value::U64(1)));
    }

    #[test]
    fn test_multiple_snapshots() {
        let (engine, _dir) = test_engine();
        let id = DocumentId::new();

        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(1))].into())).unwrap();
        let s1 = engine.snapshot();

        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(2))].into())).unwrap();
        let s2 = engine.snapshot();

        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(3))].into())).unwrap();

        // Each snapshot sees its own point in time
        assert_eq!(s1.get(&id).unwrap().unwrap().get("x"), Some(&Value::U64(1)));
        assert_eq!(s2.get(&id).unwrap().unwrap().get("x"), Some(&Value::U64(2)));
        assert_eq!(engine.get(&id).unwrap().unwrap().get("x"), Some(&Value::U64(3)));
    }

    #[test]
    fn test_snapshot_gc_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 256,
            compaction_threshold: 2, // low threshold to trigger compaction
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        let id = DocumentId::new();
        engine.put(IBlob::with_id(id, [("x".into(), Value::U64(1))].into())).unwrap();
        engine.flush_memtable().unwrap();

        {
            let _snap = engine.snapshot();
            engine.put(IBlob::with_id(id, [("x".into(), Value::U64(2))].into())).unwrap();
            engine.flush_memtable().unwrap();
            // Snapshot is alive — compaction should keep both versions
        }
        // Snapshot dropped — next compaction can GC old version

        // Latest should still work
        let found = engine.get(&id).unwrap().unwrap();
        assert_eq!(found.get("x"), Some(&Value::U64(2)));
    }

    // ---- Index consistency tests ----

    #[test]
    fn test_index_stale_entry_skipped_after_flush() {
        // Update a doc after index is built, flush both updates.
        // The old index entry should be skipped (stale check).
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        // Insert 10 docs and flush to create primary SSTables
        let target_id = deterministic_id(0);
        for i in 0..10u64 {
            engine.put(make_product_with_id(deterministic_id(i), i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Trigger index creation via sorted scan
        let sort = [SortField { field: "price".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert!(engine.secondary_index_count() >= 1);

        // Update doc 0's price (was 0.99, now 999.99)
        engine.put(IBlob::with_id(target_id, [
            ("type".into(), Value::String("product".into())),
            ("name".into(), Value::String("Updated".into())),
            ("category".into(), Value::String("electronics".into())),
            ("price".into(), Value::F64(999.99)),
            ("rating".into(), Value::F64(0.0)),
            ("stock".into(), Value::U64(0)),
            ("description".into(), Value::String("Updated".into())),
        ].into())).unwrap();
        engine.flush_memtable().unwrap();

        // Scan for cheap products — doc 0 should NOT appear (price is now 999.99)
        let results = engine.scan(
            Some(&Filter::Lt { field: "price".into(), value: Value::F64(10.0) }),
            Some(&sort),
            None,
            None,
        ).unwrap();
        for r in &results {
            assert_ne!(r.id(), &target_id, "stale index entry should be skipped");
        }
    }

    #[test]
    fn test_index_new_entry_visible_after_flush() {
        // Update a doc after index is built, flush. The NEW value should
        // appear in a sorted scan at the correct position.
        // This test uses the same engine instance (no restart) —
        // the in-memory buffer handles it.
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        let target_id = deterministic_id(0);
        for i in 0..10u64 {
            engine.put(make_product_with_id(deterministic_id(i), i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Trigger index creation
        let sort = [SortField { field: "price".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert!(engine.secondary_index_count() >= 1);

        // Update doc 0's price to 999.99
        engine.put(IBlob::with_id(target_id, [
            ("type".into(), Value::String("product".into())),
            ("name".into(), Value::String("Expensive".into())),
            ("category".into(), Value::String("electronics".into())),
            ("price".into(), Value::F64(999.99)),
            ("rating".into(), Value::F64(0.0)),
            ("stock".into(), Value::U64(0)),
            ("description".into(), Value::String("Expensive".into())),
        ].into())).unwrap();
        engine.flush_memtable().unwrap();

        // Scan for expensive products — doc 0 should appear
        let results = engine.scan(
            Some(&Filter::Gt { field: "price".into(), value: Value::F64(500.0) }),
            Some(&sort),
            None,
            None,
        ).unwrap();

        let found = results.iter().any(|r| *r.id() == target_id);
        assert!(found, "updated doc should appear in sorted scan after flush (in-memory buffer)");
    }

    #[test]
    fn test_index_new_entry_visible_after_flush_and_restart() {
        // The real bug: after restart, the in-memory index buffer is lost.
        // The flush must write secondary index entries to disk atomically
        // alongside the primary SSTable.
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };

        let target_id = deterministic_id(0);

        {
            let engine = LsmEngine::open(config.clone()).unwrap();

            // Insert 10 docs and flush
            for i in 0..10u64 {
                engine.put(make_product_with_id(deterministic_id(i), i)).unwrap();
            }
            engine.flush_memtable().unwrap();

            // Trigger index creation
            let sort = [SortField { field: "price".into(), direction: SortDirection::Ascending }];
            engine.scan(None, Some(&sort), None, None).unwrap();
            assert!(engine.secondary_index_count() >= 1);

            // Update doc 0's price to 999.99
            engine.put(IBlob::with_id(target_id, [
                ("type".into(), Value::String("product".into())),
                ("name".into(), Value::String("Expensive".into())),
                ("category".into(), Value::String("electronics".into())),
                ("price".into(), Value::F64(999.99)),
                ("rating".into(), Value::F64(0.0)),
                ("stock".into(), Value::U64(0)),
                ("description".into(), Value::String("Expensive".into())),
            ].into())).unwrap();

            // Flush — should write both primary SSTable AND secondary index entries
            engine.flush_memtable().unwrap();
        }

        // Restart — in-memory index buffer is gone
        {
            let engine = LsmEngine::open(config).unwrap();
            // Note: secondary indexes not loaded here (that's Database's job).
            // For this test, we manually load the index if it exists on disk.
            // The point: after flush, the index SSTable on disk should contain
            // the new entry, not just the in-memory buffer.

            // For now: just verify via a full scan (no index) that the doc is there
            let results = engine.scan(
                Some(&Filter::Gt { field: "price".into(), value: Value::F64(500.0) }),
                None, // no sort — bypass index
                None,
                None,
            ).unwrap();
            let found = results.iter().any(|r| *r.id() == target_id);
            assert!(found, "updated doc visible via full scan after restart");

            // TODO: Once flush writes index entries to disk atomically,
            // this test should also verify sorted scan via index works after restart.
        }
    }

    #[test]
    fn test_filter_uses_secondary_index() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        // Insert docs with categories
        for i in 0..20u64 {
            let cat = if i % 2 == 0 { "electronics" } else { "books" };
            engine.put(IBlob::from_pairs(vec![
                ("category", Value::String(cat.into())),
                ("n", Value::U64(i)),
            ])).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Create index on category by doing a sorted scan
        let sort = [SortField { field: "category".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert!(engine.secondary_index_count() >= 1);

        // Now filter-only scan (no sort) should use the index
        let results = engine.scan(
            Some(&Filter::Eq { field: "category".into(), value: Value::String("electronics".into()) }),
            None,
            None,
            None,
        ).unwrap();

        assert_eq!(results.len(), 10, "should find 10 electronics docs via index");
        for r in &results {
            assert_eq!(r.get("category"), Some(&Value::String("electronics".into())));
        }
    }

    #[test]
    fn test_filter_scan_creates_unsorted_block() {
        // A filter-only scan that falls to a full scan materializes an unsorted
        // block (not a sorted index); repeats are served from the block; the
        // block is promoted to a sorted partial index at compaction.
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        // Insert 20 docs with distinct numeric values.
        for i in 0..20u64 {
            engine.put(IBlob::from_pairs(vec![("n", Value::U64(i))])).unwrap();
        }
        engine.flush_memtable().unwrap();

        // A multi-row RANGE filter (not Eq, not trivially sorted) → unsorted block.
        let filter = Filter::Range { field: "n".into(), low: Value::U64(0), high: Value::U64(10) };

        // First scan: full scan, materializes a block. No sorted index built.
        let results = engine.scan(Some(&filter), None, None, None).unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(engine.secondary_index_count(), 0, "no sorted index from a range filter scan");
        assert_eq!(engine.unsorted_block_count(), 1, "range filter scan materializes a block");

        // Second scan: served by the block (no sorted index, no pre-emption gate).
        let results = engine.scan(Some(&filter), None, None, None).unwrap();
        assert_eq!(results.len(), 10);
        assert_eq!(engine.unsorted_hits(), 1, "repeat served from the block");
        assert_eq!(engine.secondary_index_count(), 0);

        // Compaction promotes the block to a sorted partial index.
        engine.maybe_compact_indexes().unwrap();
        assert_eq!(engine.secondary_index_count(), 1, "block promoted to sorted partial index");
        assert_eq!(engine.unsorted_block_count(), 0);

        // Still correct, now served by the sorted index.
        let results = engine.scan(Some(&filter), None, None, None).unwrap();
        assert_eq!(results.len(), 10, "filter scan via promoted sorted index");
    }

    // ── Consistency level tests ──

    fn test_config_with_consistency(
        dir: &std::path::Path,
        c: Consistency,
    ) -> LsmConfig {
        LsmConfig {
            data_dir: dir.to_path_buf(),
            memtable_size: 4 * 1024 * 1024,
            block_size: 4096,
            compaction_threshold: 100,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false,
            adaptive_w_cooldown_secs: 900,
            adaptive_w_max_step: 2,
            adaptive_w_min: -8,
            adaptive_w_max: 8,
            min_consistency: c,
            commit_wait_usec: 0,
            commit_wait_count: 0,
            commit_busy_mode: false,
        }
    }

    #[test]
    fn commit_mode_select_maps_correctly() {
        assert_eq!(
            CommitMode::select(Consistency::default()).unwrap(),
            CommitMode::Optimistic
        );
        assert_eq!(
            CommitMode::select(Consistency::single_node(ConsistencyModel::LINEARIZABLE)).unwrap(),
            CommitMode::Visible
        );
        assert_eq!(
            CommitMode::select(Consistency::single_node(ConsistencyModel::READ_YOUR_WRITES))
                .unwrap(),
            CommitMode::Visible,
        );
        assert_eq!(
            CommitMode::select(Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE))
                .unwrap(),
            CommitMode::Durable
        );
        assert_eq!(
            CommitMode::select(Consistency::single_node(ConsistencyModel::DURABLE)).unwrap(),
            CommitMode::Durable
        );
    }

    #[test]
    fn cluster_consistency_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::cluster(ConsistencyModel::LINEARIZABLE),
        );
        match LsmEngine::open(config) {
            Err(LsmError::UnsupportedConsistency(_, _)) => {}
            Err(other) => panic!("expected UnsupportedConsistency, got {other}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    /// Read-your-writes under concurrent writers, Visible mode: every put()
    /// that returns must immediately be observable via get() from any thread.
    #[test]
    fn ryw_holds_under_visible_mode() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::single_node(ConsistencyModel::LINEARIZABLE),
        );
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        const THREADS: usize = 8;
        const PER_THREAD: usize = 200;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let mut misses = 0usize;
                for i in 0..PER_THREAD {
                    let blob = make_product_with_id(
                        deterministic_id((t * PER_THREAD + i) as u64),
                        (t * PER_THREAD + i) as u64,
                    );
                    let id = *blob.id();
                    e.put(blob).unwrap();
                    // Read-your-writes: the leader has signaled us → blob is
                    // in the memtable → get() must succeed.
                    if e.get(&id).unwrap().is_none() {
                        misses += 1;
                    }
                }
                misses
            }));
        }
        let total_misses: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_misses, 0, "RYW violation: {total_misses} misses under Visible mode");
    }

    /// Same RYW property under Durable mode (fsync per batch).
    #[test]
    fn ryw_holds_under_durable_mode() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE),
        );
        let engine = Arc::new(LsmEngine::open(config).unwrap());

        const THREADS: usize = 8;
        const PER_THREAD: usize = 100;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let mut misses = 0usize;
                for i in 0..PER_THREAD {
                    let blob = make_product_with_id(
                        deterministic_id((t * PER_THREAD + i) as u64),
                        (t * PER_THREAD + i) as u64,
                    );
                    let id = *blob.id();
                    e.put(blob).unwrap();
                    if e.get(&id).unwrap().is_none() {
                        misses += 1;
                    }
                }
                misses
            }));
        }
        let total_misses: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total_misses, 0, "RYW violation under Durable mode");
    }

    /// Durability: with STRICT_LINEARIZABLE, every put() that returns must be
    /// recoverable after dropping the engine without an explicit flush.
    #[test]
    fn durable_mode_survives_reopen_without_flush() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE),
        );

        const N: u64 = 500;
        let ids: Vec<DocumentId> = (0..N).map(deterministic_id).collect();
        {
            let engine = LsmEngine::open(config.clone()).unwrap();
            for (i, id) in ids.iter().enumerate() {
                engine.put(make_product_with_id(*id, i as u64)).unwrap();
            }
            // No flush_memtable(), no explicit sync — drop the engine here.
            // STRICT_LINEARIZABLE means every put() already fsync'd the WAL.
        }

        // Reopen and read.
        let engine = LsmEngine::open(config).unwrap();
        let mut missing = Vec::new();
        for id in &ids {
            if engine.get(id).unwrap().is_none() {
                missing.push(*id);
            }
        }
        assert!(
            missing.is_empty(),
            "{} of {} blobs missing after reopen — durability broken",
            missing.len(),
            N
        );
    }

    /// Same scenario with Optimistic mode (default): NO durability guarantee.
    /// This test documents the contract; it's allowed to lose writes.
    /// We assert only that the engine reopens cleanly — not that data is intact.
    #[test]
    fn optimistic_mode_reopens_cleanly_even_if_writes_lost() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(dir.path(), Consistency::default());
        {
            let engine = LsmEngine::open(config.clone()).unwrap();
            for i in 0..100u64 {
                engine.put(make_product_with_id(deterministic_id(i), i)).unwrap();
            }
        }
        let engine = LsmEngine::open(config).unwrap();
        // Some or all writes may survive (BufWriter happens to flush on close).
        // The contract only guarantees that reopen doesn't error.
        let _ = engine.get(&deterministic_id(0));
    }

    /// Busy-mode hysteresis: large fsync flips it on; 3 consecutive small
    /// fsyncs flip it off; middle-band fsyncs reset the quiet counter
    /// without changing state.
    #[test]
    fn busy_mode_trips_up_and_down() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE),
        );
        // The test_config helper sets commit_busy_mode=false explicitly;
        // override to exercise the hysteresis.
        let config = LsmConfig {
            commit_busy_mode: true,
            ..config
        };
        let engine = LsmEngine::open(config).unwrap();

        let up = engine.commit_busy_threshold;
        let down = up / 4;
        let mid = (down + up) / 2;

        // Initially off.
        assert!(!engine.commit_busy_active.load(Ordering::Relaxed));

        // A single big fsync flips on.
        engine.maybe_toggle_busy(up);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));

        // Middle-band fsyncs neither flip off nor reset on.
        for _ in 0..5 {
            engine.maybe_toggle_busy(mid);
            assert!(engine.commit_busy_active.load(Ordering::Relaxed));
        }

        // Two consecutive quiet fsyncs — still on.
        engine.maybe_toggle_busy(0);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));
        engine.maybe_toggle_busy(0);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));

        // Third consecutive quiet fsync flips off.
        engine.maybe_toggle_busy(0);
        assert!(!engine.commit_busy_active.load(Ordering::Relaxed));

        // A middle-band fsync after off-state should reset quiet counter
        // without flipping anything (still off).
        engine.maybe_toggle_busy(mid);
        assert!(!engine.commit_busy_active.load(Ordering::Relaxed));

        // 2 quiets after a mid-band reset: should not flip off (it's already
        // off) — really we're checking the counter was reset by mid.
        engine.maybe_toggle_busy(0);
        engine.maybe_toggle_busy(0);
        // Mid-band again to reset.
        engine.maybe_toggle_busy(mid);
        engine.maybe_toggle_busy(0);
        // Only 1 quiet since reset; if we trip on again, then need 3 more.
        engine.maybe_toggle_busy(up);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));

        // Quiet counter was reset by the up-trip; needs 3 fresh quiets.
        engine.maybe_toggle_busy(0);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));
        engine.maybe_toggle_busy(0);
        assert!(engine.commit_busy_active.load(Ordering::Relaxed));
        engine.maybe_toggle_busy(0);
        assert!(!engine.commit_busy_active.load(Ordering::Relaxed));
    }

    /// When commit_busy_mode is disabled, maybe_toggle_busy is a no-op even
    /// for large fsyncs.
    #[test]
    fn busy_mode_disabled_never_trips() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config_with_consistency(
            dir.path(),
            Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE),
        );
        // test_config_with_consistency leaves commit_busy_mode=false.
        let engine = LsmEngine::open(config).unwrap();
        let up = engine.commit_busy_threshold;
        engine.maybe_toggle_busy(up * 100);
        assert!(!engine.commit_busy_active.load(Ordering::Relaxed));
    }

    // ── Unsorted (materialized-subset) index tests ──

    fn unsorted_engine() -> (LsmEngine, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024, // large: avoid auto-flush mid-test
            block_size: 512,
            compaction_threshold: 1000, // high: no background promotion mid-test
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        (LsmEngine::open(config).unwrap(), dir)
    }

    fn doc_x(id: u64, x: u64) -> IBlob {
        IBlob::with_id(deterministic_id(id), [("x".into(), Value::U64(x))].into())
    }

    fn xs(blobs: &[IBlob]) -> Vec<u64> {
        let mut v: Vec<u64> = blobs
            .iter()
            .filter_map(|b| match b.get("x") {
                Some(Value::U64(n)) => Some(*n),
                _ => None,
            })
            .collect();
        v.sort_unstable();
        v
    }

    fn range_x(low: u64, high: u64) -> Filter {
        Filter::Range { field: "x".into(), low: Value::U64(low), high: Value::U64(high) }
    }

    fn eq_x(v: u64) -> Filter {
        Filter::Eq { field: "x".into(), value: Value::U64(v) }
    }

    #[test]
    fn test_equality_index_serves_eq() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i % 5)).unwrap(); // x in 0..5, ten docs each
        }
        engine.flush_memtable().unwrap();

        // First Eq scan: full scan, materializes the posting (no equality hit yet).
        let r1 = engine.scan(Some(&eq_x(3)), None, None, None).unwrap();
        assert_eq!(xs(&r1), vec![3; 10]);
        assert_eq!(engine.equality_hits(), 0, "first scan was a full scan");
        assert_eq!(engine.equality_field_count(), 1, "posting materialized for field x");
        // Eq must NOT promote a sorted or unsorted range index.
        assert_eq!(engine.secondary_index_count(), 0, "Eq no longer promotes sorted partials");
        assert_eq!(engine.unsorted_block_count(), 0, "Eq no longer promotes unsorted blocks");

        // Second Eq scan: served by the equality index, same results.
        let r2 = engine.scan(Some(&eq_x(3)), None, None, None).unwrap();
        assert_eq!(xs(&r2), vec![3; 10]);
        assert_eq!(engine.equality_hits(), 1, "second scan served by equality index");
    }

    #[test]
    fn test_equality_index_serves_in() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i % 5)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // In = Or(Eq, Eq) over the same field — no dedicated AST node.
        let in_filter = Filter::Or(vec![eq_x(1), eq_x(4)]);
        let r1 = engine.scan(Some(&in_filter), None, None, None).unwrap();
        assert_eq!(r1.len(), 20, "ten docs each for x=1 and x=4");
        assert_eq!(engine.equality_hits(), 0, "first scan was a full scan");

        let r2 = engine.scan(Some(&in_filter), None, None, None).unwrap();
        assert_eq!(r2.len(), 20);
        assert_eq!(engine.equality_hits(), 1, "In served by equality index");
    }

    #[test]
    fn test_equality_verify_drops_stale_after_update() {
        let (engine, _dir) = unsorted_engine();
        // 12 docs x=7 among 60 → selective (≤ K), so the posting is Exact and the
        // query is served by the equality index (exercising verify-on-read).
        for i in 0..12u64 {
            engine.put(doc_x(i, 7)).unwrap();
        }
        for i in 12..60u64 {
            engine.put(doc_x(i, i)).unwrap(); // distinct other values
        }
        engine.flush_memtable().unwrap();

        // Build the Exact posting for x=7 (12 ids).
        assert_eq!(engine.scan(Some(&eq_x(7)), None, None, None).unwrap().len(), 12);

        // Update one doc's x away from 7 (same _id, new version).
        engine.put(doc_x(5, 99)).unwrap();

        // The posting still lists id 5, but verifying against the latest doc
        // (now x=99) drops it. 11 survive — no stale positive leaks through.
        let r = engine.scan(Some(&eq_x(7)), None, None, None).unwrap();
        assert_eq!(r.len(), 11, "updated doc dropped by verify-on-read");
    }

    #[test]
    fn test_equality_negative_cache_self_heals() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..10u64 {
            engine.put(doc_x(i, 1)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Eq(42): no match → a negative posting is materialized and reused.
        assert_eq!(engine.scan(Some(&eq_x(42)), None, None, None).unwrap().len(), 0);
        assert_eq!(engine.scan(Some(&eq_x(42)), None, None, None).unwrap().len(), 0);
        assert_eq!(engine.equality_hits(), 1, "negative cache served the repeat");

        // A later matching insert self-heals the negative posting via notify_put.
        engine.put(doc_x(100, 42)).unwrap();
        let r = engine.scan(Some(&eq_x(42)), None, None, None).unwrap();
        assert_eq!(xs(&r), vec![42], "negative cache self-healed on insert");
    }

    #[test]
    fn test_equality_index_snapshot_reads_bypass_and_stay_correct() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..10u64 {
            engine.put(doc_x(i, 5)).unwrap(); // x=5
        }
        engine.flush_memtable().unwrap();

        // Materialize posting for x=5 at the latest version.
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 10);

        // Snapshot, then move every doc to x=6.
        let snap = engine.snapshot();
        for i in 0..10u64 {
            engine.put(doc_x(i, 6)).unwrap();
        }

        // Latest read: all docs now verify to x=6, so Eq(5) matches nothing.
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 0);

        // Snapshot read bypasses the equality index (latest-only) and full-scans
        // with MVCC version filtering, so it still sees the pre-update x=5 values.
        let via_snapshot = snap.scan(Some(&eq_x(5)), None, None, None).unwrap();
        assert_eq!(via_snapshot.len(), 10, "snapshot sees pre-update values");
    }

    #[test]
    fn test_equality_limit_served_from_overflow_sample() {
        let (engine, _dir) = unsorted_engine();
        // 100 docs all x=7 → x=7 is 100% of the SSTable → Overflow (non-selective).
        for i in 0..100u64 {
            engine.put(doc_x(i, 7)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // No LIMIT → non-selective Eq declines to a full scan, returns all 100.
        // (This also builds and caches the Overflow posting.)
        assert_eq!(engine.scan(Some(&eq_x(7)), None, None, None).unwrap().len(), 100);
        assert_eq!(engine.equality_hits(), 0, "no-limit Overflow query declined, not served");

        // LIMIT 10 → served from the cached K-sized Overflow sample, no full scan.
        let r = engine.scan(Some(&eq_x(7)), None, None, Some(10)).unwrap();
        assert_eq!(r.len(), 10);
        assert!(r.iter().all(|b| matches!(b.get("x"), Some(Value::U64(7)))));
        assert_eq!(engine.equality_hits(), 1, "LIMIT served warm from the Overflow sample");

        // A LIMIT larger than the sample can confirm still works (more survive
        // than the limit asks for, here 16 ≥ 12), served from the sample.
        assert_eq!(engine.scan(Some(&eq_x(7)), None, None, Some(12)).unwrap().len(), 12);
    }

    #[test]
    fn test_equality_unions_across_sstables() {
        let (engine, _dir) = unsorted_engine();
        // Two SSTables, each with 10 docs of x=7 (10 ≤ K, so each posting is Exact).
        for i in 0..10u64 {
            engine.put(doc_x(i, 7)).unwrap();
        }
        engine.flush_memtable().unwrap();
        for i in 10..20u64 {
            engine.put(doc_x(i, 7)).unwrap();
        }
        engine.flush_memtable().unwrap();
        assert!(engine.sstable_count() >= 2, "two SSTables");

        // Eq(7) unions both SSTables' per-SSTable postings → 20 docs.
        assert_eq!(engine.scan(Some(&eq_x(7)), None, None, None).unwrap().len(), 20);
        // Second scan is warm: every (SSTable, value) is a cached Exact hit.
        assert_eq!(engine.scan(Some(&eq_x(7)), None, None, None).unwrap().len(), 20);
        assert_eq!(engine.equality_hits(), 1, "second scan warm from Exact postings");
    }

    #[test]
    fn test_equality_correct_across_compaction() {
        // Low threshold so flushes compact; inline (no background thread).
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 2,
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();

        // Distinct x values → selective Eq served by Exact postings (so the
        // rebuild-on-read after compaction is actually exercised, not declined).
        for i in 0..10u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        for i in 10..20u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        engine.maybe_compact().unwrap();

        // Build Exact postings over the current SSTables.
        assert_eq!(engine.scan(Some(&eq_x(15)), None, None, None).unwrap().len(), 1);

        // More data + compaction: postings for the merged-away SSTables are
        // dropped, then rebuilt on the next read.
        for i in 20..30u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        engine.maybe_compact().unwrap();

        // Still correct after the drop/rebuild cycle — values from before and
        // after the compaction both resolve.
        assert_eq!(engine.scan(Some(&eq_x(15)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.scan(Some(&eq_x(25)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.scan(Some(&eq_x(99)), None, None, None).unwrap().len(), 0,
            "absent value → negative posting, no match");
    }

    #[test]
    fn test_equality_capacity_eviction_reloads_from_sidecar() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..20u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        // Force eviction on any build by shrinking the RAM budget to ~nothing.
        engine.equality_postings.lock().set_max_bytes(1);

        // First query builds x=5; the post-build budget check then flushes its
        // sidecar and evicts it from RAM.
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.equality_posting_count(), 0, "evicted from RAM after build");

        // Next query reloads x=5 from its sidecar (not a rebuild scan) and serves
        // it warm — capacity eviction is non-destructive because postings persist.
        let hits0 = engine.equality_hits();
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.equality_hits(), hits0 + 1, "reloaded from sidecar, served warm");
    }

    #[test]
    fn test_equality_postings_persist_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 1000, // no auto-compaction
            scaling_parameter: 0,
            compaction_threads: 1,
            max_ranges_per_field: 500,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };

        {
            let engine = LsmEngine::open(config.clone()).unwrap();
            for i in 0..20u64 {
                engine.put(doc_x(i, i)).unwrap();
            }
            engine.flush_memtable().unwrap();
            // Build an Exact posting for x=5 and a negative for x=99.
            assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
            assert_eq!(engine.scan(Some(&eq_x(99)), None, None, None).unwrap().len(), 0);
            assert!(engine.equality_posting_count() > 0);
            // engine dropped here → flush_equality_sidecars writes the sidecars.
        }

        // Reopen: postings are on disk, not yet in RAM (lazy).
        let engine = LsmEngine::open(config).unwrap();
        let hits0 = engine.equality_hits();

        // A query loads the sidecar and serves warm — no rebuild scan.
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.equality_hits(), hits0 + 1, "warm from the persisted sidecar after restart");
        // The negative cache survived too.
        assert_eq!(engine.scan(Some(&eq_x(99)), None, None, None).unwrap().len(), 0);
    }

    #[test]
    fn test_equality_postings_dropped_when_full_sorted_index_covers() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..20u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Build Eq postings on x.
        engine.scan(Some(&eq_x(5)), None, None, None).unwrap();
        assert!(engine.equality_posting_count() > 0, "Eq postings built on x");

        // A no-filter sort scan on x builds a sorted *full-range* index on x.
        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        engine.scan(None, Some(&sort), None, None).unwrap();
        assert!(
            engine.secondary_indexes.lock().iter().any(|i| i.range.is_none() && i.fields == ["x"]),
            "full-range sorted index on x exists"
        );

        // The redundancy drop (runs at compaction) removes x's now-dead postings.
        engine.drop_redundant_equality_postings();
        assert_eq!(engine.equality_posting_count(), 0, "x Eq postings dropped — sorted index covers Eq");

        // Eq(x=5) is now served by the sorted index, still correct, and does not
        // rebuild equality postings (range index is routed first).
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.equality_posting_count(), 0, "not rebuilt — served by the sorted index");
    }

    #[test]
    fn test_equality_postings_rebuilt_not_dropped_on_compaction() {
        // No auto-compaction (threshold 1000); we compact explicitly so the test
        // is deterministic.
        let (engine, _dir) = unsorted_engine();
        for i in 0..10u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        for i in 10..20u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();
        assert_eq!(engine.sstable_count(), 2);

        // Build postings (two selective values + one negative) across both SSTables.
        engine.scan(Some(&eq_x(5)), None, None, None).unwrap();
        engine.scan(Some(&eq_x(15)), None, None, None).unwrap();
        engine.scan(Some(&eq_x(99)), None, None, None).unwrap(); // negative cache
        let before = engine.equality_posting_count();
        assert!(before > 0, "postings built");

        // Compact both SSTables into one. drop-on-compact would zero the postings
        // (until a future read); rebuild carries them forward immediately.
        let inputs: Vec<PathBuf> = engine
            .sstables
            .read()
            .iter()
            .map(|s| s.path().to_path_buf())
            .collect();
        engine.run_compaction(&inputs, None).unwrap();
        assert_eq!(engine.sstable_count(), 1);

        // Carried forward *without any intervening read* — this is the Phase C
        // refresh, not the Phase B drop-and-rebuild-lazily.
        assert!(
            engine.equality_posting_count() > 0,
            "postings rebuilt onto the merged SSTable during compaction, not dropped"
        );

        // And the carried-forward postings serve correct, warm results.
        assert_eq!(engine.scan(Some(&eq_x(5)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.scan(Some(&eq_x(15)), None, None, None).unwrap().len(), 1);
        assert_eq!(engine.scan(Some(&eq_x(99)), None, None, None).unwrap().len(), 0);
        let hits = engine.equality_hits();
        engine.scan(Some(&eq_x(5)), None, None, None).unwrap();
        assert_eq!(engine.equality_hits(), hits + 1, "served warm from the carried-forward Exact posting");
    }

    #[test]
    fn test_unified_lru_evicts_coldest_across_tiers() {
        // Sorted partials and unsorted blocks share one LRU budget. A range used
        // recently survives even when it sits in a "full" tier, and the globally
        // coldest range is evicted regardless of which tier it's in.
        let dir = tempfile::tempdir().unwrap();
        let config = LsmConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_size: 1024 * 1024,
            block_size: 512,
            compaction_threshold: 1000, // no background compaction mid-test
            scaling_parameter: 0,
            max_ranges_per_field: 3, // small cap to exercise eviction
            compaction_threads: 1,
            adaptive_w: false, adaptive_w_cooldown_secs: 1, adaptive_w_max_step: 2, adaptive_w_min: -8, adaptive_w_max: 8, min_consistency: Consistency::default(), commit_wait_usec: 0, commit_wait_count: 0, commit_busy_mode: false,
        };
        let engine = LsmEngine::open(config).unwrap();
        for i in 0..100u64 {
            engine.put(doc_x(i, i * 10)).unwrap(); // x = 0,10,...,990
        }
        engine.flush_memtable().unwrap();

        // Three single-row range filters → three sorted partials (a 1-row result
        // is trivially sorted, so it spills straight to a sorted partial). NB: Eq
        // filters now route to the equality index, not the range indexes, so we
        // use degenerate single-value ranges to exercise the unified range LRU.
        engine.scan(Some(&range_x(10, 11)), None, None, None).unwrap(); // x=10
        engine.scan(Some(&range_x(20, 21)), None, None, None).unwrap(); // x=20
        engine.scan(Some(&range_x(30, 31)), None, None, None).unwrap(); // x=30
        assert_eq!(engine.secondary_index_count(), 3);
        assert_eq!(engine.unsorted_block_count(), 0);

        // Touch range(10,11) so it is the most-recently-used sorted partial.
        engine.scan(Some(&range_x(10, 11)), None, None, None).unwrap();

        // A multi-row range filter → a 4th range as an unsorted block. Total now
        // exceeds the cap (3), so the globally-coldest range — range(20,21) — is evicted.
        let r = engine.scan(Some(&range_x(40, 70)), None, None, None).unwrap();
        assert_eq!(xs(&r), vec![40, 50, 60]);

        let total = engine.secondary_index_count() + engine.unsorted_block_count();
        assert_eq!(total, 3, "unified cap holds across both tiers");
        assert_eq!(engine.unsorted_block_count(), 1, "the new block survives (most recent)");

        let ranges: Vec<Filter> = engine
            .secondary_indexes
            .lock()
            .iter()
            .filter_map(|i| i.range.clone())
            .collect();
        assert!(ranges.contains(&range_x(10, 11)), "recently-used range(10,11) survives");
        assert!(ranges.contains(&range_x(30, 31)));
        assert!(!ranges.contains(&range_x(20, 21)), "coldest range(20,21) evicted across tiers");
    }

    #[test]
    fn test_unsorted_block_created_and_reused() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let f = range_x(10, 13); // 10,11,12
        // First scan: full scan, materializes a block.
        let r1 = engine.scan(Some(&f), None, None, None).unwrap();
        assert_eq!(xs(&r1), vec![10, 11, 12]);
        assert_eq!(engine.unsorted_block_count(), 1, "block materialized");
        assert_eq!(engine.unsorted_hits(), 0, "first scan was a full scan");

        // Second scan (same range): served by the block, same results.
        let r2 = engine.scan(Some(&f), None, None, None).unwrap();
        assert_eq!(xs(&r2), vec![10, 11, 12]);
        assert_eq!(engine.unsorted_hits(), 1, "second scan served by block");

        // Contained sub-range [11,12) ⊆ [10,13): served by the same block.
        let r3 = engine.scan(Some(&range_x(11, 12)), None, None, None).unwrap();
        assert_eq!(xs(&r3), vec![11]);
        assert_eq!(engine.unsorted_hits(), 2, "contained query served by block");
        assert_eq!(engine.unsorted_block_count(), 1, "no new block for contained query");
    }

    #[test]
    fn test_unsorted_block_update_and_delete() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let f = range_x(10, 13); // 10,11,12
        engine.scan(Some(&f), None, None, None).unwrap(); // materialize block

        engine.put(doc_x(11, 99)).unwrap(); // move 11 out of range
        engine.put(doc_x(40, 11)).unwrap(); // move 40 into range (x=11)
        engine.delete(&deterministic_id(12)).unwrap(); // delete 12

        let r = engine.scan(Some(&f), None, None, None).unwrap();
        // 10 stays; 11(orig) moved out; 12 deleted; 40 moved in with x=11.
        assert_eq!(xs(&r), vec![10, 11]);
        assert!(engine.unsorted_hits() >= 1, "served via block");
    }

    #[test]
    fn test_empty_filter_creates_negative_sorted_partial() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // An empty result is trivially sorted → an empty sorted partial index
        // (the negative cache lives in the sorted index, not a block).
        let f = range_x(100, 110); // empty
        assert_eq!(engine.scan(Some(&f), None, None, None).unwrap().len(), 0);
        assert_eq!(engine.secondary_index_count(), 1, "empty result → empty sorted partial");
        assert_eq!(engine.unsorted_block_count(), 0);

        // Repeat is served by the partial (range_scan over an empty range).
        assert_eq!(engine.scan(Some(&f), None, None, None).unwrap().len(), 0);

        // A later insert into the range shows up (the partial's buffer caught it).
        engine.put(doc_x(105, 105)).unwrap();
        let r = engine.scan(Some(&f), None, None, None).unwrap();
        assert_eq!(r.len(), 1, "insert into the formerly-empty range is visible");
    }

    #[test]
    fn test_sort_range_promotes_block() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let f = range_x(10, 15); // 10..14
        engine.scan(Some(&f), None, None, None).unwrap(); // materialize block
        assert_eq!(engine.unsorted_block_count(), 1);
        assert_eq!(engine.secondary_index_count(), 0);

        // Sort+range scan: served by the block, promoted to a sorted partial
        // index, then the block range is dropped.
        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        let r = engine.scan(Some(&f), Some(&sort), None, None).unwrap();
        let got: Vec<u64> = r
            .iter()
            .filter_map(|b| match b.get("x") {
                Some(Value::U64(n)) => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(got, vec![10, 11, 12, 13, 14], "ascending-sorted results");
        assert!(engine.unsorted_hits() >= 1, "served via block");
        assert_eq!(engine.secondary_index_count(), 1, "promoted to sorted partial index");
        assert_eq!(engine.unsorted_block_count(), 0, "block dropped after promotion");
    }

    #[test]
    fn test_compaction_promotes_unsorted_blocks() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let f = range_x(10, 13);
        engine.scan(Some(&f), None, None, None).unwrap(); // materialize block
        assert_eq!(engine.unsorted_block_count(), 1);
        assert_eq!(engine.secondary_index_count(), 0);

        engine.maybe_compact_indexes().unwrap(); // promotes block → sorted partial

        assert_eq!(engine.unsorted_block_count(), 0, "non-empty block promoted away");
        assert_eq!(engine.secondary_index_count(), 1, "promoted to sorted partial index");

        // Results still correct, now served by the sorted partial index.
        let r = engine.scan(Some(&f), None, None, None).unwrap();
        assert_eq!(xs(&r), vec![10, 11, 12]);
    }

    #[test]
    fn test_block_promoted_despite_unrelated_same_field_index() {
        // Regression for the range-blind guard (found by the ClickBench bench):
        // a sort query `WHERE b=0 ORDER BY a` spills a sorted index keyed by [a]
        // but covering range Eq(b=0). A later [a]-range block must still promote
        // — the old guard skipped it because an [a]-keyed index existed, ignoring
        // that its range covers an unrelated column.
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(IBlob::with_id(deterministic_id(i), [
                ("a".into(), Value::U64(i)),
                ("b".into(), Value::U64(i % 5)),
            ].into())).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Sort query → sorted index fields=[a], range=Eq(b=0).
        let sort = [SortField { field: "a".into(), direction: SortDirection::Ascending }];
        engine.scan(Some(&Filter::Eq { field: "b".into(), value: Value::U64(0) }), Some(&sort), None, None).unwrap();
        assert!(engine.secondary_indexes.lock().iter().any(|i| i.fields == ["a"]));

        // Filter-only range scan on `a` → materializes an [a]-range block (the
        // b-ranged [a] index can't serve it, so it full-scans).
        let arange = Filter::Range { field: "a".into(), low: Value::U64(10), high: Value::U64(20) };
        engine.scan(Some(&arange), None, None, None).unwrap();
        assert_eq!(engine.unsorted_block_count(), 1, "a-range block materialized");

        // Compaction must PROMOTE the a-range block (not drop it).
        engine.maybe_compact_indexes().unwrap();
        assert_eq!(engine.unsorted_block_count(), 0, "block promoted, not dropped");
        let covered = engine.secondary_indexes.lock().iter().any(|idx| {
            idx.fields == ["a"]
                && idx.range.as_ref().map_or(true, |r| unsorted::range_contains(r, &arange))
        });
        assert!(covered, "a-range block promoted to a sorted index that covers it");

        // A contained query is now served correctly.
        let r = engine.scan(Some(&Filter::Range { field: "a".into(), low: Value::U64(12), high: Value::U64(18) }), None, None, None).unwrap();
        assert_eq!(r.len(), 6, "a = 12..17");
    }

    #[test]
    fn test_sorted_index_reports_empty_gap() {
        // A sorted index that covers a range encodes emptiness as the gap
        // between adjacent keys — no negative block is needed.
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i * 10)).unwrap(); // x = 0,10,...,490 (sparse)
        }
        engine.flush_memtable().unwrap();

        // Build a partial sorted index covering (0,200) via a sort+range scan.
        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        engine.scan(Some(&range_x(0, 200)), Some(&sort), None, None).unwrap();
        assert!(engine.secondary_index_count() >= 1, "partial sorted index built");
        let hits_before = engine.unsorted_hits();

        // Empty gap (101,109) ⊆ (0,200): served by the sorted index, returns empty.
        let r = engine.scan(Some(&range_x(101, 109)), None, None, None).unwrap();
        assert!(r.is_empty(), "gap between adjacent keys is known-empty via the sorted index");
        assert_eq!(engine.unsorted_block_count(), 0, "no negative block needed — sorted index covers it");
        assert_eq!(engine.unsorted_hits(), hits_before, "served by sorted index, not a block");
    }

    #[test]
    fn test_empty_partial_absorbed_by_covering() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i * 10)).unwrap(); // x = 0,10,...,490
        }
        engine.flush_memtable().unwrap();

        // Empty gap (101,109) → an empty sorted partial (negative cache).
        let gap = range_x(101, 109);
        assert_eq!(engine.scan(Some(&gap), None, None, None).unwrap().len(), 0);
        // A covering partial (0,200), via a sort+range scan.
        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        engine.scan(Some(&range_x(0, 200)), Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 2, "empty partial + covering partial");

        // At compaction the overlapping partials merge into one (the empty one is
        // absorbed — its emptiness is encoded as the gap in the covering partial).
        engine.maybe_compact_indexes().unwrap();
        assert_eq!(engine.secondary_index_count(), 1, "partials merged into one");

        // The gap still correctly reads empty via the merged sorted partial.
        assert!(engine.scan(Some(&gap), None, None, None).unwrap().is_empty());
    }

    #[test]
    fn test_compaction_merges_contiguous_partials_with_union_range() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        // Two contiguous partial sorted indexes via sort+range scans.
        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        engine.scan(Some(&range_x(0, 20)), Some(&sort), None, None).unwrap();
        engine.scan(Some(&range_x(20, 40)), Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 2, "two partial indexes spilled");

        engine.maybe_compact_indexes().unwrap();

        let idxs = engine.secondary_indexes.lock();
        assert_eq!(idxs.len(), 1, "contiguous partials merged into one");
        assert_eq!(
            idxs[0].range,
            Some(range_x(0, 40)),
            "merged index records the UNION range, not a false full-range claim"
        );
    }

    #[test]
    fn test_compaction_keeps_disjoint_partials_separate() {
        let (engine, _dir) = unsorted_engine();
        for i in 0..50u64 {
            engine.put(doc_x(i, i)).unwrap();
        }
        engine.flush_memtable().unwrap();

        let sort = [SortField { field: "x".into(), direction: SortDirection::Ascending }];
        engine.scan(Some(&range_x(0, 10)), Some(&sort), None, None).unwrap();
        engine.scan(Some(&range_x(30, 40)), Some(&sort), None, None).unwrap();
        assert_eq!(engine.secondary_index_count(), 2);

        engine.maybe_compact_indexes().unwrap();

        let idxs = engine.secondary_indexes.lock();
        assert_eq!(idxs.len(), 2, "disjoint partials are not falsely merged");
        assert!(idxs.iter().all(|i| i.range.is_some()), "neither claims full coverage");
    }
}
