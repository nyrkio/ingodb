//! Unsorted (materialized-subset) indexes.
//!
//! An *unsorted index* is a per-column collection of **blocks**. Each block is a
//! materialized result of a range/filter scan: a projected partial index
//! (predicate field + `_id`) over a recorded `Filter` range, stored `_id`-ordered
//! (i.e. *not* sorted by the field — that is what "unsorted" means). Blocks are a
//! cache: created reactively when a scan falls back to a full primary scan and
//! returns a proper subset, reused for any later query whose predicate is
//! *contained* in the block's range.
//!
//! Unlike the sorted [`crate::secondary::SecondaryIndex`], a block does not have to
//! pay a field-sort at materialization time. Field-sorting only happens during
//! compaction (cheap reorganization while we are rewriting anyway), which also
//! merges overlapping/adjacent ranges.
//!
//! Blocks store *projected* blobs (field + `_id`) like the sorted index, so reads
//! still `get()` back to primary; the win is a smaller scan input, not a covered
//! query. Deletes need no propagation: the read path resolves each `_id` against
//! primary and re-checks the filter, so a stale `_id` is dropped at read time.

use ingodb_blob::{DocumentId, IBlob, Value};
use ingodb_query::{compare_values, Filter};
use ingodb_sstable::{IdKeyExtractor, SSTableReader, SSTableWriter};
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

use crate::LsmError;

/// A scan result larger than this fraction of the collection is not materialized
/// (scanning the whole collection is as cheap as scanning such a large subset).
pub const UNSORTED_MATERIALIZE_MAX_FRACTION: f64 = 0.5;

// ---------------------------------------------------------------------------
// Interval / containment helpers (single column)
// ---------------------------------------------------------------------------

/// One end of an interval over the `Value` domain.
#[derive(Debug, Clone, PartialEq)]
enum Bound {
    NegInf,
    PosInf,
    Inc(Value),
    Exc(Value),
}

/// Convert a single-field filter into `(field, low, high)`. Returns `None` for
/// filters that are not single-column intervals (And/Or/Not).
fn interval_of(f: &Filter) -> Option<(&str, Bound, Bound)> {
    match f {
        Filter::Eq { field, value } => Some((field, Bound::Inc(value.clone()), Bound::Inc(value.clone()))),
        Filter::Gt { field, value } => Some((field, Bound::Exc(value.clone()), Bound::PosInf)),
        Filter::Lt { field, value } => Some((field, Bound::NegInf, Bound::Exc(value.clone()))),
        Filter::Range { field, low, high } => Some((field, Bound::Inc(low.clone()), Bound::Exc(high.clone()))),
        Filter::Exists { field } => Some((field, Bound::NegInf, Bound::PosInf)),
        _ => None,
    }
}

/// Compare two *lower* bounds. `None` if the underlying values are incomparable.
fn cmp_lower(a: &Bound, b: &Bound) -> Option<Ordering> {
    match (a, b) {
        (Bound::NegInf, Bound::NegInf) => Some(Ordering::Equal),
        (Bound::NegInf, _) => Some(Ordering::Less),
        (_, Bound::NegInf) => Some(Ordering::Greater),
        // PosInf as a lower bound means "starts above everything".
        (Bound::PosInf, Bound::PosInf) => Some(Ordering::Equal),
        (Bound::PosInf, _) => Some(Ordering::Greater),
        (_, Bound::PosInf) => Some(Ordering::Less),
        (a, b) => {
            let (va, ia) = bound_val_incl(a);
            let (vb, ib) = bound_val_incl(b);
            match compare_values(va, vb)? {
                Ordering::Equal => {
                    // Inclusive lower bound starts at-or-before exclusive lower bound.
                    Some(ib.cmp(&ia)) // ia/ib: true=inclusive; inclusive(=lower) < exclusive
                }
                ord => Some(ord),
            }
        }
    }
}

/// Compare two *upper* bounds. `None` if the underlying values are incomparable.
fn cmp_upper(a: &Bound, b: &Bound) -> Option<Ordering> {
    match (a, b) {
        (Bound::PosInf, Bound::PosInf) => Some(Ordering::Equal),
        (Bound::PosInf, _) => Some(Ordering::Greater),
        (_, Bound::PosInf) => Some(Ordering::Less),
        (Bound::NegInf, Bound::NegInf) => Some(Ordering::Equal),
        (Bound::NegInf, _) => Some(Ordering::Less),
        (_, Bound::NegInf) => Some(Ordering::Greater),
        (a, b) => {
            let (va, ia) = bound_val_incl(a);
            let (vb, ib) = bound_val_incl(b);
            match compare_values(va, vb)? {
                Ordering::Equal => {
                    // Inclusive upper bound ends at-or-after exclusive upper bound.
                    Some(ia.cmp(&ib)) // inclusive(=higher) > exclusive
                }
                ord => Some(ord),
            }
        }
    }
}

fn bound_val_incl(b: &Bound) -> (&Value, bool) {
    match b {
        Bound::Inc(v) => (v, true),
        Bound::Exc(v) => (v, false),
        _ => unreachable!("bound_val_incl on infinite bound"),
    }
}

/// Does the `outer` filter's range fully contain the `inner` filter's range?
/// Same field required. Incomparable value types → `false`.
pub fn range_contains(outer: &Filter, inner: &Filter) -> bool {
    let (of, olo, ohi) = match interval_of(outer) {
        Some(x) => x,
        None => return false,
    };
    let (inf, ilo, ihi) = match interval_of(inner) {
        Some(x) => x,
        None => return false,
    };
    if of != inf {
        return false;
    }
    // outer.lo <= inner.lo  AND  inner.hi <= outer.hi
    matches!(cmp_lower(&olo, &ilo), Some(Ordering::Less) | Some(Ordering::Equal))
        && matches!(cmp_upper(&ihi, &ohi), Some(Ordering::Less) | Some(Ordering::Equal))
}

/// Render a `(field, low, high)` interval back into a representable `Filter`.
/// Returns `None` when the interval cannot be expressed by a simple filter
/// (e.g. an inclusive upper bound, which only `Range`'s exclusive high cannot
/// represent). Used when merging blocks; an un-representable union is skipped.
fn filter_of_interval(field: &str, lo: &Bound, hi: &Bound) -> Option<Filter> {
    match (lo, hi) {
        (Bound::NegInf, Bound::PosInf) => Some(Filter::Exists { field: field.to_string() }),
        (Bound::NegInf, Bound::Exc(v)) => Some(Filter::Lt { field: field.to_string(), value: v.clone() }),
        (Bound::Exc(v), Bound::PosInf) => Some(Filter::Gt { field: field.to_string(), value: v.clone() }),
        (Bound::Inc(a), Bound::Exc(b)) => Some(Filter::Range { field: field.to_string(), low: a.clone(), high: b.clone() }),
        (Bound::Inc(a), Bound::Inc(b)) if compare_values(a, b) == Some(Ordering::Equal) => {
            Some(Filter::Eq { field: field.to_string(), value: a.clone() })
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Block
// ---------------------------------------------------------------------------

/// A single materialized result block: projected blobs over a `Filter` range.
pub struct Block {
    /// The indexed field.
    pub field: String,
    /// The predicate range this block covers.
    pub range: Filter,
    /// On-disk projected entries (`None` for an empty / negative-cache block).
    reader: Option<SSTableReader>,
    /// Path to the block SSTable (may not exist yet for an empty block).
    path: PathBuf,
    /// In-memory delta from `notify_put`, keyed by `_id` (unsorted by field).
    buffer: Mutex<BTreeMap<DocumentId, IBlob>>,
    /// Last time this block served a query (LRU).
    last_used: Mutex<Instant>,
}

impl Block {
    /// Materialize a block from the matching documents of a scan.
    /// `matching` are full IBlobs that satisfied `range`; they are projected to
    /// `[field]` and written `_id`-ordered. An empty `matching` yields a valid
    /// negative-cache block (no file written).
    pub fn materialize(
        field: &str,
        range: Filter,
        matching: &[IBlob],
        path: PathBuf,
        block_size: usize,
    ) -> Result<Self, LsmError> {
        let fields = vec![field.to_string()];
        let reader = if matching.is_empty() {
            None
        } else {
            let mut projected: Vec<IBlob> = matching.iter().map(|b| b.project(&fields)).collect();
            SSTableWriter::with_block_size(block_size).write(&path, &mut projected, &IdKeyExtractor)?;
            Some(SSTableReader::open(&path)?)
        };
        Ok(Block {
            field: field.to_string(),
            range,
            reader,
            path,
            buffer: Mutex::new(BTreeMap::new()),
            last_used: Mutex::new(Instant::now()),
        })
    }

    /// The predicate range this block covers.
    pub fn range(&self) -> &Filter {
        &self.range
    }

    /// Live (non-tombstone) projected entries, for promotion to a sorted index.
    pub fn live_entries(&self) -> Result<Vec<IBlob>, LsmError> {
        Ok(self.entries()?.into_iter().filter(|b| !b.is_deleted()).collect())
    }

    /// All projected entries (on-disk + buffer), deduped by `_id` (buffer wins).
    fn entries(&self) -> Result<Vec<IBlob>, LsmError> {
        let mut by_id: BTreeMap<DocumentId, IBlob> = BTreeMap::new();
        if let Some(reader) = &self.reader {
            for (_, blob) in reader.iter()? {
                by_id.insert(*blob.id(), blob);
            }
        }
        for (id, blob) in self.buffer.lock().iter() {
            by_id.insert(*id, blob.clone());
        }
        Ok(by_id.into_values().collect())
    }

    /// Candidate `(id, projected)` pairs whose projected field value satisfies
    /// `query`. Tombstones in the buffer are skipped.
    pub fn scan(&self, query: &Filter) -> Result<Vec<(DocumentId, IBlob)>, LsmError> {
        let mut out = Vec::new();
        for blob in self.entries()? {
            if blob.is_deleted() {
                continue;
            }
            if query.matches(&|f| blob.get_field(f)) {
                out.push((*blob.id(), blob));
            }
        }
        Ok(out)
    }

    /// If the written document falls in this block's range, buffer its projection.
    pub fn notify_put(&self, blob: &IBlob) {
        if self.range.matches(&|f| blob.get_field(f)) {
            let projected = blob.project(std::slice::from_ref(&self.field));
            self.buffer.lock().insert(*blob.id(), projected);
        }
    }

    /// Does this block's range fully contain `query`?
    pub fn contains_query(&self, query: &Filter) -> bool {
        range_contains(&self.range, query)
    }

    /// Total entry count (doc-count) for budget accounting.
    pub fn entry_count(&self) -> u64 {
        self.entries().map(|e| e.len() as u64).unwrap_or(0)
    }

    pub fn mark_used(&self) {
        *self.last_used.lock() = Instant::now();
    }

    /// When this block last served a query (LRU eviction).
    pub fn last_used(&self) -> Instant {
        *self.last_used.lock()
    }

    /// Remove this block's backing SSTable file.
    pub fn delete_file(&self) {
        std::fs::remove_file(&self.path).ok();
    }
}

// ---------------------------------------------------------------------------
// UnsortedIndex (per column)
// ---------------------------------------------------------------------------

/// The per-column collection of materialized-subset blocks.
pub struct UnsortedIndex {
    pub field: String,
    pub blocks: Vec<Block>,
    dir: PathBuf,
    next_id: Mutex<u64>,
}

impl UnsortedIndex {
    pub fn new(field: String, dir: PathBuf) -> Self {
        UnsortedIndex { field, blocks: Vec::new(), dir, next_id: Mutex::new(0) }
    }

    fn next_path(&self) -> PathBuf {
        let mut n = self.next_id.lock();
        let id = *n;
        *n += 1;
        // Sanitize field for a filename (simple fields only in v1).
        let safe: String = self.field.chars().map(|c| if c.is_alphanumeric() { c } else { '_' }).collect();
        self.dir.join(format!("u_{safe}_{id:012}.sst"))
    }

    /// Materialize a new block covering `range` from the scan's matching docs.
    /// Skips creation if an identical-range block already exists.
    pub fn materialize(&mut self, range: Filter, matching: &[IBlob], block_size: usize) -> Result<(), LsmError> {
        if self.blocks.iter().any(|b| b.range == range) {
            return Ok(());
        }
        let path = self.next_path();
        let block = Block::materialize(&self.field, range, matching, path, block_size)?;
        self.blocks.push(block);
        Ok(())
    }

    /// Find the smallest block that fully contains `query`, if any.
    pub fn find_covering(&self, query: &Filter) -> Option<&Block> {
        self.blocks
            .iter()
            .filter(|b| b.contains_query(query))
            .min_by_key(|b| b.entry_count())
    }

    /// Fan a write out to every block whose range matches.
    pub fn notify_put(&self, blob: &IBlob) {
        for b in &self.blocks {
            b.notify_put(blob);
        }
    }

    /// Drain ALL blocks for promotion to sorted partial indexes (compaction).
    /// Returns each block's `(range, live entries)` — including empty blocks,
    /// which promote to empty sorted partials (negative cache: a range proven to
    /// hold zero keys, expressed as the gap between adjacent sorted keys).
    pub fn drain_all(&mut self) -> Result<Vec<(Filter, Vec<IBlob>)>, LsmError> {
        let mut out = Vec::new();
        for block in std::mem::take(&mut self.blocks) {
            out.push((block.range.clone(), block.live_entries()?));
            block.delete_file();
        }
        Ok(out)
    }

    /// Remove the smallest block covering `query` (used after a sort+range query
    /// promotes that range into a sorted partial index). Returns true if dropped.
    pub fn remove_covering(&mut self, query: &Filter) -> bool {
        let pos = self
            .blocks
            .iter()
            .enumerate()
            .filter(|(_, b)| b.contains_query(query))
            .min_by_key(|(_, b)| b.entry_count())
            .map(|(i, _)| i);
        if let Some(i) = pos {
            self.blocks.remove(i).delete_file();
            true
        } else {
            false
        }
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
}

/// The union of two single-field interval filters on the same field, if the
/// result is a single representable interval (overlapping or touching). `None`
/// if disjoint, different fields, or not representable.
pub fn union_filter(a: &Filter, b: &Filter) -> Option<Filter> {
    let (af, alo, ahi) = interval_of(a)?;
    let (bf, blo, bhi) = interval_of(b)?;
    if af != bf {
        return None;
    }
    // Require overlap or touching: a.lo <= b.hi AND b.lo <= a.hi (treating
    // touching exclusive/inclusive at the same value as adjacent).
    if !ranges_touch(&alo, &ahi, &blo, &bhi) {
        return None;
    }
    let lo = min_lower(&alo, &blo)?;
    let hi = max_upper(&ahi, &bhi)?;
    filter_of_interval(af, &lo, &hi)
}

fn ranges_touch(alo: &Bound, ahi: &Bound, blo: &Bound, bhi: &Bound) -> bool {
    // a.lo <= b.hi and b.lo <= a.hi (lower-vs-upper comparison).
    lower_le_upper(alo, bhi) && lower_le_upper(blo, ahi)
}

/// Is `lo` (a lower bound) at-or-below `hi` (an upper bound)?
fn lower_le_upper(lo: &Bound, hi: &Bound) -> bool {
    match (lo, hi) {
        (Bound::NegInf, _) | (_, Bound::PosInf) => true,
        (Bound::PosInf, _) | (_, Bound::NegInf) => false,
        (lo, hi) => {
            let (lv, _) = bound_val_incl(lo);
            let (hv, _) = bound_val_incl(hi);
            // Touching counts (e.g. Range high == next Range low).
            matches!(compare_values(lv, hv), Some(Ordering::Less) | Some(Ordering::Equal))
        }
    }
}

fn min_lower(a: &Bound, b: &Bound) -> Option<Bound> {
    match cmp_lower(a, b)? {
        Ordering::Greater => Some(b.clone()),
        _ => Some(a.clone()),
    }
}

fn max_upper(a: &Bound, b: &Bound) -> Option<Bound> {
    match cmp_upper(a, b)? {
        Ordering::Less => Some(b.clone()),
        _ => Some(a.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::Value;

    fn blob(id: u64, x: u64) -> IBlob {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&id.to_be_bytes());
        let mut b = IBlob::with_id(DocumentId::from_bytes(bytes), [("x".into(), Value::U64(x))].into());
        b.set_version(DocumentId::new());
        b
    }

    fn range(low: u64, high: u64) -> Filter {
        Filter::Range { field: "x".into(), low: Value::U64(low), high: Value::U64(high) }
    }

    #[test]
    fn test_range_contains() {
        // [10,20) contains [12,18)
        assert!(range_contains(&range(10, 20), &range(12, 18)));
        // [10,20) contains [10,20)
        assert!(range_contains(&range(10, 20), &range(10, 20)));
        // [10,20) does NOT contain [5,18)
        assert!(!range_contains(&range(10, 20), &range(5, 18)));
        // [10,20) does NOT contain [12,25)
        assert!(!range_contains(&range(10, 20), &range(12, 25)));
        // Eq contained in Range
        assert!(range_contains(&range(10, 20), &Filter::Eq { field: "x".into(), value: Value::U64(15) }));
        // Different field
        assert!(!range_contains(&range(10, 20), &Filter::Range { field: "y".into(), low: Value::U64(12), high: Value::U64(18) }));
        // Exists contains everything on the field
        assert!(range_contains(&Filter::Exists { field: "x".into() }, &range(12, 18)));
        // Gt / Lt
        assert!(range_contains(&Filter::Gt { field: "x".into(), value: Value::U64(5) }, &range(10, 20)));
        assert!(range_contains(&Filter::Lt { field: "x".into(), value: Value::U64(50) }, &range(10, 20)));
        // Type mismatch → false
        assert!(!range_contains(&range(10, 20), &Filter::Eq { field: "x".into(), value: Value::String("z".into()) }));
    }

    #[test]
    fn test_block_materialize_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let matching: Vec<IBlob> = (10..20).map(|i| blob(i, i)).collect();
        let block = Block::materialize(
            "x",
            range(10, 20),
            &matching,
            dir.path().join("b.sst"),
            4096,
        )
        .unwrap();
        assert_eq!(block.entry_count(), 10);

        // Contained query [12,15) → 3 docs (12,13,14)
        let hits = block.scan(&range(12, 15)).unwrap();
        assert_eq!(hits.len(), 3);
    }

    #[test]
    fn test_negative_block() {
        let dir = tempfile::tempdir().unwrap();
        let block = Block::materialize("x", range(100, 200), &[], dir.path().join("neg.sst"), 4096).unwrap();
        assert_eq!(block.entry_count(), 0);
        // A contained query returns empty (negative cache).
        assert!(block.scan(&range(120, 130)).unwrap().is_empty());
        // A put in range makes it non-empty.
        block.notify_put(&blob(150, 150));
        assert_eq!(block.entry_count(), 1);
        assert_eq!(block.scan(&range(120, 160)).unwrap().len(), 1);
    }

    #[test]
    fn test_notify_put_range_filter() {
        let dir = tempfile::tempdir().unwrap();
        let block = Block::materialize("x", range(10, 20), &[blob(10, 10)], dir.path().join("b.sst"), 4096).unwrap();
        block.notify_put(&blob(15, 15)); // in range
        block.notify_put(&blob(99, 99)); // out of range — ignored
        assert_eq!(block.entry_count(), 2);
    }

    #[test]
    fn test_find_covering() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = UnsortedIndex::new("x".into(), dir.path().to_path_buf());
        idx.materialize(range(0, 100), &(0..100).map(|i| blob(i, i)).collect::<Vec<_>>(), 4096).unwrap();
        idx.materialize(range(0, 10), &(0..10).map(|i| blob(i, i)).collect::<Vec<_>>(), 4096).unwrap();

        // Query [2,5) is contained by both; should pick the smaller block.
        let b = idx.find_covering(&range(2, 5)).unwrap();
        assert_eq!(b.entry_count(), 10);

        // Query [50,60) only contained by the big block.
        let b = idx.find_covering(&range(50, 60)).unwrap();
        assert_eq!(b.entry_count(), 100);

        // Query [50,200) not contained by anything.
        assert!(idx.find_covering(&range(50, 200)).is_none());
    }

    #[test]
    fn test_union_filter() {
        // [0,10) ∪ [10,20) = [0,20)
        assert_eq!(union_filter(&range(0, 10), &range(10, 20)).unwrap(), range(0, 20));
        // [0,10) ∪ [5,15) = [0,15)
        assert_eq!(union_filter(&range(0, 10), &range(5, 15)).unwrap(), range(0, 15));
        // Disjoint → None
        assert!(union_filter(&range(0, 10), &range(20, 30)).is_none());
    }

    #[test]
    fn test_drain_all_includes_negative() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = UnsortedIndex::new("x".into(), dir.path().to_path_buf());
        idx.materialize(range(0, 10), &(0..10).map(|i| blob(i, i)).collect::<Vec<_>>(), 4096).unwrap();
        idx.materialize(range(100, 200), &[], 4096).unwrap(); // negative block
        assert_eq!(idx.block_count(), 2);

        let drained = idx.drain_all().unwrap();
        assert_eq!(drained.len(), 2, "all blocks drained for promotion (incl. empty)");
        assert_eq!(idx.block_count(), 0);
        // The empty block drains as an empty entry list (→ empty sorted partial).
        let empty = drained.iter().find(|(r, _)| *r == range(100, 200)).unwrap();
        assert!(empty.1.is_empty());
    }

    #[test]
    fn test_remove_covering() {
        let dir = tempfile::tempdir().unwrap();
        let mut idx = UnsortedIndex::new("x".into(), dir.path().to_path_buf());
        idx.materialize(range(0, 100), &(0..100).map(|i| blob(i, i)).collect::<Vec<_>>(), 4096).unwrap();
        assert!(idx.remove_covering(&range(10, 20)));
        assert_eq!(idx.block_count(), 0);
        assert!(!idx.remove_covering(&range(10, 20)));
    }

}
