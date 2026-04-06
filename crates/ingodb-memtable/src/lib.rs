use ingodb_blob::{DocumentId, IBlob};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// In-memory sorted table of IBlobs keyed by document ID.
///
/// Thread-safe via RwLock. Tracks approximate memory usage
/// and signals when it should be flushed to an SSTable.
pub struct MemTable {
    entries: RwLock<BTreeMap<DocumentId, IBlob>>,
    /// Approximate bytes used (sum of encoded blob sizes)
    size_bytes: AtomicUsize,
    /// Flush threshold in bytes
    max_size: usize,
}

/// Iterator over memtable entries in ID-sorted order.
pub struct MemTableIter {
    entries: Vec<(DocumentId, IBlob)>,
    pos: usize,
}

impl Iterator for MemTableIter {
    type Item = (DocumentId, IBlob);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entries.len() {
            let item = self.entries[self.pos].clone();
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }
}

impl MemTable {
    /// Create a new MemTable with the given flush threshold.
    pub fn new(max_size: usize) -> Self {
        MemTable {
            entries: RwLock::new(BTreeMap::new()),
            size_bytes: AtomicUsize::new(0),
            max_size,
        }
    }

    /// Default MemTable with 64 MB threshold.
    pub fn with_default_size() -> Self {
        Self::new(64 * 1024 * 1024)
    }

    /// Insert a blob. Returns true if the memtable should be flushed.
    pub fn insert(&self, mut blob: IBlob) -> bool {
        let encoded_size = blob.encode().len();
        let id = *blob.id();

        let mut entries = self.entries.write();
        if let Some(mut old) = entries.insert(id, blob) {
            // Replacing existing entry (upsert) — adjust size
            let old_size = old.encode().len();
            self.size_bytes.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.size_bytes.fetch_add(encoded_size, Ordering::Relaxed);

        self.size_bytes.load(Ordering::Relaxed) >= self.max_size
    }

    /// Look up a blob by its document ID.
    pub fn get(&self, id: &DocumentId) -> Option<IBlob> {
        self.entries.read().get(id).cloned()
    }

    /// Check if a document ID exists in the memtable.
    pub fn contains(&self, id: &DocumentId) -> bool {
        self.entries.read().contains_key(id)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Whether the memtable is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Approximate size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Whether the memtable has reached its flush threshold.
    pub fn should_flush(&self) -> bool {
        self.size_bytes.load(Ordering::Relaxed) >= self.max_size
    }

    /// Drain all entries for flushing to an SSTable.
    /// Returns entries sorted by document ID.
    pub fn drain(&self) -> Vec<(DocumentId, IBlob)> {
        let mut entries = self.entries.write();
        let drained: Vec<_> = entries.iter().map(|(k, v)| (*k, v.clone())).collect();
        entries.clear();
        self.size_bytes.store(0, Ordering::Relaxed);
        drained
    }

    /// Create an iterator over entries in ID-sorted order.
    pub fn iter(&self) -> MemTableIter {
        let entries: Vec<_> = self.entries.read().iter().map(|(k, v)| (*k, v.clone())).collect();
        MemTableIter { entries, pos: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::{DocumentId, Value};

    fn make_blob(n: u64) -> IBlob {
        IBlob::from_pairs(vec![
            ("n", Value::U64(n)),
            ("data", Value::String(format!("item-{n}"))),
        ])
    }

    #[test]
    fn test_insert_and_get() {
        let mt = MemTable::new(1024 * 1024);
        let blob = make_blob(1);
        let id = *blob.id();

        mt.insert(blob.clone());
        let retrieved = mt.get(&id).unwrap();
        assert_eq!(retrieved.id(), &id);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn test_upsert_same_id() {
        let mt = MemTable::new(1024 * 1024);
        let id = DocumentId::new();

        let blob1 = IBlob::with_id(id, [("x".into(), Value::U64(1))].into());
        let blob2 = IBlob::with_id(id, [("x".into(), Value::U64(2))].into());

        mt.insert(blob1);
        mt.insert(blob2.clone());
        assert_eq!(mt.len(), 1, "same _id replaces old entry");

        let retrieved = mt.get(&id).unwrap();
        assert_eq!(retrieved.get("x"), Some(&Value::U64(2)));
    }

    #[test]
    fn test_drain() {
        let mt = MemTable::new(1024 * 1024);
        for i in 0..10 {
            mt.insert(make_blob(i));
        }
        assert_eq!(mt.len(), 10);

        let drained = mt.drain();
        assert_eq!(drained.len(), 10);
        assert!(mt.is_empty());
        assert_eq!(mt.size_bytes(), 0);

        // Verify sorted by document ID
        for i in 1..drained.len() {
            assert!(drained[i - 1].0 <= drained[i].0);
        }
    }

    #[test]
    fn test_flush_threshold() {
        let mt = MemTable::new(1);
        let should = mt.insert(make_blob(1));
        assert!(should);
        assert!(mt.should_flush());
    }

    #[test]
    fn test_contains() {
        let mt = MemTable::new(1024 * 1024);
        let blob = make_blob(42);
        let id = *blob.id();

        assert!(!mt.contains(&id));
        mt.insert(blob);
        assert!(mt.contains(&id));
    }

    #[test]
    fn test_iter_sorted() {
        let mt = MemTable::new(1024 * 1024);
        for i in 0..20 {
            mt.insert(make_blob(i));
        }

        let entries: Vec<_> = mt.iter().collect();
        assert_eq!(entries.len(), 20);
        for i in 1..entries.len() {
            assert!(entries[i - 1].0 <= entries[i].0);
        }
    }

    #[test]
    fn test_tombstone_replaces_live() {
        let mt = MemTable::new(1024 * 1024);
        let id = DocumentId::new();

        let blob = IBlob::with_id(id, [("x".into(), Value::U64(1))].into());
        mt.insert(blob);
        assert!(!mt.get(&id).unwrap().is_deleted());

        let tombstone = IBlob::tombstone(id);
        mt.insert(tombstone);
        assert_eq!(mt.len(), 1, "tombstone replaces live entry");
        assert!(mt.get(&id).unwrap().is_deleted());
    }
}
