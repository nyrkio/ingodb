use ingodb_blob::{ContentHash, IBlob};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// In-memory sorted table of IBlobs keyed by content hash.
///
/// Thread-safe via RwLock. Tracks approximate memory usage
/// and signals when it should be flushed to an SSTable.
pub struct MemTable {
    entries: RwLock<BTreeMap<ContentHash, IBlob>>,
    /// Approximate bytes used (sum of encoded blob sizes)
    size_bytes: AtomicUsize,
    /// Flush threshold in bytes
    max_size: usize,
}

/// Iterator over memtable entries in hash-sorted order.
pub struct MemTableIter {
    entries: Vec<(ContentHash, IBlob)>,
    pos: usize,
}

impl Iterator for MemTableIter {
    type Item = (ContentHash, IBlob);

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
    pub fn insert(&self, blob: IBlob) -> bool {
        let encoded_size = blob.encode().len();
        let hash = *blob.hash();

        let mut entries = self.entries.write();
        if let Some(old) = entries.insert(hash, blob) {
            // Replacing existing entry — adjust size
            let old_size = old.encode().len();
            self.size_bytes.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.size_bytes.fetch_add(encoded_size, Ordering::Relaxed);

        self.size_bytes.load(Ordering::Relaxed) >= self.max_size
    }

    /// Look up a blob by its content hash.
    pub fn get(&self, hash: &ContentHash) -> Option<IBlob> {
        self.entries.read().get(hash).cloned()
    }

    /// Check if a hash exists in the memtable.
    pub fn contains(&self, hash: &ContentHash) -> bool {
        self.entries.read().contains_key(hash)
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
    /// Returns entries sorted by content hash.
    pub fn drain(&self) -> Vec<(ContentHash, IBlob)> {
        let mut entries = self.entries.write();
        let drained: Vec<_> = entries.iter().map(|(k, v)| (*k, v.clone())).collect();
        entries.clear();
        self.size_bytes.store(0, Ordering::Relaxed);
        drained
    }

    /// Create an iterator over entries in hash-sorted order.
    pub fn iter(&self) -> MemTableIter {
        let entries: Vec<_> = self.entries.read().iter().map(|(k, v)| (*k, v.clone())).collect();
        MemTableIter { entries, pos: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::Value;

    fn make_blob(id: u64) -> IBlob {
        IBlob::from_pairs(vec![
            ("id", Value::U64(id)),
            ("data", Value::String(format!("item-{id}"))),
        ])
    }

    #[test]
    fn test_insert_and_get() {
        let mt = MemTable::new(1024 * 1024);
        let blob = make_blob(1);
        let hash = *blob.hash();

        mt.insert(blob.clone());
        let retrieved = mt.get(&hash).unwrap();
        assert_eq!(retrieved.hash(), &hash);
        assert_eq!(mt.len(), 1);
    }

    #[test]
    fn test_duplicate_insert() {
        let mt = MemTable::new(1024 * 1024);
        let blob = make_blob(1);
        mt.insert(blob.clone());
        mt.insert(blob.clone()); // same hash, should replace
        assert_eq!(mt.len(), 1);
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

        // Verify sorted by hash
        for i in 1..drained.len() {
            assert!(drained[i - 1].0 <= drained[i].0);
        }
    }

    #[test]
    fn test_flush_threshold() {
        // Tiny threshold
        let mt = MemTable::new(1);
        let should = mt.insert(make_blob(1));
        assert!(should);
        assert!(mt.should_flush());
    }

    #[test]
    fn test_contains() {
        let mt = MemTable::new(1024 * 1024);
        let blob = make_blob(42);
        let hash = *blob.hash();

        assert!(!mt.contains(&hash));
        mt.insert(blob);
        assert!(mt.contains(&hash));
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
}
