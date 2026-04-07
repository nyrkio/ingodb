use ingodb_blob::{DocumentId, IBlob};
use ingodb_sstable::{FieldKeyExtractor, SSTableReader, SSTableWriter};
use std::path::{Path, PathBuf};

use crate::LsmError;

/// A secondary index: an SSTable sorted by field values instead of _id.
///
/// Each entry is a projected IBlob containing just the indexed fields.
/// The _id on each projected blob points back to the primary document.
/// Stale entries (from updated/deleted documents) are detected at read
/// time by checking the primary via get().
pub struct SecondaryIndex {
    /// Fields this index covers (the sort key)
    pub fields: Vec<String>,
    /// The index SSTable
    pub reader: SSTableReader,
    /// Path to the index SSTable
    pub path: PathBuf,
}

impl SecondaryIndex {
    /// Build a secondary index from primary SSTables.
    ///
    /// Reads all primary SSTables, deduplicates by _id (newest version wins),
    /// skips tombstones, projects to indexed fields, and writes an SSTable
    /// sorted by those field values.
    pub fn build(
        fields: &[String],
        primary_sstables: &[&SSTableReader],
        output_path: impl AsRef<Path>,
        block_size: usize,
    ) -> Result<Self, LsmError> {
        // Collect all blobs from primary SSTables
        let mut all: Vec<IBlob> = Vec::new();
        for sst in primary_sstables {
            all.extend(sst.iter()?.into_iter().map(|(_, blob)| blob));
        }

        // Dedup by _id, keep highest _version
        all.sort_by(|a, b| a.id().cmp(b.id()).then_with(|| b.version().cmp(a.version())));
        all.dedup_by(|a, b| a.id() == b.id());

        // Skip tombstones and project to indexed fields
        let mut projected: Vec<IBlob> = all
            .into_iter()
            .filter(|blob| !blob.is_deleted())
            .map(|blob| blob.project(fields))
            .collect();

        if projected.is_empty() {
            return Err(LsmError::SSTable(ingodb_sstable::SSTableError::Empty));
        }

        // Write SSTable sorted by field values
        let extractor = FieldKeyExtractor::new(fields.to_vec());
        let output_path = output_path.as_ref().to_path_buf();
        SSTableWriter::with_block_size(block_size)
            .write(&output_path, &mut projected, &extractor)?;

        let reader = SSTableReader::open(&output_path)?;
        Ok(SecondaryIndex {
            fields: fields.to_vec(),
            reader,
            path: output_path,
        })
    }

    /// Open an existing secondary index from disk.
    pub fn open(fields: Vec<String>, path: impl AsRef<Path>) -> Result<Self, LsmError> {
        let path = path.as_ref().to_path_buf();
        let reader = SSTableReader::open(&path)?;
        Ok(SecondaryIndex { fields, reader, path })
    }

    /// Iterate the index in sorted order, yielding (_id, projected IBlob) pairs.
    pub fn iter_sorted(&self) -> Result<Vec<(DocumentId, IBlob)>, LsmError> {
        let entries = self.reader.iter()?;
        Ok(entries
            .into_iter()
            .map(|(_, blob)| (*blob.id(), blob))
            .collect())
    }

    /// Check if this index covers the given sort fields.
    pub fn matches_sort(&self, sort_fields: &[String]) -> bool {
        self.fields == sort_fields
    }
}

/// Default number of query executions before building an index reactively.
pub const DEFAULT_INDEX_THRESHOLD: u64 = 3;

#[cfg(test)]
mod tests {
    use super::*;
    use ingodb_blob::Value;
    use ingodb_sstable::IdKeyExtractor;

    fn make_primary_sst(dir: &Path, blobs: &mut [IBlob]) -> SSTableReader {
        let path = dir.join(format!("{}.sst", blobs[0].id()));
        SSTableWriter::new().write(&path, blobs, &IdKeyExtractor).unwrap();
        SSTableReader::open(&path).unwrap()
    }

    #[test]
    fn test_build_secondary_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut blobs = vec![
            IBlob::from_pairs(vec![("name", Value::String("Charlie".into())), ("age", Value::U64(30))]),
            IBlob::from_pairs(vec![("name", Value::String("Alice".into())), ("age", Value::U64(25))]),
            IBlob::from_pairs(vec![("name", Value::String("Bob".into())), ("age", Value::U64(35))]),
        ];
        // Stamp versions (normally done by engine)
        for blob in &mut blobs {
            blob.set_version(DocumentId::new());
        }

        let primary = make_primary_sst(dir.path(), &mut blobs);
        let idx_path = dir.path().join("idx_name.sst");

        let index = SecondaryIndex::build(
            &["name".into()],
            &[&primary],
            &idx_path,
            4096,
        ).unwrap();

        // Iterate — should be sorted by name
        let sorted = index.iter_sorted().unwrap();
        let names: Vec<_> = sorted.iter()
            .filter_map(|(_, blob)| blob.get("name").cloned())
            .collect();
        assert_eq!(names, vec![
            Value::String("Alice".into()),
            Value::String("Bob".into()),
            Value::String("Charlie".into()),
        ]);
    }

    #[test]
    fn test_index_skips_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let mut live = IBlob::from_pairs(vec![("name", Value::String("Alice".into()))]);
        live.set_version(DocumentId::new());

        let mut tomb = IBlob::tombstone(DocumentId::new());
        tomb.set_version(DocumentId::new());

        let mut blobs = vec![live, tomb];
        let primary = make_primary_sst(dir.path(), &mut blobs);
        let idx_path = dir.path().join("idx_name.sst");

        let index = SecondaryIndex::build(
            &["name".into()],
            &[&primary],
            &idx_path,
            4096,
        ).unwrap();

        let sorted = index.iter_sorted().unwrap();
        assert_eq!(sorted.len(), 1, "tombstone should be excluded from index");
    }

    #[test]
    fn test_index_matches_sort() {
        let dir = tempfile::tempdir().unwrap();
        let mut blobs = vec![
            IBlob::from_pairs(vec![("name", Value::String("A".into()))]),
        ];
        blobs[0].set_version(DocumentId::new());
        let primary = make_primary_sst(dir.path(), &mut blobs);

        let index = SecondaryIndex::build(
            &["name".into()],
            &[&primary],
            dir.path().join("idx.sst"),
            4096,
        ).unwrap();

        assert!(index.matches_sort(&["name".into()]));
        assert!(!index.matches_sort(&["age".into()]));
        assert!(!index.matches_sort(&["name".into(), "age".into()]));
    }

    #[test]
    fn test_index_open_existing() {
        let dir = tempfile::tempdir().unwrap();
        let mut blobs = vec![
            IBlob::from_pairs(vec![("x", Value::U64(1))]),
        ];
        blobs[0].set_version(DocumentId::new());
        let primary = make_primary_sst(dir.path(), &mut blobs);
        let idx_path = dir.path().join("idx.sst");

        SecondaryIndex::build(&["x".into()], &[&primary], &idx_path, 4096).unwrap();

        // Reopen
        let index = SecondaryIndex::open(vec!["x".into()], &idx_path).unwrap();
        assert_eq!(index.iter_sorted().unwrap().len(), 1);
    }
}
