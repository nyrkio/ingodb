//! # IngoDB
//!
//! A self-morphing, AI-native storage engine built on an adaptive LSM tree.
//!
//! IngoDB accepts arbitrary documents identified by stable UUIDv7 `_id`s,
//! and stores them in an LSM structure that can evolve its physical layout
//! over time based on access patterns and benchmarking data.

pub use ingodb_blob::{self as blob, ContentHash, DocumentId, IBlob, Value};
pub use ingodb_lsm::{self as lsm, LsmConfig, LsmEngine, LsmError};
pub use ingodb_memtable::{self as memtable, MemTable};
pub use ingodb_query::{self as query, Filter, Query, QueryResult};
pub use ingodb_sstable::{self as sstable, SSTableReader, SSTableWriter};
pub use ingodb_wal::{self as wal, Wal};

/// Convenience: open an IngoDB engine with default config at the given path.
pub fn open(path: impl Into<std::path::PathBuf>) -> Result<LsmEngine, LsmError> {
    let config = LsmConfig {
        data_dir: path.into(),
        ..LsmConfig::default()
    };
    LsmEngine::open(config)
}
