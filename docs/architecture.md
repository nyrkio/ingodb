# Architecture

## Crate Structure

```
ingodb/
├── ingodb-blob         I-Blob document format
├── ingodb-wal          Write-Ahead Log
├── ingodb-memtable     Multi-version in-memory table
├── ingodb-sstable      SSTable reader/writer with parameterized sort keys
├── ingodb-lsm          LSM engine, compaction, secondary indexes, statistics
├── ingodb-query        Liquid AST query types and filter predicates
└── ingodb              Top-level API crate (re-exports)
```

## The I-Blob Format

Every document is an **IBlob** — a binary-encoded collection of typed fields with metadata:

```
[magic: 4B "INGO"]
[format_version: 2B]
[document_id: 16B UUIDv7]       ← stable identity, client or server generated
[doc_version: 16B UUIDv7]       ← server-assigned at write time
[flags: 1B]                     ← bit 0: deleted, bit 1: projection
[content_hash: 32B BLAKE3]      ← covers id + version + flags + fields
[index_count: 4B]
[index_entries: N * 16B]        ← field key hash → offset/length
[payload: variable]             ← encoded field values in key-sorted order
```

**Header: 75 bytes.** The content hash provides full integrity protection — any corrupted byte (including `_id`, `_version`, or `flags`) is detected on decode.

### Key Types

- **`DocumentId`** — Newtype around `[u8; 16]`, a UUIDv7. Timestamp-prefixed and lexicographically sortable. Both client and server can generate these independently.

- **`Value`** — Tagged union of supported field types:
  - `Null`, `Bool`, `I64`, `U64`, `F64`, `String`, `Bytes`, `Uuid(DocumentId)`, `Array`, `Document` (nested)
  - No special pointer/reference type. Graph edges are query-time joins, not stored relationships.

- **`ContentHash`** — `[u8; 32]`, BLAKE3 hash. Used for integrity verification, not as a primary key.

### Identity Model

- **`_id`**: Stable document identity. Persists across updates. Client or server can generate.
- **`_version`**: Server-assigned UUIDv7, stamped at `put()` time. Monotonically increasing. Used for MVCC snapshot reads and compaction dedup.
- **`flags`**: Bitfield. Bit 0 = tombstone (deleted). Bit 1 = projection (partial view of a document).

## Data Flow

### Write Path

```
Application
  → LsmEngine::put(IBlob)
    → stamp _version = DocumentId::new()
    → WAL append (CRC32-checksummed)
    → notify secondary indexes (buffer new entries)
    → MemTable insert (keyed by (_id, _version))
    → if memtable full:
        → flush to SSTable (MvccKeyExtractor: _id + _version)
        → flush secondary index buffers to disk
        → reset WAL
        → signal background compaction
```

### Read Path

```
Application
  → LsmEngine::get(id)
    → MemTable lookup (highest _version ≤ snapshot)
    → SSTable scan (L0 → L1 → L2, within level newest first)
      → bloom filter check (skips SSTables that don't contain the _id)
      → binary search block index
      → decompress + search within block
    → first match with _version ≤ snapshot is returned
    → tombstone check: if is_deleted, return None
```

### Delete Path

```
Application
  → LsmEngine::delete(id)
    → write tombstone IBlob (is_deleted flag, empty fields)
    → same path as put()
    → tombstones purged during compaction when output level > max level
```

## SSTable Format

SSTables use parameterized sort keys via the `KeyExtractor` trait:

- **`MvccKeyExtractor`**: Primary SSTables. Key = `_id + _version` (32 bytes). Supports multiple versions per document.
- **`FieldKeyExtractor`**: Secondary indexes. Key = comparable byte encoding of field values. Supports binary search range scans.
- **`IdKeyExtractor`**: Legacy 16-byte key (backward compat).

### Comparable Byte Encoding

Field values are encoded to bytes where lexicographic byte comparison matches value comparison:

| Type | Encoding |
|------|----------|
| Null | `[0x00]` |
| Bool | `[0x01][0 or 1]` |
| I64 | `[0x02][sign-flipped big-endian]` |
| U64 | `[0x03][big-endian]` |
| F64 | `[0x04][IEEE sortable]` |
| String | `[0x05][utf8 bytes][0x00]` |
| Uuid | `[0x07][16 bytes]` |

This enables binary search on secondary indexes sorted by arbitrary field values.

## Collections

A `Database` wraps multiple `LsmEngine` instances, each in its own subdirectory:

```
data_dir/
  system/       ← reserved for engine metadata (index definitions)
  users/        ← user collection
  orders/       ← user collection
```

The `system` collection stores secondary index metadata as regular IBlobs, making it crash-safe via WAL.

## Threading Model

- **Reads**: Multiple concurrent readers via `RwLock` on the SSTable list. Near-linear scaling measured up to 8 threads (500K ops/sec at 100K docs).
- **Writes**: Serialized through WAL `Mutex`. Single writer at a time.
- **Compaction**: Background thread pool (configurable, default 4 threads). Coordinator picks all eligible compaction groups, dispatches to workers in parallel.
- **Snapshots**: Lock-free creation (`DocumentId::new()` + insert into `BTreeSet`). Released on drop.
