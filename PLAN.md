# IngoDB Implementation Plan

## Philosophy

Build bottom-up: get bytes on disk correctly first, then layer intelligence on top.
Each phase produces a working, testable artifact. No phase depends on network or external services.

---

## Phase 1: Foundation — I-Blob + MemTable + WAL

**Goal**: Accept arbitrary documents, hash them, store them in memory, and survive restarts.

### 1a. I-Blob Format (`ingodb-blob` crate)
- Define the on-wire/on-disk binary format:
  - `[magic: 4B][version: 2B][hash: 32B][index_count: 4B][index_entries...][payload...]`
  - Each index entry: `[key_hash: 8B][offset: 4B][length: 4B]` — 16 bytes per field
- BLAKE3 content hash over the canonical (sorted-key) serialization
- Encode/decode from: raw key-value map (`BTreeMap<Vec<u8>, Vec<u8>>`)
- Standard value types as tagged unions: U64, I64, F64, Bytes, String, Bool, Null, Hash (reference), Array, Nested document
- Zero-copy reads: the index table lets you extract a single field without parsing the whole payload
- Property: identical logical documents produce identical hashes (canonical key ordering)

### 1b. Write-Ahead Log (`ingodb-wal` crate)
- Append-only file of length-prefixed, CRC32-checksummed records
- Each record: `[length: 4B][crc32: 4B][blob bytes...]`
- Recovery: read forward, skip corrupted tail
- Sync policy: `fsync` after every write (configurable batch mode later)

### 1c. MemTable (`ingodb-memtable` crate)
- Sorted in-memory structure keyed by content hash
- Use a `BTreeMap<[u8; 32], IBlob>` initially (swap to skip list later if needed)
- Size-bounded: flush to SSTable when memtable exceeds threshold (default 64 MB)
- Support point lookup by hash and full iteration (for flush)

---

## Phase 2: Persistent Storage — SSTables

**Goal**: Flush memtables to immutable sorted files on disk. Read from both memtable and SSTables.

### 2a. SSTable Writer
- Block-based format:
  - Data blocks (configurable size, default 4 KB, aligned for direct I/O)
  - Index block: maps last-key-of-block → block offset
  - Bloom filter block (per-SSTable)
  - Footer: offsets to index and bloom blocks + magic
- LZ4 compression per data block (optional, configurable)
- Each data block entry: `[hash: 32B][blob_length: 4B][blob...]`

### 2b. SSTable Reader
- Memory-mapped or pread-based reads
- Bloom filter check before binary search on index
- Block cache (LRU, configurable size)

### 2c. LSM Read Path
- Merge reads across: active memtable → immutable memtable(s) → L0 SSTables (newest first)
- Point lookup: check bloom filters, return first match
- Scan: merge iterators with dedup by hash

---

## Phase 3: Compaction — Size-Tiered → UCS

**Goal**: Background compaction that merges SSTables and reclaims space.

### 3a. Size-Tiered Compaction (baseline)
- Group SSTables by similar size
- When a group reaches threshold (default 4), merge into one
- Tombstone handling: deleted hashes marked, purged during compaction

### 3b. UCS (Universal Compaction Strategy)
- Implement fan-out and density-based tiering inspired by Cassandra UCS
- Each level can have T tiers before triggering compaction
- Configurable parameters: min/max fan-out, density threshold
- This is the "evolutionary chamber" where future morphing will happen

### 3c. Compaction Filter Hook
- Pluggable `CompactionFilter` trait:
  ```rust
  trait CompactionFilter {
      fn filter(&mut self, hash: &[u8; 32], blob: &IBlob) -> CompactionAction;
  }
  enum CompactionAction { Keep, Drop, Transform(IBlob) }
  ```
- This is the extension point for Phase 5's semantic shredding

---

## Phase 4: Query Interface — Liquid AST

**Goal**: Programmatic query API that doesn't require SQL parsing.

### 4a. Query AST
- Rust enum-based AST:
  - `Get { hash }` — point lookup
  - `Scan { start, end, filter, limit }` — range scan over hashes
  - `Filter` predicates: `Eq(field, value)`, `Gt`, `Lt`, `Range`, `Exists(field)`
  - `Project { fields }` — return only specified fields from blobs
- No SQL parser. The AST *is* the interface.

### 4b. Secondary Index (field → hash set)
- Automatically maintained for "hot" fields discovered by access counting
- Initially: manual `create_index(field_name)` API
- Stored as its own SSTable-like structure

### 4c. Hash References / Graph Traversal
- `Traverse { start_hash, edge_field, depth }` — follow hash references
- The engine resolves references by point-lookup, benefiting from co-location in later phases

---

## Phase 5: Adaptive Morphing (Future)

**Goal**: The engine learns and restructures data during compaction.

- Access pattern tracker: count field access frequency per SSTable
- Semantic shredding: extract hot fields into columnar side-structures during compaction
- Co-location: physically interleave related documents (by hash reference graph)
- Compression adaptation: choose LZ4/Zstd/dictionary per level based on data characteristics
- WASM comparator support for custom sort orders
- Nyrkiö integration: benchmark before/after each morphing decision

---

## Phase 6: Infrastructure (Future, needs GitHub + CI)

- gRPC server with Liquid AST protocol (tonic)
- Benchmarks with criterion, integrated with Nyrkiö
- CI pipeline
- Distributed/replication layer (much later)

---

## Crate Structure

```
ingodb/
├── Cargo.toml              (workspace)
├── crates/
│   ├── ingodb-blob/        Phase 1a: I-Blob format
│   ├── ingodb-wal/         Phase 1b: Write-ahead log
│   ├── ingodb-memtable/    Phase 1c: MemTable
│   ├── ingodb-sstable/     Phase 2: SSTable read/write
│   ├── ingodb-lsm/         Phase 2-3: LSM tree + compaction
│   ├── ingodb-query/       Phase 4: Query AST + execution
│   └── ingodb/             Top-level API crate (re-exports)
├── tests/                  Integration tests
└── benches/                Criterion benchmarks
```

## What I'll Build Tonight

Phases 1a through 2b — the I-Blob format, WAL, MemTable, and SSTable writer/reader —
with unit tests for each. This gives us a working storage engine that can accept documents,
persist them, and read them back. Compaction and queries come next.
