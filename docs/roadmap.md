# Roadmap — What's Built, What's Next

## Completed

### Storage Engine (Phases 1-3)
- I-Blob binary format with UUIDv7 identity, BLAKE3 integrity hash, tombstone/projection flags
- Write-Ahead Log (CRC32-checksummed, crash-recoverable)
- Multi-version MemTable (keyed by `(_id, _version)` for MVCC)
- SSTables with parameterized sort keys (KeyExtractor trait), LZ4 compression, bloom filters
- UCS-inspired compaction with configurable scaling parameter W
- Level-aware read ordering (L0→L1→..., first match is current version)
- Tombstone-based deletes, purged during compaction when safe
- Background compaction thread pool (configurable, default 4 threads)
- Concurrent read scaling via RwLock (near-linear to 8 threads)

### Query Layer (Phase 4)
- Liquid AST: Get, Scan (filter/sort/projection/limit), Traverse (join-by-value)
- Filter predicates: Eq, Gt, Lt, Range, Exists, And, Or, Not
- `_id` as a first-class queryable/joinable field
- Graph traversal as join-by-value (any field, any depth, no pointer types)
- Comparable byte encoding for sort keys (I64 sign flip, F64 IEEE trick)

### MVCC (Phase 5b)
- Snapshot isolation: `engine.snapshot()` captures a consistent point-in-time view
- Multi-version memtable and SSTables (MVCC key = `_id + _version`)
- Compaction preserves versions referenced by active snapshots
- Zero overhead when no snapshots active

### Reactive Optimization (Phase 5a)
- Always-on query statistics (pattern, latency, docs scanned vs returned)
- Sort queries always spill to disk as secondary index SSTables
- Filter queries trigger reactive index creation after 2 executions with low selectivity
- Binary search range scan on secondary indexes: O(log N + R) for filter queries
- Partial index ranges: each index covers a specific filter range, evolved through compaction
- Smart compaction: merge vs rebuild at M > 0.5 * N * ln(N) threshold
- Index metadata crash-safe in system collection
- Atomic flush: primary SSTable + index entries written together before WAL reset

### Collections & Database
- Database wraps multiple LsmEngines in subdirectories
- System collection for engine metadata (index definitions)
- Collections survive restart, indexes loaded from system collection

### Benchmarks
- E-commerce product catalog benchmark (100K-1M documents)
- UCS W parameter comparison across 5 configurations
- Concurrent read scaling measurements
- Compaction statistics (runs, bytes, write amplification)

## In Progress / Next

### Performance Optimizations
- **Covered queries**: Store enough fields in the index to answer queries without primary lookup. Currently every index hit does a `get()` back to the primary.
- **Compound indexes**: Multi-field indexes for queries like `WHERE category = 'x' ORDER BY price`
- **Adaptive W**: Auto-tune the UCS scaling parameter based on observed read/write ratio

### Advanced Reactive Morphing (Phase 5c)
- **Semantic shredding**: Extract hot fields into columnar side-structures during compaction. When the engine notices a field is frequently filtered or aggregated, it can store that field's values in a column-oriented format alongside the row-oriented IBlobs.
- **Co-location**: Physically interleave related documents during compaction. If traverse queries frequently join orders→users, compact them into the same SSTable blocks for locality.
- **Compression adaptation**: Choose LZ4/Zstd/dictionary per level based on data characteristics.

### Predictive Optimization (Phase 5d)
- Anticipate future query patterns from recent trends
- Pre-emptively build indexes before a pattern becomes a bottleneck
- Speculative optimization with reactive as the safety net

### Custom Sort Orders (Phase 6)
- WASM comparator functions for user-defined sort orders
- The `KeyExtractor` trait is ready — custom implementations can plug in
- Comparator metadata stored in system collection

### Infrastructure (Phase 7)
- **gRPC server**: Expose the Liquid AST over the network via tonic
- **Criterion benchmarks**: Microbenchmarks for individual operations
- **CI pipeline**: Automated testing and performance regression detection
- **Distributed/replication**: Much later — the single-node engine must be solid first

## Design Debts

- Index buffer entries rely on atomic flush (not WAL-persisted independently)
- Bloom filter false positive rate may increase with many MVCC key prefixes
- `scan_with_secondary_index` memtable merge path could be more efficient
- No query plan caching — each scan re-evaluates whether to use an index
- Secondary index compaction doesn't run in the background thread pool (runs inline during flush)

## 170 Tests

The test suite covers:
- Blob encoding/decoding roundtrips (19 tests)
- WAL recovery and corruption handling (5 tests)
- Memtable multi-version operations (7 tests)
- SSTable read/write with variable-length keys (17 tests)
- LSM engine: put/get/delete, scan/sort/filter, traverse, MVCC snapshots, reactive indexes, compaction, concurrent access (88 tests)
- Query filter evaluation (6 tests)
- Database collections and index persistence (10 tests)
- Integration tests (24 tests)
- Secondary index build, maintenance, compaction (included in LSM tests)
