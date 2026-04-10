# IngoDB Implementation Plan

## Philosophy

Build bottom-up: get bytes on disk correctly first, then layer intelligence on top.
Each phase produces a working, testable artifact. No phase depends on network or external services.

---

## Completed Phases

### Phase 1: Foundation — I-Blob + MemTable + WAL ✓
- I-Blob format: UUIDv7 `_id`, server-assigned `_version`, flags (deleted/projection), BLAKE3 integrity hash
- Write-Ahead Log: append-only, CRC32-checksummed, crash-recoverable
- MemTable: BTreeMap keyed by DocumentId, size-bounded with flush signaling

### Phase 2: Persistent Storage — SSTables ✓
- Parameterized sort keys via KeyExtractor trait (variable-length, comparable byte encoding)
- LZ4-compressed data blocks, bloom filters, block index
- IdKeyExtractor (primary) and FieldKeyExtractor (secondary indexes)

### Phase 3: Compaction ✓
- UCS-inspired compaction: level assignment by file size, overlap detection, configurable scaling parameter W
- Level-aware read ordering (L0→L1→..., first match is current version)
- Tombstone-based deletes, purged when compaction output exceeds max level

### Phase 4: Query Interface ✓
- Liquid AST: Get (O(log n)), Scan (filter/sort/projection/limit), Traverse (join-by-value)
- `_id` queryable/joinable, `_version` read-only. Graph edges = query-time joins.
- Value::Uuid for UUID field values (no pointer types)
- Sort always spills to disk as partial secondary index (above threshold)

### Phase 5a: Reactive Optimization ✓
- Query statistics: pattern/latency/docs_scanned/docs_returned, selectivity detection
- Secondary indexes: SSTables sorted by field values, created automatically on sort queries
- Partial index ranges: each index covers a specific filter range, accumulates, merged at compaction
- Index maintenance: eager buffer on put, lazy stale check on read
- Smart compaction: merge vs full rebuild at M > 0.5*N*ln(N), extend to full range at ≥50% coverage
- Multiple partial indexes for the same field are merged via full rebuild from primary SSTables (ensures complete coverage)
- Drop unused indexes after ≥10 compaction cycles AND ≥30 days
- Collections: Database wraps multiple LsmEngines, system collection for crash-safe metadata

### Phase 5b: MVCC — Snapshot Reads ✓
- Read from a stable snapshot: `_version <= snapshot_version` at scan start
- `get_at()` and `scan_at()` filter by snapshot version via MvccKeyExtractor
- Old versions retained until no snapshot references them (GC via `active_snapshots` BTreeSet)
- Enables consistent scan results during concurrent writes

---

## Remaining Phases

### Phase 5c: Advanced Reactive Morphing
- **Semantic shredding**: extract hot fields into columnar side-structures during compaction
- **Co-location**: physically interleave related documents (by traversal graph) during compaction
- **Compression adaptation**: choose LZ4/Zstd/dictionary per level based on data characteristics

### Phase 5d: Predictive Optimization
- Small predictive engine that anticipates future query patterns from recent history
- Pre-emptively restructure data ahead of observed trends

### Phase 6: Custom Comparators / Sort Orders
- **WASM comparator support**: user-defined sort orders via WebAssembly functions
- The `KeyExtractor` trait is ready — custom implementations can plug in
- Comparator functions stored as metadata in the system collection

### Phase 5e: Performance TODOs
- **Scan performance**: Full scan of 100K docs takes ~200-370ms. Mixed workload with scans hangs — need scan optimization (secondary indexes for filter fields, not just sort fields)
- **Index speedup modest** (~1.2x): secondary index still does per-document get() lookup back to primary. Consider storing full IBlobs in the index for covered queries.
- **Bloom filter for MVCC keys**: Currently skipped for prefix lookups. Consider adding a separate _id-only bloom filter alongside the MVCC key bloom.

### Phase 7: Infrastructure (Future)
- gRPC server with Liquid AST protocol (tonic)
- Benchmarks with criterion (external benchmarks can be wired to Nyrkiö)
- CI pipeline
- Distributed/replication layer (much later)
