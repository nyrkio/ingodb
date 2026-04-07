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
- Drop unused indexes after ≥10 compaction cycles AND ≥30 days
- Collections: Database wraps multiple LsmEngines, system collection for crash-safe metadata

---

## Remaining Phases

### Phase 5b: Advanced Reactive Morphing
- **Semantic shredding**: extract hot fields into columnar side-structures during compaction
- **Co-location**: physically interleave related documents (by traversal graph) during compaction
- **Compression adaptation**: choose LZ4/Zstd/dictionary per level based on data characteristics

### Phase 5c: Predictive Optimization
- Small predictive engine that anticipates future query patterns from recent history
- Pre-emptively restructure data ahead of observed trends

### Phase 6: Custom Comparators
- **WASM comparator support**: user-defined sort orders via WebAssembly functions
- The `KeyExtractor` trait is ready — custom implementations can plug in
- Comparator functions stored as metadata in the system collection

### Phase 7: Infrastructure
- gRPC server with Liquid AST protocol (tonic)
- Benchmarks with criterion (external benchmarks can be wired to Nyrkiö)
- CI pipeline
- Distributed/replication layer (much later)
