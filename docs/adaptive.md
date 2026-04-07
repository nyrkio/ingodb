# Adaptive Engine — How IngoDB Learns

The defining feature of IngoDB is that it adapts its physical data layout based on observed query patterns. No `CREATE INDEX` command. No DBA tuning. The engine watches what's slow and fixes itself.

## The Feedback Loop

```
Query executes
  → Statistics recorded (fields, latency, docs scanned vs returned)
  → Pattern detected (repeated filter/sort with low selectivity)
  → Index created reactively (SSTable sorted by the relevant field)
  → Next query uses the index (O(log N + R) instead of O(N))
  → Compaction maintains/evolves the index over time
```

## Query Statistics

Every query execution records:

| Metric | What it measures |
|--------|-----------------|
| `query_type` | get / scan / traverse |
| `filter_fields` | Which fields appear in the filter predicate |
| `sort_fields` | Which fields are sorted on |
| `join_edge` | (from_field, to_field) for traverse queries |
| `docs_scanned` | Documents examined before filtering |
| `docs_returned` | Documents that passed the filter |
| `latency` | Wall-clock duration |

These are aggregated by **query pattern** — two queries with the same structure (same filter fields, same sort fields) share one counter. This is how the engine detects "this pattern is hot."

### Selectivity

The key metric: `selectivity = docs_returned / docs_scanned`.

- Selectivity 1.0: every scanned document is returned (no index needed)
- Selectivity 0.01: scanning 10,000 documents to find 100 (index would help enormously)

`QueryStats::low_selectivity(threshold, min_count)` finds patterns where an index would help most.

## Reactive Index Creation

### Sort Indexes (always-spill)

When a sorted scan produces more results than `sort_spill_threshold` (default 1000), the sorted results are written to disk as a **secondary index SSTable**. This is not an optimization decision — it's the sort mechanism. Sorting to disk IS creating the index.

The index is stored as an SSTable sorted by the field values (via `FieldKeyExtractor`), containing projected IBlobs (only the indexed fields). The `_id` on each entry points back to the primary document.

### Filter Indexes (reactive)

When a filter-only query (no sort) is repeated with low selectivity:
- Count >= 2 executions of the same filter pattern
- Selectivity <= 0.5 (returns at most half the scanned documents)
- Result count > spill threshold

The engine builds a full-range secondary index on the filter field. Subsequent scans use **binary search range scan** — O(log N + R) instead of O(N).

### Partial Index Ranges

Indexes can cover a **partial range** of values. If the query was `WHERE price < 100 ORDER BY price`, the resulting index covers only `price < 100`. A subsequent query `WHERE price < 50 ORDER BY price` can use this index (the range is a superset). A query `WHERE price > 200` cannot — it falls back to full scan and creates a new partial index.

At compaction time, multiple partial ranges for the same field are merged into one contiguous range. If the index covers >= 50% of primary documents, it's extended to full range.

## Index Lifecycle

### Creation

1. Sort query spills to disk → partial index (covers the query's filter range)
2. Filter query repeated with low selectivity → full-range index on the filter field

### Maintenance

- **On put()**: new/updated documents are eagerly written to matching index buffers (in-memory)
- **On flush()**: index buffers are flushed to disk atomically alongside the primary SSTable
- **On read**: stale index entries (field value changed since index was written) are detected by comparing with the primary document and skipped

### Compaction

- **Merge vs rebuild**: threshold at `M > 0.5 * N * ln(N)` where M = index entries, N = primary doc count
  - Below threshold: simple merge (flush buffer into index SSTable) — O(M)
  - Above threshold: full rebuild from primary SSTables — O(N log N)
- **Range extension**: if index covers >= 50% of primary docs, extend to full range
- **Drop unused**: indexes not queried for >= 10 compaction cycles AND >= 30 days are dropped

### Crash Safety

Index metadata is stored in the `system` collection as regular IBlobs. On restart, `Database::open()` scans the system collection, loads existing indexes, and replays WAL-recovered memtable entries into index buffers.

## Secondary Index Storage

Secondary indexes are SSTables with a different sort order. They use the same on-disk format, compression, bloom filters, and block indexes as primary SSTables. The only difference is the `KeyExtractor`:

- Primary: `MvccKeyExtractor` → key = `_id + _version` (32 bytes)
- Secondary: `FieldKeyExtractor` → key = comparable encoding of field values

This means secondary indexes get the same infrastructure for free: LZ4 compression, CRC32 block checksums, bloom filter lookups, binary search within blocks.

## What the Engine Does NOT Do (Yet)

- **Covered queries**: The index stores projected IBlobs (only the indexed field). For every match, it does a `get()` back to the primary to fetch the full document. A "covered query" optimization would store enough fields in the index to answer the query without the primary lookup.

- **Compound indexes**: Currently only single-field indexes. A compound index on `(category, price)` would enable queries like `WHERE category = 'electronics' ORDER BY price` with a single index scan.

- **Predictive optimization**: The engine is purely reactive — it waits for patterns to emerge. A predictive engine could anticipate future patterns from recent trends.

- **Semantic shredding**: Hot fields could be extracted into columnar side-structures during compaction for faster analytical queries.
