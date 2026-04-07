# MVCC & Snapshot Isolation

IngoDB supports multi-version concurrency control (MVCC). Multiple versions of the same document coexist, and readers can see a consistent point-in-time view without blocking writers.

## How It Works

### Version Assignment

Every `put()` stamps a new `_version` (UUIDv7) on the document. Since UUIDv7 is timestamp-prefixed and lexicographically ordered, version comparison is chronological comparison.

### Multi-Version Storage

- **Memtable**: Keyed by `(_id, _version)`. Multiple versions of the same document coexist in the BTreeMap.
- **SSTables**: Use `MvccKeyExtractor` which produces 32-byte keys (`_id + _version`). Multiple versions of the same document can exist in the same SSTable (after compaction with active snapshots).

### Snapshot Reads

```rust
let snapshot = engine.snapshot();
// snapshot.version() is a UUIDv7 captured at this moment

// All reads through the snapshot see only versions <= snapshot.version()
let doc = snapshot.get(&id)?;     // highest version <= snapshot
let docs = snapshot.scan(...)?;   // all versions filtered to <= snapshot
```

### Read Resolution

For `get(id, snapshot_version)`:

1. **Memtable**: Range scan for `(_id, *)`, find highest `_version <= snapshot`. O(log N) via BTreeMap.
2. **SSTables** (L0 first, then L1, ...): For each SSTable, binary search for `(_id, snapshot_version)` and scan backward to find the highest matching version. Bloom filter skips SSTables that don't contain the `_id`.
3. **First match wins**: The level ordering guarantees correctness — lower levels have newer data.

For `scan(filter, snapshot_version)`:

1. Collect all entries from memtable + SSTables
2. Filter to `_version <= snapshot`
3. Sort by `_id`, dedup keeping highest remaining version per `_id`
4. Apply filter, sort, projection, limit

### Without Snapshots

When no snapshot is used (the common case), all reads use `DocumentId::max()` as the snapshot version, which matches any version. This adds zero overhead — the version comparison is a no-op because max is always >= any real version.

## Compaction and GC

### With Active Snapshots

Compaction preserves all versions >= the oldest active snapshot. For each `_id`, it keeps:
- The latest version (always)
- All versions with `_version >= oldest_active_snapshot`

Tombstones are also preserved while snapshots are active.

### Without Active Snapshots

Standard dedup: keep only the latest version per `_id`. This is the common case and has zero overhead compared to a non-MVCC engine.

### Snapshot Lifecycle

```
snapshot = engine.snapshot()     → registers version in active_snapshots set
snapshot.get() / scan() / ...   → reads filter by version
drop(snapshot)                  → removes from active set
next compaction                 → can now GC old versions
```

Snapshots should be short-lived. A long-held snapshot prevents GC of old versions, causing disk growth.

## Bloom Filter and MVCC

The SSTable bloom filter stores both:
- The full 32-byte MVCC key (`_id + _version`) — for exact key lookups
- The 16-byte `_id` prefix — for point lookups by document ID

This allows `get_by_id()` to use the bloom filter to skip SSTables that don't contain the document, even though the stored keys are 32-byte MVCC composites.
