# Compaction — Unified Compaction Strategy (UCS)

IngoDB's compaction is inspired by the [Unified Compaction Strategy (UCS)](https://github.com/apache/cassandra/blob/88e3fcfe8b436b2adf8f686d2e86e5bb9a94b50b/doc/modules/cassandra/pages/managing/operating/compaction/ucs.adoc) from Apache Cassandra, designed by Branimir Lambov.

## Core Concept

UCS unifies leveled and tiered compaction through a single **scaling parameter W**:

| W | Mode | Fanout (f) | Threshold (t) | Behavior |
|---|------|-----------|---------------|----------|
| < 0 | Leveled | 2 - W | 2 | Aggressive compaction, fewer SSTables, higher write amp |
| 0 | Balanced | 2 | 2 | Middle ground |
| > 0 | Tiered | 2 + W | f | Deferred compaction, more SSTables, lower write amp |

### Level Assignment

Each SSTable is assigned a level based on its file size:

```
level = floor(log_f(file_size / flush_size)).max(0)
```

Where `f` is the fanout factor and `flush_size` is the memtable size.

### Compaction Trigger

Within each level, SSTables with overlapping key ranges are grouped. When a group reaches the threshold `t`, compaction merges them into a single SSTable that moves to the next level.

### Overlap Detection

SSTables are sorted by `min_id`. A sweep with a running `max_id` finds chains of overlapping ranges:

```
SSTable A: [10..50]
SSTable B: [30..80]   ← overlaps A (30 < 50)
SSTable C: [90..120]  ← does not overlap B (90 > 80) — new group
```

## Read Ordering Invariant

SSTables are ordered for reads: **L0 first → L1 → L2 → ..., within each level newest first.**

This guarantees that for any `_id`, the first match found is the current version. The invariant holds because:
1. Data enters at L0 via memtable flush
2. Compaction merges within a level, keeping only the highest `_version`, and outputs to L(x+1)
3. Higher levels never have newer versions than lower levels

## The W Tradeoff — Benchmark Results

At 1M documents with 500K random updates (100K docs, all W values comparable):

| W | Settle time | Final SSTables | Compaction runs | Lookup ops/sec |
|---|------------|---------------|----------------|---------------|
| 9 (tiered) | 450ms | 5 | 0 | 23K |
| 4 (tiered) | 455ms | 5 | 0 | 23K |
| 0 (balanced) | 1.72s | 1 | 2 | 69K |
| -4 (leveled) | 1.74s | 1 | 2 | 68K |
| -9 (leveled) | 1.67s | 1 | 2 | 72K |

**Tiered (W > 0)**: Settles 4x faster (no compaction work) but leaves more SSTables, slowing reads.

**Leveled (W < 0)**: Spends time compacting but produces fewer SSTables for fast reads.

At 1M scale, higher |W| values (±9) produce fewer, larger merges that are more efficient than many small merges (W=0).

See [BENCHMARKS.md](../BENCHMARKS.md) for full results.

## Background Compaction

Compaction runs in a background thread pool (configurable, default 4 threads):

1. **Coordinator thread** wakes on signal from `flush_memtable()`
2. Calls `pick_all_compactions()` to find ALL eligible groups across all levels
3. Dispatches each group to a worker thread
4. Workers run compaction independently in parallel
5. After completion, re-signals in case output created new eligible groups

### Inline Fallback

If `start_background_compaction()` is not called, compaction runs inline during `flush_memtable()`. This is the default for tests (deterministic behavior).

## Tombstone Purging

Tombstones (deleted documents) are purged when:
- The compaction output level exceeds the max level (no older SSTable could hold a shadowed entry)
- No active snapshots exist (conservative: don't purge while any snapshot is alive)

## MVCC-Aware Dedup

During compaction, dedup keeps:
- **Without snapshots**: Only the latest version per `_id` (standard behavior)
- **With snapshots**: Latest version per `_id` PLUS all versions with `_version >= oldest active snapshot`

## Compaction Statistics

The engine tracks:
- Compaction runs completed
- Bytes read / written
- SSTables read / produced
- Write amplification ratio (`bytes_written / bytes_read`)

```rust
let cs = engine.compaction_stats();
println!("Write amplification: {:.2}x", cs.write_amplification());
```

## Secondary Index Compaction

Secondary indexes have their own compaction logic, running as part of `flush_memtable()`:

- **Merge vs rebuild**: threshold at `M > 0.5 * N * ln(N)`
- **Range extension**: partial index covering >= 50% of primary → extend to full range
- **Range merging**: multiple partial ranges for the same field → merged into one contiguous range
- **Drop unused**: indexes not queried for >= 10 compaction cycles AND >= 30 days

## Configuration

```rust
LsmConfig {
    memtable_size: 64 * 1024 * 1024,     // 64 MB flush threshold
    block_size: 4096,                      // SSTable block size
    compaction_threshold: 4,               // legacy size-tiered threshold
    scaling_parameter: 0,                  // UCS W parameter
    sort_spill_threshold: 1000,            // sort results spill to disk above this
    compaction_threads: 4,                 // background compaction workers
}
```
