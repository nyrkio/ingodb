# IngoDB Benchmark Results

Benchmark: E-commerce product catalog (100K products, 7 fields, ~382 bytes/doc).

Run with: `cargo run --release --example benchmark`

---

## 2026-04-08 — 100K Products with Random Updates, UCS W Comparison

100K inserts + 50K random updates. Shows UCS tradeoff with compaction settle time.

| W | Mode | Settle time | Final SSTables | Compact runs | WA | Point lookup p50 | Lookup ops/sec |
|---|------|------------|---------------|-------------|-----|-----------------|---------------|
| 9 | Tiered (t=11) | **450ms** | **5** | 0 | 0x | 43.9 us | 23K |
| 4 | Tiered (t=6) | **455ms** | **5** | 0 | 0x | 44.4 us | 23K |
| 0 | Balanced (t=2) | **1.72s** | **1** | 2 | 0.82x | 12.9 us | 69K |
| -4 | Leveled (t=2) | **1.74s** | **1** | 2 | 0.82x | 12.8 us | 68K |
| -9 | Leveled (t=2) | **1.67s** | **1** | 2 | 0.82x | 12.2 us | 72K |

Key findings:
- **Tiered (W=4,9)**: 0 compaction runs, settles in ~450ms, but 5 SSTables remain.
  23K lookup ops/sec (slow — must search 5 SSTables).
- **Balanced/Leveled (W=0,-4,-9)**: 2 compaction runs, settles in ~1.7s, but only
  1 SSTable. 69-72K lookup ops/sec (fast — single SSTable).
- The tradeoff: tiered settles 4x faster but reads 3x slower.
- Update throughput: ~90K updates/sec for W=0 (compaction running concurrently),
  ~67K for W=9 (no compaction, pure writes with more index overhead).

---

## 2026-04-07 — Commit 023999a

Background compaction, RwLock on SSTable list, MVCC snapshot reads.
Config: 16 MB memtable, 4096 byte blocks, W=0 (balanced UCS).

### Write Performance

| Metric | Value |
|--------|-------|
| Bulk ingest (100K docs) | 103K docs/sec |
| Ingest time | 968ms |
| SSTables after settling | 3 |

### Point Lookup Performance

| Metric | Value |
|--------|-------|
| Single-thread throughput | 37K ops/sec |
| p50 latency | 29.0 us |
| p95 latency | 40.7 us |
| p99 latency | 45.7 us |

### Scan + Sort Performance

| Query | Results | Time |
|-------|---------|------|
| category='electronics' ORDER BY price (cold) | 10,000 | 193ms |
| category='electronics' ORDER BY price (warm, index) | 10,000 | 259ms |
| price>50 AND rating>3.0 ORDER BY rating DESC LIMIT 20 | 20 | 334ms |
| category='electronics' ORDER BY price LIMIT 10 PROJECT(name,price) | 10 | 22ms |

Note: warm scan with index currently slower than cold — index overhead
from per-document get() back to primary. Optimization TODO.

### Snapshot Isolation

| Metric | Value |
|--------|-------|
| 1000 updates | 4.6ms |
| 100 snapshot reads | 2.9ms |
| Isolation correctness | 100/100 |

### Concurrent Read Scaling

| Threads | Ops/sec | Scaling |
|---------|---------|---------|
| 1 | 35,756 | 1.0x |
| 2 | 71,295 | 2.0x |
| 4 | 139,255 | 3.9x |
| 8 | 245,713 | 6.9x |

4 readers + 1 writer: 144K ops/sec

### Mixed Read/Write

| Metric | Value |
|--------|-------|
| 10K ops (80% reads, 20% writes) | 55K ops/sec |

---

## 2026-04-07 — 1M Products with Random Updates, UCS W Comparison

1M inserts + 500K random updates. Five W configurations.
Shows UCS read/write amplification tradeoff.

| W | Mode | f | t | Final SSTables | Compaction reads | Write amp | Point lookup p50 | Lookup ops/sec | 8-thread ops/sec |
|---|------|---|---|---------------|-----------------|-----------|-----------------|---------------|-----------------|
| 9 | Tiered | 11 | 11 | **1** | 411 MB | 0.81x | 14.5 us | **61K** | **246K** |
| 4 | Tiered | 6 | 6 | 2 | 247 MB | 0.87x | 26.9 us | 40K | 157K |
| 0 | Balanced | 2 | 2 | 2 | 247 MB | 0.87x | 25.8 us | 43K | 163K |
| -4 | Leveled | 6 | 2 | 2 | 247 MB | 0.87x | 25.4 us | 42K | 160K |
| -9 | Leveled | 11 | 2 | **1** | 411 MB | 0.81x | 13.7 us | **66K** | **256K** |

Findings:
- Extreme W values (±9) compact to 1 SSTable via larger merges (411 MB read).
  Best read performance but most compaction I/O.
- Middle values (W=0, ±4) land at 2 SSTables with less compaction I/O (247 MB).
- Both extremes have f=11 (large fanout). W=9 triggers at t=11 (many SSTables
  before merge), W=-9 triggers at t=2 (aggressive) but with larger level sizes.
- Write amplification < 1.0 because dedup removes old versions during merge.

---

## 2026-04-07 — 1M Products with Random Updates (W=0)

1M inserts + 500K random updates. Updates create overlapping key ranges
that trigger UCS compaction. This is a realistic CRUD workload.

| Phase | Metric | Value |
|-------|--------|-------|
| Ingest | 1M docs | 22 SSTables |
| Updates | 500K random | 34 SSTables → compacted to **2** |
| Point lookup | p50 latency | 26.7 us |
| Point lookup | single-thread | 39K ops/sec |
| Scan cold | 100K results | 3.74s |
| Scan warm (index) | 100K results | 2.59s (**1.4x speedup**) |
| 8-thread reads | | 268K ops/sec |
| Mixed read/write | | 765K ops/sec |
| Snapshot isolation | | 100/100 correct |

Key finding: random updates trigger compaction, reducing 34 SSTables
to 2. This dramatically improves read performance vs the insert-only
workload (23 SSTables). The secondary index now provides a real 1.4x
speedup (vs 0.2x regression in insert-only).

W=4, W=0, W=-4 all produce same result (2 SSTables). The W parameter
affects write amplification (number of compaction rounds) but we don't
measure that yet.

---

## 2026-04-07 — 1M Products, UCS Scaling Parameter Comparison (insert-only)

1M products (~382 bytes each, ~382 MB total), 16 MB memtable.

| Metric | W=-4 (leveled) | W=0 (balanced) | W=4 (tiered) |
|--------|---------------|----------------|--------------|
| Ingest | 98K docs/sec | 96K docs/sec | 97K docs/sec |
| Ingest time | 10.2s | 10.4s | 10.3s |
| SSTables | 23 | 23 | 23 |
| Point lookup p50 | 123 us | 124 us | 122 us |
| Point lookup ops/sec | 8.1K | 8.0K | 8.2K |
| Scan (cold, 100K results) | 2.66s | 2.68s | 2.67s |
| Scan (warm, index) | 13.1s | 13.3s | 13.0s |
| 8-thread reads | 51K ops/sec | 51K ops/sec | 52K ops/sec |
| Snapshot isolation | 100/100 | 100/100 | 100/100 |

**Finding**: All W values produce identical SSTable counts (23) because
sequential UUIDv7 inserts create non-overlapping SSTables. The UCS
overlap detection correctly avoids unnecessary merges, but this means
W has no effect on this workload. Need a workload with key-range overlap
(updates, random keys) to exercise W's read/write amplification tradeoff.

**Performance note**: Warm index scan (13s) is 5x slower than cold scan
(2.7s) because the secondary index does per-document get() back to
primary (100K individual lookups). This is the top optimization target.

**Write slowdown**: Ingest rate drops from 190K to 36K docs/sec over
the 1M run as more SSTables accumulate and flushes become more expensive.

---

## 2026-04-07 — 100K Products, UCS Scaling Parameter Comparison

100K products, 16 MB memtable. Dataset too small to differentiate W.

| Metric | W=-4 (leveled) | W=0 (balanced) | W=4 (tiered) |
|--------|---------------|----------------|--------------|
| Ingest | 98K docs/sec | 96K docs/sec | 97K docs/sec |
| SSTables | 3 | 3 | 3 |
| Point lookup p50 | 28.8 us | 29.1 us | 29.4 us |
| Point lookup ops/sec | 38K | 37K | 37K |
| Scan (cold) | 198ms | 196ms | 192ms |
| 8-thread reads | 242K ops/sec | 260K ops/sec | 252K ops/sec |

All values within noise.

---

## 2026-04-07 — Pre-RwLock baseline (Mutex on SSTable list)

Before switching SSTable list from Mutex to RwLock.

### Concurrent Read Scaling (Mutex)

| Threads | Ops/sec | Scaling |
|---------|---------|---------|
| 1 | 60,138 | 1.0x |
| 2 | 56,355 | 0.94x |
| 4 | 51,578 | 0.86x |
| 8 | 45,450 | 0.76x |

Throughput *decreased* with more threads due to Mutex contention.
RwLock fix gave 8.5x improvement at 8 threads.
