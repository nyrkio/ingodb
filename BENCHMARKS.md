# IngoDB Benchmark Results

Benchmark: E-commerce product catalog (100K products, 7 fields, ~382 bytes/doc).

Run with: `cargo run --release --example benchmark`

---

## 2026-04-08 — 1M Products, batch writes + double-buffer memtable + adaptive W

Batch writes (1000 docs/batch), double-buffered memtable, adaptive W (unlimited step).

| Phase | Metric | Value |
|-------|--------|-------|
| Ingest | 1M docs (batch=1000) | **210-235K docs/sec sustained** |
| Ingest total | | 4.0s (was 42s before O(N) fix + double-buffer) |
| Updates | 1M random | starts 230K, settles ~120K during compaction |
| Compaction settle | after updates | 7.7s → 2 SSTables |
| Point lookups | 20K gets (2 SSTables) | 56K ops/sec, p50=14µs |
| Scan cold | category filter, 100K results | 3.9s |
| Scan warm (index) | | 1.8s (**2.2x speedup**) |
| 8-thread concurrent | | 413K ops/sec |
| Mixed read/write | | 781K ops/sec |
| Pure reads | 2M gets | 51K ops/sec |

Adaptive W journey: 0 → 8 (writes) → -8 (scans) → -3 (mixed) → -8 (reads).
3 compaction runs, 586 MB read, 423 MB written, WA=0.72x.

---

## 2026-04-08 — 1M Products, Adaptive W unlimited step (starting W=0)

Same workload as below but with max_step=16 (effectively unlimited).
W jumps immediately to the target value.

```
Phase          W    target   SSTables   Settle time
──────────────────────────────────────────────────────
Ingest         0→8    8       23        5.1s
Updates        8      8        5        9.5s
Lookups        8      8        5        ~0s
Scans          8→-8  -8        2        ~0s
Mixed         -8→-3  -3        2        12.5s
Concurrent    -3→-8  -8        3        9.1s
Pure reads    -8     -8        3        0.8s
```

### Performance Metrics

| Phase | Metric | Value |
|-------|--------|-------|
| Ingest | 1M docs | starts 210K/sec, degrades as SSTables grow |
| Updates | 1M random (W=8, tiered) | settle 9.5s, 5 SSTables |
| Point lookups | 20K gets (5 SSTables, W=8) | 40K ops/sec, p50=17µs, p95=40µs |
| Scan cold | after W→-8 jump, 2 SSTables | 7.2s (100K results) |
| Scan warm (index) | | 5.2s (**1.4x speedup**) |
| 8-thread concurrent | | 411K ops/sec |
| Pure reads | 2M gets (3 SSTables, W=-8) | 50K ops/sec |
| Mixed 80/20 | W adjusted to -3 | 742K ops/sec |

Key observation: the unlimited step causes W to oscillate more aggressively.
The mixed phase (80% reads, 20% writes) settles at W=-3, which triggers
12.5s of compaction. With step=±2, this transition is smoother.

Total: 5 compaction runs, 719 MB read, 556 MB written, WA=0.77x.

---

## 2026-04-08 — 1M Products, Adaptive W step=±2 (starting W=0)

1M inserts + 1M random updates + 20K lookups + scans + 2M pure reads.
Adaptive W enabled, cooldown=1s, max step=±2.

```
Phase          W    target   read_ratio   SSTables
────────────────────────────────────────────────────
Ingest         0→8    8      0.00          22
Updates        8      8      0.00           4
Lookups        8      8      —              4
Scans          8→2   -8      0.40→1.00      4
Mixed          2     -8      —              4
Concurrent     2→0   -7      0.95           3
Pure reads     0→-8  -8      1.00           2
```

The engine started balanced (W=0), shifted to full tiered (W=8) during
the write-heavy phase, then gradually shifted back to full leveled (W=-8)
during the read-heavy phase. 5 compaction runs total, 623 MB read,
481 MB written (WA=0.77x).

### Performance Metrics

| Phase | Metric | Value |
|-------|--------|-------|
| Ingest | 1M docs | starts 210K/sec, degrades to 64K/sec as SSTables grow |
| Updates | 1M random | starts 230K/sec, degrades as compaction runs |
| Point lookups | 20K gets (4 SSTables) | 52K ops/sec, p50=15µs, p95=36µs, p99=43µs |
| Scan cold | category filter, 100K results | 4.8s |
| Scan warm (index) | same query | 2.2s (**2.2x speedup**) |
| Compound filter+sort | price>50 AND rating>3, limit 20 | 9.5s |
| Snapshot isolation | | 100/100 correct |
| Mixed read/write | 10K ops | 737K ops/sec |
| 8-thread concurrent | 80K reads | 254K ops/sec |
| Pure reads | 2M gets (2 SSTables, W=-8) | 57K ops/sec |

Final state: W=-8 (fully leveled), 2 SSTables, 57K point lookup ops/sec.

---

## 2026-04-08 — 1M Products with Random Updates, UCS W Comparison

1M inserts + 500K random updates. Compaction settle time and read performance.

| W | Mode | f | t | Settle time | Final SSTables | Compact runs | Bytes read | WA | Lookup p50 | Lookup ops/sec | Update rate |
|---|------|---|---|------------|---------------|-------------|-----------|-----|-----------|---------------|-------------|
| 9 | Tiered | 11 | 11 | **12.1s** | **2** | 2 | 410 MB | 0.81x | 13.8 us | **66K** | 10K/sec |
| 4 | Tiered | 6 | 6 | **9.5s** | **2** | 3 | 286 MB | 0.89x | 26.1 us | 34K | 10K/sec |
| 0 | Balanced | 2 | 2 | **16.3s** | **3** | 8 | 416 MB | 0.93x | 39.1 us | 24K | 12K/sec |
| -4 | Leveled | 6 | 2 | **12.7s** | **2** | 8 | 486 MB | 0.94x | 26.3 us | 36K | 12K/sec |
| -9 | Leveled | 11 | 2 | **15.9s** | **1** | 4 | 749 MB | 0.90x | 14.3 us | **62K** | 16K/sec |

Key findings at 1M scale:
- **W=9 (tiered)**: fewest compaction runs (2), reads 410 MB, produces 2 SSTables.
  Best read performance (66K ops/sec) after settling. But settle takes 12s.
- **W=0 (balanced)**: most compaction runs (8), reads 416 MB, but 3 SSTables remain.
  Worst read performance (24K ops/sec). Most write amplification (0.93x).
- **W=-9 (leveled)**: 4 compaction runs, reads 749 MB (most I/O), but produces 1 SSTable.
  Best single-SSTable result. Settle takes 16s.
- **W=4 vs W=-4**: similar final state (2 SSTables), but W=4 gets there with
  fewer compaction rounds (3 vs 8) and less I/O (286 vs 486 MB).

The tradeoff is now clear: higher |W| = larger fanout = bigger individual merges
but fewer total rounds. W=0 does many small merges (8 rounds) which is less
efficient at scale.

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
