# IngoDB Benchmark Results

Benchmark: E-commerce product catalog (100K products, 7 fields, ~382 bytes/doc).

Run with: `cargo run --release --example benchmark`

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

## 2026-04-07 — 1M Products, UCS Scaling Parameter Comparison

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
