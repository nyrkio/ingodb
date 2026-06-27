# IngoDB Benchmark Results

Benchmark: E-commerce product catalog (100K products, 7 fields, ~382 bytes/doc).

Run with: `cargo run --release --example benchmark`

---

## 2026-06-27 — Equality index rebuilt LSM-native (Phases B + C)

The equality index from earlier today was redesigned from the in-memory,
write-maintained v0 to an **LSM-native** structure (`docs/equality-index.md`):
per-SSTable postings of `_id` references, built lazily on read, carried forward
across compaction, with **no write-side maintenance** (immutable SSTables make
read-build correct; writes only touch the memtable, which is always live-scanned).

Same setup: 200,000 rows of real `hits_0`, `--release`, `max_ranges_per_field=50`.
Illustrative, not a score.

| Query | Cold | Warm (med) | Speedup | Served by |
|---|---:|---:|---:|---|
| `UserID = <id>` — point Eq (2 rows) | 604 ms | **25.3 µs** | **~23,900×** | per-SSTable `Exact` postings |
| `CounterID = <dominant>` (99.99%) | 572 ms | 547 ms | 1.0× | full scan — `Overflow` declines (non-selective) |
| `RegionID = x ORDER BY EventTime DESC LIMIT 10` | 1.19 s | 61 ms | 19.5× | sorted partial |
| `EventTime ∈ [lo,hi)` (~10%) | 667 ms | 350 ms | 1.9× | unsorted block → sorted partial |

`AdvEngineID > 0` 6.4×, containment 177 ms, writes ~11k/s, warm `AdvEngineID>0`
flat across 10k inserts (107 → 119 ms). Drift: 70 distinct `UserID` Eq → 1
equality field, zero range-index churn. No regression vs the v0 numbers below.

What the redesign adds over v0 (none of it visible as a single-query speedup, but
all exercised by tests):
- **No write-path cost** — `notify_put` removed; writes don't touch the index.
- **Warm across compaction** — postings rebuilt onto the merged SSTable from the
  merged rows (no cold re-read).
- **`Eq(v) LIMIT n` on a non-selective value** served from the cached 16-id
  `Overflow` sample instead of a full scan (the exhaustive `Eq(v)` still scans).
- **Redundancy drop** — a column's Eq postings are dropped once a sorted
  full-range index covers it; **field-LRU** bounds tracked fields.

Caveat: warm point-Eq has run-to-run variance (~25–49 µs across runs) — the cost
of iterating each SSTable's postings vs v0's single global lookup.

---

## 2026-06-27 — Lazy equality (Eq/In) index added; ClickBench re-run

New reactive index: a **lazy equality index** for `Eq` / `In` predicates (the
latter expressed as `Or(Eq, Eq, …)` on one field — no new AST node). It's an
inverted posting list `field=value → [_id]` storing **only `_id` references**;
reads resolve each candidate against the primary via `get_at(id, snapshot)` and
re-verify the predicate, so staleness (updated/deleted docs) and MVCC fall out of
the verify step — the posting list itself is version-agnostic. Maintenance is
additive (`notify_put` appends to already-materialized values only); LRU is at
**field granularity** with its own budget; in-memory in v1. `Eq`/`In` now route
here and **no longer promote sorted/unsorted *range* indexes**.

Same setup as the 2026-06-23 run: 200,000 rows of real `hits_0`, `--release`,
`max_ranges_per_field=50`. Illustrative, not a score.

| Query | Cold | Warm (med) | Speedup | Served by |
|---|---:|---:|---:|---|
| `UserID = <id>` — point Eq by field (2 rows) | 715 ms | **32.7 µs** | **~21800×** | equality index (in-memory posting) |
| `CounterID = <dominant>` (99.99% of rows) | 555 ms | 573 ms | 1.0× | **not materialized** (>50% guard applies to Eq too) |

The `UserID` point-Eq warm path is now an in-memory `_id` posting (was a
projected sorted-partial SSTable: 39 µs / 18,900× on 2026-06-23) — marginally
faster, but both are dominated by the 2 `get()`-backs to primary.

**Drift, where the structural win is:** 70 distinct high-cardinality `UserID` Eq
queries now collapse into **one equality field** holding 70 in-memory value
postings — `sorted_idx` stays at 3 (the `EventTime`/`RegionID`/`AdvEngineID`
ranges), **zero** UserID range-index files minted or LRU-evicted. The old path
churned up to 50 projected sorted-partial SSTables under the unified range
budget; high-cardinality point lookups now cost `_id`-list memory instead of
files, and all 70 are retained (better warm-hit rate on re-query) rather than
capped at 50.

Unchanged from 2026-06-23: range/top-N/containment paths (Eq removal doesn't
touch them), and the per-doc `get()`-back ceiling on wide warm scans.

---

## 2026-06-23 — ClickBench reactive-index benchmark (real `hits` data)

`cargo run --release --example clickbench [rows]`. Runs the IngoDB-expressible
subset of ClickBench (point / equality / range / top-N / filtered-count) against
the **real ClickBench `hits_0` partition** and watches the reactive index system
create indexes and either help or correctly decline. Query params are derived from
the data. Prepare the data:

```
curl -sO https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
uv run --with pyarrow python prep_clickbench.py   # -> clickbench_hits.tsv (gitignored)
```

Run: 200,000 rows of `hits_0`, `--release`, single dev machine,
`max_ranges_per_field=50`. Illustrative (one machine/partition), not a score.

**Reactive indexing helps selective queries** (cold = first run / full scan +
materialize; warm = served by the reactive index):

| Query | Cold | Warm (med) | Speedup |
|---|---:|---:|---:|
| `UserID = <id>` — point lookup by field (2 rows) | 745 ms | **39 µs** | **~18900×** |
| `RegionID = x ORDER BY EventTime DESC LIMIT 10` | 1.34 s | 72 ms | **18.5×** |
| `AdvEngineID > 0` — selective (~2.9%) | 747 ms | 105 ms | 7.1× |
| `EventTime ∈ [lo,hi)` — range (~10%, ~20k rows) | 774 ms | 360 ms | 2.2× |

**…and correctly declines where it can't help:** `CounterID = <dominant value>`
returns 99.99% of rows → over the 50% threshold → **not materialized**, 574 ms cold
≈ 569 ms warm (1.0×). No wasted index.

**Containment:** a sub-range of a materialized range is served by the promoted
sorted partial via interval containment — `EventTime` sub-range, 10,405 rows in
**193 ms** (`served_by=sorted index`), vs ~774 ms cold.

**Drift / LRU:** 70 distinct high-cardinality `UserID` queries against a cap of 50 →
materialized ranges settle at ~50 (one unified LRU budget across both index tiers).

**Writes:** 10k single inserts interleaved with a warm read pattern → ~5k ins/sec;
warm `AdvEngineID>0` stays flat (108 ms → 133 ms) — maintenance doesn't regress reads.

Findings:
- **Per-doc `get()`-back-to-primary is the warm-path ceiling.** Serving ~20k rows via
  an index does ~20k primary lookups (`get` pattern: ~285k calls @ ~17 µs). That caps
  `AdvEngineID>0` at 7× and the `EventTime` range at 2.2×, vs ~18900× for a 2-row point
  lookup. This is the "covering index" work — now quantified.
- **Range-blind promotion guard — FIXED (commit before this entry).** A sort query
  `WHERE b=x ORDER BY a` spills an index keyed by `a` but covering `Eq(b=x)`; the old
  guard then dropped genuine `a`-*range* blocks. Now coverage-aware (`has_covering_index`),
  so the `EventTime` range block promotes and containment reuse works (above).
- **Query-language gaps** (drive future work): ClickBench is ~70% aggregation. Unsupported
  today: aggregation, GROUP BY, DISTINCT, `LIKE` substring, scalar functions. Joins/traverse
  would be exercised by a relational benchmark (TPC-C/H), not single-table ClickBench.

---

## 2026-05-18 — fsync amortization via pipelined group commit

`cargo run --release --example fsync_bench` — TOTAL_OPS_TARGET=200K per cell,
ops/thread clamped to [200, 5000]. Three consistency levels × 1 to 1024
concurrent writers. Mode-specific leader paths: Durable releases the WAL
writer mutex before fsync (pipelined); Visible keeps the looping leader.

| Level      |  1t |  2t |  4t |  8t | 16t | 32t | 64t |128t |256t |512t |1024t|
|------------|----:|----:|----:|----:|----:|----:|----:|----:|----:|----:|----:|
| Optimistic | 217K| 308K| 245K| 225K| 200K| 186K| 174K| 170K| 176K| 162K| 150K|
| Visible    | 274K| 231K| 289K| 358K| 347K| 387K| 365K| 339K| 356K| 368K| 281K|
| Durable    | 143 | 175 | 329 | 664 |   1K|   3K|   7K|  13K|  21K|  32K|  34K|

Read:
- **Durable scales 236×** from 1→1024 threads (143 → 34K ops/sec).
  Saturates around 512-1024 threads — that's the practical fsync-throughput
  ceiling of this system. At 1024 threads, one fsync amortizes ~240 writes
  (34K ops/sec × 7ms per fsync).
- **Pipelining is what unlocks the scaling beyond ~32 threads.** Before
  pipelining (leader holds WAL mutex through fsync), Durable plateaued at
  ~2K ops/sec at 32 threads. After releasing the writer mutex before
  `sync_all()` (using a cloned File handle), batch N+1's append can run
  concurrently with batch N's fsync.
- **Visible recovers to ~390K at 32 threads** with the mode-specific path
  (keeping the looping leader). Pipelining hurt it: handoff overhead
  dominates when there's no slow stage to overlap with.
- **Optimistic gradually loses ground at extreme concurrency** due to
  unbridled WAL-lock contention from every thread — exactly the case the
  Visible leader/follower fixes.

Self-regulation:
- Durable leader releases `leader_active` *before* fsync, then re-checks
  after. If no new leader stepped in AND work is queued, it continues.
  At low contention this collapses to a single leader looping; at high
  fsync load it naturally fragments into many pipelined leaders.

Headroom (partially implemented below):
- `commit_wait_usec` / `commit_wait_count` knobs — deliberately delay the
  leader to grow the batch. Implemented as static config; see the next
  section for results.
- An adaptive scheme: the engine could observe its own fsync latency and
  tune wait_usec from data (the IngoDB "liquid" thesis).

### Batch-growing wait: commit_wait_usec

MariaDB exposes two knobs (`binlog_commit_wait_usec`, `binlog_commit_wait_count`)
that let the leader deliberately delay fsync to gather more writers into one
batch. We added the same as `LsmConfig.commit_wait_usec` /
`commit_wait_count`. Defaults are zero (no wait, preserving the prior
behavior).

When enabled (Durable mode only), the leader drains the queue, then if the
batch is below `commit_wait_count` (or `commit_wait_count == 0` meaning
"always wait"), sleeps for up to `commit_wait_usec` while still holding
leadership. New arrivals during the wait join *this* batch as followers.
After the wait, leadership is released for pipelining and the leader
proceeds with append + fsync.

| Level             |   1t |   2t |   4t |   8t |  16t |  32t |  64t | 128t | 256t | 512t |1024t |
|-------------------|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|
| Optimistic        | 213K | 289K | 202K | 219K | 205K | 207K | 198K | 170K | 154K | 141K | 143K |
| Visible           | 274K | 267K | 264K | 308K | 367K | 335K | 326K | 343K | 382K | 358K | 279K |
| Durable           |  162 |  169 |  356 |  708 |   1K |   3K |   6K |  11K |  25K |  27K |  27K |
| Durable+wait100µs |  147 |  170 |  377 |  635 |   1K |   3K |   6K |  10K |  21K |  58K | **123K** |
| Durable+wait500µs |  139 |  302 |  577 |   1K |   1K |   3K |   5K |  12K |  23K |  58K | 112K |
| Durable+wait1ms   |  133 |  231 |  505 |  961 |   2K |   3K |   6K |  11K |  26K |  53K | 110K |

Read:
- **wait_usec=100 at 1024t turns 27K into 123K ops/sec — a 4.5× peak speedup.**
  Without the wait, Durable plateaus at ~27K from 256t onward; with it,
  throughput keeps scaling. The plateau was pipelining-limited (too many
  small fsyncs); the wait converts those into fewer larger fsyncs.
- **The sweet spot is 100µs.** Going to 500µs or 1ms doesn't go higher —
  by 100µs the batches are already nearly full at that concurrency, and
  longer waits just add latency.
- **Crossover is ~256t.** Below that, the wait adds latency with no batch
  growth (not enough writers to gather). At 256t it's roughly break-even;
  above, the wait wins by progressively larger margins.
- **Cost at low concurrency is bounded by wait_usec.** Single-thread
  wait1ms drops 162 → 133 (~18%). For latency-sensitive workloads, use
  `commit_wait_count` > 0 so the leader skips the wait when the batch is
  already large.
- **Per-fsync amortization at 1024t with wait100**: ~123K ÷ 143 fsync/s ≈
  860 ops/fsync, vs ~190 before — closer to the 1024-thread ceiling.

This validates the original hypothesis that pipelining alone doesn't
saturate the available batch capacity — it spreads work across more,
smaller fsyncs. The wait collapses those into fewer larger ones.

### Busy mode: automatic activation (default)

Static `commit_wait_usec` is a knob — useful, but the user has to know
when concurrency justifies the latency cost. We added a "busy mode" that
auto-detects this:

- Engine starts in *quiet mode* (wait=0).
- Each Durable fsync measures the number of ops it durabilizes
  (`ops_appended` snapshot at fsync start — `ops_durable` high-water
  mark). This is the right metric because in pipelined mode each fsync
  piggybacks on appends from other concurrent leaders, so it's larger
  than `entries.len()` of any single leader's batch.
- On the first fsync that covers `>= num_cpus × 8` ops, the booster
  trips on permanently; the leader switches to a 100 µs wait per batch.
- One-way switch — once tripped, stays on.

This is now the default (`LsmConfig::commit_busy_mode = true`). An
explicit `commit_wait_usec > 0` overrides it.

| Level             |   1t |   2t |   4t |   8t |  16t |  32t |  64t | 128t | 256t | 512t | 1024t |
|-------------------|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|-----:|------:|
| Durable           |  163 |  167 |  332 |  714 |   1K |   3K |   6K |  13K |  22K |  30K |   33K |
| Durable+wait100µs |  163 |  225 |  322 |  626 |   1K |   3K |   6K |  11K |  26K |  63K |  124K |
| Durable+busy      |  166 |  167 |  327 |  617 |   1K |   3K |   7K |  12K |  26K |  58K |  **122K** |

Read:
- **Busy mode matches wait100 at peak** (122K vs 124K @ 1024t) — within
  noise.
- **No latency cost at low concurrency.** 1t busy is 166 vs plain 163;
  4t busy is 327 vs plain 332; essentially identical. wait100 at 1t is
  also fine (163) but wait100 at 2t (225) shows the kind of
  per-thread noise the booster avoids.
- **Trips at 128 threads on this machine** (num_cpus = 16 → threshold
  = 128). Observed trip points: 137, 130, 130, 212 ops/fsync —
  immediately above threshold once concurrency is high enough.
- **Two-way with hysteresis.** Busy mode now also turns off after 3
  consecutive fsyncs each covering fewer than `num_cpus × 2` ops
  (= `commit_busy_threshold / 4`). The gap between up-threshold
  (`× 8`) and down-threshold (`× 2`) gives natural hysteresis: small
  bursts of activity don't oscillate the flag.

The key implementation detail: the trigger measures *ops per fsync*,
not ops per drained batch. In our pipelined design, multiple leaders'
appends share each fsync (the kernel flushes everything that landed in
the OS page cache by the time `sync_all()` is called), so the natural
"how busy is this engine" signal is the rise in the durable high-water
mark per fsync syscall.

Next steps (not implemented):
- Per-second throughput-based trigger as an alternative signal
  (`ops_appended` rate over a sliding window) — might trip earlier
  under variable workloads.
- Configurable threshold / wait value (currently hardcoded
  `num_cpus × 8` up, `num_cpus × 2` down, 100 µs wait).

### Cross-check against MariaDB's group commit work

For validation we compared to two Percona benchmarks of Kristian Nielsen's
MariaDB 5.3/10.0 group commit fix (MWL#116), which is the prior art our
leader/follower design is modeled on.

**Percona, "Testing the Group Commit Fix"** — sysbench update_non_index,
`innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, slow fsync (no
write-back cache). This isolates the value of group commit itself:

| Threads | Without fix | With fix    | speedup |
|--------:|------------:|------------:|--------:|
|       1 |       21.51 |       21.99 |     1×  |
|       8 |       52.30 |       95.41 |   1.8×  |
|     128 |       58.18 |     1066.05 |    18×  |
|     256 |       57.62 |     1669.11 |    29×  |

Without the fix, throughput stays flat ~50–60 tps — that's the broken-
binlog-group-commit case (Bug#13669). With the fix, 1 → 256 threads
scales **78×**.

**Percona, "Maximal write throughput in MySQL"** — same engine on Dell
R900 + FusionIO + battery-backed RAID. This isolates the cost of
durability, ie. the **with-fsync vs without-fsync** ratio that
corresponds to our Visible→Durable comparison:

| Config                                          |    tps |
|-------------------------------------------------|-------:|
| No fsync (`innodb=0`, no binlog) — baseline     | 36,332 |
| `innodb=1`, no binlog (redo fsync on BBWC)      | 23,115 |
| `innodb=1` + binlog (binlog not sync'd)         | 12,097 |
| Full durability (`innodb=1`, `sync_binlog=1`)   |  3,086 |

The clean comparison to us is first-row vs last-row — single-log no-fsync
vs single-log with real fsync per commit:

|                                   | No fsync     | Fsync per commit | Ratio |
|-----------------------------------|-------------:|-----------------:|------:|
| **MariaDB** (sysbench, fix on)    | 36,332 tps   |   3,086 tps      | **11.8×** |
| **IngoDB** today                  | 387K (Visible @ 32t) | 34K (Durable @ 1024t) | **11.4×** |

Both engines land at the same ~11× memory-vs-durable ratio. That ratio
is set by the storage's durable-write IOPS divided by what one fsync can
amortize — once group commit is working, the engine layer can't push the
ratio further; it's a hardware fact.

Notes on the comparison:
- MariaDB's `innodb=1, no binlog` (23,115) ÷ baseline (36,332) ≈ **1.57×**
  — only 60% slowdown — because InnoDB's redo log on their setup hits
  battery-backed RAID where fsync returns in microseconds. Not comparable
  to either of us without BBWC.
- MariaDB's `sync_binlog=1` step (12,097 → 3,086, a **4× drop**) is the
  binlog fsync hitting non-BBWC FusionIO. That's their real-fsync penalty.
  We don't have a second log, so we skip this entirely.
- We get more aggressive thread scaling (236× vs 78×) because of
  pipelining: we release the WAL writer mutex *before* fsync, letting
  batch N+1's append overlap batch N's fsync. MariaDB's 5.3/10.0 fix
  holds its equivalent lock through fsync — the MDEV-232 work that
  followed (reducing 3 fsyncs to 2) gave another 30–60%, still less than
  full pipelining.

Sources:
- [Percona — Testing the Group Commit Fix](https://www.percona.com/blog/testing-the-group-commit-fix/)
- [Percona — Maximal write throughput in MySQL](https://www.percona.com/blog/maximal-write-througput-in-mysql/)
- [Kristian Nielsen — Fixing MySQL group commit](https://knielsen-hq.org/w/fixing-mysql-group-commit-part-1/)
- [Kristian Nielsen — Even faster group commit](https://kristiannielsen.livejournal.com/16382.html)

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
