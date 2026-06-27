# Equality index (Eq / In) — design

Status: **design converged 2026-06-27**, supersedes the in-memory v0 landed the
same day (`crates/ingodb-lsm/src/equality.rs`, which used write-side `notify_put`
maintenance — see "Migration" below). Reactive, lazy, LSM-native.

## Purpose

Serve `Eq` and `In` predicates (the latter as `Or(Eq, Eq, …)` on one field — no
AST node) without a full collection scan, as a sibling to the sorted/unsorted
*range* indexes.

**Routing:** `Eq`/`In` never *populate* range indexes (an Eq result no longer
mints a sorted partial / unsorted block). But they may still be *served* by an
existing range index that covers the value — Eq is a reader of range indexes, not
a writer. A full sorted index, in particular, answers `Eq(col=v)` directly (and
once one exists, compaction drops that column's Eq postings as redundant).

## Core idea: postings follow the data through LSM levels

An equality posting is an inverted list `value → [_id]` storing **only `_id`
references** (16 bytes each), **scoped to a single immutable SSTable**.

Because an SSTable is immutable until compaction, a posting derived from it is
**exact and stable until that SSTable is compacted** — there is no within-SSTable
staleness, and therefore **no write-side maintenance** (`notify_put`) is needed:

- Writes only touch the **memtable** (always live-scanned) and later flush to a
  **new** L0 SSTable (indexed lazily on first read). No write invalidates an
  existing posting.
- At **compaction**, postings are rebuilt/merged alongside the data they describe
  (the morph-on-compaction extension point).

This is *why* "materialize on reads" is correct: a global read-built posting
would suffer false negatives on later writes; a per-SSTable one cannot.

## Data model

Per `(SSTable S, value V)`: a `Posting` enum — `Exact` | `Overflow` | `Partial`
(see Build rules).

Two independent knobs:
- **R** — completeness threshold (selectivity fraction, default **0.20**,
  configurable). Decides *whether to fully index a value*. Bounds *complete*
  posting size (≤ R·|S|).
- **K** — overflow retention (**16**). How many ids to keep once we give up on
  completeness. Bounds *incomplete* postings.

### Build rules (lazy, on read scan of S for V)

The three truth-table states are encoded as an **enum** (Phase A,
`equality.rs::Posting`), not `{ids, complete}` + a "`len == K`" inference — the
explicit variant makes overflow unambiguous and removes two sharp edges (below).

| variant | meaning | how it got here |
|:--|:--|:--|
| `Exact(ids)` | every match in S; absence authoritative. `Exact([])` = negative cache | scan exhausted S, and `m < R·\|S\|` **or** `m ≤ K` |
| `Overflow(ids)` | V is ≥ R in S **and** `m > K`; K-sized sample, rest discarded; *final* | scan exhausted S, hit R, more than K matches |
| `Partial(ids)` | sample of unknown completeness; *refinable* | LIMIT satisfied before S exhausted |

Two refinements vs. the original `{ids, complete}` sketch, both from implementing it:
- **Over-R but `m ≤ K` stays `Exact`.** If a value clears R but we still captured
  *all* of its matches (≤ K), completeness is free and beats a pessimistic
  overflow — and it avoids a `< K` overflow that would loop on re-scan. So
  `Overflow` requires `m > K`, i.e. it always holds exactly K ids.
- **No `K−1` cap on `Partial`.** That hack only existed to keep `len==16 ∧
  ¬complete` meaning overflow under the inference encoding; the explicit `Partial`
  variant makes it unnecessary (`Partial` keeps up to K).

`Exact` vs `Overflow` vs `Partial` is the reactive signal: `Overflow` is a *final*
fact about immutable S (exhaustive `Eq(V)` should just scan, never re-sample);
`Partial` is *provisional* and a later exhaustive scan **upgrades** it to `Exact`
or `Overflow`.

## Serve path: `Eq(V)` (optionally `LIMIT n`)

1. Live-scan the **memtable** for V (mutable, never indexed).
2. For each SSTable S, consult its posting for V:
   - `Exact` → exhaustive contribution (verify ids).
   - `Overflow` → K candidates; serve `LIMIT ≤ survivors`, else scan S.
     **(Phase B: declines the whole query → sequential full scan, since
     verifying a non-selective value's candidates is slower than scanning. The
     LIMIT-from-sample fast path is Phase C.)**
   - `Partial` → k candidates; if more needed, scan S (refines the posting).
   - no posting yet → an existing range index covering V (sorted partial /
     unsorted block / full sorted index) may serve it; otherwise scan S, building
     the posting per the rules above. Eq reads range indexes but never creates them.
3. **Verify-on-read**: resolve each candidate via `get_at(id, snapshot)` and
   re-check the predicate. With per-SSTable immutability this is no longer about
   within-SSTable staleness — it now resolves **cross-level MVCC** (an id with V
   in L2 but a newer version in L0 where field≠V) and applies the snapshot.
4. Union, dedup by `_id`, apply limit.

A **global negative** for V means *every* SSTable's V-posting is complete-empty
**and** the memtable has no V.

### LIMIT-1 corner

`Eq(X) LIMIT 1` against an incomplete posting can't just return `ids[0]` (it may
be superseded cross-level). Verify candidates until one survives — bounded by the
stored count (≤16); only if *all* fail do we scan S. K=16 is the buffer that lets
small-LIMIT queries absorb verification misses without rescanning.

## MVCC

Postings are version-agnostic; `get_at` is the version oracle. Consulted only for
latest reads (`snapshot == max`) in v1, matching the other indexes; snapshot reads
bypass and full-scan. The per-SSTable read path is forward-compatible with serving
older snapshots (it already threads `snapshot` into the verify step).

## Compaction

Postings follow the data:
1. **Keep + rebuild/refresh.** Recompute carried-forward postings exactly from the
   merged output rows (free — compaction already streams every row), so warm
   indexes survive compaction instead of cold-starting.
2. **Drop a column** whose compaction output includes a **sorted full-range
   index** — that index already answers `Eq(col=v)`, so the postings are
   redundant. (Promotion ladder: Eq postings → sorted partials → full sorted
   index subsumes Eq.)
3. **Drop** postings the field-granularity LRU marks cold.

## Budget / LRU

Field-granularity LRU, separate budget from the range indexes
(`MAX_EQUALITY_FIELDS`). Touching any value marks the field used; eviction drops a
whole field's postings. No per-value or per-version recency.

## Migration from v0 (in-memory, landed 2026-06-27)

v0 is a single global `EqualityIndexSet` with write-side `notify_put` and a 50%
build guard. The redesign:
- **Remove** write-side maintenance: the 4 `notify_equality_put` call sites and
  `EqualityIndexSet::notify_put`.
- **Move** postings from one global set to **per-SSTable** (a side map keyed by
  SSTable id; needs `SSTableReader` to expose row count |S| for R·|S|).
- **Build on read** per-SSTable during the scan (the scan path must attribute
  matches to their source SSTable, not merge-then-filter globally).
- **Replace** the 50% global guard with **R (per-SSTable) + K + complete** rules.
- **Add** compaction refresh + redundancy/LRU drops.
- **Keep**: value-keyed-by-encoding, verify-on-read via `get_at`, `Or(Eq…)`→In,
  field-LRU, negative cache.

## Staged implementation

- **A. ✅ DONE** (`equality.rs::Posting` enum — `Exact`/`Overflow`/`Partial`, R/K
  build rules, `satisfies` serve rule, 7 unit tests; pure, `#[allow(dead_code)]`
  until wired). Coexists with v0 in the same file.
- **B. ✅ DONE.** Per-SSTable read-built postings (`EqualityPostings`, keyed by
  SSTable path) + build-on-read serve path + verify-on-read; v0's `notify_put`
  removed. Eq/In routed *after* the range indexes (read, don't populate). Postings
  dropped per SSTable at compaction, rebuilt on next read. `equality_hits` counts
  warm serves (all `Exact`, no rescan). Tests: multi-SSTable union, correctness
  across compaction, + the 5 behavior tests carried over green. Deferred to C:
  `Partial`/early-stop and LIMIT-from-`Overflow`-sample (built, `allow(dead_code)`);
  per-SSTable scan once per value for `In` (currently once per value); field-LRU
  budget (postings currently bounded only by live SSTables × queried values).
- **C.** Compaction refresh (rebuild postings from merged output) + redundancy
  drop (sorted full-range index) + LRU drop.
- **D.** Re-run ClickBench; confirm UserID still fast, CounterID `LIMIT k` now
  served from overflow, exhaustive CounterID still scans.
