# Refactoring backlog

Imminent, low-risk cleanups — roughly in priority order. Not features or design
work (those live in `PLAN.md`); this is "the code would be healthier if…". Add to
it when you spot something while working, with a file:line anchor where useful.

## 1. `LsmConfig` literals should use `..Default::default()`

There are ~34 exhaustive `LsmConfig { … }` struct literals across tests, examples,
and `database.rs`. `impl Default for LsmConfig` already exists
(`crates/ingodb-lsm/src/lib.rs:110`), but the construction sites don't use the
`..Default::default()` spread — they spell out every field. So **adding one config
field touches all 34 sites**.

Fix: rewrite each literal to list only its overrides, e.g.
`LsmConfig { data_dir, memtable_size: …, ..Default::default() }`. Mechanical, no
behavior change, own commit. Afterward, adding a field (e.g. promoting the
equality index's `MAX_EQUALITY_FIELDS` const to a real config knob) is a one-liner.

## 2. Clean up compiler warnings

`cargo build --workspace` currently emits dead-code warnings — mostly unused
index-build scaffolding in `secondary.rs` (`build`, `has_secondary_index`,
`build_secondary_index`, `DEFAULT_INDEX_THRESHOLD`) plus an unused
`ingodb_consistency::Consistency` import. Decide per item: wire it up, gate behind
`#[cfg(test)]`, or delete. Also worth a `cargo clippy` pass (e.g.
`EqualityIndex::new()` wants a `Default` impl / `or_default()` call site).

## 3. (observed) Consolidate the per-write index fan-out

The write path notifies indexes at four sites (optimistic, visible-leader,
durable-leader, `put_batch`) and each now calls both `notify_unsorted_put` and
`notify_equality_put` side by side — duplicated, and every new reactive index adds
another line × four. Consider a single `notify_indexes(&blob)` choke point.

## 4. (observed) De-duplicate the "serve from index" tail in `scan_at`

Each index attempt in `scan_at` (equality, sorted-filter, unsorted) repeats the
same project → `set_docs_scanned` → `record` → return boilerplate. A small helper
(`fn finish_index_scan(results, project, timer)`) would remove the copy-paste.

## 5. ~~`MAX_EQUALITY_FIELDS` bounds the wrong dimension~~ — DONE

Resolved in Phase D: the field-count cap is replaced by a memory-byte capacity
(`EQUALITY_RAM_BUDGET_BYTES`) with field-granularity LRU eviction, and postings
now persist to disk so eviction reloads instead of rebuilding.

## 7. (observed) Unify the two compaction trigger paths

There are two schedulers over the same `run_compaction` merge core: inline
`maybe_compact` (synchronous, on flush, single job via `pick_compaction`, used by
tests) and the background coordinator loop (`pick_all_compactions` + worker
threads). Cross-cutting maintenance (sidecar flush, index promotion, adaptive W)
must be wired into both, and it's easy to cover one and miss the other — that's
how the equality-sidecar persistence gap happened (inline test passed; background
read-heavy workload never flushed + `Drop` never runs because the worker holds an
engine `Arc`). Unify into one driver with a run-here-vs-dispatch strategy and a
single hook set. (Related: the worker's `Arc` clone means `Drop`-based shutdown
flush never fires under background compaction — persistence relies on the periodic
loop flush instead, losing ≤ one wake interval of read-built postings on a hard
exit.)

## 6. (observed) Equality RAM budget should be configurable

`EQUALITY_RAM_BUDGET_BYTES` is a module const. It should be an `LsmConfig` field
(blocked on refactor #1, the `..Default::default()` cleanup). Also: orphaned `.eq`
sidecars from a crash mid-compaction are never cleaned (harmless leak — SSTable
ids are monotonic so they're never re-read); a one-time sweep at `open()` would
tidy them.
