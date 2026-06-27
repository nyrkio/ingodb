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

## 5. (observed) `MAX_EQUALITY_FIELDS` bounds the wrong dimension

`equality.rs`'s field-LRU caps the number of distinct *fields* (64). It's a vestige
of the v0 in-memory design: in the per-SSTable model it rarely fires (collections
rarely have 64+ queried fields), doesn't bound the dimension that costs memory
(values × postings — a hot high-cardinality field accumulates postings the
field-LRU never evicts), and compaction is already the primary GC. Decision
pending (Henrik): either drop the field-count cap and lean on compaction GC, or
keep the field-granularity eviction but trigger it on `posting_count()` / bytes.
Either way, document the limit.
