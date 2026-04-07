# IngoDB: A Database That Learns From Its Own Workload

*By Henrik Ingo, April 2026*

With everyone else vibe coding their favorite database from scratch these days, I wanted to try something different.

Sunny Bains (InnoDB veteran, now at PingCAP) has been building [database components in Rust](https://github.com/sunbains) including a WAL implementation. Laurynas Biveinis (Percona's Technical Director) has been [maintaining MySQL forks](https://github.com/laurynas-biveinis) and pushing the boundaries of InnoDB internals. Baotiao implemented the [InnoDB doublewrite buffer concept in PostgreSQL](http://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg219771.html) — with [impressive results: 2x write throughput](http://baotiao.github.io/2026/02/05/fpw-dwb.html) compared to PostgreSQL's full page writes. And a friend who shall remain anonymous has reimplemented Cassandra in Rust.

Instead of piling on all the projects I always wanted to do but didn't have time or budget for, I started brainstorming with Gemini on a database design that is **only possible because of AI** — and whose primary users going forward will also be **AIs, not humans**.

And this is what Claude and yours truly came up with in the past week.

## The Question

What if the storage engine could watch what queries are slow, and fix itself?

Not "fix" as in "a DBA runs EXPLAIN, notices a missing index, and types CREATE INDEX." Fix as in: the engine observes that a particular filter query scans 100,000 documents to return 100, and *automatically* builds an index on the filter field. Without anyone asking.

That's IngoDB.

## What Is It?

IngoDB is an adaptive, self-morphing document storage engine written in Rust. It's an LSM-tree engine (like RocksDB, Cassandra, or LevelDB) with a twist: it tracks its own query performance and reactively restructures its physical data layout based on what it observes.

Key features as of this week:

- **Liquid AST query interface** — No SQL parser. Queries are Rust data structures (enums). The intended consumer is an AI agent or application code, not a person typing at a terminal. (My friend Kaj Arnö might provide a SQL frontend later. I leave it as an exercise for the reader to suggest a name for that product.)

- **Graph traversal as join-by-value** — There are no foreign keys, no special pointer types. Any field can become a graph edge at query time. `Traverse { from_field: "user_id", to_field: "_id", depth: 2 }` follows relationships that are discovered, not declared.

- **MVCC snapshot isolation** — Multi-version concurrency control with UUIDv7 versions. Take a snapshot, do reads, see a consistent point-in-time view while writers continue in parallel.

- **UCS-inspired compaction** — Unified Compaction Strategy adapted from [Cassandra's UCS](https://github.com/apache/cassandra/blob/88e3fcfe8b436b2adf8f686d2e86e5bb9a94b50b/doc/modules/cassandra/pages/managing/operating/compaction/ucs.adoc) (designed by my former colleague Branimir Lambov at DataStax). A single scaling parameter W controls the read/write amplification tradeoff, with background multi-threaded compaction.

- **Reactive secondary indexes** — This is the core differentiator. Sort queries automatically spill to disk as secondary index SSTables. Filter queries with low selectivity trigger index creation after 2 executions. Indexes use the same SSTable format as primary storage — just sorted by different fields. Partial index ranges evolve through compaction.

- **170 tests** — Every feature has tests. The engine is young but not fragile.

## How It Learns

The feedback loop:

```
Query executes
  → Stats recorded (fields, latency, docs scanned vs returned)
  → Pattern detected (repeated filter with low selectivity)
  → Index created reactively (SSTable sorted by the filter field)
  → Next query uses the index (binary search instead of full scan)
  → Compaction maintains and evolves the index over time
```

The engine decides when to create an index, when to merge index fragments, when to rebuild vs merge (threshold: `M > 0.5 * N * ln(N)`), when to extend a partial index to full range (when it covers ≥50% of primary documents), and when to drop an index that nobody queries anymore (after 10 compaction cycles and 30 days of disuse).

No DBA involved. The engine just watches and adapts.

## Performance (So Far)

On a 100K product catalog benchmark (382 bytes per document):

| Operation | Performance |
|-----------|------------|
| Bulk ingest | 103K docs/sec |
| Point lookup | 69K ops/sec, p50=13µs |
| 8-thread concurrent reads | 500K ops/sec |
| Snapshot isolation | 100% correct |
| Mixed read/write | 780K ops/sec |

At 1M documents with the UCS scaling parameter W=-9 (aggressive leveled compaction):
- 62K point lookup ops/sec after compaction settles to 1 SSTable
- 749 MB of compaction I/O, write amplification 0.90x

Not bad for a week of work. Not production-ready either. But the architecture is sound.

## Why AI-Native?

Two reasons:

**1. The query interface is for machines.** SQL is a human language. IngoDB's Liquid AST is a data structure. An AI agent constructing `Query::Scan { filter: Some(Filter::Gt { field: "age".into(), value: Value::U64(30) }), ... }` is the intended use case. No parsing ambiguity, no SQL dialects, no impedance mismatch. The query IS the program.

**2. The engine is built by AI.** IngoDB was implemented in a continuous dialogue between me and Claude (Anthropic's coding agent). The entire codebase — 170 tests, 7 crates, ~10K lines — was produced in about a week of pair programming sessions. Claude wrote the code; I made the architectural decisions, caught the design flaws, and steered the direction. (For example: "Why does the content hash not include the _id? If those bytes get zeroed by corruption, you'd silently produce a valid-looking tombstone." That caught a real bug.)

This isn't a toy. It has MVCC, background compaction, crash-safe index metadata, and a real UCS implementation. It's also not done. But the velocity of AI-assisted development makes it possible to build a serious storage engine in days instead of years.

## The Origin Story

The full architectural vision is documented in a [dialogue between me and Gemini](https://github.com/nyrkio/ingodb/blob/main/origin-story.dialogue-between-gemini-and-henrik.txt) that predates any code. That conversation produced the ideas: self-morphing LSM trees, reactive optimization during compaction, graph traversal as join. Claude turned those ideas into working code.

My background — MySQL/MariaDB architecture, MongoDB performance engineering, managing the core database teams at DataStax (Cassandra) and CrateDB — all fed into this design. IngoDB is what happens when you take lessons from five different database engines and ask "what if the engine could learn?"

## What's Next

- **Adaptive W parameter**: Auto-tune the compaction strategy based on observed read/write ratio
- **Semantic shredding**: Extract hot fields into columnar structures during compaction
- **Co-location**: Group related documents together based on observed traversal patterns
- **Custom comparators**: WASM-based sort functions for user-defined ordering
- **gRPC server**: Network-accessible Liquid AST protocol

The code is at [github.com/nyrkio/ingodb](https://github.com/nyrkio/ingodb). Documentation is in `docs/`. Benchmarks are reproducible with `cargo run --release --example benchmark`.

This is the beginning. The engine learns. So do we.

---

*Henrik Ingo is the founder of [Nyrkio](https://nyrkio.com) and IngoDB. Previously: MySQL, MariaDB, MongoDB, DataStax, CrateDB.*
