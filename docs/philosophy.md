# Philosophy & Origin Story

## The Dialogue

IngoDB was conceived in a dialogue between Henrik Ingo and Google Gemini, recorded in [`origin-story.dialogue-between-gemini-and-henrik.txt`](../origin-story.dialogue-between-gemini-and-henrik.txt). Henrik — former MySQL/MariaDB architect, MongoDB performance engineer, database team leader at DataStax (Cassandra) and CrateDB, and Nyrkio founder — sketched a vision for a database engine that doesn't require upfront schema decisions because it makes those decisions itself, reactively, from observed workload. All of these experiences have provided inspiration for this adventure.

The name "IngoDB" is an old joke between Henrik and Kaj Arnö. It is expected that Kaj will provide a SQL or SQL-like query frontend for IngoDB. (It is left as an exercise for the reader to suggest a name for that product based on Kaj's name.)

The core question: *What if the storage engine could watch what queries are slow, and fix itself?*

## Design Principles

### 1. Physics-first

Minimize layers between electrons and bits. No ORM, no SQL parser, no query planner with cost models based on guesses. The engine operates directly on binary document formats with zero-copy field extraction, LZ4-compressed SSTable blocks, and BLAKE3 integrity hashing.

### 2. No relational hangover

No foreign keys, no schema-on-write, no JOIN syntax. Documents are arbitrary collections of typed fields. Relationships between documents are discovered at query time through join-by-value — any field can become a graph edge when a query says so. The engine doesn't need to know about relationships at write time.

### 3. The engine learns

This is the central thesis. Traditional databases require a human DBA to:
- Design schemas
- Create indexes
- Choose compaction strategies
- Tune buffer sizes

IngoDB does this itself. It tracks every query: what fields are filtered, what fields are sorted, how many documents are scanned vs returned, how long it takes. When it detects a pattern — a filter query that repeatedly scans 100,000 documents to return 100 — it builds a secondary index. When a sort query repeats, it persists the sorted result as a reusable index. When compaction runs, it decides whether to merge or rebuild indexes based on staleness ratios.

The human (or more likely, an AI agent) just writes documents and queries them. The engine adapts.

### 4. The primary user is an AI agent

IngoDB's query interface is not SQL. It's not a string-based language at all. It's a Rust enum AST — the **Liquid AST** — designed for programmatic construction by code, not for humans typing at a terminal.

This is deliberate. The expected usage is:
1. An AI agent builds an application
2. The application stores and retrieves documents through the Liquid AST
3. The engine optimizes itself for the application's actual access patterns
4. No human DBA is involved

The query language is a data structure, not a grammar. An AI agent constructing a `Query::Scan { filter: Some(Filter::Gt { field: "age".into(), value: Value::U64(30) }), sort: None, project: None, limit: Some(10) }` is the intended interface. SQL would be a layer on top, if needed, but it's not the native language.

### 5. Code is disposable

The engine is built with AI assistance. Components are generated, tested, measured, and rewritten. The architecture matters; the specific code is replaceable. Empirical measurement — benchmarks, query statistics, selectivity ratios — validates changes, not code review aesthetics.

### 6. Nyrkio is external

The database's self-optimization loop is built-in: it observes its own real query workload and adapts. External benchmarking tools like [Nyrkio](https://nyrkio.com) can wire into the engine for CI performance tracking, but they're not a dependency. The engine doesn't need an external system to tell it what's slow.
