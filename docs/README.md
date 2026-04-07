# IngoDB Documentation

IngoDB is a self-morphing, adaptive storage engine that learns from its own workload. It observes query patterns, tracks performance metrics, and reactively restructures its physical data layout to optimize for the queries it actually serves.

## Contents

- [Philosophy & Origin Story](philosophy.md) — Why IngoDB exists and its design principles
- [Architecture](architecture.md) — Crate structure, data flow, and key types
- [API Guide](api.md) — The Liquid AST query interface (designed for programmatic, not human, consumption)
- [Adaptive Engine](adaptive.md) — How the engine learns: statistics, reactive indexes, compaction
- [MVCC & Snapshots](mvcc.md) — Multi-version concurrency control and snapshot isolation
- [Compaction (UCS)](compaction.md) — Unified Compaction Strategy and the W parameter
- [Benchmarks](../BENCHMARKS.md) — Performance results across configurations
- [Roadmap](roadmap.md) — What's built, what's next
