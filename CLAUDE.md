# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

IngoDB is an AI-native, adaptive storage engine conceived by Henrik Ingo (former MySQL/MariaDB architect, MongoDB query optimizer engineer, Nyrkio CEO). The project is in its earliest stage — no code exists yet, only the architectural vision documented in `origin-story.dialogue-between-gemini-and-henrik.txt`.

## Architectural Vision

IngoDB is a **self-morphing LSM-tree storage engine** that adapts its physical data layout based on continuous performance benchmarking (via Nyrkio/Apache Otava).

### Core Concepts

- **Object-native ingestion**: Accept arbitrary blobs (JSON, BSON, Protobuf, etc.) — no schema required at write time. Each blob is identified by a **BLAKE3 content hash** (content-addressable identity).
- **I-Blob format**: Lazy-parsable documents with a fixed header (magic bytes + content hash), an offset/index table of (KeyHash, Offset, Length) entries, and a raw payload section.
- **Adaptive LSM compaction (UCS-inspired)**: Based on Cassandra's Universal Compaction Strategy. During compaction, the engine morphs data layout:
  - **L0-L1**: Raw, high-entropy blobs optimized for write throughput (no/minimal compression)
  - **L2-L3**: "Semantic shredding" — frequently-queried fields extracted into indexed columns; related objects physically interleaved; dictionary compression introduced
  - **L4+**: Cold data in columnar format (Parquet-style) with RLE, bit-packing, and global dictionaries
- **Performance-driven morphing**: Nyrkio change-point detection serves as the fitness function — structural changes during compaction are only promoted if benchmarks show global improvement.
- **Graph references via hashes**: Relationships are hash references embedded in blobs. The engine learns access patterns and physically co-locates related objects during compaction ("Ghost Pointers").
- **WASM lambda comparators**: Custom sort orders provided as sandboxed WebAssembly modules — no baked-in collations or locales.
- **Liquid AST query interface**: Queries arrive as serialized ASTs over gRPC (not SQL). Graph-style traversals instead of joins.

### Technology Choices

- **Language**: Rust (with io_uring via monoio/glommio for thread-per-core architecture)
- **No GC languages** (Java, Go) — deterministic latency is required for Nyrkio benchmarking
- **Target hardware**: ARM64 (AWS Graviton4) preferred for deterministic performance and SVE vectorization; local NVMe with ZNS (Zoned Namespaces) SSDs for direct I/O control
- **Standard primitive types**: U64/I64, UUID/Hash, Timestamp (unix micros), Blob/String — all with CPU-intrinsic-friendly sort logic (SIMD where applicable)
- **Compression strategy**: Level-embedded — LZ4 at hot levels, Zstd with trained dictionaries at cold levels, RLE and bit-packing for columnar segments

### Key Design Principles

1. **Physics-first**: Minimize layers between electrons and bits. No buffer pool — use direct I/O and memory-mapped files.
2. **No relational hangover**: No SQL parser, no foreign keys, no schema-on-write, no locale/collation complexity.
3. **The engine learns**: Access pattern monitoring drives all optimization decisions (indexing, shredding, co-location, compression).
4. **Nyrkio as the nervous system**: Every structural change is validated by continuous benchmarking before promotion to production.
5. **Code is disposable**: AI generates and rewrites components; empirical measurement (not human intuition) validates changes.
