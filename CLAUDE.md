# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all 107 tests
cargo test -p ingodb-blob        # Test just the blob format crate
cargo test -p ingodb-wal         # Test just the WAL crate
cargo test -p ingodb-memtable    # Test just the memtable crate
cargo test -p ingodb-sstable     # Test just the SSTable crate
cargo test -p ingodb-lsm         # Test just the LSM engine crate
cargo test -p ingodb-query       # Test just the query layer crate
cargo test -p ingodb             # Run integration tests
cargo test -p ingodb-blob -- test_content_addressable  # Run a single test
```

## Project Overview

IngoDB is an AI-native, adaptive storage engine conceived by Henrik Ingo (former MySQL/MariaDB architect, MongoDB query optimizer engineer, Nyrkio CEO). The architectural vision is documented in `origin-story.dialogue-between-gemini-and-henrik.txt` and the phased implementation plan is in `PLAN.md`.

## Crate Architecture

```
ingodb/                         Top-level API crate (re-exports everything)
├── ingodb-blob                 I-Blob format: UUIDv7 document identity, BLAKE3 content hash,
│                               DocumentId, Value types, encode/decode, zero-copy field extraction
├── ingodb-wal                  Write-Ahead Log: append-only, CRC32-checksummed, crash-recoverable
├── ingodb-memtable             In-memory sorted BTreeMap of IBlobs, size-bounded with flush signaling
├── ingodb-sstable              SSTable writer/reader: LZ4-compressed data blocks, bloom filters, block index
├── ingodb-lsm                  LSM engine: WAL + MemTable + SSTables, UCS compaction, delete/tombstones
└── ingodb-query                Liquid AST: enum-based query interface (Get/Scan/Traverse) with filter predicates
```

**Data flow**: Application → `LsmEngine::put(IBlob)` → stamp `_version` → WAL append → MemTable insert → (on threshold) flush to SSTable → (on accumulation) UCS compaction merges SSTables.

**Delete flow**: `LsmEngine::delete(&DocumentId)` → write tombstone (IBlob with `is_deleted=true`) → same path as put. Tombstones purged during compaction when output level > max level.

**Read path**: MemTable (keyed by `_id`) → SSTables (L0 first → L1 → ..., within each level newest first, bloom filter check before each). First match is guaranteed current due to level ordering invariant.

## Key Types

- `DocumentId` — Newtype around `[u8; 16]`, a UUIDv7. Stable document identity. Client or server can generate. Lexicographically sortable by creation time.
- `IBlob` — The document. Has `_id` (DocumentId), `_version` (DocumentId, server-assigned at write time), `hash` (ContentHash), and `fields` (BTreeMap<String, Value>). Binary format v2: `[INGO magic:4][version:2][document_id:16][doc_version:16][content_hash:32][index_count:4][index_entries...][payload...]` (74-byte header).
- `Value` — Tagged union: Null, Bool, I64, U64, F64, String, Bytes, Uuid(DocumentId), Array, Document (nested)
- `ContentHash` — `[u8; 32]`, BLAKE3 hash covering `_id` + `_version` + `is_deleted` + fields. Full integrity protection — catches corruption of any byte. NOT used as primary key.
- `CompactionFilter` trait — Extension point for future adaptive morphing during compaction
- `UcsCompaction` — Unified Compaction Strategy: level assignment by file size, overlap detection, configurable scaling parameter W (leveled/balanced/tiered)
- `TombstoneFilter` — Purges tombstones during compaction when safe (output at last level)

## Architectural Vision

IngoDB is a **self-morphing LSM-tree storage engine** that adapts its physical data layout based on observed query performance.

### Core Concepts

- **Object-native ingestion**: Accept arbitrary documents — no schema required at write time. Each document has a stable UUIDv7 `_id` and a server-assigned `_version` for write ordering. BLAKE3 content hash is kept for integrity/dedup.
- **I-Blob format**: Lazy-parsable with offset index for zero-copy single-field extraction.
- **Adaptive LSM compaction (UCS-inspired)**: Future work based on Cassandra's Universal Compaction Strategy. During compaction, the engine will morph data layout per level (raw blobs → shredded columns → columnar cold storage).
- **Graph traversal as join**: Relationships are not stored as special pointer types. Any field value can become a graph edge at query time via traverse-as-join. The engine observes join patterns and optimizes (indexes, co-location) reactively.
- **Liquid AST query interface**: Queries are Rust enum ASTs, not SQL strings.

### Key Design Principles

1. **Physics-first**: Minimize layers between electrons and bits.
2. **No relational hangover**: No SQL parser, no foreign keys, no schema-on-write.
3. **The engine learns**: The database itself tracks query latencies, field access patterns, and traversal costs. Reactive optimization restructures data during compaction based on what's actually slow. A second-phase predictive engine can anticipate and optimize ahead of time.
4. **Nyrkiö is external**: External benchmarks and CI can wire to Nyrkiö, but the database's self-optimization loop is built-in — it observes its own real query workload, not an external benchmark harness.
5. **Code is disposable**: AI generates and rewrites components; empirical measurement validates changes.
