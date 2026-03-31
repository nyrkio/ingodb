# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
cargo build --workspace          # Build all crates
cargo test --workspace           # Run all 53 tests
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
├── ingodb-blob                 I-Blob format: content-addressable documents with BLAKE3 hashing
│                               Value types, encode/decode, zero-copy field extraction
├── ingodb-wal                  Write-Ahead Log: append-only, CRC32-checksummed, crash-recoverable
├── ingodb-memtable             In-memory sorted BTreeMap of IBlobs, size-bounded with flush signaling
├── ingodb-sstable              SSTable writer/reader: LZ4-compressed data blocks, bloom filters, block index
├── ingodb-lsm                  LSM engine: ties WAL + MemTable + SSTables together with compaction
└── ingodb-query                Liquid AST: enum-based query interface (Get/Scan/Traverse) with filter predicates
```

**Data flow**: Application → `LsmEngine::put(IBlob)` → WAL append → MemTable insert → (on threshold) flush to SSTable → (on accumulation) size-tiered compaction merges SSTables.

**Read path**: MemTable → SSTables (newest first, bloom filter check before each).

## Key Types

- `IBlob` — The document. A sorted `BTreeMap<String, Value>` identified by its BLAKE3 content hash. Binary format: `[INGO magic][version][hash][index_count][index_entries...][payload...]`
- `Value` — Tagged union: Null, Bool, I64, U64, F64, String, Bytes, Hash (reference to another IBlob), Array, Document (nested)
- `ContentHash` — `[u8; 32]`, BLAKE3 hash. Two IBlobs with identical fields always produce the same hash.
- `CompactionFilter` trait — Extension point for future adaptive morphing during compaction

## Architectural Vision

IngoDB is a **self-morphing LSM-tree storage engine** that adapts its physical data layout based on continuous performance benchmarking (via Nyrkio/Apache Otava).

### Core Concepts

- **Object-native ingestion**: Accept arbitrary documents — no schema required at write time. Each document identified by BLAKE3 content hash (content-addressable identity).
- **I-Blob format**: Lazy-parsable with offset index for zero-copy single-field extraction.
- **Adaptive LSM compaction (UCS-inspired)**: Future work based on Cassandra's Universal Compaction Strategy. During compaction, the engine will morph data layout per level (raw blobs → shredded columns → columnar cold storage).
- **Graph references via hashes**: Relationships are Hash values embedded in documents. Future: engine co-locates related documents during compaction.
- **Liquid AST query interface**: Queries are Rust enum ASTs, not SQL strings.

### Key Design Principles

1. **Physics-first**: Minimize layers between electrons and bits.
2. **No relational hangover**: No SQL parser, no foreign keys, no schema-on-write.
3. **The engine learns**: Access pattern monitoring will drive optimization decisions.
4. **Nyrkio as the nervous system**: Structural changes validated by continuous benchmarking.
5. **Code is disposable**: AI generates and rewrites components; empirical measurement validates changes.
