# API Guide — The Liquid AST

IngoDB's query interface is the **Liquid AST**: a Rust enum-based query language designed for programmatic construction, not human typing. The primary consumer is an AI agent or application code, not a person at a REPL.

There is no SQL parser. Queries are data structures.

## Setup

```rust
use ingodb::{LsmConfig, LsmEngine, IBlob, Value, DocumentId, Query, Filter, SortField, SortDirection};

let config = LsmConfig {
    data_dir: "my_data".into(),
    ..LsmConfig::default()
};
let engine = LsmEngine::open(config)?;
```

For concurrent access, wrap in `Arc`:

```rust
use std::sync::Arc;

let engine = Arc::new(LsmEngine::open(config)?);
engine.start_background_compaction(); // enables background compaction threads
```

## Writing Documents

### Insert

```rust
// Auto-generated _id
let doc = IBlob::from_pairs(vec![
    ("name", Value::String("Henrik".into())),
    ("age", Value::U64(42)),
    ("email", Value::String("henrik@example.com".into())),
]);
engine.put(doc)?;
```

### Insert with explicit _id

```rust
let id = DocumentId::new(); // or from a client-generated UUID
let doc = IBlob::with_id(id, [
    ("name".into(), Value::String("Henrik".into())),
    ("age".into(), Value::U64(42)),
].into());
engine.put(doc)?;
```

### Update (upsert)

Same `_id`, new fields. The engine stamps a new `_version` automatically.

```rust
let updated = IBlob::with_id(id, [
    ("name".into(), Value::String("Henrik".into())),
    ("age".into(), Value::U64(43)), // birthday
].into());
engine.put(updated)?;
```

### Delete

```rust
engine.delete(&id)?;
```

Writes a tombstone. The document becomes invisible to reads. Tombstones are purged during compaction when safe.

## Reading Documents

### Point Lookup

```rust
if let Some(doc) = engine.get(&id)? {
    println!("name: {:?}", doc.get("name"));
}
```

### Scan with Filter

```rust
let results = engine.scan(
    Some(&Filter::Gt { field: "age".into(), value: Value::U64(30) }),
    None,       // no sort
    None,       // no projection
    Some(100),  // limit
)?;
```

### Scan with Sort

```rust
let results = engine.scan(
    Some(&Filter::Eq { field: "type".into(), value: Value::String("product".into()) }),
    Some(&[SortField { field: "price".into(), direction: SortDirection::Ascending }]),
    None,
    Some(20),
)?;
```

### Scan with Projection

Returns IBlobs with only the requested fields (projection flag set).

```rust
let results = engine.scan(
    None,
    Some(&[SortField { field: "price".into(), direction: SortDirection::Ascending }]),
    Some(&["name".into(), "price".into()]),
    Some(10),
)?;
// results[0].get("name")  → Some(...)
// results[0].get("email") → None (not projected)
// results[0].is_projection() → true
```

### Execute via Query enum

```rust
let results = engine.execute(&Query::Scan {
    filter: Some(Filter::And(vec![
        Filter::Eq { field: "type".into(), value: Value::String("order".into()) },
        Filter::Gt { field: "amount".into(), value: Value::U64(100) },
    ])),
    sort: Some(vec![SortField { field: "amount".into(), direction: SortDirection::Descending }]),
    project: None,
    limit: Some(50),
})?;
```

### Graph Traversal (Join by Value)

Any field can become a graph edge at query time. No special pointer types needed.

```rust
// Find users referenced by orders over $100
let users = engine.execute(&Query::Traverse {
    start: Some(Filter::Gt { field: "amount".into(), value: Value::U64(100) }),
    from_field: "user_id".into(),  // field on orders containing user's _id
    to_field: "_id".into(),        // field on users to match against
    depth: 1,                      // 1 = simple join
})?;
```

Deeper traversals repeat the same edge:

```rust
// Follow a management chain: employee → manager → manager's manager
let chain = engine.execute(&Query::Traverse {
    start: Some(Filter::Eq { field: "name".into(), value: Value::String("Alice".into()) }),
    from_field: "reports_to".into(),
    to_field: "_id".into(),
    depth: 3,
})?;
```

## Filter Predicates

```rust
Filter::Eq { field, value }            // field == value
Filter::Gt { field, value }            // field > value
Filter::Lt { field, value }            // field < value
Filter::Range { field, low, high }     // low <= field < high
Filter::Exists { field }               // field is present
Filter::And(vec![...])                 // all must match
Filter::Or(vec![...])                  // any must match
Filter::Not(Box::new(...))             // negation
```

## Value Types

```rust
Value::Null
Value::Bool(true)
Value::I64(-42)
Value::U64(42)
Value::F64(3.14)
Value::String("hello".into())
Value::Bytes(vec![0, 1, 2])
Value::Uuid(DocumentId::new())         // 16-byte UUID
Value::Array(vec![Value::U64(1), Value::U64(2)])
Value::Document(vec![("key".into(), Value::String("val".into()))])
```

## Snapshot Isolation (MVCC)

```rust
let snapshot = engine.snapshot();

// Concurrent writes won't affect snapshot reads
engine.put(some_update)?;

// Snapshot sees the state at the moment it was created
let old_value = snapshot.get(&id)?;
let consistent_scan = snapshot.scan(None, None, None, None)?;

// Snapshot released on drop — old versions become eligible for GC
drop(snapshot);
```

## Virtual Fields

- **`_id`**: Queryable and joinable. `Filter::Eq { field: "_id".into(), value: Value::Uuid(id) }` works.
- **`_version`**: Readable via `blob.version()` but not queryable in filters (the read path only surfaces the latest version per snapshot).

## Collections

```rust
use ingodb::lsm::Database;

let db = Database::open(config)?;

// Each collection is an isolated LsmEngine
db.with_collection("users", |engine| {
    engine.put(user_doc)?;
    Ok(())
})?;

db.with_collection("orders", |engine| {
    let orders = engine.scan(Some(&filter), None, None, None)?;
    Ok(orders)
})?;

// System collection stores engine metadata (index definitions)
// Always exists, cannot be dropped
```

## Statistics

```rust
let stats = engine.query_stats();

// Most frequent query patterns
for (pattern, ps) in stats.top_by_count(5) {
    println!("{}: count={}, avg={:?}, selectivity={:.3}",
        pattern.query_type, ps.count, ps.avg_latency(), ps.selectivity());
}

// Patterns that would benefit from an index
for (pattern, ps) in stats.low_selectivity(0.1, 5) {
    println!("Index candidate: {:?}", pattern.filter_fields);
}
```

## Compaction Statistics

```rust
let cs = engine.compaction_stats();
println!("Compaction runs: {}", cs.runs.load(Ordering::Relaxed));
println!("Write amplification: {:.2}x", cs.write_amplification());
```
