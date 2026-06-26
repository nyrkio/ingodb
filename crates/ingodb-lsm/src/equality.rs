//! Lazy reactive **equality index** for `Eq` / `In` queries.
//!
//! A sibling to the sorted [`crate::secondary`] and unsorted [`crate::unsorted`]
//! *range* indexes — but for point-equality predicates, and structured entirely
//! differently. Where a range index materializes a projected scan result, an
//! equality index is an **inverted posting list**: per field, a map from a field
//! value to the `_id`s that — *at some version* — had `field == value`.
//!
//! ```text
//!   field "country" ─┬─ "FI" → [id3, id9, id40, …]
//!                    ├─ "US" → [id1, id2, …]
//!                    └─ "XX" → []          (negative cache: proven-empty)
//! ```
//!
//! It stores **`_id` references only** — never values or blobs. That is the
//! whole trick:
//!
//! * **Reads verify against the main collection.** A lookup yields *candidate*
//!   `_id`s; the caller resolves each via `get_at(id, snapshot)` and re-checks
//!   the predicate on the authoritative IBlob, dropping stale positives (the
//!   doc's value changed, or it was deleted). Cost is `|candidates|` point-gets
//!   — not as fast as a hash index, but far better than a full collection scan.
//! * **MVCC is free.** The posting list is version-agnostic (a time-union
//!   superset of ids that *ever* matched); `get_at` is the version oracle, so an
//!   old snapshot and a fresh read get correct — different — results from the
//!   same posting list.
//! * **Maintenance is additive.** [`EqualityIndex::notify_value`] appends on
//!   write, but *only* for values that are already materialized (keeping the
//!   index reactive: unqueried values stay untracked, and negatives self-heal
//!   when a matching doc is inserted). Entries are never removed inline; staleness
//!   is resolved at read time by the verify step above.
//!
//! ## LRU / budget
//!
//! Recency is tracked at **field granularity** — one `last_used` per field's
//! whole index. Touching any value marks the field used; eviction drops the
//! entire field's index (every value, every posting) as a unit. Coarser than the
//! range indexes' per-range LRU, deliberately simpler for v1. The set is bounded
//! to [`MAX_EQUALITY_FIELDS`] fields, separate from the range-index budget.
//!
//! v1 is in-memory only (cold after restart) and does not GC posting lists;
//! compaction-time GC gated by the oldest snapshot is future work.

use ingodb_blob::{DocumentId, IBlob, Value};
use ingodb_query::Filter;
use std::collections::HashMap;
use std::time::Instant;

/// Max distinct fields kept across the whole equality index (its own budget,
/// separate from the range indexes' `max_ranges_per_field`). The globally
/// least-recently-used field is evicted when this is exceeded.
pub const MAX_EQUALITY_FIELDS: usize = 64;

/// Encode a [`Value`] to its canonical byte key. `Value` is only `PartialEq`
/// (an `F64` variant rules out `Eq`/`Hash`/`Ord`), so we key posting lists by
/// the value's wire encoding — which is canonical per variant and therefore
/// consistent with `Value`'s equality for all non-float cases.
fn value_key(v: &Value) -> Vec<u8> {
    let mut b = Vec::new();
    v.encode(&mut b);
    b
}

/// If `f` is an `Eq`, or an `Or` of `Eq`s over a *single* field, return that
/// field and the list of equality values (`In` semantics). `None` for anything
/// else — those are not served by the equality index.
pub fn equality_terms(f: &Filter) -> Option<(String, Vec<Value>)> {
    match f {
        Filter::Eq { field, value } => Some((field.clone(), vec![value.clone()])),
        Filter::Or(subs) if !subs.is_empty() => {
            let mut field: Option<String> = None;
            let mut values = Vec::with_capacity(subs.len());
            for s in subs {
                let Filter::Eq { field: ff, value } = s else {
                    return None;
                };
                match &field {
                    None => field = Some(ff.clone()),
                    Some(seen) if seen != ff => return None,
                    _ => {}
                }
                values.push(value.clone());
            }
            field.map(|f| (f, values))
        }
        _ => None,
    }
}

/// Per-field inverted posting lists `value -> [_id]`, plus a single field-level
/// LRU timestamp.
pub struct EqualityIndex {
    postings: HashMap<Vec<u8>, Vec<DocumentId>>,
    last_used: Instant,
}

impl EqualityIndex {
    pub fn new() -> Self {
        EqualityIndex {
            postings: HashMap::new(),
            last_used: Instant::now(),
        }
    }

    /// Seed `value`'s posting from a full scan's matching ids (empty → negative
    /// cache). Replaces any prior posting for that value.
    pub fn materialize(&mut self, value: &Value, ids: Vec<DocumentId>) {
        self.postings.insert(value_key(value), ids);
    }

    /// Candidate ids for `value`, or `None` if never materialized (→ full scan).
    pub fn candidates(&self, value: &Value) -> Option<&[DocumentId]> {
        self.postings.get(&value_key(value)).map(Vec::as_slice)
    }

    /// Append `id` to `value`'s posting — but only if that value is already
    /// materialized, so we never start tracking a value no query has asked for.
    /// A trailing-duplicate guard absorbs the common "same doc re-put" case
    /// cheaply; full dedup is left to the read-side `seen` set.
    pub fn notify_value(&mut self, value: &Value, id: DocumentId) {
        if let Some(list) = self.postings.get_mut(&value_key(value)) {
            if list.last() != Some(&id) {
                list.push(id);
            }
        }
    }

    pub fn mark_used(&mut self) {
        self.last_used = Instant::now();
    }

    pub fn last_used(&self) -> Instant {
        self.last_used
    }
}

/// The whole equality index: a field-keyed collection of [`EqualityIndex`]es
/// under a shared field-count budget.
pub struct EqualityIndexSet {
    fields: HashMap<String, EqualityIndex>,
    max_fields: usize,
}

impl EqualityIndexSet {
    pub fn new(max_fields: usize) -> Self {
        EqualityIndexSet {
            fields: HashMap::new(),
            max_fields,
        }
    }

    /// Gather candidate ids for `(field, values)`, marking the field used.
    /// Returns `None` if the field is untracked or *any* requested value is
    /// unmaterialized — in which case the caller full-scans and then calls
    /// [`Self::materialize`], so a later identical query is fully served.
    pub fn candidates(&mut self, field: &str, values: &[Value]) -> Option<Vec<DocumentId>> {
        let idx = self.fields.get_mut(field)?;
        let mut ids = Vec::new();
        for v in values {
            ids.extend_from_slice(idx.candidates(v)?);
        }
        idx.mark_used();
        Some(ids)
    }

    /// Materialize a posting for each value from a full scan's `results` (the
    /// docs matching the whole `Eq`/`In` filter), splitting by field value.
    /// Values matching nothing get an empty (negative) posting. Evicts the LRU
    /// field if this pushes the set over budget.
    pub fn materialize(&mut self, field: &str, values: &[Value], results: &[IBlob]) {
        let idx = self
            .fields
            .entry(field.to_string())
            .or_insert_with(|| EqualityIndex::new());
        for v in values {
            let ids: Vec<DocumentId> = results
                .iter()
                .filter(|b| b.get_field(field).as_ref() == Some(v))
                .map(|b| *b.id())
                .collect();
            idx.materialize(v, ids);
        }
        idx.mark_used();
        self.enforce_budget();
    }

    /// Fan a write out to every tracked field, appending the doc's id to its
    /// current value's posting (additive, materialized-values-only).
    pub fn notify_put(&mut self, blob: &IBlob) {
        if self.fields.is_empty() {
            return;
        }
        for (field, idx) in self.fields.iter_mut() {
            if let Some(v) = blob.get_field(field) {
                idx.notify_value(&v, *blob.id());
            }
        }
    }

    fn enforce_budget(&mut self) {
        while self.fields.len() > self.max_fields {
            let victim = self
                .fields
                .iter()
                .min_by_key(|(_, e)| e.last_used())
                .map(|(k, _)| k.clone());
            match victim {
                Some(k) => {
                    self.fields.remove(&k);
                }
                None => break,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Number of tracked fields (observability / tests).
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: u8, country: &str) -> IBlob {
        let mut b = IBlob::with_id(
            DocumentId::from_bytes([id; 16]),
            [("country".into(), Value::String(country.into()))].into(),
        );
        b.set_version(DocumentId::new());
        b
    }

    fn id(n: u8) -> DocumentId {
        DocumentId::from_bytes([n; 16])
    }

    fn fi() -> Value {
        Value::String("FI".into())
    }

    #[test]
    fn equality_terms_eq_and_in() {
        // Eq → single value.
        let (f, vs) = equality_terms(&Filter::Eq {
            field: "country".into(),
            value: fi(),
        })
        .unwrap();
        assert_eq!(f, "country");
        assert_eq!(vs, vec![fi()]);

        // Or of same-field Eqs → In.
        let or = Filter::Or(vec![
            Filter::Eq { field: "c".into(), value: Value::U64(1) },
            Filter::Eq { field: "c".into(), value: Value::U64(2) },
        ]);
        let (f, vs) = equality_terms(&or).unwrap();
        assert_eq!(f, "c");
        assert_eq!(vs, vec![Value::U64(1), Value::U64(2)]);

        // Mixed fields → not an equality term.
        let mixed = Filter::Or(vec![
            Filter::Eq { field: "a".into(), value: Value::U64(1) },
            Filter::Eq { field: "b".into(), value: Value::U64(2) },
        ]);
        assert!(equality_terms(&mixed).is_none());

        // Range → not an equality term.
        assert!(equality_terms(&Filter::Gt { field: "c".into(), value: Value::U64(1) }).is_none());
    }

    #[test]
    fn materialize_then_serve_candidates() {
        let mut set = EqualityIndexSet::new(MAX_EQUALITY_FIELDS);
        let results = vec![doc(3, "FI"), doc(9, "FI")];
        set.materialize("country", &[fi()], &results);

        let got = set.candidates("country", &[fi()]).unwrap();
        assert_eq!(got, vec![id(3), id(9)]);
        // Unmaterialized value → None (caller full-scans).
        assert!(set.candidates("country", &[Value::String("US".into())]).is_none());
    }

    #[test]
    fn negative_cache_self_heals_on_put() {
        let mut set = EqualityIndexSet::new(MAX_EQUALITY_FIELDS);
        // Materialize a proven-empty value.
        set.materialize("country", &[Value::String("XX".into())], &[]);
        assert_eq!(set.candidates("country", &[Value::String("XX".into())]).unwrap().len(), 0);

        // A later matching write self-heals the negative posting.
        set.notify_put(&doc(7, "XX"));
        assert_eq!(set.candidates("country", &[Value::String("XX".into())]).unwrap(), vec![id(7)]);
    }

    #[test]
    fn notify_only_tracks_materialized_values() {
        let mut set = EqualityIndexSet::new(MAX_EQUALITY_FIELDS);
        set.materialize("country", &[fi()], &[doc(1, "FI")]);
        // A write for an unmaterialized value must not start a posting for it.
        set.notify_put(&doc(2, "US"));
        assert!(set.candidates("country", &[Value::String("US".into())]).is_none());
        // But a write for the materialized value is appended.
        set.notify_put(&doc(5, "FI"));
        assert_eq!(set.candidates("country", &[fi()]).unwrap(), vec![id(1), id(5)]);
    }

    #[test]
    fn trailing_dup_guard() {
        let mut set = EqualityIndexSet::new(MAX_EQUALITY_FIELDS);
        set.materialize("country", &[fi()], &[]);
        set.notify_put(&doc(1, "FI"));
        set.notify_put(&doc(1, "FI")); // same doc re-put — not appended twice
        assert_eq!(set.candidates("country", &[fi()]).unwrap(), vec![id(1)]);
    }

    #[test]
    fn in_query_unions_value_postings() {
        let mut set = EqualityIndexSet::new(MAX_EQUALITY_FIELDS);
        let results = vec![doc(1, "FI"), doc(2, "US"), doc(3, "FI")];
        set.materialize(
            "country",
            &[Value::String("FI".into()), Value::String("US".into())],
            &results,
        );
        let got = set
            .candidates("country", &[Value::String("FI".into()), Value::String("US".into())])
            .unwrap();
        assert_eq!(got, vec![id(1), id(3), id(2)]);
    }

    #[test]
    fn field_lru_evicts_coldest_field() {
        let mut set = EqualityIndexSet::new(2);
        set.materialize("a", &[Value::U64(1)], &[]);
        set.materialize("b", &[Value::U64(1)], &[]);
        // Touch "a" so "b" is the coldest field.
        assert!(set.candidates("a", &[Value::U64(1)]).is_some());
        set.materialize("c", &[Value::U64(1)], &[]); // over budget (2) → evict "b"
        assert_eq!(set.field_count(), 2);
        assert!(set.candidates("b", &[Value::U64(1)]).is_none(), "coldest field evicted");
        assert!(set.candidates("a", &[Value::U64(1)]).is_some());
        assert!(set.candidates("c", &[Value::U64(1)]).is_some());
    }
}
