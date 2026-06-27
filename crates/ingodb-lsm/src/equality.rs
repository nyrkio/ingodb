//! Lazy reactive **equality index** for `Eq` / `In` queries — LSM-native,
//! per-SSTable. See `docs/equality-index.md` for the full design.
//!
//! A sibling to the sorted [`crate::secondary`] and unsorted [`crate::unsorted`]
//! *range* indexes, but for point-equality predicates. A posting is an inverted
//! list `value → [_id]` storing **only `_id` references**, scoped to a single
//! **immutable SSTable**.
//!
//! Because an SSTable is immutable until compaction, a posting derived from it is
//! exact and stable for that data — so postings are **built lazily on read** and
//! need **no write-side maintenance**: writes only touch the memtable (always
//! live-scanned) and later flush to a *new* SSTable (indexed on first read); no
//! write invalidates an existing posting. Compaction drops a posting when its
//! SSTable is rewritten ([`EqualityPostings::drop_sstable`]).
//!
//! Reads resolve each candidate `_id` against the primary via `get_at(id,
//! snapshot)` and re-check the predicate — so cross-level MVCC and any staleness
//! fall out of the verify step; the posting itself is version-agnostic.

use ingodb_blob::{DocumentId, Value};
use ingodb_query::Filter;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// Completeness threshold **R**: a value occupying < R of an SSTable is fully
/// indexed (`Exact`); at/above R, if it *also* has more than `EQUALITY_K`
/// matches, only a K-sized sample is kept (`Overflow`). Configurable later.
pub const EQUALITY_R: f64 = 0.20;

/// Sample/overflow retention **K**: ids kept for an incomplete posting.
pub const EQUALITY_K: usize = 16;

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

// ---------------------------------------------------------------------------
// Posting (per SSTable, per value)
// ---------------------------------------------------------------------------

/// What the engine knows about which `_id`s in *one immutable SSTable* hold a
/// given field value. The three variants are the truth table from
/// `docs/equality-index.md`. An explicit enum (rather than `{ids, complete}` +
/// a "len == K" inference) makes `Overflow` unambiguous and removes two sharp
/// edges (the `K−1` cap on early-stop, and a small-SSTable `Overflow` with < K
/// ids being misread as provisional).
///
/// `Partial` / [`Posting::from_stopped_early`] / [`Posting::satisfies`] are the
/// LIMIT-from-sample fast path — built but not yet exercised by the serve path
/// (Phase C), hence `allow(dead_code)`.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub enum Posting {
    /// Every matching `_id` in the SSTable — absence from the list is
    /// authoritative. The empty vector is the negative cache.
    Exact(Vec<DocumentId>),
    /// The value is ≥ R of the SSTable **and** has more than K matches: a K-sized
    /// sample, rest discarded. A *final* fact about the immutable SSTable.
    Overflow(Vec<DocumentId>),
    /// A scan stopped early (a LIMIT was satisfied before the SSTable was
    /// exhausted): a sample of unknown completeness. *Provisional*.
    Partial(Vec<DocumentId>),
}

#[allow(dead_code)]
impl Posting {
    /// Build from an **exhaustive** scan of one SSTable for one value. `matches`
    /// = every matching id found; `sstable_rows` = |S|.
    ///
    /// A value over R but with ≤ K matches is still `Exact`: we kept everything,
    /// so completeness is free and strictly better than a pessimistic overflow.
    pub fn from_exhaustive(mut matches: Vec<DocumentId>, sstable_rows: usize) -> Posting {
        let m = matches.len();
        let exceeds_r = sstable_rows > 0 && (m as f64) >= EQUALITY_R * sstable_rows as f64;
        if exceeds_r && m > EQUALITY_K {
            matches.truncate(EQUALITY_K);
            Posting::Overflow(matches)
        } else {
            Posting::Exact(matches) // m == 0 is the negative cache
        }
    }

    /// Build from a scan that **stopped early** (query LIMIT hit before the
    /// SSTable was exhausted). Keeps at most K ids.
    pub fn from_stopped_early(mut found: Vec<DocumentId>) -> Posting {
        found.truncate(EQUALITY_K);
        Posting::Partial(found)
    }

    /// Stored candidate ids: a superset to verify (for `Exact`, the exact set).
    pub fn ids(&self) -> &[DocumentId] {
        match self {
            Posting::Exact(v) | Posting::Overflow(v) | Posting::Partial(v) => v,
        }
    }

    /// Is absence from [`Self::ids`] authoritative ("not in this SSTable")? Only
    /// `Exact` — and `Exact([])` is the negative cache.
    pub fn is_complete(&self) -> bool {
        matches!(self, Posting::Exact(_))
    }

    /// A *final* incomplete posting: the value is ≥ R, re-scanning won't complete it.
    pub fn is_overflow(&self) -> bool {
        matches!(self, Posting::Overflow(_))
    }

    /// A *provisional* incomplete posting a future exhaustive scan can upgrade.
    pub fn is_refinable(&self) -> bool {
        matches!(self, Posting::Partial(_))
    }

    /// Can this single SSTable's contribution be answered from the posting alone,
    /// given how many candidates survived verification and the query's optional
    /// LIMIT? `Exact` always can; an incomplete posting only when a LIMIT is
    /// already met by the survivors. (Phase C serve rule.)
    pub fn satisfies(&self, survivors: usize, limit: Option<usize>) -> bool {
        match self {
            Posting::Exact(_) => true,
            Posting::Overflow(_) | Posting::Partial(_) => {
                matches!(limit, Some(n) if survivors >= n)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// EqualityPostings (the whole index: per-SSTable postings)
// ---------------------------------------------------------------------------

/// Per-SSTable equality postings: `sstable path → field → value → Posting`.
/// Each posting is exact and stable for its (immutable) SSTable until that
/// SSTable is compacted away, when [`Self::drop_sstable`] removes it.
#[derive(Default)]
pub struct EqualityPostings {
    by_sstable: HashMap<PathBuf, HashMap<String, HashMap<Vec<u8>, Posting>>>,
}

impl EqualityPostings {
    pub fn new() -> Self {
        Self::default()
    }

    /// The cached posting for `(sstable, field, value)`, if any.
    pub fn get(&self, sstable: &Path, field: &str, value: &Value) -> Option<&Posting> {
        self.by_sstable.get(sstable)?.get(field)?.get(&value_key(value))
    }

    /// Record the posting for `(sstable, field, value)`.
    pub fn insert(&mut self, sstable: &Path, field: &str, value: &Value, posting: Posting) {
        self.by_sstable
            .entry(sstable.to_path_buf())
            .or_default()
            .entry(field.to_string())
            .or_default()
            .insert(value_key(value), posting);
    }

    /// Drop every posting for an SSTable being compacted away.
    pub fn drop_sstable(&mut self, sstable: &Path) {
        self.by_sstable.remove(sstable);
    }

    /// Distinct fields with at least one posting (observability / tests).
    pub fn field_count(&self) -> usize {
        let mut fields = HashSet::new();
        for per_field in self.by_sstable.values() {
            for f in per_field.keys() {
                fields.insert(f.as_str());
            }
        }
        fields.len()
    }

    /// Total postings across all SSTables (observability / tests).
    #[allow(dead_code)]
    pub fn posting_count(&self) -> usize {
        self.by_sstable
            .values()
            .flat_map(|per_field| per_field.values())
            .map(|per_value| per_value.len())
            .sum()
    }
}

#[cfg(test)]
mod posting_tests {
    use super::*;

    fn ids(n: usize) -> Vec<DocumentId> {
        (0..n).map(|i| DocumentId::from_bytes([i as u8; 16])).collect()
    }

    #[test]
    fn exhaustive_under_r_is_exact() {
        let p = Posting::from_exhaustive(ids(5), 100);
        assert_eq!(p, Posting::Exact(ids(5)));
        assert!(p.is_complete() && !p.is_overflow() && !p.is_refinable());
    }

    #[test]
    fn empty_is_negative_cache() {
        let p = Posting::from_exhaustive(vec![], 100);
        assert_eq!(p, Posting::Exact(vec![]));
        assert!(p.is_complete());
    }

    #[test]
    fn exhaustive_over_r_and_over_k_is_overflow() {
        let p = Posting::from_exhaustive(ids(30), 100);
        assert!(p.is_overflow() && !p.is_complete());
        assert_eq!(p.ids().len(), EQUALITY_K, "kept exactly K");
    }

    #[test]
    fn over_r_but_within_k_stays_exact() {
        // 30% of a 10-row SSTable ≥ R, but only 3 ≤ K → Exact (we have them all).
        let p = Posting::from_exhaustive(ids(3), 10);
        assert_eq!(p, Posting::Exact(ids(3)));
    }

    #[test]
    fn exactly_k_under_r_is_exact_not_overflow() {
        let p = Posting::from_exhaustive(ids(EQUALITY_K), 1000);
        assert_eq!(p, Posting::Exact(ids(EQUALITY_K)));
        assert!(p.is_complete() && !p.is_overflow());
    }

    #[test]
    fn stopped_early_is_partial_capped_at_k() {
        let p = Posting::from_stopped_early(ids(50));
        assert!(p.is_refinable());
        assert_eq!(p.ids().len(), EQUALITY_K);
    }

    #[test]
    fn satisfies_rules() {
        let exact = Posting::Exact(ids(3));
        let overflow = Posting::Overflow(ids(EQUALITY_K));
        assert!(exact.satisfies(3, None));
        assert!(!overflow.satisfies(16, None), "exhaustive over overflow must scan");
        assert!(overflow.satisfies(10, Some(10)));
        assert!(!overflow.satisfies(3, Some(10)));
    }
}

#[cfg(test)]
mod postings_tests {
    use super::*;
    use std::path::PathBuf;

    fn id(n: u8) -> DocumentId {
        DocumentId::from_bytes([n; 16])
    }
    fn sv(s: &str) -> Value {
        Value::String(s.into())
    }

    #[test]
    fn equality_terms_eq_and_in() {
        let (f, vs) = equality_terms(&Filter::Eq { field: "c".into(), value: sv("FI") }).unwrap();
        assert_eq!((f.as_str(), vs), ("c", vec![sv("FI")]));

        let or = Filter::Or(vec![
            Filter::Eq { field: "c".into(), value: Value::U64(1) },
            Filter::Eq { field: "c".into(), value: Value::U64(2) },
        ]);
        let (f, vs) = equality_terms(&or).unwrap();
        assert_eq!((f.as_str(), vs), ("c", vec![Value::U64(1), Value::U64(2)]));

        // mixed fields / non-Eq → None
        assert!(equality_terms(&Filter::Or(vec![
            Filter::Eq { field: "a".into(), value: Value::U64(1) },
            Filter::Eq { field: "b".into(), value: Value::U64(2) },
        ]))
        .is_none());
        assert!(equality_terms(&Filter::Gt { field: "c".into(), value: Value::U64(1) }).is_none());
    }

    #[test]
    fn exact_ids_round_trip_and_overflow_is_not_exact() {
        let mut p = EqualityPostings::new();
        let s = PathBuf::from("000000000001.sst");

        p.insert(&s, "country", &sv("FI"), Posting::Exact(vec![id(3), id(9)]));
        p.insert(&s, "country", &sv("XX"), Posting::Overflow(vec![id(1)]));

        assert_eq!(p.get(&s, "country", &sv("FI")), Some(&Posting::Exact(vec![id(3), id(9)])));
        // Overflow is present but not a complete answer.
        assert!(p.get(&s, "country", &sv("XX")).unwrap().is_overflow());
        // Unknown value → None.
        assert_eq!(p.get(&s, "country", &sv("US")), None);
        assert_eq!(p.field_count(), 1);
        assert_eq!(p.posting_count(), 2);
    }

    #[test]
    fn negative_cache_is_exact_empty() {
        let mut p = EqualityPostings::new();
        let s = PathBuf::from("a.sst");
        p.insert(&s, "country", &sv("XX"), Posting::Exact(vec![]));
        // Present and Exact, but empty: an authoritative "none here".
        assert_eq!(p.get(&s, "country", &sv("XX")), Some(&Posting::Exact(vec![])));
    }

    #[test]
    fn drop_sstable_removes_its_postings() {
        let mut p = EqualityPostings::new();
        let (a, b) = (PathBuf::from("a.sst"), PathBuf::from("b.sst"));
        p.insert(&a, "f", &Value::U64(1), Posting::Exact(vec![id(1)]));
        p.insert(&b, "f", &Value::U64(1), Posting::Exact(vec![id(2)]));
        assert_eq!(p.posting_count(), 2);

        p.drop_sstable(&a);
        assert!(p.get(&a, "f", &Value::U64(1)).is_none());
        assert_eq!(p.get(&b, "f", &Value::U64(1)), Some(&Posting::Exact(vec![id(2)])));
        assert_eq!(p.posting_count(), 1);
    }
}
