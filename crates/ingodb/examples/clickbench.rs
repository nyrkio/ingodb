//! ClickBench-style reactive-index benchmark for IngoDB.
//!
//! Run with:  cargo run --release --example clickbench [rows]
//!
//! ClickBench (https://github.com/ClickHouse/ClickBench) is the de-facto OLAP
//! filter/scan benchmark: one wide `hits` table, 43 queries. IngoDB has no
//! aggregation / GROUP BY / LIKE / DISTINCT yet, so we run the *expressible*
//! subset (point / equality / range / top-N / filtered-count), hand-translated
//! to the `Query` AST. Unsupported queries are listed at the end as findings —
//! covering more ClickBench is what should drive query-language expansion.
//!
//! Goal: watch the reactive index system create indexes and see whether they
//! help (or not) the workload — via the cold-vs-warm latency curve per query
//! pattern, plus index-lifecycle counts across phases.
//!
//! DATA: if `clickbench_hits.tsv` exists (real ClickBench `hits` partition,
//! prepared by `prep_clickbench.py`), it is loaded; otherwise a `hits`-shaped
//! synthetic dataset is generated. Query parameters are derived from the loaded
//! data, so the same workload is meaningful for either source.

use ingodb::{Consistency, DocumentId, Filter, IBlob, LsmConfig, LsmEngine, Query, SortDirection, SortField, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const REAL_TSV: &str = "clickbench_hits.tsv";
const WIDTHS: &[u64] = &[0, 1280, 1366, 1440, 1536, 1600, 1920, 2560, 3840];
const PHRASES: &[&str] = &[
    "weather", "news today", "buy laptop", "rust lang", "clickhouse",
    "best pizza", "flights", "stock price", "movie times", "recipes",
];

fn main() {
    // Default 200k for quick iteration; pass a larger arg (up to the 1M real
    // rows) for more realistic latencies.
    let rows: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(200_000);

    println!("=== IngoDB ClickBench-style Reactive Index Benchmark ===\n");
    let dataset = load_dataset(rows);
    let params = Params::derive(&dataset);
    println!("Loaded {} rows. Query params: {params:?}\n", dataset.len());

    let data_dir = PathBuf::from("data");
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir).unwrap();
    }
    std::fs::create_dir_all(&data_dir).unwrap();

    let config = LsmConfig {
        data_dir,
        memtable_size: 16 * 1024 * 1024,
        block_size: 4096,
        compaction_threshold: 4,
        scaling_parameter: 0,
        // Lowered from the 500 default so the drift phase can demonstrate LRU
        // eviction with a modest number of distinct queries.
        max_ranges_per_field: 50,
        compaction_threads: 4,
        adaptive_w: true,
        adaptive_w_cooldown_secs: 1,
        adaptive_w_max_step: 16,
        adaptive_w_min: -8,
        adaptive_w_max: 8,
        min_consistency: Consistency::default(),
        commit_wait_usec: 0,
        commit_wait_count: 0,
        commit_busy_mode: false,
    };
    println!("max_ranges_per_field={}\n", config.max_ranges_per_field);

    let engine = Arc::new(LsmEngine::open(config).unwrap());
    engine.start_background_compaction();

    let next_id = phase_ingest(&engine, dataset);
    engine.wait_for_compaction().unwrap();

    phase_cold_vs_warm(&engine, &params);
    phase_containment(&engine, &params);
    phase_drift_and_lru(&engine, &params);
    phase_writes(&engine, next_id);

    summary(&engine);
    findings();
}

// --------------------------------------------------------------------------
// Dataset loading: real TSV if present, else synthetic
// --------------------------------------------------------------------------

fn load_dataset(rows: u64) -> Vec<IBlob> {
    if let Ok(f) = std::fs::File::open(REAL_TSV) {
        println!("Loading real ClickBench data from {REAL_TSV} (cap {rows} rows)...");
        return load_tsv(f, rows);
    }
    println!("No {REAL_TSV} found — generating {rows} synthetic hits-shaped rows...");
    (0..rows).map(make_hit).collect()
}

/// Parse the compact TSV written by prep_clickbench.py. Fixed column order:
/// CounterID, AdvEngineID, RegionID, UserID, EventDate, EventTime,
/// ResolutionWidth, SearchPhrase, Title.
fn load_tsv(f: std::fs::File, cap: u64) -> Vec<IBlob> {
    let mut out = Vec::new();
    for (i, line) in BufReader::new(f).lines().enumerate() {
        if i as u64 >= cap {
            break;
        }
        let line = match line { Ok(l) => l, Err(_) => break };
        let c: Vec<&str> = line.split('\t').collect();
        if c.len() < 9 {
            continue;
        }
        let u = |s: &str| s.parse::<u64>().unwrap_or(0);
        out.push(IBlob::with_id(det_id(i as u64), [
            ("CounterID".into(), Value::U64(u(c[0]))),
            ("AdvEngineID".into(), Value::U64(u(c[1]))),
            ("RegionID".into(), Value::U64(u(c[2]))),
            ("UserID".into(), Value::U64(u(c[3]))),
            ("EventDate".into(), Value::U64(u(c[4]))),
            ("EventTime".into(), Value::U64(u(c[5]))),
            ("ResolutionWidth".into(), Value::U64(u(c[6]))),
            ("SearchPhrase".into(), Value::String(c[7].to_string())),
            ("Title".into(), Value::String(c[8].to_string())),
        ].into()));
    }
    out
}

// Synthetic generator (fallback) ------------------------------------------

fn h(i: u64, salt: u64) -> u64 {
    let mut x = i.wrapping_add(salt).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    x
}

fn det_id(i: u64) -> DocumentId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&i.to_be_bytes());
    bytes[8..16].copy_from_slice(&h(i, 0).to_be_bytes());
    DocumentId::from_bytes(bytes)
}

fn make_hit(i: u64) -> IBlob {
    let adv = if h(i, 2) % 10 < 9 { 0 } else { 1 + h(i, 2) % 18 };
    let search = if h(i, 9) % 10 < 8 { String::new() } else { PHRASES[(h(i, 9) % PHRASES.len() as u64) as usize].to_string() };
    let date = 18_000 + h(i, 5) % 90;
    IBlob::with_id(det_id(i), [
        ("CounterID".into(), Value::U64(h(i, 1) % 1000)),
        ("AdvEngineID".into(), Value::U64(adv)),
        ("RegionID".into(), Value::U64(h(i, 3) % 200)),
        ("UserID".into(), Value::U64(h(i, 4))),
        ("EventDate".into(), Value::U64(date)),
        ("EventTime".into(), Value::U64(date * 86_400 + h(i, 6) % 86_400)),
        ("ResolutionWidth".into(), Value::U64(WIDTHS[(h(i, 7) % WIDTHS.len() as u64) as usize])),
        ("SearchPhrase".into(), Value::String(search)),
        ("Title".into(), Value::String(format!("page {}", h(i, 10) % 100_000))),
    ].into())
}

// --------------------------------------------------------------------------
// Query parameters derived from the actual data
// --------------------------------------------------------------------------

#[derive(Debug)]
struct Params {
    counter: u64,         // dominant CounterID (mode) — typically non-selective
    region: u64,          // a common RegionID (mode)
    user: u64,            // a real UserID (point lookup)
    et_lo: u64,           // EventTime ~p45  (a ~10% window — continuous, high card)
    et_hi: u64,           // EventTime ~p55
    drift_users: Vec<u64>, // distinct UserIDs (high card) for the LRU/drift phase
}

fn u64f(b: &IBlob, k: &str) -> u64 {
    match b.get(k) { Some(Value::U64(v)) => *v, _ => 0 }
}

impl Params {
    fn derive(data: &[IBlob]) -> Params {
        let mut counter_freq: HashMap<u64, u32> = HashMap::new();
        let mut region_freq: HashMap<u64, u32> = HashMap::new();
        let mut times: Vec<u64> = Vec::with_capacity(data.len());
        let mut users: Vec<u64> = Vec::new();
        let mut seen_users = std::collections::HashSet::new();
        for b in data {
            *counter_freq.entry(u64f(b, "CounterID")).or_default() += 1;
            *region_freq.entry(u64f(b, "RegionID")).or_default() += 1;
            times.push(u64f(b, "EventTime"));
            if users.len() < 70 {
                let u = u64f(b, "UserID");
                if seen_users.insert(u) {
                    users.push(u);
                }
            }
        }
        let mode = |m: &HashMap<u64, u32>| m.iter().max_by_key(|(_, c)| **c).map(|(k, _)| *k).unwrap_or(0);
        times.sort_unstable();
        let pct = |p: usize| times.get(times.len() * p / 100).copied().unwrap_or(0);
        let et_lo = pct(45);
        let mut et_hi = pct(55);
        if et_lo == et_hi {
            et_hi = et_lo + 1; // guard against a degenerate (constant) column
        }
        Params {
            counter: mode(&counter_freq),
            region: mode(&region_freq),
            user: u64f(&data[data.len() / 2], "UserID"),
            et_lo,
            et_hi,
            drift_users: users,
        }
    }
}

// --------------------------------------------------------------------------
// Phases
// --------------------------------------------------------------------------

fn phase_ingest(engine: &Arc<LsmEngine>, data: Vec<IBlob>) -> u64 {
    let n = data.len() as u64;
    println!("--- Ingest {n} rows ---");
    let start = Instant::now();
    for chunk in data.chunks(1000) {
        engine.put_batch(chunk.to_vec()).unwrap();
    }
    let dt = start.elapsed();
    println!("  {n} rows in {dt:?} ({:.0} rows/sec), SSTables={}\n",
        n as f64 / dt.as_secs_f64(), engine.sstable_count());
    n
}

fn run(engine: &LsmEngine, q: &Query) -> (usize, Duration) {
    let t = Instant::now();
    let r = engine.execute(q).unwrap();
    (r.len(), t.elapsed())
}

fn idx_state(engine: &LsmEngine) -> String {
    format!("sorted={} blocks={} hits={}",
        engine.secondary_index_count(), engine.unsorted_block_count(), engine.unsorted_hits())
}

fn measure(engine: &LsmEngine, name: &str, build: impl Fn() -> Query, warm_reps: usize) {
    let (n, cold) = run(engine, &build());
    let after_cold = idx_state(engine);
    let mut ws: Vec<Duration> = (0..warm_reps).map(|_| run(engine, &build()).1).collect();
    ws.sort();
    let warm = ws[ws.len() / 2];
    let speedup = cold.as_secs_f64() / warm.as_secs_f64().max(1e-9);
    println!("  {name}");
    println!("    rows={n:<7} cold={cold:>10.2?}  warm(med)={warm:>10.2?}  speedup={speedup:>5.1}x");
    println!("    after 1st run: {after_cold}   |  now: {}", idx_state(engine));
}

fn phase_cold_vs_warm(engine: &Arc<LsmEngine>, p: &Params) {
    println!("--- Cold vs warm (reactive index forms on first run) ---");

    const WARM: usize = 5;

    measure(engine, "AdvEngineID > 0  (selective filter)",
        || Query::Scan { filter: Some(Filter::Gt { field: "AdvEngineID".into(), value: Value::U64(0) }), sort: None, project: None, limit: None }, WARM);

    let c = p.counter;
    measure(engine, &format!("CounterID = {c}  (dominant value — non-selective, NOT indexed)"),
        move || Query::Scan { filter: Some(Filter::Eq { field: "CounterID".into(), value: Value::U64(c) }), sort: None, project: None, limit: None }, WARM);

    let r = p.region;
    measure(engine, &format!("RegionID = {r} ORDER BY EventTime DESC LIMIT 10"),
        move || Query::Scan {
            filter: Some(Filter::Eq { field: "RegionID".into(), value: Value::U64(r) }),
            sort: Some(vec![SortField { field: "EventTime".into(), direction: SortDirection::Descending }]),
            project: None, limit: Some(10),
        }, WARM);

    let (lo, hi) = (p.et_lo, p.et_hi);
    measure(engine, &format!("EventTime in [{lo},{hi})  (range, ~10%)"),
        move || Query::Scan { filter: Some(Filter::Range { field: "EventTime".into(), low: Value::U64(lo), high: Value::U64(hi) }), sort: None, project: None, limit: None }, WARM);

    let u = p.user;
    measure(engine, &format!("UserID = {u}  (point lookup by field)"),
        move || Query::Scan { filter: Some(Filter::Eq { field: "UserID".into(), value: Value::U64(u) }), sort: None, project: None, limit: None }, WARM);

    println!();
}

fn phase_containment(engine: &Arc<LsmEngine>, p: &Params) {
    println!("--- Containment reuse (sub-range served by an existing block) ---");
    // Inner ~half of the EventTime window materialized above.
    let q = (p.et_hi - p.et_lo) / 4;
    let lo = p.et_lo + q;
    let hi = p.et_hi - q;
    let hits_before = engine.unsorted_hits();
    let (n, d) = run(engine, &Query::Scan {
        filter: Some(Filter::Range { field: "EventTime".into(), low: Value::U64(lo), high: Value::U64(hi) }),
        sort: None, project: None, limit: None,
    });
    let served = engine.unsorted_hits() > hits_before;
    println!("  EventTime in [{lo},{hi}) ⊆ [{},{}): rows={n} in {d:?}  served_from_block={served}\n", p.et_lo, p.et_hi);
}

fn phase_drift_and_lru(engine: &Arc<LsmEngine>, p: &Params) {
    println!("--- Drift + LRU (many distinct ranges; cap=50) ---");
    let n = p.drift_users.len();
    let start = Instant::now();
    for &u in &p.drift_users {
        let _ = run(engine, &Query::Scan {
            filter: Some(Filter::Eq { field: "UserID".into(), value: Value::U64(u) }),
            sort: None, project: None, limit: None,
        });
    }
    println!("  queried {n} distinct UserID values in {:?}", start.elapsed());
    println!("  {}  (UserID ranges capped at ~50 despite {n} distinct queries)\n", idx_state(engine));
}

fn phase_writes(engine: &Arc<LsmEngine>, next_id: u64) {
    println!("--- Writes interleaved with a warm read pattern (index maintenance) ---");
    let q = || Query::Scan { filter: Some(Filter::Gt { field: "AdvEngineID".into(), value: Value::U64(0) }), sort: None, project: None, limit: None };
    let before = median((0..10).map(|_| run(engine, &q()).1).collect());

    let start = Instant::now();
    for k in 0..10_000u64 {
        engine.put(make_hit(next_id + k)).unwrap();
        if k % 2000 == 0 {
            let _ = run(engine, &q());
        }
    }
    let write_dt = start.elapsed();

    let after = median((0..10).map(|_| run(engine, &q()).1).collect());
    println!("  inserted 10000 rows in {write_dt:?} ({:.0} rows/sec)", 10_000.0 / write_dt.as_secs_f64());
    println!("  warm 'AdvEngineID>0' latency: before writes={before:?}  after writes={after:?}\n");
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

// --------------------------------------------------------------------------
// Summary + findings
// --------------------------------------------------------------------------

fn summary(engine: &LsmEngine) {
    println!("=== Summary ===");
    println!("SSTables: {}   {}", engine.sstable_count(), idx_state(engine));
    let stats = engine.query_stats();
    println!("\nTop query patterns (query_type + filter fields):");
    for (p, ps) in stats.top_by_count(8) {
        let fields = if p.filter_fields.is_empty() { "(none)".into() } else { p.filter_fields.join(",") };
        println!("  {:>5} {:24} count={:<6} avg={:>9.2?} selectivity={:.4}",
            p.query_type, fields, ps.count, ps.avg_latency(), ps.selectivity());
    }
}

fn findings() {
    println!("\n=== ClickBench coverage / query-language gaps (drives next work) ===");
    let unsupported = [
        ("aggregation (COUNT/SUM/AVG/MIN/MAX)", "most of the 43 queries — needs aggregate operators"),
        ("GROUP BY", "the bulk of ClickBench — needs grouping"),
        ("DISTINCT / COUNT(DISTINCT ...)", "Q5 etc. — needs distinct"),
        ("LIKE '%...%' substring match", "Q20-23,28 — Filter has no substring/text predicate"),
        ("scalar functions (MINUTE(), regex, ...)", "various — no scalar functions"),
    ];
    for (feature, note) in unsupported {
        println!("  - {feature:42} {note}");
    }
    println!("\n  Expressible today: point lookups, equality/range filters, top-N (sort+limit),");
    println!("  filtered counts. These exercise the reactive index path. Joins/traverse would");
    println!("  be surfaced by a relational benchmark (TPC-C/H), not single-table ClickBench.");
}
