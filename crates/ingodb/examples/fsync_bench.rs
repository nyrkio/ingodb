//! fsync benchmark: shows how group commit amortizes fsync cost across
//! concurrent writers under different consistency levels.
//!
//! Run with: cargo run --release --example fsync_bench
//!
//! The interesting result is the comparison between:
//! - Optimistic (no fsync, follower returns early): baseline upper bound
//! - Visible (no fsync, follower waits): cost of the leader/follower wait
//! - Durable (fsync per batch, follower waits): real durability cost
//!
//! With group commit working, Durable throughput should *scale with
//! concurrency* — many writers share one fsync — even as single-thread
//! throughput is fsync-bound.

use ingodb::{
    Consistency, ConsistencyModel, DocumentId, IBlob, LsmConfig, LsmEngine, Value,
};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Target total ops per cell. ops_per_thread is derived so memory stays bounded
// at high concurrency. Clamped to [200, 5000].
const TOTAL_OPS_TARGET: usize = 200_000;
const CONCURRENCIES: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

/// Per-cell wall-clock budget. Each writer thread checks the deadline per op
/// and exits early if reached. Throughput is reported from ops actually
/// completed and the actual elapsed time, so a timed-out cell still gives a
/// valid (lower-bound) rate.
const MAX_CELL_SECS: u64 = 180;

fn ops_per_thread(threads: usize) -> usize {
    (TOTAL_OPS_TARGET / threads).clamp(200, 5_000)
}

/// One row of the benchmark: label + consistency level + wait_usec + busy_mode flag.
struct Row {
    label: &'static str,
    level: Consistency,
    wait_usec: u64,
    /// If true, ignore wait_usec and enable busy-mode (engine starts with
    /// wait=0; flips to 100µs once a batch reaches num_cpus × 8 entries).
    busy_mode: bool,
}

fn main() {
    let strict = Consistency::single_node(ConsistencyModel::STRICT_LINEARIZABLE);
    let rows = vec![
        Row {
            label: "Durable        ",
            level: strict,
            wait_usec: 0,
            busy_mode: false,
        },
        Row {
            label: "Durable+wait100",
            level: strict,
            wait_usec: 100,
            busy_mode: false,
        },
        Row {
            label: "Durable+busy   ",
            level: strict,
            wait_usec: 0,
            busy_mode: true,
        },
    ];

    println!();
    println!("fsync benchmark — group commit under different consistency levels");
    println!("ops/thread varies: TOTAL_OPS_TARGET={TOTAL_OPS_TARGET}, clamped to [200, 5000]");
    println!();
    print!("{:16}", "Level");
    for c in CONCURRENCIES {
        print!("{:>10}", format!("{c}t"));
    }
    println!();
    println!("{}", "─".repeat(16 + 10 * CONCURRENCIES.len()));

    use std::io::Write;
    for row in &rows {
        print!("{}", row.label);
        let _ = std::io::stdout().flush();
        for &threads in CONCURRENCIES {
            let ops_per_sec = run_one(row.level, threads, row.wait_usec, row.busy_mode);
            print!("{:>10}", format_kops(ops_per_sec));
            let _ = std::io::stdout().flush();
        }
        println!();
    }
    println!();
    println!("Notes:");
    println!(" - Optimistic / Visible / Durable: see crate docs.");
    println!(" - Durable+waitX: commit_wait_usec=X, commit_wait_count=0 (always wait).");
    println!("   Trades up to X µs of per-op latency for larger batches and");
    println!("   fewer fsyncs per second — should help most at high concurrency");
    println!("   where many threads can join the same batch.");
}

fn run_one(level: Consistency, threads: usize, wait_usec: u64, busy_mode: bool) -> f64 {
    let dir = tempfile::tempdir().unwrap();
    let config = LsmConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size: 256 * 1024 * 1024, // large — keep flushes out of the timing
        block_size: 4096,
        compaction_threshold: 100,
        scaling_parameter: 0,
        compaction_threads: 1,
        max_ranges_per_field: 500,
        adaptive_w: false,
        adaptive_w_cooldown_secs: 900,
        adaptive_w_max_step: 2,
        adaptive_w_min: -8,
        adaptive_w_max: 8,
        min_consistency: level,
        commit_wait_usec: wait_usec,
        commit_wait_count: 0, // always wait the full wait_usec
        commit_busy_mode: busy_mode,
    };
    let engine = Arc::new(LsmEngine::open(config).unwrap());

    // Warmup: a small batch to ensure file handles, allocators etc. are hot.
    for i in 0..100u64 {
        engine.put(make_blob(deterministic_id(i), i)).unwrap();
    }

    let per_thread = ops_per_thread(threads);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(MAX_CELL_SECS);
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let e = engine.clone();
        handles.push(std::thread::spawn(move || -> usize {
            let base = (t * per_thread + 100_000) as u64;
            let mut completed = 0usize;
            for i in 0..per_thread {
                // Check deadline once per op. Instant::now() is ~50 ns —
                // negligible relative to even a no-fsync put.
                if Instant::now() >= deadline {
                    break;
                }
                let id = deterministic_id(base + i as u64);
                e.put(make_blob(id, base + i as u64)).unwrap();
                completed += 1;
            }
            completed
        }));
    }
    let total_ops: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = start.elapsed();
    total_ops as f64 / elapsed.as_secs_f64()
}

fn format_kops(ops: f64) -> String {
    if ops >= 1_000_000.0 {
        format!("{:.2}M", ops / 1_000_000.0)
    } else if ops >= 1000.0 {
        format!("{:.0}K", ops / 1000.0)
    } else {
        format!("{ops:.0}")
    }
}

fn deterministic_id(i: u64) -> DocumentId {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&i.to_be_bytes());
    let hash = i.wrapping_mul(0x517cc1b727220a95);
    bytes[8..16].copy_from_slice(&hash.to_be_bytes());
    DocumentId::from_bytes(bytes)
}

fn make_blob(id: DocumentId, i: u64) -> IBlob {
    IBlob::with_id(
        id,
        [
            ("type".into(), Value::String("product".into())),
            ("name".into(), Value::String(format!("Product #{i}"))),
            ("price".into(), Value::F64((i % 1000) as f64 + 0.99)),
            ("stock".into(), Value::U64(i % 500)),
        ]
        .into(),
    )
}

// Silence unused-import warning in case PathBuf isn't directly referenced
// after a future refactor.
#[allow(dead_code)]
type _PathBufAlias = PathBuf;
