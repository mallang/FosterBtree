/// Exp 7: HTAP Fresh-Query Latency (total cost including setup)
///
/// Scenario: base MVCC table exists. A query is issued right after N% updates arrive.
/// How long until the reader receives fresh results?
///
/// Cost breakdown per approach:
///   SNAP:  [setup O(|R|)] → [rebuild O(|R|)] → [probe O(|L|)]
///            rebuild is full re-scan of base table, independent of N%
///   IVMH:  [setup O(|R|)] → [apply O(|Δ|) in-place] → [probe O(|L|)]
///            no rebuild; updates go directly to current_table; probe reads it
///   MVHT:  [setup O(|R|+overhead)] → [probe || apply, concurrent via Barrier]
///            reader fixes query_ts at build time; writer applies N% updates;
///            both start simultaneously; reader never waits for writer.
///
/// X-axis: update % of PART    Y-axis: latency (ms)
/// Columns: setup_ms / rebuild_ms / update_ms / query_ms / total_ms
use clap::{Parser, ValueEnum};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::MvccIndex;
use fbtree::naive_hash_index::IvmHashTable;
use fbtree::naive_hash_index::NaiveMvHashTable;
use fbtree::prelude::AccessMethodError;
use fbtree::prelude::*;
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

// ---------------------------------------------------------------------------
// Shared MVHT type alias
// ---------------------------------------------------------------------------

type ArcMvccIndex = Arc<
    dyn MvccIndex<
        InMemPool,
        Key = Vec<u8>,
        PKey = Vec<u8>,
        Value = Vec<u8>,
        Error = AccessMethodError,
    >,
>;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,
    Heap,
    Par,
    Snap,
    Ivmh,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepairMode {
    Nr,
    Rr,
    Wr,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Exp 7: HTAP fresh-query latency (setup + rebuild/apply + probe)")]
struct Cli {
    #[arg(long)]
    part_file: String,

    #[arg(long)]
    lineitem_file: String,

    /// Update percentage of PART rows, e.g. 0.0001 = 0.01%
    #[arg(long, default_value_t = 0.0001)]
    update_pct: f64,

    #[arg(long, value_enum, default_value = "heap")]
    table_type: TableType,

    #[arg(long, value_enum, default_value = "wr")]
    repair_mode: RepairMode,

    #[arg(long, default_value_t = 2048)]
    bucket_num: usize,

    #[arg(long, default_value_t = 1)]
    warmup: usize,

    #[arg(long, default_value_t = 5)]
    repeat: usize,

    #[arg(long, default_value_t = 1)]
    trim: usize,

    #[arg(long)]
    output_csv: Option<String>,
}

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PartEntry {
    partkey: Vec<u8>,
    ptype: Vec<u8>,
}

#[derive(Clone)]
struct ProbeRow {
    partkey: Vec<u8>,
}

static UPDATED_PTYPE: &[u8] = b"UPDATED_ECONOMY ANODIZED STEEL";

fn load_part(path: &str) -> Result<Vec<PartEntry>, Box<dyn Error>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let cols: Vec<&str> = line.split('|').collect();
        if cols.len() < 5 { continue; }
        out.push(PartEntry {
            partkey: cols[0].trim().as_bytes().to_vec(),
            ptype:   cols[4].trim().as_bytes().to_vec(),
        });
    }
    Ok(out)
}

fn load_lineitem(path: &str) -> Result<Vec<ProbeRow>, Box<dyn Error>> {
    let reader = BufReader::new(std::fs::File::open(path)?);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let cols: Vec<&str> = line.split('|').collect();
        if cols.is_empty() { continue; }
        out.push(ProbeRow { partkey: cols[0].trim().as_bytes().to_vec() });
    }
    Ok(out)
}

fn n_updates_for(pct: f64, n_parts: usize) -> usize {
    ((pct * n_parts as f64).round() as usize).max(1)
}

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

struct RunResult {
    setup_ms:   f64,  // initial hash table build
    rebuild_ms: f64,  // SNAP: mark_ts(1) O(|R|);  IVMH/MVHT: 0
    update_ms:  f64,  // SNAP: log-append (tiny);   IVMH: in-place O(|Δ|); MVHT: writer elapsed
    query_ms:   f64,  // probe LINEITEM (reader elapsed for MVHT)
    total_ms:   f64,  // SNAP: setup+rebuild+query; IVMH: setup+update+query; MVHT: setup+max(update,query)
    n_updates:  u64,
    n_probes:   u64,
}

fn trim_mean(mut v: Vec<f64>, trim: usize) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = v.len();
    if 2 * trim >= n { return v.iter().sum::<f64>() / n as f64; }
    let s = &v[trim..n - trim];
    s.iter().sum::<f64>() / s.len() as f64
}

fn get_pool() -> Arc<InMemPool> {
    Arc::new(InMemPool::new())
}

// ---------------------------------------------------------------------------
// SNAP: setup = insert all parts + mark_ts(0)
//        rebuild = mark_ts(1) after N% updates are applied to the current table → O(|R|) always
//        query  = probe at ts=1
// ---------------------------------------------------------------------------

fn run_snap(parts: &[PartEntry], probe_rows: &[ProbeRow], update_pct: f64, bucket_num: usize) -> RunResult {
    let n_upd = n_updates_for(update_pct, parts.len());
    let pool = get_pool();
    let c_key = ContainerKey::new(0, 0);
    let snap = NaiveMvHashTable::new_with_bucket_num(c_key, pool, bucket_num);

    // SETUP: populate the page-based current table + build initial snapshot
    let setup_start = Instant::now();
    for p in parts { snap.add_insert_rec_new(&p.partkey, &p.partkey, &p.ptype); }
    snap.mark_ts(0);
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Apply N% updates to the current table (O(Δ))
    let log_start = Instant::now();
    for p in &parts[..n_upd] {
        snap.add_update_rec_new(&p.partkey, &p.partkey, UPDATED_PTYPE);
    }
    let update_ms = log_start.elapsed().as_secs_f64() * 1000.0;

    // REBUILD: must rebuild full table O(|R|) to serve fresh query
    let rebuild_start = Instant::now();
    snap.mark_ts(1);
    let rebuild_ms = rebuild_start.elapsed().as_secs_f64() * 1000.0;

    // QUERY: probe at ts=1 (after rebuild)
    let query_start = Instant::now();
    let mut probes = 0u64;
    for row in probe_rows {
        let _ = snap.scan_key_vec(&row.partkey, 1);
        probes += 1;
    }
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    RunResult {
        setup_ms, rebuild_ms, update_ms, query_ms,
        total_ms: setup_ms + rebuild_ms + query_ms,
        n_updates: n_upd as u64, n_probes: probes,
    }
}

// ---------------------------------------------------------------------------
// IVMH: setup = insert all parts directly into current_table (in-place, O(|R|))
//        update = add_update_rec in-place → O(|Δ|), no rebuild
//        query  = scan_key_vec with recent ts → reads current_table directly
//                 (is_recent = true since no mark_ts called → no snapshot needed)
//        rebuild_ms = 0
// ---------------------------------------------------------------------------

fn run_ivmh(parts: &[PartEntry], probe_rows: &[ProbeRow], update_pct: f64, bucket_num: usize) -> RunResult {
    let n_upd = n_updates_for(update_pct, parts.len());
    let pool = get_pool();
    let c_key = ContainerKey::new(0, 0);
    let ivmh = IvmHashTable::new_with_bucket_num(c_key, pool, bucket_num);

    // SETUP: insert all parts into current_table (O(|R|) in-place insertions)
    let setup_start = Instant::now();
    for p in parts { ivmh.add_insert_rec(&p.partkey, &p.partkey, &p.ptype); }
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // UPDATE: apply N% updates in-place on current_table (O(|Δ|))
    // Reader must wait for this to complete (RefCell, not Sync).
    let update_start = Instant::now();
    for p in &parts[..n_upd] {
        ivmh.add_update_rec(&p.partkey, &p.partkey, UPDATED_PTYPE);
    }
    let update_ms = update_start.elapsed().as_secs_f64() * 1000.0;

    // QUERY: probe current_table directly — no mark_ts needed.
    // latest_mark_ts = None → is_recent(ts) = true for any ts → reads current_table.
    let query_start = Instant::now();
    let fresh_ts: Timestamp = 1; // any value; is_recent = true since no mark_ts called
    let mut probes = 0u64;
    for row in probe_rows {
        let _ = ivmh.scan_key_vec(&row.partkey, fresh_ts);
        probes += 1;
    }
    let query_ms = query_start.elapsed().as_secs_f64() * 1000.0;

    RunResult {
        setup_ms, rebuild_ms: 0.0, update_ms, query_ms,
        total_ms: setup_ms + update_ms + query_ms,
        n_updates: n_upd as u64, n_probes: probes,
    }
}

// ---------------------------------------------------------------------------
// MVHT: setup = build versioned hash table O(|R| + overhead)
//        concurrent: reader + writer start simultaneously via Barrier
//          reader  → probe at query_ts (fixed at build time)
//          writer  → apply N% updates with incrementing ts
//        rebuild_ms = 0 (reader never blocked by writer)
//        total_ms   = setup + max(writer_elapsed, reader_elapsed)
// ---------------------------------------------------------------------------

fn build_mvht(parts: &[PartEntry], table_type: TableType, bucket_num: usize) -> ArcMvccIndex {
    let pool = get_pool();
    let c_key = ContainerKey::new(0, 0);
    let t: ArcMvccIndex = match table_type {
        TableType::Heap  => Arc::new(HeapHashTable::create_with_bucket_num(c_key, pool, bucket_num).unwrap()),
        TableType::Chain => Arc::new(ChainedHashTable::create_with_bucket_num(c_key, pool, bucket_num).unwrap()),
        TableType::Par   => Arc::new(TsPartitionedTable::create_with_bucket_num(c_key, pool, bucket_num).unwrap()),
        _ => unreachable!(),
    };
    for (i, p) in parts.iter().enumerate() {
        t.insert_ref(&p.partkey, &p.partkey, i as Timestamp, 0, &p.ptype).unwrap();
    }
    t
}

fn run_mvht_concurrent(
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    update_pct: f64,
    table_type: TableType,
    repair_mode: RepairMode,
    bucket_num: usize,
) -> RunResult {
    let n_upd = n_updates_for(update_pct, parts.len());

    // SETUP: build versioned hash table
    let setup_start = Instant::now();
    let base_ts: Timestamp = parts.len() as u64;
    let table = build_mvht(parts, table_type, bucket_num);
    let setup_ms = setup_start.elapsed().as_secs_f64() * 1000.0;

    // Reader sees data as of base_ts (query issued at build time)
    let query_ts: Timestamp = base_ts;
    let ts_counter = Arc::new(AtomicU64::new(base_ts + 1));
    let barrier = Arc::new(Barrier::new(3)); // reader + writer + main

    // -- Reader thread: probe at query_ts --
    let reader = thread::spawn({
        let table_r  = Arc::clone(&table);
        let rows     = probe_rows.to_vec();
        let barrier_r = Arc::clone(&barrier);
        let repair   = repair_mode;
        move || {
            barrier_r.wait();
            let start = Instant::now();
            let mut probes = 0u64;
            for row in &rows {
                match repair {
                    RepairMode::Rr => { let _ = table_r.scan_key_vec_read_repair(&row.partkey, query_ts); }
                    _              => { let _ = table_r.scan_key_vec(&row.partkey, query_ts); }
                }
                probes += 1;
            }
            (probes, start.elapsed())
        }
    });

    // -- Writer thread: apply N% updates with increasing ts --
    // Represents: base MVCC table receives N% updates → propagated to MVHT
    let writer = thread::spawn({
        let table_w  = Arc::clone(&table);
        let barrier_w = Arc::clone(&barrier);
        let ts_w     = Arc::clone(&ts_counter);
        let update_parts: Vec<PartEntry> = parts[..n_upd].to_vec();
        move || {
            barrier_w.wait();
            let start = Instant::now();
            for p in &update_parts {
                let ts = ts_w.fetch_add(1, Ordering::SeqCst);
                table_w.update(p.partkey.clone(), p.partkey.clone(), ts, 0, UPDATED_PTYPE.to_vec()).ok();
            }
            (update_parts.len() as u64, start.elapsed())
        }
    });

    // Release both threads simultaneously
    barrier.wait();
    let (n_probes, reader_elapsed) = reader.join().unwrap();
    let (_, writer_elapsed)        = writer.join().unwrap();

    let query_ms  = reader_elapsed.as_secs_f64() * 1000.0;
    let update_ms = writer_elapsed.as_secs_f64() * 1000.0;

    RunResult {
        setup_ms, rebuild_ms: 0.0, update_ms, query_ms,
        total_ms: setup_ms + f64::max(update_ms, query_ms),
        n_updates: n_upd as u64, n_probes,
    }
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn write_csv(path: &str, table_type: &str, repair_mode: &str, update_pct: f64, r: &RunResult) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists();
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    if !exists {
        writeln!(f, "table_type,repair_mode,update_pct,n_updates,n_probes,setup_ms,rebuild_ms,update_ms,query_ms,total_ms")?;
    }
    writeln!(
        f,
        "{},{},{:.4},{},{},{:.3},{:.3},{:.3},{:.3},{:.3}",
        table_type, repair_mode,
        update_pct * 100.0, // store as percentage, e.g. 0.01
        r.n_updates, r.n_probes,
        r.setup_ms, r.rebuild_ms, r.update_ms, r.query_ms, r.total_ms
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    let parts     = load_part(&cli.part_file)?;
    let probe_rows = load_lineitem(&cli.lineitem_file)?;
    let n_upd     = n_updates_for(cli.update_pct, parts.len());

    eprintln!(
        "Loaded: {} parts, {} probe rows | update_pct={:.4}% ({} updates)",
        parts.len(), probe_rows.len(),
        cli.update_pct * 100.0, n_upd
    );

    let table_str  = format!("{:?}", cli.table_type).to_lowercase();
    let repair_str = format!("{:?}", cli.repair_mode).to_lowercase();

    let do_run = || -> RunResult {
        match cli.table_type {
            TableType::Heap | TableType::Chain | TableType::Par => run_mvht_concurrent(
                &parts, &probe_rows, cli.update_pct,
                cli.table_type, cli.repair_mode, cli.bucket_num,
            ),
            TableType::Snap => run_snap(&parts, &probe_rows, cli.update_pct, cli.bucket_num),
            TableType::Ivmh => run_ivmh(&parts, &probe_rows, cli.update_pct, cli.bucket_num),
        }
    };

    // Warmup
    for _ in 0..cli.warmup { let _ = do_run(); }

    // Measured runs
    let mut setup_v   = Vec::with_capacity(cli.repeat);
    let mut rebuild_v = Vec::with_capacity(cli.repeat);
    let mut update_v  = Vec::with_capacity(cli.repeat);
    let mut query_v   = Vec::with_capacity(cli.repeat);
    let mut total_v   = Vec::with_capacity(cli.repeat);
    let mut last_n_probes  = 0u64;
    let mut last_n_updates = 0u64;

    for _ in 0..cli.repeat {
        let r = do_run();
        setup_v.push(r.setup_ms);
        rebuild_v.push(r.rebuild_ms);
        update_v.push(r.update_ms);
        query_v.push(r.query_ms);
        total_v.push(r.total_ms);
        last_n_probes  = r.n_probes;
        last_n_updates = r.n_updates;
    }

    let result = RunResult {
        setup_ms:   trim_mean(setup_v,   cli.trim),
        rebuild_ms: trim_mean(rebuild_v, cli.trim),
        update_ms:  trim_mean(update_v,  cli.trim),
        query_ms:   trim_mean(query_v,   cli.trim),
        total_ms:   trim_mean(total_v,   cli.trim),
        n_updates: last_n_updates,
        n_probes:  last_n_probes,
    };

    eprintln!(
        "  [{}/{}] pct={:.4}% n={} | setup={:.1}ms rebuild={:.1}ms update={:.1}ms query={:.1}ms total={:.1}ms",
        table_str, repair_str,
        cli.update_pct * 100.0, result.n_updates,
        result.setup_ms, result.rebuild_ms, result.update_ms, result.query_ms, result.total_ms
    );

    if let Some(csv_path) = &cli.output_csv {
        write_csv(csv_path, &table_str, &repair_str, cli.update_pct, &result)?;
    }

    Ok(())
}
