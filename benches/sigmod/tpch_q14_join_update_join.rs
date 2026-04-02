use clap::{Parser, ValueEnum};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, MvccIndex};
use fbtree::naive_hash_index::{HeapBaseMvccTable, IvmHashTable, NaiveMvHashTable};
use fbtree::prelude::*;
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,
    Heap,
    Par,
    Rust,
    Naive,
    Ivmh,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepairMode {
    Nr,
    Rr,
    Wr,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "TPC-H Q14 style join-update-join benchmark for MVHT")]
struct Cli {
    /// PART table file (build side)
    #[arg(long)]
    part_file: String,
    /// Pre-filtered LINEITEM probe file (probe side, already filtered by shipdate)
    #[arg(long)]
    lineitem_file: String,
    /// Update operations file
    #[arg(long)]
    updates_file: String,

    #[arg(long, value_enum, default_value = "heap")]
    table_type: TableType,
    #[arg(long, value_enum, default_value = "nr")]
    repair_mode: RepairMode,

    #[arg(long, default_value_t = 128)]
    bucket_num: usize,

    /// Number of warmup iterations (full J-U-J cycle, results discarded).
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Number of measured iterations (averaged for final result).
    #[arg(long, default_value_t = 7)]
    repeat: usize,

    /// Trim top/bottom N runs by total time before averaging.
    #[arg(long, default_value_t = 2)]
    trim: usize,

    #[arg(long)]
    output_csv: Option<String>,

    #[arg(long)]
    update_pct: Option<f64>,
    #[arg(long)]
    distribution: Option<String>,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

struct PartEntry {
    partkey: Vec<u8>,
    ptype: Vec<u8>,
}

struct ProbeRow {
    partkey: Vec<u8>,
    revenue: f64,
}

struct JoinStats {
    matched_rows: u64,
    promo_revenue: f64,
    total_revenue: f64,
}

enum TableEngine {
    Mvcc(BoxMvccIndexMemPool),
    Snap(NaiveMvHashTable<InMemPool>),
    Ivmh(IvmHashTable<InMemPool>),
}

// ---------------------------------------------------------------------------
// File readers
// ---------------------------------------------------------------------------

const PTYPE_VALUE_SIZE: usize = 32;

fn normalize_ptype_value(raw: &[u8]) -> Vec<u8> {
    let mut out = vec![b' '; PTYPE_VALUE_SIZE];
    let n = raw.len().min(PTYPE_VALUE_SIZE);
    out[..n].copy_from_slice(&raw[..n]);
    out
}

fn parse_tpch_line(line: &str) -> Option<Vec<&str>> {
    let mut fields: Vec<&str> = line.split('|').collect();
    if fields.last().is_some_and(|f| f.is_empty()) {
        fields.pop();
    }
    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

fn read_part_table(path: &str) -> Result<Vec<PartEntry>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        // PART: P_PARTKEY | P_NAME | P_MFGR | P_BRAND | P_TYPE | ...
        if fields.len() < 5 {
            continue;
        }
        entries.push(PartEntry {
            partkey: fields[0].as_bytes().to_vec(),
            ptype: normalize_ptype_value(fields[4].as_bytes()),
        });
    }

    Ok(entries)
}

/// Read pre-filtered LINEITEM probe file.
/// Expected format: L_PARTKEY|L_EXTENDEDPRICE|L_DISCOUNT  (3 columns, already filtered)
fn read_probe_rows(path: &str) -> Result<Vec<ProbeRow>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        if fields.len() < 3 {
            continue;
        }
        let partkey = fields[0].as_bytes().to_vec();
        let extended_price = fields[1].parse::<f64>().unwrap_or(0.0);
        let discount = fields[2].parse::<f64>().unwrap_or(0.0);
        let revenue = extended_price * (1.0 - discount);

        rows.push(ProbeRow { partkey, revenue });
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Table creation & probe
// ---------------------------------------------------------------------------

fn create_table(table_type: TableType, bucket_num: usize) -> Result<TableEngine, Box<dyn Error>> {
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let t = match table_type {
        TableType::Chain => {
            Box::new(ChainedHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?)
                as BoxMvccIndexMemPool
        }
        TableType::Heap => {
            Box::new(HeapHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?)
                as BoxMvccIndexMemPool
        }
        TableType::Par => {
            Box::new(TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?)
                as BoxMvccIndexMemPool
        }
        TableType::Rust => {
            Box::new(MvccRustHashMap::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?)
                as BoxMvccIndexMemPool
        }
        TableType::Naive => {
            return Ok(TableEngine::Snap(NaiveMvHashTable::new_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )));
        }
        TableType::Ivmh => {
            return Ok(TableEngine::Ivmh(IvmHashTable::new_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )));
        }
    };
    Ok(TableEngine::Mvcc(t))
}

fn run_probe(
    table: &TableEngine,
    probe_rows: &[ProbeRow],
    ts: Timestamp,
    repair_mode: RepairMode,
) -> Result<JoinStats, Box<dyn Error>> {
    let mut matched_rows: u64 = 0;
    let mut promo_revenue: f64 = 0.0;
    let mut total_revenue: f64 = 0.0;

    for row in probe_rows {
        let matches = match table {
            TableEngine::Mvcc(t) => match repair_mode {
                RepairMode::Rr => t.scan_key_vec_read_repair(&row.partkey, ts)?,
                RepairMode::Nr | RepairMode::Wr => t.scan_key_vec(&row.partkey, ts)?,
            },
            TableEngine::Snap(t) => t.scan_key_vec(&row.partkey, ts)?,
            TableEngine::Ivmh(t) => t.scan_key_vec(&row.partkey, ts)?,
        };
        for (_pkey, value) in matches {
            matched_rows += 1;
            total_revenue += row.revenue;
            if value.starts_with(b"PROMO") {
                promo_revenue += row.revenue;
            }
        }
    }

    Ok(JoinStats {
        matched_rows,
        promo_revenue,
        total_revenue,
    })
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn write_result_csv(
    output_csv: &str,
    table_type: TableType,
    repair_mode: RepairMode,
    update_pct: Option<f64>,
    distribution: Option<&str>,
    part_rows: usize,
    lineitem_rows: usize,
    update_ops: usize,
    j1_alloc_ms: f64,
    j1_insert_ms: f64,
    join1_build_ms: f64,
    join1_probe_ms: f64,
    update_ms: f64,
    join2_build_ms: f64,
    join2_probe_ms: f64,
    total_j_u_j_ms: f64,
    join1_stats: &JoinStats,
    join2_stats: &JoinStats,
) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(output_csv).exists() && metadata(output_csv)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(output_csv)?;

    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,update_pct,distribution,part_rows,lineitem_rows,update_ops,\
             j1_alloc_ms,j1_insert_ms,\
             join1_build_ms,join1_probe_ms,update_ms,join2_build_ms,join2_probe_ms,total_ms,\
             join1_matched,join2_matched,join1_q14_ratio,join2_q14_ratio"
        )?;
    }

    let q14 = |s: &JoinStats| -> f64 {
        if s.total_revenue > 0.0 {
            100.0 * s.promo_revenue / s.total_revenue
        } else {
            0.0
        }
    };

    writeln!(
        file,
        "{:?},{:?},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{:.6}",
        table_type,
        repair_mode,
        update_pct
            .map(|v| format!("{:.6}", v))
            .unwrap_or_default(),
        distribution.unwrap_or(""),
        part_rows,
        lineitem_rows,
        update_ops,
        j1_alloc_ms,
        j1_insert_ms,
        join1_build_ms,
        join1_probe_ms,
        update_ms,
        join2_build_ms,
        join2_probe_ms,
        total_j_u_j_ms,
        join1_stats.matched_rows,
        join2_stats.matched_rows,
        q14(join1_stats),
        q14(join2_stats),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

/// Run a full J-U-J cycle. Returns (j1_alloc, j1_insert, j1_build, j1_probe, update, j2_build, j2_probe, j1_stats, j2_stats).
fn run_juj(
    cli: &Cli,
    part_entries: &[PartEntry],
    probe_rows: &[ProbeRow],
    updates: &[PartEntry],
) -> Result<(f64, f64, f64, f64, f64, f64, f64, JoinStats, JoinStats), Box<dyn Error>> {
    // ====================================================================
    //  Phase 1: JOIN 1  (table allocation + build + probe)
    //
    //  Table allocation is included in build timing for fairness:
    //  SNAP's mark_ts internally allocates a new hash table each time,
    //  so MVHT's table allocation should also be measured.
    // ====================================================================

    // -- join1 build: table allocation --
    let j1_alloc_start = Instant::now();
    let table = create_table(cli.table_type, cli.bucket_num)?;
    let j1_alloc_ms = j1_alloc_start.elapsed().as_secs_f64() * 1000.0;

    // -- join1 build: base MVCC population (untimed for SNAP / IVMH only) --
    let shared_base = HeapBaseMvccTable::new();
    for entry in part_entries {
        shared_base.insert_at_ts(&entry.partkey, &entry.partkey, &entry.ptype, 0);
    }

    if let TableEngine::Snap(t) = &table {
        for entry in part_entries {
            t.add_insert_rec_at_ts(&entry.partkey, &entry.partkey, &entry.ptype, 0);
        }
    }
    if let TableEngine::Ivmh(t) = &table {
        for entry in part_entries {
            t.prepare_insert_base(&entry.partkey, &entry.partkey, &entry.ptype);
        }
    }

    // -- join1 build: index construction (timed) --
    let j1_build_start = Instant::now();
    match &table {
        TableEngine::Mvcc(t) => {
            for (key, pkey, value) in shared_base.scan_as_of(0) {
                t.insert_ref(&key, &pkey, 0, 0, &value)?;
            }
        }
        TableEngine::Snap(t) => {
            t.build_table_from_base_and_ts(0);
        }
        TableEngine::Ivmh(t) => {
            t.populate_current_from_base(0);
        }
    }
    let j1_insert_ms = j1_build_start.elapsed().as_secs_f64() * 1000.0;
    let join1_build_ms = j1_alloc_ms + j1_insert_ms;

    // -- join1 probe --
    let j1_probe_start = Instant::now();
    let join1_stats = run_probe(&table, probe_rows, 0, cli.repair_mode)?;
    let join1_probe_ms = j1_probe_start.elapsed().as_secs_f64() * 1000.0;

    // ====================================================================
    //  Phase 2: UPDATE
    // ====================================================================

    // Split epoch before updates: creates a new partition for the update phase.
    // No-op for non-partitioned implementations (Heap, Chain, Rust).
    if let TableEngine::Mvcc(t) = &table {
        t.split_at_ts(1)?;
    }

    // Base-table updates are untimed for the baseline structures.
    if let TableEngine::Snap(t) = &table {
        for (idx, update) in updates.iter().enumerate() {
            let ts = (idx + 1) as u64;
            t.add_update_rec_at_ts(&update.partkey, &update.partkey, &update.ptype, ts);
        }
    }
    if let TableEngine::Ivmh(t) = &table {
        for (idx, update) in updates.iter().enumerate() {
            let ts = (idx + 1) as u64;
            t.prepare_update_base(&update.partkey, &update.partkey, &update.ptype, ts);
        }
    }

    let update_start = Instant::now();
    match &table {
        TableEngine::Mvcc(t) => {
            if matches!(cli.repair_mode, RepairMode::Wr) {
                t.bulk_update_start()?;
                for (idx, update) in updates.iter().enumerate() {
                    let ts = (idx + 1) as u64;
                    t.update_write_repair(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        ts,
                        0,
                        update.ptype.clone(),
                    )?;
                }
                t.bulk_update_end()?;
            } else {
                for (idx, update) in updates.iter().enumerate() {
                    let ts = (idx + 1) as u64;
                    t.update(
                        update.partkey.clone(),
                        update.partkey.clone(),
                        ts,
                        0,
                        update.ptype.clone(),
                    )?;
                }
            }
        }
        TableEngine::Snap(_) => {}
        TableEngine::Ivmh(t) => {
            // In-place maintenance on the latest derived hash state.
            for update in updates {
                t.update_current(&update.partkey, &update.partkey, &update.ptype);
            }
        }
    }
    let update_ms = if matches!(&table, TableEngine::Snap(_)) {
        0.0
    } else {
        update_start.elapsed().as_secs_f64() * 1000.0
    };

    // ====================================================================
    //  Phase 3: JOIN 2  (after updates)
    // ====================================================================

    let after_update_ts = updates.len() as u64;

    let (join2_build_ms, join2_probe_ms, join2_stats) = match &table {
        TableEngine::Snap(t) => {
            // -- join2 build: rebuild the derived snapshot from the current base snapshot --
            let j2_build_start = Instant::now();
            t.build_table_from_base_and_ts(after_update_ts);
            let j2_build = j2_build_start.elapsed().as_secs_f64() * 1000.0;

            // -- join2 probe --
            let j2_probe_start = Instant::now();
            let stats = run_probe(&table, probe_rows, after_update_ts, cli.repair_mode)?;
            let j2_probe = j2_probe_start.elapsed().as_secs_f64() * 1000.0;

            (j2_build, j2_probe, stats)
        }
        TableEngine::Ivmh(_) => {
            // IVMH: current_table already updated in-place (O(|Δ|) during update phase).
            // No mark_ts needed — scan_key_vec at a recent ts probes current_table directly.
            let j2_probe_start = Instant::now();
            let stats = run_probe(&table, probe_rows, after_update_ts, cli.repair_mode)?;
            let j2_probe = j2_probe_start.elapsed().as_secs_f64() * 1000.0;
            (0.0, j2_probe, stats)
        }
        TableEngine::Mvcc(_) => {
            let j2_probe_start = Instant::now();
            let stats = run_probe(&table, probe_rows, after_update_ts, cli.repair_mode)?;
            let j2_probe = j2_probe_start.elapsed().as_secs_f64() * 1000.0;

            (0.0, j2_probe, stats)
        }
    };

    Ok((
        j1_alloc_ms,
        j1_insert_ms,
        join1_build_ms,
        join1_probe_ms,
        update_ms,
        join2_build_ms,
        join2_probe_ms,
        join1_stats,
        join2_stats,
    ))
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    // ── Read input files (outside timing) ──────────────────────────────
    let part_entries = read_part_table(&cli.part_file)?;
    let probe_rows = read_probe_rows(&cli.lineitem_file)?;
    let updates = read_part_table(&cli.updates_file)?;

    println!(
        "Loaded: part_rows={}, probe_rows={}, update_ops={}",
        part_entries.len(),
        probe_rows.len(),
        updates.len()
    );

    // ── Warmup (discard results) ───────────────────────────────────────
    for w in 0..cli.warmup {
        let _ = run_juj(&cli, &part_entries, &probe_rows, &updates)?;
        println!("warmup {}/{} done", w + 1, cli.warmup);
    }

    // ── Measured runs ──────────────────────────────────────────────────
    let n = cli.repeat.max(1);
    // Each run: [j1_alloc, j1_insert, j1_build, j1_probe, update, j2_build, j2_probe]
    let mut runs: Vec<[f64; 7]> = Vec::with_capacity(n);
    let mut last_j1_stats = None;
    let mut last_j2_stats = None;

    for r in 0..n {
        let (j1alloc, j1ins, j1b, j1p, upd, j2b, j2p, j1s, j2s) =
            run_juj(&cli, &part_entries, &probe_rows, &updates)?;
        let total = j1b + j1p + upd + j2b + j2p;
        println!(
            "  run {}/{}: j1_alloc={:.3} j1_insert={:.3} j1_build={:.3} j1_probe={:.3} update={:.3} j2_build={:.3} j2_probe={:.3} total={:.3}",
            r + 1, n, j1alloc, j1ins, j1b, j1p, upd, j2b, j2p, total
        );
        runs.push([j1alloc, j1ins, j1b, j1p, upd, j2b, j2p]);
        last_j1_stats = Some(j1s);
        last_j2_stats = Some(j2s);
    }

    // ── Trimmed mean: sort by total, drop top/bottom `trim` runs ──
    let trim = cli.trim.min(n / 2); // can't trim more than half
    runs.sort_by(|a, b| {
        let ta: f64 = a.iter().sum();
        let tb: f64 = b.iter().sum();
        ta.partial_cmp(&tb).unwrap()
    });
    let trimmed = &runs[trim..n - trim];
    let kept = trimmed.len() as f64;
    println!(
        "trimmed mean: {} runs total, dropped {} lowest + {} highest, averaging {} runs",
        n, trim, trim, trimmed.len()
    );

    let mut avg = [0.0_f64; 7];
    for run in trimmed {
        for i in 0..7 {
            avg[i] += run[i];
        }
    }
    for v in &mut avg {
        *v /= kept;
    }

    let j1_alloc_ms = avg[0];
    let j1_insert_ms = avg[1];
    let join1_build_ms = avg[2];
    let join1_probe_ms = avg[3];
    let update_ms = avg[4];
    let join2_build_ms = avg[5];
    let join2_probe_ms = avg[6];
    let total_j_u_j_ms =
        join1_build_ms + join1_probe_ms + update_ms + join2_build_ms + join2_probe_ms;

    let join1_stats = last_j1_stats.unwrap();
    let join2_stats = last_j2_stats.unwrap();

    // ====================================================================
    //  Report
    // ====================================================================

    let q14 = |s: &JoinStats| -> f64 {
        if s.total_revenue > 0.0 {
            100.0 * s.promo_revenue / s.total_revenue
        } else {
            0.0
        }
    };

    println!("=== tpch_q14_join_update_join (avg of {} runs) ===", n);
    println!("table_type={:?}, repair_mode={:?}", cli.table_type, cli.repair_mode);
    println!(
        "part_rows={}, probe_rows={}, update_ops={}",
        part_entries.len(), probe_rows.len(), updates.len()
    );
    println!(
        "j1_alloc_ms={:.6}, j1_insert_ms={:.6}, join1_build_ms={:.6}, join1_probe_ms={:.6}, \
         update_ms={:.6}, join2_build_ms={:.6}, join2_probe_ms={:.6}, total_ms={:.6}",
        j1_alloc_ms,
        j1_insert_ms,
        join1_build_ms,
        join1_probe_ms,
        update_ms,
        join2_build_ms,
        join2_probe_ms,
        total_j_u_j_ms
    );
    println!(
        "join1: matched={}, q14={:.6}",
        join1_stats.matched_rows, q14(&join1_stats)
    );
    println!(
        "join2: matched={}, q14={:.6}",
        join2_stats.matched_rows, q14(&join2_stats)
    );

    if let Some(output_csv) = &cli.output_csv {
        write_result_csv(
            output_csv,
            cli.table_type,
            cli.repair_mode,
            cli.update_pct,
            cli.distribution.as_deref(),
            part_entries.len(),
            probe_rows.len(),
            updates.len(),
            j1_alloc_ms,
            j1_insert_ms,
            join1_build_ms,
            join1_probe_ms,
            update_ms,
            join2_build_ms,
            join2_probe_ms,
            total_j_u_j_ms,
            &join1_stats,
            &join2_stats,
        )?;
        println!("result_appended_to_csv={}", output_csv);
    }

    Ok(())
}
