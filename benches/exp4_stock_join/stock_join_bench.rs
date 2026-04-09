use clap::{Parser, ValueEnum};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, MvccIndex, VersionsMap};
use fbtree::naive_hash_index::NaiveMvHashTable;
use fbtree::prelude::*;
use std::collections::HashMap;
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
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepairMode {
    Nr,
    Rr,
    Wr,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "CH-benCHmark ORDER_LINE ⋈ STOCK join-update-join benchmark (Exp 4)")]
struct Cli {
    /// STOCK table file (build side, pipe-separated)
    #[arg(long)]
    stock_file: String,

    /// ORDER_LINE table file (probe side, pipe-separated)
    #[arg(long)]
    orderline_file: String,

    /// Update operations file (same format as STOCK file; each row = one update op)
    #[arg(long)]
    updates_file: String,

    #[arg(long, value_enum, default_value = "heap")]
    table_type: TableType,

    #[arg(long, value_enum, default_value = "nr")]
    repair_mode: RepairMode,

    #[arg(long, default_value_t = 1024)]
    bucket_num: usize,

    /// Number of warmup iterations (full J-U-J cycle, results discarded).
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Number of measured iterations (averaged for final result).
    #[arg(long, default_value_t = 10)]
    repeat: usize,

    /// Trim top/bottom N runs by total time before averaging.
    #[arg(long, default_value_t = 2)]
    trim: usize,

    #[arg(long)]
    output_csv: Option<String>,

    /// Update intensity percentage (for CSV annotation only)
    #[arg(long)]
    update_pct: Option<f64>,

    /// Distribution label (e.g. "zipf0.99") for CSV annotation
    #[arg(long)]
    distribution: Option<String>,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Fixed-size value stored in the hash table for each STOCK row.
/// We store S_QUANTITY as a space-padded decimal string (mirrors Q14 P_TYPE layout).
const QUANTITY_VALUE_SIZE: usize = 8;

fn normalize_quantity_value(raw: &[u8]) -> Vec<u8> {
    let mut out = vec![b' '; QUANTITY_VALUE_SIZE];
    let n = raw.len().min(QUANTITY_VALUE_SIZE);
    out[..n].copy_from_slice(&raw[..n]);
    out
}

fn parse_quantity_value(value: &[u8]) -> i64 {
    let s = std::str::from_utf8(value).unwrap_or("0");
    s.trim().parse::<i64>().unwrap_or(0)
}

struct StockEntry {
    s_i_id: Vec<u8>,    // hash key
    s_quantity: Vec<u8>, // normalized value (QUANTITY_VALUE_SIZE bytes)
}

struct OrderLineRow {
    ol_i_id: Vec<u8>,   // probe key (matches S_I_ID)
    ol_quantity: i64,
}

struct JoinStats {
    matched_rows: u64,
    total_qty_product: i64, // sum of S_QUANTITY * OL_QUANTITY for matched pairs
}

enum TableEngine {
    Mvcc(BoxMvccIndexMemPool),
    Snap(NaiveMvHashTable<InMemPool>),
}

// ---------------------------------------------------------------------------
// File readers
// ---------------------------------------------------------------------------

fn parse_tpcc_line(line: &str) -> Option<Vec<&str>> {
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

/// Read STOCK table.
/// Format: S_I_ID|S_W_ID|S_QUANTITY|S_DIST_01|...|S_DIST_10|S_YTD|S_ORDER_CNT|S_REMOTE_CNT|S_DATA
fn read_stock_table(path: &str) -> Result<Vec<StockEntry>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpcc_line(&line) else {
            continue;
        };
        if fields.len() < 3 {
            continue;
        }
        entries.push(StockEntry {
            s_i_id: fields[0].as_bytes().to_vec(),
            s_quantity: normalize_quantity_value(fields[2].as_bytes()),
        });
    }

    Ok(entries)
}

/// Read ORDER_LINE probe file.
/// Format: OL_O_ID|OL_D_ID|OL_W_ID|OL_NUMBER|OL_I_ID|OL_SUPPLY_W_ID|OL_DELIVERY_D|OL_QUANTITY|OL_AMOUNT|OL_DIST_INFO
fn read_orderline_rows(path: &str) -> Result<Vec<OrderLineRow>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpcc_line(&line) else {
            continue;
        };
        if fields.len() < 8 {
            continue;
        }
        let ol_i_id = fields[4].as_bytes().to_vec();
        let ol_quantity = fields[7].parse::<i64>().unwrap_or(5);
        rows.push(OrderLineRow { ol_i_id, ol_quantity });
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
            Box::new(TsPartitionedTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?)
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
    };
    Ok(TableEngine::Mvcc(t))
}

fn run_probe(
    table: &TableEngine,
    probe_rows: &[OrderLineRow],
    ts: Timestamp,
    repair_mode: RepairMode,
) -> Result<JoinStats, Box<dyn Error>> {
    let mut matched_rows: u64 = 0;
    let mut total_qty_product: i64 = 0;
    let mut nr_buf = HashMap::new();
    let mut rr_dedup = HashMap::new();
    let mut rr_versions: VersionsMap = HashMap::new();

    for row in probe_rows {
        let matches = match table {
            TableEngine::Mvcc(t) => match repair_mode {
                RepairMode::Rr => t.scan_key_vec_rr(&row.ol_i_id, ts, &mut rr_dedup, &mut rr_versions)?,
                RepairMode::Nr => t.scan_key_vec_nr(&row.ol_i_id, ts, &mut nr_buf)?,
                RepairMode::Wr => t.scan_key_vec(&row.ol_i_id, ts)?,
            },
            TableEngine::Snap(t) => t.scan_key_vec(&row.ol_i_id, ts)?,
        };
        for (_pk, value) in matches {
            matched_rows += 1;
            let s_quantity = parse_quantity_value(&value);
            total_qty_product += s_quantity * row.ol_quantity;
        }
    }

    Ok(JoinStats { matched_rows, total_qty_product })
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
    stock_rows: usize,
    orderline_rows: usize,
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
            "table_type,repair_mode,update_pct,distribution,stock_rows,orderline_rows,update_ops,\
             j1_alloc_ms,j1_insert_ms,\
             join1_build_ms,join1_probe_ms,update_ms,join2_build_ms,join2_probe_ms,total_ms,\
             join1_matched,join2_matched,join1_total_qty,join2_total_qty"
        )?;
    }

    writeln!(
        file,
        "{:?},{:?},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{},{}",
        table_type,
        repair_mode,
        update_pct
            .map(|v| format!("{:.6}", v))
            .unwrap_or_default(),
        distribution.unwrap_or(""),
        stock_rows,
        orderline_rows,
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
        join1_stats.total_qty_product,
        join2_stats.total_qty_product,
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Main J-U-J cycle
// ---------------------------------------------------------------------------

/// Run one full Join-Update-Join cycle.
/// Returns (j1_alloc, j1_insert, j1_build, j1_probe, update, j2_build, j2_probe, j1_stats, j2_stats).
fn run_juj(
    cli: &Cli,
    stock_entries: &[StockEntry],
    probe_rows: &[OrderLineRow],
    updates: &[StockEntry],
) -> Result<(f64, f64, f64, f64, f64, f64, f64, JoinStats, JoinStats), Box<dyn Error>> {
    // ====================================================================
    //  Phase 1: JOIN 1  (table allocation + build + probe)
    // ====================================================================

    // -- join1 build: table allocation --
    let j1_alloc_start = Instant::now();
    let table = create_table(cli.table_type, cli.bucket_num)?;
    let j1_alloc_ms = j1_alloc_start.elapsed().as_secs_f64() * 1000.0;

    // -- join1 build: index construction --
    let j1_build_start = Instant::now();
    match &table {
        TableEngine::Mvcc(t) => {
            for entry in stock_entries {
                t.insert(
                    entry.s_i_id.clone(),
                    entry.s_i_id.clone(),
                    0,
                    0,
                    entry.s_quantity.clone(),
                )?;
            }
        }
        TableEngine::Snap(t) => {
            for entry in stock_entries {
                t.add_insert_rec_new(&entry.s_i_id, &entry.s_i_id, &entry.s_quantity);
            }
            t.build_table_from_base_and_ts(0);
        }
    }
    let j1_insert_ms = j1_build_start.elapsed().as_secs_f64() * 1000.0;
    let join1_build_ms = j1_alloc_ms + j1_insert_ms;

    // -- join1 probe --
    let j1_probe_start = Instant::now();
    let join1_stats = run_probe(&table, probe_rows, 0, cli.repair_mode)?;
    let join1_probe_ms = j1_probe_start.elapsed().as_secs_f64() * 1000.0;

    // ====================================================================
    //  Phase 2: UPDATE  (skewed update on STOCK.S_QUANTITY)
    // ====================================================================

    // Split epoch before updates (no-op for non-partitioned impls)
    if let TableEngine::Mvcc(t) = &table {
        t.split_at_ts(1)?;
    }

    let update_start = Instant::now();
    match &table {
        TableEngine::Mvcc(t) => {
            if matches!(cli.repair_mode, RepairMode::Wr) {
                t.bulk_update_start()?;
                for (idx, update) in updates.iter().enumerate() {
                    let ts = (idx + 1) as u64;
                    t.update_write_repair(
                        update.s_i_id.clone(),
                        update.s_i_id.clone(),
                        ts,
                        0,
                        update.s_quantity.clone(),
                    )?;
                }
                t.bulk_update_end()?;
            } else {
                for (idx, update) in updates.iter().enumerate() {
                    let ts = (idx + 1) as u64;
                    t.update(
                        update.s_i_id.clone(),
                        update.s_i_id.clone(),
                        ts,
                        0,
                        update.s_quantity.clone(),
                    )?;
                }
            }
        }
        TableEngine::Snap(_) => {}
    }
    let update_ms = update_start.elapsed().as_secs_f64() * 1000.0;

    // ====================================================================
    //  Phase 3: JOIN 2  (after updates)
    // ====================================================================

    let after_update_ts = updates.len() as u64;

    let (join2_build_ms, join2_probe_ms, join2_stats) = match &table {
        TableEngine::Snap(_) => {
            // Build override map with the LAST update value per key
            let mut overrides: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(updates.len());
            for update in updates {
                overrides.insert(update.s_i_id.clone(), update.s_quantity.clone());
            }

            // Rebuild hash table with updated values (outside of timed section)
            let rebuilt = match create_table(TableType::Naive, cli.bucket_num)? {
                TableEngine::Snap(t) => t,
                TableEngine::Mvcc(_) => unreachable!(),
            };
            for entry in stock_entries {
                let qty = overrides
                    .get(&entry.s_i_id)
                    .cloned()
                    .unwrap_or_else(|| entry.s_quantity.clone());
                rebuilt.add_insert_rec_new(&entry.s_i_id, &entry.s_i_id, &qty);
            }

            // -- join2 build: materialize the fresh snapshot, timed --
            let j2_build_start = Instant::now();
            rebuilt.build_table_from_base_and_ts(after_update_ts);
            let j2_build = j2_build_start.elapsed().as_secs_f64() * 1000.0;

            // -- join2 probe --
            let rebuilt_engine = TableEngine::Snap(rebuilt);
            let j2_probe_start = Instant::now();
            let stats = run_probe(&rebuilt_engine, probe_rows, after_update_ts, cli.repair_mode)?;
            let j2_probe = j2_probe_start.elapsed().as_secs_f64() * 1000.0;

            (j2_build, j2_probe, stats)
        }
        TableEngine::Mvcc(_) => {
            // MVHT: just probe the same table at the new timestamp
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
    let stock_entries = read_stock_table(&cli.stock_file)?;
    let probe_rows = read_orderline_rows(&cli.orderline_file)?;
    let updates = read_stock_table(&cli.updates_file)?;

    println!(
        "Loaded: stock_rows={}, orderline_rows={}, update_ops={}",
        stock_entries.len(),
        probe_rows.len(),
        updates.len()
    );

    // ── Warmup (discard results) ───────────────────────────────────────
    for w in 0..cli.warmup {
        let _ = run_juj(&cli, &stock_entries, &probe_rows, &updates)?;
        println!("warmup {}/{} done", w + 1, cli.warmup);
    }

    // ── Measured runs ──────────────────────────────────────────────────
    let n = cli.repeat.max(1);
    let mut runs: Vec<[f64; 7]> = Vec::with_capacity(n);
    let mut last_j1_stats = None;
    let mut last_j2_stats = None;

    for r in 0..n {
        let (j1alloc, j1ins, j1b, j1p, upd, j2b, j2p, j1s, j2s) =
            run_juj(&cli, &stock_entries, &probe_rows, &updates)?;
        let total = j1b + j1p + upd + j2b + j2p;
        println!(
            "  run {}/{}: j1_alloc={:.3} j1_insert={:.3} j1_build={:.3} j1_probe={:.3} \
             update={:.3} j2_build={:.3} j2_probe={:.3} total={:.3}",
            r + 1, n, j1alloc, j1ins, j1b, j1p, upd, j2b, j2p, total
        );
        runs.push([j1alloc, j1ins, j1b, j1p, upd, j2b, j2p]);
        last_j1_stats = Some(j1s);
        last_j2_stats = Some(j2s);
    }

    // ── Trimmed mean ──────────────────────────────────────────────────
    let trim = cli.trim.min(n / 2);
    runs.sort_by(|a, b| {
        let ta: f64 = a.iter().sum();
        let tb: f64 = b.iter().sum();
        ta.partial_cmp(&tb).unwrap()
    });
    let trimmed = &runs[trim..n - trim];
    let kept = trimmed.len() as f64;
    println!(
        "trimmed mean: {} runs total, dropped {} lowest + {} highest, averaging {} runs",
        n,
        trim,
        trim,
        trimmed.len()
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

    // ── Report ─────────────────────────────────────────────────────────
    println!("=== stock_join_update_join (avg of {} runs) ===", trimmed.len());
    println!("table_type={:?}, repair_mode={:?}", cli.table_type, cli.repair_mode);
    println!(
        "stock_rows={}, orderline_rows={}, update_ops={}",
        stock_entries.len(),
        probe_rows.len(),
        updates.len()
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
        "join1: matched={}, total_qty_product={}",
        join1_stats.matched_rows, join1_stats.total_qty_product
    );
    println!(
        "join2: matched={}, total_qty_product={}",
        join2_stats.matched_rows, join2_stats.total_qty_product
    );

    if let Some(output_csv) = &cli.output_csv {
        write_result_csv(
            output_csv,
            cli.table_type,
            cli.repair_mode,
            cli.update_pct,
            cli.distribution.as_deref(),
            stock_entries.len(),
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
