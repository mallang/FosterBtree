use clap::{Parser, ValueEnum};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{MvccIndex, VersionsMap};
use fbtree::mvcc_index::hash_join_page::record::RecordRef;
use fbtree::naive_hash_index::NaiveHashTable;
use fbtree::naive_hash_index::IvmHashTable;
use fbtree::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

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
#[command(about = "Symmetric Hash Join IVM benchmark (TPC-H PART ⟕ LINEITEM)")]
struct Cli {
    /// PART table file (build side hash table)
    #[arg(long)]
    part_file: String,

    /// LINEITEM probe file (stream side). Format: L_PARTKEY|L_EXTENDEDPRICE|L_DISCOUNT
    #[arg(long)]
    lineitem_file: String,

    /// Update operations file for PART table
    #[arg(long)]
    updates_file: String,

    #[arg(long, value_enum, default_value = "heap")]
    table_type: TableType,

    #[arg(long, value_enum, default_value = "wr")]
    repair_mode: RepairMode,

    #[arg(long, default_value_t = 2048)]
    bucket_num: usize,

    /// Number of reader threads (probe PART HT with LINEITEM rows)
    #[arg(long, default_value_t = 1)]
    reader_threads: usize,

    /// Number of writer threads (apply updates to PART HT)
    #[arg(long, default_value_t = 1)]
    writer_threads: usize,

    /// Number of warmup iterations
    #[arg(long, default_value_t = 1)]
    warmup: usize,

    /// Number of measured iterations
    #[arg(long, default_value_t = 5)]
    repeat: usize,

    /// Trim top/bottom N runs before averaging
    #[arg(long, default_value_t = 1)]
    trim: usize,

    #[arg(long)]
    output_csv: Option<String>,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct PartEntry {
    partkey: Vec<u8>,
    ptype: Vec<u8>, // P_TYPE as value
}

#[derive(Clone)]
struct ProbeRow {
    partkey: Vec<u8>,
    revenue: f64,
}

#[derive(Clone)]
struct UpdateOp {
    partkey: Vec<u8>,
    new_ptype: Vec<u8>,
}

struct RunResult {
    reader_throughput: f64, // probes/sec
    writer_throughput: f64, // updates/sec
    total_ms: f64,
    total_probes: u64,
    total_updates: u64,
}

// ---------------------------------------------------------------------------
// Shared table wrapper (trait object behind Arc)
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
// File readers (reuse TPC-H format from sigmod benchmark)
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
        rows.push(ProbeRow {
            partkey,
            revenue: extended_price * (1.0 - discount),
        });
    }
    Ok(rows)
}

fn read_updates(path: &str) -> Result<Vec<UpdateOp>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut ops = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let Some(fields) = parse_tpch_line(&line) else {
            continue;
        };
        // Full PART row: P_PARTKEY|P_NAME|P_MFGR|P_BRAND|P_TYPE|...
        if fields.len() < 5 {
            continue;
        }
        ops.push(UpdateOp {
            partkey: fields[0].as_bytes().to_vec(),
            new_ptype: normalize_ptype_value(fields[4].as_bytes()),
        });
    }
    Ok(ops)
}

// ---------------------------------------------------------------------------
// Table creation
// ---------------------------------------------------------------------------

fn create_mvcc_table(
    table_type: TableType,
    bucket_num: usize,
) -> Result<ArcMvccIndex, Box<dyn Error>> {
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let t: ArcMvccIndex = match table_type {
        TableType::Chain => Arc::new(ChainedHashTable::create_with_bucket_num(
            c_key,
            mem_pool.clone(),
            bucket_num,
        )?),
        TableType::Heap => Arc::new(HeapHashTable::create_with_bucket_num(
            c_key,
            mem_pool.clone(),
            bucket_num,
        )?),
        TableType::Par => Arc::new(TsPartitionedTable::create_with_bucket_num(
            c_key,
            mem_pool.clone(),
            bucket_num,
        )?),
        TableType::Snap => {
            // SNAP doesn't implement MvccIndex trait directly in the same way,
            // so we handle it separately in the run loop.
            panic!("Use run_snap() for SNAP table type");
        }
    };
    Ok(t)
}

fn build_table(
    table: &ArcMvccIndex,
    parts: &[PartEntry],
    ts: Timestamp,
) {
    for (i, p) in parts.iter().enumerate() {
        table
            .insert(
                p.partkey.clone(),
                p.partkey.clone(), // pkey = partkey
                ts,
                i as u64,
                p.ptype.clone(),
            )
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// Multi-threaded IVM run (MVCC tables)
// ---------------------------------------------------------------------------

fn run_ivm_mvcc(
    table: &ArcMvccIndex,
    probe_rows: &[ProbeRow],
    update_ops: &[UpdateOp],
    repair_mode: RepairMode,
    reader_threads: usize,
    writer_threads: usize,
    base_ts: Timestamp,
) -> RunResult {
    let total_threads = reader_threads + writer_threads;
    let barrier = Arc::new(Barrier::new(total_threads + 1)); // +1 for main thread
    let ts_counter = Arc::new(AtomicU64::new(base_ts + 1));

    // Split probe rows across reader threads
    let chunk_size = (probe_rows.len() + reader_threads - 1) / reader_threads;
    let probe_chunks: Vec<Vec<ProbeRow>> = probe_rows
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    // Split update ops across writer threads
    let update_chunk_size = if writer_threads > 0 {
        (update_ops.len() + writer_threads - 1) / writer_threads
    } else {
        0
    };
    let update_chunks: Vec<Vec<UpdateOp>> = if writer_threads > 0 {
        update_ops
            .chunks(update_chunk_size)
            .map(|c| c.to_vec())
            .collect()
    } else {
        vec![]
    };

    let mut handles = Vec::new();

    // Spawn reader threads
    for i in 0..reader_threads {
        let table = Arc::clone(table);
        let barrier = Arc::clone(&barrier);
        let chunk = probe_chunks[i.min(probe_chunks.len() - 1)].clone();
        let repair = repair_mode;

        handles.push(thread::spawn(move || {
            let mut nr_buf = HashMap::new();
            let mut rr_dedup = HashMap::new();
            let mut rr_versions: VersionsMap = HashMap::new();

            barrier.wait(); // sync start

            let start = Instant::now();
            let mut probes = 0u64;
            let mut matched = 0u64;

            // Probe the latest timestamp
            let ts = u64::MAX - 1; // always read latest
            for row in &chunk {
                let matches = match repair {
                    RepairMode::Nr => table.scan_key_vec_nr(&row.partkey, ts, &mut nr_buf),
                    RepairMode::Rr => table.scan_key_vec_rr(
                        &row.partkey,
                        ts,
                        &mut rr_dedup,
                        &mut rr_versions,
                    ),
                    RepairMode::Wr => table.scan_key_vec(&row.partkey, ts),
                };
                if let Ok(results) = matches {
                    matched += results.len() as u64;
                }
                probes += 1;
            }

            let elapsed = start.elapsed();
            (probes, matched, elapsed)
        }));
    }

    // Spawn writer threads
    for i in 0..writer_threads {
        let table = Arc::clone(table);
        let barrier = Arc::clone(&barrier);
        let ts_counter = Arc::clone(&ts_counter);
        let chunk = update_chunks[i.min(update_chunks.len() - 1)].clone();
        let repair = repair_mode;

        handles.push(thread::spawn(move || {
            barrier.wait(); // sync start

            let start = Instant::now();
            let mut updates = 0u64;

            for op in &chunk {
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                let result = match repair {
                    RepairMode::Wr => table.update_write_repair(
                        op.partkey.clone(),
                        op.partkey.clone(),
                        ts,
                        0,
                        op.new_ptype.clone(),
                    ),
                    _ => table.update(
                        op.partkey.clone(),
                        op.partkey.clone(),
                        ts,
                        0,
                        op.new_ptype.clone(),
                    ),
                };
                if result.is_ok() {
                    updates += 1;
                }
            }

            let elapsed = start.elapsed();
            (updates, 0u64, elapsed)
        }));
    }

    // Start all threads
    let global_start = Instant::now();
    barrier.wait();

    // Collect results: first reader_threads handles are readers, rest are writers
    let mut total_probes = 0u64;
    let mut total_updates = 0u64;

    for (idx, handle) in handles.into_iter().enumerate() {
        let (count, _extra, _elapsed) = handle.join().unwrap();
        if idx < reader_threads {
            total_probes += count;
        } else {
            total_updates += count;
        }
    }

    let total_ms = global_start.elapsed().as_secs_f64() * 1000.0;
    let total_secs = total_ms / 1000.0;

    let reader_throughput = total_probes as f64 / total_secs;
    let writer_throughput = if writer_threads > 0 {
        total_updates as f64 / total_secs
    } else {
        0.0
    };

    RunResult {
        reader_throughput,
        writer_throughput,
        total_ms,
        total_probes,
        total_updates,
    }
}

// ---------------------------------------------------------------------------
// SNAP baseline: must lock for rebuild
// ---------------------------------------------------------------------------

fn run_ivm_snap(
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    update_ops: &[UpdateOp],
    bucket_num: usize,
    reader_threads: usize,
) -> RunResult {
    // SNAP: full rebuild on update (blocks readers), then multi-thread probe.
    // We use NaiveHashTable directly (it is Sync) rather than NaiveMvHashTable
    // (which is RefCell-based and !Sync).
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);

    // Phase 1: Build updated table (= rebuild cost).
    // Build an update map, then insert all rows with updated values.
    let rebuild_start = Instant::now();
    let table = {
        let mut update_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for op in update_ops {
            update_map.insert(op.partkey.clone(), op.new_ptype.clone());
        }
        let t = NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_num);
        for p in parts {
            let value = update_map.get(&p.partkey).unwrap_or(&p.ptype);
            t.insert(RecordRef::new(&p.partkey, &p.partkey, value)).unwrap();
        }
        Arc::new(t)
    };
    let rebuild_ms = rebuild_start.elapsed().as_secs_f64() * 1000.0;

    // Phase 2: Multi-thread probe on the rebuilt snapshot.
    let chunk_size = (probe_rows.len() + reader_threads - 1) / reader_threads;
    let probe_chunks: Vec<Vec<ProbeRow>> = probe_rows
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let barrier = Arc::new(Barrier::new(reader_threads + 1));
    let mut handles = Vec::new();

    for i in 0..reader_threads {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        let chunk = probe_chunks[i.min(probe_chunks.len() - 1)].clone();

        handles.push(thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let mut probes = 0u64;
            let mut results_buf = Vec::new();
            for row in &chunk {
                results_buf.clear();
                let _ = table.scan_key_vec(&row.partkey, &mut results_buf);
                probes += 1;
            }
            let elapsed = start.elapsed();
            (probes, elapsed)
        }));
    }

    let probe_start = Instant::now();
    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let probe_ms = probe_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = rebuild_ms + probe_ms;
    let total_probes = probe_rows.len() as u64;
    let reader_throughput = total_probes as f64 / (total_ms / 1000.0);

    RunResult {
        reader_throughput,
        writer_throughput: 0.0,
        total_ms,
        total_probes,
        total_updates: update_ops.len() as u64,
    }
}

// ---------------------------------------------------------------------------
// IVMH baseline: in-place update + rebuild for probe
// ---------------------------------------------------------------------------

fn run_ivm_ivmh(
    parts: &[PartEntry],
    probe_rows: &[ProbeRow],
    update_ops: &[UpdateOp],
    bucket_num: usize,
    reader_threads: usize,
) -> RunResult {
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);

    // Phase 1: In-place update current state, then rebuild snapshot.
    // IVMH advantage: update is O(|Δ|) in-place, then rebuild O(|R|).
    // SNAP: must also rebuild O(|R|), but update is merged into rebuild.
    let ivmh = IvmHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_num);

    // Populate current state
    for p in parts {
        ivmh.add_insert_rec(&p.partkey, &p.partkey, &p.ptype);
    }

    // Apply updates in-place
    let rebuild_start = Instant::now();
    for op in update_ops {
        ivmh.add_update_rec(&op.partkey, &op.partkey, &op.new_ptype);
    }
    // Materialise snapshot for readers
    let _build_duration = ivmh.mark_ts(1);
    let rebuild_ms = rebuild_start.elapsed().as_secs_f64() * 1000.0;

    // Phase 2: Multi-thread probe on the materialised snapshot.
    // IVMH snapshots use NaiveHashTable internally, same as SNAP.
    // We probe via scan_key_vec which delegates to the materialised snapshot.
    let chunk_size = (probe_rows.len() + reader_threads - 1) / reader_threads;
    let probe_chunks: Vec<Vec<ProbeRow>> = probe_rows
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    // IVMH's snapshots are RefCell-based (not Sync), so we extract the snapshot
    // and wrap it for multi-threaded probe. For now, do single-threaded probe
    // since IVMH shares the same rebuild-then-probe model as SNAP.
    let probe_start = Instant::now();
    let mut total_probes = 0u64;
    for chunk in &probe_chunks {
        for row in chunk {
            let _ = ivmh.scan_key_vec(&row.partkey, 1);
            total_probes += 1;
        }
    }
    let probe_ms = probe_start.elapsed().as_secs_f64() * 1000.0;

    let total_ms = rebuild_ms + probe_ms;
    let reader_throughput = total_probes as f64 / (total_ms / 1000.0);

    RunResult {
        reader_throughput,
        writer_throughput: 0.0,
        total_ms,
        total_probes,
        total_updates: update_ops.len() as u64,
    }
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn write_csv(
    path: &str,
    cli: &Cli,
    result: &RunResult,
) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,reader_threads,writer_threads,bucket_num,\
             total_probes,total_updates,total_ms,reader_throughput,writer_throughput"
        )?;
    }

    writeln!(
        file,
        "{:?},{:?},{},{},{},{},{},{:.3},{:.1},{:.1}",
        cli.table_type,
        cli.repair_mode,
        cli.reader_threads,
        cli.writer_threads,
        cli.bucket_num,
        result.total_probes,
        result.total_updates,
        result.total_ms,
        result.reader_throughput,
        result.writer_throughput,
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    eprintln!("=== IVM Symmetric Hash Join Benchmark ===");
    eprintln!(
        "table={:?}  repair={:?}  readers={}  writers={}  buckets={}",
        cli.table_type, cli.repair_mode, cli.reader_threads, cli.writer_threads, cli.bucket_num,
    );

    // Load data
    eprintln!("Loading data...");
    let parts = read_part_table(&cli.part_file)?;
    let probe_rows = read_probe_rows(&cli.lineitem_file)?;
    let update_ops = read_updates(&cli.updates_file)?;
    eprintln!(
        "  PART: {} rows, LINEITEM probe: {} rows, Updates: {} ops",
        parts.len(),
        probe_rows.len(),
        update_ops.len(),
    );

    let mut results = Vec::new();

    for iter in 0..(cli.warmup + cli.repeat) {
        let is_warmup = iter < cli.warmup;

        let result = match cli.table_type {
            TableType::Snap => {
                run_ivm_snap(
                    &parts,
                    &probe_rows,
                    &update_ops,
                    cli.bucket_num,
                    cli.reader_threads,
                )
            }
            TableType::Ivmh => {
                run_ivm_ivmh(
                    &parts,
                    &probe_rows,
                    &update_ops,
                    cli.bucket_num,
                    cli.reader_threads,
                )
            }
            _ => {
                // Build MVCC table
                let table = create_mvcc_table(cli.table_type, cli.bucket_num)?;
                build_table(&table, &parts, 1);

                // For EPOCH: split before updates
                if matches!(cli.table_type, TableType::Par) {
                    table.split_at_ts(2)?;
                }

                run_ivm_mvcc(
                    &table,
                    &probe_rows,
                    &update_ops,
                    cli.repair_mode,
                    cli.reader_threads,
                    cli.writer_threads,
                    2,
                )
            }
        };

        if is_warmup {
            eprintln!("  [warmup] total={:.1}ms", result.total_ms);
        } else {
            eprintln!(
                "  [iter {}] total={:.1}ms  read_tput={:.0} ops/s  write_tput={:.0} ops/s",
                iter - cli.warmup + 1,
                result.total_ms,
                result.reader_throughput,
                result.writer_throughput,
            );
            results.push(result);
        }
    }

    // Trim and average
    results.sort_by(|a, b| a.total_ms.partial_cmp(&b.total_ms).unwrap());
    let trim = cli.trim.min(results.len() / 2);
    let trimmed = &results[trim..results.len() - trim];

    if trimmed.is_empty() {
        eprintln!("No results after trimming!");
        return Ok(());
    }

    let n = trimmed.len() as f64;
    let avg = RunResult {
        reader_throughput: trimmed.iter().map(|r| r.reader_throughput).sum::<f64>() / n,
        writer_throughput: trimmed.iter().map(|r| r.writer_throughput).sum::<f64>() / n,
        total_ms: trimmed.iter().map(|r| r.total_ms).sum::<f64>() / n,
        total_probes: trimmed[0].total_probes,
        total_updates: trimmed[0].total_updates,
    };

    eprintln!("\n=== Average (trimmed) ===");
    eprintln!("  total_ms:    {:.1}", avg.total_ms);
    eprintln!("  read_tput:   {:.0} ops/s", avg.reader_throughput);
    eprintln!("  write_tput:  {:.0} ops/s", avg.writer_throughput);

    if let Some(ref csv_path) = cli.output_csv {
        write_csv(csv_path, &cli, &avg)?;
        eprintln!("  -> wrote {}", csv_path);
    }

    Ok(())
}
