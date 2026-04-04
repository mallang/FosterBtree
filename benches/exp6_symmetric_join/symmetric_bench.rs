use clap::{Parser, ValueEnum};
use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{MvccIndex, VersionsMap};
use fbtree::naive_hash_index::{IvmHashTable, NaiveMvHashTable};
use fbtree::prelude::*;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{metadata, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,  // DUAL
    Heap,   // MONO
    Par,    // EPOCH
    Snap,
    Ivmh,   // IVM-style incremental baseline
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RepairMode {
    Nr,
    Rr,
    Wr,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "Symmetric Hash Join: IVM delta processing with two MVHTs")]
struct Cli {
    /// R-side table file (PART)
    #[arg(long)]
    r_file: String,

    /// S-side initial table file (LINEITEM initial batch)
    #[arg(long)]
    s_file: String,

    /// Delta-R file: updates to R-side (PART updates)
    #[arg(long)]
    delta_r_file: String,

    /// Delta-S file: new rows for S-side (LINEITEM new rows)
    #[arg(long)]
    delta_s_file: String,

    /// Number of delta rounds
    #[arg(long, default_value_t = 5)]
    rounds: usize,

    /// Delta batch size per round (0 = use all deltas in one round)
    #[arg(long, default_value_t = 0)]
    delta_batch_size: usize,

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

    /// Per-round CSV (one row per round, for trend analysis)
    #[arg(long)]
    per_round_csv: Option<String>,

    /// EPOCH split frequency: split every N rounds (0 = no split during rounds).
    /// An initial split after the build phase always happens for EPOCH.
    #[arg(long, default_value_t = 0)]
    split_every: usize,
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct REntry {
    key: Vec<u8>,   // P_PARTKEY
    value: Vec<u8>, // P_TYPE
}

#[derive(Clone)]
struct SEntry {
    key: Vec<u8>,   // L_PARTKEY
    value: Vec<u8>, // revenue as bytes
}

#[derive(Clone)]
struct UpdateOp {
    key: Vec<u8>,
    new_value: Vec<u8>,
}

#[derive(Clone, Default)]
struct RoundResult {
    delta_r_extract_ms: f64,
    delta_r_probe_ms: f64,
    delta_s_extract_ms: f64,
    delta_s_probe_ms: f64,
    total_round_ms: f64,
    delta_r_count: usize,
    delta_s_count: usize,
}

#[derive(Clone, Default)]
struct FullResult {
    build_r_ms: f64,
    build_s_ms: f64,
    rounds: Vec<RoundResult>,
    total_ms: f64,
}

// ---------------------------------------------------------------------------
// File readers
// ---------------------------------------------------------------------------

const VALUE_SIZE: usize = 32;

fn normalize_value(raw: &[u8]) -> Vec<u8> {
    let mut out = vec![b' '; VALUE_SIZE];
    let n = raw.len().min(VALUE_SIZE);
    out[..n].copy_from_slice(&raw[..n]);
    out
}

fn parse_line(line: &str) -> Option<Vec<&str>> {
    let mut fields: Vec<&str> = line.split('|').collect();
    if fields.last().is_some_and(|f| f.is_empty()) {
        fields.pop();
    }
    if fields.is_empty() { None } else { Some(fields) }
}

fn read_r_table(path: &str) -> Result<Vec<REntry>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut entries = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let Some(fields) = parse_line(&line) else { continue; };
        if fields.len() < 5 { continue; }
        entries.push(REntry {
            key: fields[0].as_bytes().to_vec(),
            value: normalize_value(fields[4].as_bytes()),
        });
    }
    Ok(entries)
}

fn read_s_table(path: &str) -> Result<Vec<SEntry>, Box<dyn Error>> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() { continue; }
        let Some(fields) = parse_line(&line) else { continue; };
        if fields.len() < 3 { continue; }
        let key = fields[0].as_bytes().to_vec();
        let extended_price = fields[1].parse::<f64>().unwrap_or(0.0);
        let discount = fields[2].parse::<f64>().unwrap_or(0.0);
        let revenue = extended_price * (1.0 - discount);
        rows.push(SEntry {
            key,
            value: normalize_value(format!("{:.2}", revenue).as_bytes()),
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
        if line.trim().is_empty() { continue; }
        let Some(fields) = parse_line(&line) else { continue; };
        if fields.len() < 5 { continue; }
        ops.push(UpdateOp {
            key: fields[0].as_bytes().to_vec(),
            new_value: normalize_value(fields[4].as_bytes()),
        });
    }
    Ok(ops)
}

// ---------------------------------------------------------------------------
// MVCC table helpers
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

fn create_mvcc_table(
    table_type: TableType,
    bucket_num: usize,
) -> Result<ArcMvccIndex, Box<dyn Error>> {
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let t: ArcMvccIndex = match table_type {
        TableType::Chain => Arc::new(ChainedHashTable::create_with_bucket_num(
            c_key, mem_pool.clone(), bucket_num,
        )?),
        TableType::Heap => Arc::new(HeapHashTable::create_with_bucket_num(
            c_key, mem_pool.clone(), bucket_num,
        )?),
        TableType::Par => Arc::new(TsPartitionedTable::create_with_bucket_num(
            c_key, mem_pool.clone(), bucket_num,
        )?),
        TableType::Snap => panic!("Use run_symmetric_snap for SNAP"),
        TableType::Ivmh => panic!("Use run_symmetric_ivmh for IVMH"),
    };
    Ok(t)
}

// ---------------------------------------------------------------------------
// MVCC symmetric run
// ---------------------------------------------------------------------------

fn run_symmetric_mvcc(
    r_entries: &[REntry],
    s_entries: &[SEntry],
    delta_r_batches: &[Vec<UpdateOp>],
    delta_s_batches: &[Vec<SEntry>],
    table_type: TableType,
    repair_mode: RepairMode,
    bucket_num: usize,
    split_every: usize,
) -> Result<FullResult, Box<dyn Error>> {
    let mut result = FullResult::default();
    let total_start = Instant::now();

    // Phase 0: Build both tables
    let t0 = Instant::now();
    let table_r = create_mvcc_table(table_type, bucket_num)?;
    for (i, e) in r_entries.iter().enumerate() {
        table_r.insert(e.key.clone(), e.key.clone(), 1, i as u64, e.value.clone())?;
    }
    result.build_r_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    let table_s = create_mvcc_table(table_type, bucket_num)?;
    for (i, e) in s_entries.iter().enumerate() {
        table_s.insert(e.key.clone(), e.key.clone(), 1, i as u64, e.value.clone())?;
    }
    result.build_s_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // EPOCH: split before delta rounds
    if matches!(table_type, TableType::Par) {
        table_r.split_at_ts(2)?;
        table_s.split_at_ts(2)?;
    }

    let num_rounds = delta_r_batches.len().min(delta_s_batches.len());
    let mut nr_buf_r: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rr_dedup_r: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rr_versions_r: VersionsMap = HashMap::new();
    let mut nr_buf_s: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rr_dedup_s: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rr_versions_s: VersionsMap = HashMap::new();

    let base_ts: u64 = 10;
    let mut latest_r_ts: u64 = 1;
    let mut latest_s_ts: u64 = 1;

    for round in 0..num_rounds {
        let mut rr = RoundResult::default();
        let round_start = Instant::now();
        let ts = base_ts + (round as u64) * 2;
        let ts_s = ts + 1;

        // Step 1: apply ΔR untimed, then publish/extract the delta from the structure.
        for op in &delta_r_batches[round] {
            match repair_mode {
                RepairMode::Wr => {
                    let _ = table_r.update_write_repair(
                        op.key.clone(), op.key.clone(), ts, 0, op.new_value.clone(),
                    );
                }
                _ => {
                    let _ = table_r.update(
                        op.key.clone(), op.key.clone(), ts, 0, op.new_value.clone(),
                    );
                }
            }
        }
        let t1 = Instant::now();
        if matches!(table_type, TableType::Par)
            && split_every > 0
            && (round + 1) % split_every == 0
        {
            let _ = table_r.split_at_ts(ts + 1);
        }
        let delta_r_keys: Vec<Vec<u8>> = match repair_mode {
            RepairMode::Rr => table_r
                .delta_scan_read_repair(latest_r_ts, ts)?
                .map(|(key, _, _)| key)
                .collect(),
            _ => table_r
                .delta_scan(latest_r_ts, ts)?
                .map(|(key, _, _)| key)
                .collect(),
        };
        rr.delta_r_extract_ms = t1.elapsed().as_secs_f64() * 1000.0;
        rr.delta_r_count = delta_r_keys.len();

        // Step 1b: probe table_S with ΔR keys
        let t2 = Instant::now();
        for key in &delta_r_keys {
            let _ = match repair_mode {
                RepairMode::Nr => table_s.scan_key_vec_nr(key, latest_s_ts, &mut nr_buf_s),
                RepairMode::Rr => table_s.scan_key_vec_rr(
                    key, latest_s_ts, &mut rr_dedup_s, &mut rr_versions_s,
                ),
                RepairMode::Wr => table_s.scan_key_vec(key, latest_s_ts),
            };
        }
        rr.delta_r_probe_ms = t2.elapsed().as_secs_f64() * 1000.0;

        // Step 2: apply ΔS untimed, then publish/extract the delta from the structure.
        let s_base_id = (s_entries.len() + round * delta_s_batches[round].len()) as u64;
        for (i, e) in delta_s_batches[round].iter().enumerate() {
            let _ = table_s.insert(
                e.key.clone(), e.key.clone(), ts_s, s_base_id + i as u64, e.value.clone(),
            );
        }
        let t3 = Instant::now();
        if matches!(table_type, TableType::Par)
            && split_every > 0
            && (round + 1) % split_every == 0
        {
            let _ = table_s.split_at_ts(ts_s + 1);
        }
        let delta_s_keys: Vec<Vec<u8>> = match repair_mode {
            RepairMode::Rr => table_s
                .delta_scan_read_repair(latest_s_ts, ts_s)?
                .map(|(key, _, _)| key)
                .collect(),
            _ => table_s
                .delta_scan(latest_s_ts, ts_s)?
                .map(|(key, _, _)| key)
                .collect(),
        };
        rr.delta_s_extract_ms = t3.elapsed().as_secs_f64() * 1000.0;
        rr.delta_s_count = delta_s_keys.len();

        // Step 2b: probe table_R with ΔS keys
        let t4 = Instant::now();
        for key in &delta_s_keys {
            let _ = match repair_mode {
                RepairMode::Nr => table_r.scan_key_vec_nr(key, ts, &mut nr_buf_r),
                RepairMode::Rr => table_r.scan_key_vec_rr(
                    key, ts, &mut rr_dedup_r, &mut rr_versions_r,
                ),
                RepairMode::Wr => table_r.scan_key_vec(key, ts),
            };
        }
        rr.delta_s_probe_ms = t4.elapsed().as_secs_f64() * 1000.0;

        rr.total_round_ms = round_start.elapsed().as_secs_f64() * 1000.0;
        latest_r_ts = ts;
        latest_s_ts = ts_s;

        result.rounds.push(rr);
    }

    result.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    Ok(result)
}

// ---------------------------------------------------------------------------
// SNAP symmetric run
// ---------------------------------------------------------------------------

fn run_symmetric_snap(
    r_entries: &[REntry],
    s_entries: &[SEntry],
    delta_r_batches: &[Vec<UpdateOp>],
    delta_s_batches: &[Vec<SEntry>],
    bucket_num: usize,
) -> Result<FullResult, Box<dyn Error>> {
    let mut result = FullResult::default();
    let total_start = Instant::now();
    let mem_pool = get_in_mem_pool();
    let c_key_r = ContainerKey::new(0, 0);
    let c_key_s = ContainerKey::new(0, 1);

    let snap_r = NaiveMvHashTable::new_with_bucket_num(c_key_r, mem_pool.clone(), bucket_num);
    for e in r_entries {
        snap_r.add_insert_rec_at_ts(&e.key, &e.key, &e.value, 0);
    }
    let snap_s = NaiveMvHashTable::new_with_bucket_num(c_key_s, mem_pool.clone(), bucket_num);
    for e in s_entries {
        snap_s.add_insert_rec_at_ts(&e.key, &e.key, &e.value, 0);
    }

    // Build initial retained snapshots.
    let t0 = Instant::now();
    snap_r.build_table_from_base_and_ts(1);
    result.build_r_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let t0 = Instant::now();
    snap_s.build_table_from_base_and_ts(1);
    result.build_s_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let num_rounds = delta_r_batches.len().min(delta_s_batches.len());
    let base_ts: u64 = 10;
    let mut latest_r_ts: u64 = 1;
    let mut latest_s_ts: u64 = 1;

    for round in 0..num_rounds {
        let mut rr = RoundResult::default();
        let round_start = Instant::now();
        let ts = base_ts + (round as u64) * 2;

        // Step 1: apply ΔR untimed, then publish/extract the retained delta.
        for op in &delta_r_batches[round] {
            snap_r.add_update_rec_at_ts(&op.key, &op.key, &op.new_value, ts);
        }
        let t1 = Instant::now();
        let delta_r_keys: Vec<Vec<u8>> = snap_r
            .advance_readable_epoch_and_collect_delta(latest_r_ts, ts)?
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        rr.delta_r_extract_ms = t1.elapsed().as_secs_f64() * 1000.0;
        rr.delta_r_count = delta_r_keys.len();

        // Step 1b: probe table_S with ΔR keys
        let t2 = Instant::now();
        for key in &delta_r_keys {
            let _ = snap_s.scan_key_vec(key, latest_s_ts);
        }
        rr.delta_r_probe_ms = t2.elapsed().as_secs_f64() * 1000.0;

        // Step 2: apply ΔS untimed, then publish/extract the retained delta.
        let ts_s = ts + 1;
        for e in &delta_s_batches[round] {
            snap_s.add_insert_rec_at_ts(&e.key, &e.key, &e.value, ts_s);
        }
        let t3 = Instant::now();
        let delta_s_keys: Vec<Vec<u8>> = snap_s
            .advance_readable_epoch_and_collect_delta(latest_s_ts, ts_s)?
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        rr.delta_s_extract_ms = t3.elapsed().as_secs_f64() * 1000.0;
        rr.delta_s_count = delta_s_keys.len();

        // Step 2b: probe table_R with ΔS keys
        let t4 = Instant::now();
        for key in &delta_s_keys {
            let _ = snap_r.scan_key_vec(key, ts);
        }
        rr.delta_s_probe_ms = t4.elapsed().as_secs_f64() * 1000.0;

        rr.total_round_ms = round_start.elapsed().as_secs_f64() * 1000.0;
        latest_r_ts = ts;
        latest_s_ts = ts_s;
        result.rounds.push(rr);
    }

    result.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    Ok(result)
}

// ---------------------------------------------------------------------------
// IVMH symmetric run
// ---------------------------------------------------------------------------

fn run_symmetric_ivmh(
    r_entries: &[REntry],
    s_entries: &[SEntry],
    delta_r_batches: &[Vec<UpdateOp>],
    delta_s_batches: &[Vec<SEntry>],
    bucket_num: usize,
) -> Result<FullResult, Box<dyn Error>> {
    let mut result = FullResult::default();
    let total_start = Instant::now();
    let mem_pool = get_in_mem_pool();
    let c_key_r = ContainerKey::new(0, 0);
    let c_key_s = ContainerKey::new(0, 1);

    let ivmh_r = IvmHashTable::new_with_bucket_num(c_key_r, mem_pool.clone(), bucket_num);
    for e in r_entries {
        ivmh_r.prepare_insert_base(&e.key, &e.key, &e.value);
    }
    let t0 = Instant::now();
    ivmh_r.populate_current_from_iter(
        r_entries
            .iter()
            .map(|e| (e.key.as_slice(), e.key.as_slice(), e.value.as_slice())),
    );
    ivmh_r.cache_current_as_snapshot(1);
    result.build_r_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let ivmh_s = IvmHashTable::new_with_bucket_num(c_key_s, mem_pool.clone(), bucket_num);
    for e in s_entries {
        ivmh_s.prepare_insert_base(&e.key, &e.key, &e.value);
    }
    let t0 = Instant::now();
    ivmh_s.populate_current_from_iter(
        s_entries
            .iter()
            .map(|e| (e.key.as_slice(), e.key.as_slice(), e.value.as_slice())),
    );
    ivmh_s.cache_current_as_snapshot(1);
    result.build_s_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let num_rounds = delta_r_batches.len().min(delta_s_batches.len());
    let base_ts: u64 = 10;
    let mut latest_r_ts: u64 = 1;
    let mut latest_s_ts: u64 = 1;

    for round in 0..num_rounds {
        let mut rr = RoundResult::default();
        let round_start = Instant::now();
        let ts = base_ts + (round as u64) * 2;

        // Step 1: apply ΔR untimed, then publish/extract delta against the latest table.
        for op in &delta_r_batches[round] {
            ivmh_r.prepare_update_base(&op.key, &op.key, &op.new_value, ts);
            ivmh_r.update_current(&op.key, &op.key, &op.new_value);
        }
        let t1 = Instant::now();
        let delta_r_keys: Vec<Vec<u8>> = ivmh_r
            .advance_readable_epoch_and_collect_delta(latest_r_ts, ts)?
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        rr.delta_r_extract_ms = t1.elapsed().as_secs_f64() * 1000.0;
        rr.delta_r_count = delta_r_keys.len();

        // Step 1b: probe table_S with ΔR keys using the latest maintained hash.
        let t2 = Instant::now();
        for key in &delta_r_keys {
            let _ = ivmh_s.scan_key_vec(key, u64::MAX);
        }
        rr.delta_r_probe_ms = t2.elapsed().as_secs_f64() * 1000.0;

        // Step 2: apply ΔS untimed, then publish/extract delta against the latest table.
        let ts_s = ts + 1;
        for e in &delta_s_batches[round] {
            ivmh_s.prepare_insert_base_at_ts(&e.key, &e.key, &e.value, ts_s);
            ivmh_s.insert_current(&e.key, &e.key, &e.value);
        }
        let t3 = Instant::now();
        let delta_s_keys: Vec<Vec<u8>> = ivmh_s
            .advance_readable_epoch_and_collect_delta(latest_s_ts, ts_s)?
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        rr.delta_s_extract_ms = t3.elapsed().as_secs_f64() * 1000.0;
        rr.delta_s_count = delta_s_keys.len();

        // Step 2b: probe table_R with ΔS keys using the latest maintained hash.
        let t4 = Instant::now();
        for key in &delta_s_keys {
            let _ = ivmh_r.scan_key_vec(key, u64::MAX);
        }
        rr.delta_s_probe_ms = t4.elapsed().as_secs_f64() * 1000.0;

        rr.total_round_ms = round_start.elapsed().as_secs_f64() * 1000.0;
        latest_r_ts = ts;
        latest_s_ts = ts_s;
        result.rounds.push(rr);
    }

    result.total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
    Ok(result)
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn write_csv(
    path: &str,
    cli: &Cli,
    result: &FullResult,
) -> Result<(), Box<dyn Error>> {
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,round,rounds_total,bucket_num,\
             delta_r_count,delta_s_count,\
             build_r_ms,build_s_ms,\
             delta_r_extract_ms,delta_r_probe_ms,\
             delta_s_extract_ms,delta_s_probe_ms,\
             total_round_ms,total_ms"
        )?;
    }

    for (i, rr) in result.rounds.iter().enumerate() {
        writeln!(
            file,
            "{:?},{:?},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            cli.table_type,
            cli.repair_mode,
            i,
            result.rounds.len(),
            cli.bucket_num,
            rr.delta_r_count,
            rr.delta_s_count,
            result.build_r_ms,
            result.build_s_ms,
            rr.delta_r_extract_ms,
            rr.delta_r_probe_ms,
            rr.delta_s_extract_ms,
            rr.delta_s_probe_ms,
            rr.total_round_ms,
            result.total_ms,
        )?;
    }

    Ok(())
}

fn write_csv_avg(
    path: &str,
    cli: &Cli,
    avg: &FullResult,
) -> Result<(), Box<dyn Error>> {
    // Write averaged results (one row per config, averaging across rounds)
    let exists = Path::new(path).exists() && metadata(path)?.len() > 0;
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;

    if !exists {
        writeln!(
            file,
            "table_type,repair_mode,num_rounds,bucket_num,\
             build_r_ms,build_s_ms,\
             avg_delta_r_extract_ms,avg_delta_r_probe_ms,\
             avg_delta_s_extract_ms,avg_delta_s_probe_ms,\
             avg_total_round_ms,total_ms"
        )?;
    }

    let n = avg.rounds.len() as f64;
    if n > 0.0 {
        let avg_rr = RoundResult {
            delta_r_extract_ms: avg.rounds.iter().map(|r| r.delta_r_extract_ms).sum::<f64>() / n,
            delta_r_probe_ms: avg.rounds.iter().map(|r| r.delta_r_probe_ms).sum::<f64>() / n,
            delta_s_extract_ms: avg.rounds.iter().map(|r| r.delta_s_extract_ms).sum::<f64>() / n,
            delta_s_probe_ms: avg.rounds.iter().map(|r| r.delta_s_probe_ms).sum::<f64>() / n,
            total_round_ms: avg.rounds.iter().map(|r| r.total_round_ms).sum::<f64>() / n,
            ..Default::default()
        };

        writeln!(
            file,
            "{:?},{:?},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            cli.table_type,
            cli.repair_mode,
            avg.rounds.len(),
            cli.bucket_num,
            avg.build_r_ms,
            avg.build_s_ms,
            avg_rr.delta_r_extract_ms,
            avg_rr.delta_r_probe_ms,
            avg_rr.delta_s_extract_ms,
            avg_rr.delta_s_probe_ms,
            avg_rr.total_round_ms,
            avg.total_ms,
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    eprintln!("=== Symmetric Hash Join Benchmark ===");
    eprintln!(
        "table={:?}  repair={:?}  rounds={}  buckets={}  split_every={}",
        cli.table_type, cli.repair_mode, cli.rounds, cli.bucket_num, cli.split_every,
    );

    // Load data
    eprintln!("Loading data...");
    let r_entries = read_r_table(&cli.r_file)?;
    let s_entries = read_s_table(&cli.s_file)?;
    let delta_r_all = read_updates(&cli.delta_r_file)?;
    let delta_s_all = read_s_table(&cli.delta_s_file)?;

    eprintln!(
        "  R: {} rows, S: {} rows, ΔR: {} ops, ΔS: {} rows",
        r_entries.len(), s_entries.len(), delta_r_all.len(), delta_s_all.len(),
    );

    // Split deltas into round batches
    let rounds = cli.rounds;
    let delta_r_batch_size = if cli.delta_batch_size > 0 {
        cli.delta_batch_size
    } else {
        (delta_r_all.len() + rounds - 1) / rounds.max(1)
    };
    let delta_s_batch_size = if cli.delta_batch_size > 0 {
        cli.delta_batch_size
    } else {
        (delta_s_all.len() + rounds - 1) / rounds.max(1)
    };

    let delta_r_batches: Vec<Vec<UpdateOp>> = delta_r_all
        .chunks(delta_r_batch_size.max(1))
        .take(rounds)
        .map(|c| c.to_vec())
        .collect();
    let delta_s_batches: Vec<Vec<SEntry>> = delta_s_all
        .chunks(delta_s_batch_size.max(1))
        .take(rounds)
        .map(|c| c.to_vec())
        .collect();

    let actual_rounds = delta_r_batches.len().min(delta_s_batches.len());
    eprintln!(
        "  Rounds: {}, ΔR batch: ~{}, ΔS batch: ~{}",
        actual_rounds,
        delta_r_batches.first().map_or(0, |b| b.len()),
        delta_s_batches.first().map_or(0, |b| b.len()),
    );

    // Run iterations
    let mut all_results = Vec::new();

    for iter in 0..(cli.warmup + cli.repeat) {
        let is_warmup = iter < cli.warmup;

        let fr = match cli.table_type {
            TableType::Snap => run_symmetric_snap(
                &r_entries, &s_entries,
                &delta_r_batches, &delta_s_batches,
                cli.bucket_num,
            )?,
            TableType::Ivmh => run_symmetric_ivmh(
                &r_entries, &s_entries,
                &delta_r_batches, &delta_s_batches,
                cli.bucket_num,
            )?,
            _ => run_symmetric_mvcc(
                &r_entries, &s_entries,
                &delta_r_batches, &delta_s_batches,
                cli.table_type, cli.repair_mode, cli.bucket_num,
                cli.split_every,
            )?,
        };

        if is_warmup {
            eprintln!("  [warmup] total={:.1}ms", fr.total_ms);
        } else {
            eprintln!(
                "  [iter {}] total={:.1}ms  build_r={:.1}ms  build_s={:.1}ms  rounds={}",
                iter - cli.warmup + 1,
                fr.total_ms, fr.build_r_ms, fr.build_s_ms, fr.rounds.len(),
            );
            all_results.push(fr);
        }
    }

    // Trim and average
    all_results.sort_by(|a, b| a.total_ms.partial_cmp(&b.total_ms).unwrap());
    let trim = cli.trim.min(all_results.len() / 2);
    let trimmed = &all_results[trim..all_results.len() - trim];

    if trimmed.is_empty() {
        eprintln!("No results after trimming!");
        return Ok(());
    }

    // Average across iterations: for each round, average the per-round metrics
    let n_iter = trimmed.len() as f64;
    let n_rounds = trimmed[0].rounds.len();
    let mut avg = FullResult {
        build_r_ms: trimmed.iter().map(|r| r.build_r_ms).sum::<f64>() / n_iter,
        build_s_ms: trimmed.iter().map(|r| r.build_s_ms).sum::<f64>() / n_iter,
        total_ms: trimmed.iter().map(|r| r.total_ms).sum::<f64>() / n_iter,
        rounds: Vec::new(),
    };

    for round_idx in 0..n_rounds {
        let mut rr = RoundResult::default();
        for fr in trimmed {
            if round_idx < fr.rounds.len() {
                rr.delta_r_extract_ms += fr.rounds[round_idx].delta_r_extract_ms;
                rr.delta_r_probe_ms += fr.rounds[round_idx].delta_r_probe_ms;
                rr.delta_s_extract_ms += fr.rounds[round_idx].delta_s_extract_ms;
                rr.delta_s_probe_ms += fr.rounds[round_idx].delta_s_probe_ms;
                rr.total_round_ms += fr.rounds[round_idx].total_round_ms;
                rr.delta_r_count = fr.rounds[round_idx].delta_r_count;
                rr.delta_s_count = fr.rounds[round_idx].delta_s_count;
            }
        }
        rr.delta_r_extract_ms /= n_iter;
        rr.delta_r_probe_ms /= n_iter;
        rr.delta_s_extract_ms /= n_iter;
        rr.delta_s_probe_ms /= n_iter;
        rr.total_round_ms /= n_iter;
        avg.rounds.push(rr);
    }

    eprintln!("\n=== Average (trimmed) ===");
    eprintln!("  total_ms:  {:.1}", avg.total_ms);
    eprintln!("  build_r:   {:.1}ms", avg.build_r_ms);
    eprintln!("  build_s:   {:.1}ms", avg.build_s_ms);
    for (i, rr) in avg.rounds.iter().enumerate() {
        eprintln!(
            "  round {}: ΔR_ext={:.2}ms ΔR_probe={:.2}ms ΔS_ext={:.2}ms ΔS_probe={:.2}ms total={:.2}ms",
            i, rr.delta_r_extract_ms, rr.delta_r_probe_ms,
            rr.delta_s_extract_ms, rr.delta_s_probe_ms, rr.total_round_ms,
        );
    }

    if let Some(ref csv_path) = cli.output_csv {
        write_csv_avg(csv_path, &cli, &avg)?;
        eprintln!("  -> wrote {}", csv_path);
    }

    if let Some(ref csv_path) = cli.per_round_csv {
        write_csv(csv_path, &cli, &avg)?;
        eprintln!("  -> wrote per-round {}", csv_path);
    }

    Ok(())
}
