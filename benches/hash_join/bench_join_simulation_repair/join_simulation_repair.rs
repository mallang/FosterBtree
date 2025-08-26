#!/usr/bin/env rustc
// gen_res.rs
// Consistency check for the MVCC hash join table.
//
// This program:
// 1. Reads table_0.csv and applies update TX log (e.g. "txs_u0.5_t0.csv") to build the index.
// 2. Checks update consistency by comparing a full index scan (via scan_all) against expected update result files
//    in the "res" subdirectory (res_{label}_t0_r.csv and res_{label}_t0_h.csv).
// 3. Discovers join TX log files (txs_join_ts{X}_t{Y}.csv), applies each join log,
//    and for each expected join block (from an expected result file named "res_join_{label}_ts{X}_t{Y}.csv")
//    performs a join scan consistency check.
//
// Expected join result files should have a header line of the form:
//   --- Scan for join_key: <JOIN_KEY> at ts=<TS> ---,,,,
// followed by CSV rows with columns: start_ts, end_ts, pkey, join_key, value.
// The expected "value" field is decoded (i.e. without the "(16)" suffix) for comparison.

use fbtree::mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccEntry, MvccIndex};
use fbtree::prelude::*;

use rand::seq::SliceRandom;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

#[derive(PartialEq)]
enum RepairType {
    NoRepair,
    ReadRepair,
    WriteRepair,
}

// -----------------------------------------------------------------------------
// 1) Data Structures
// -----------------------------------------------------------------------------
struct TableRow {
    pkey: Vec<u8>,
    join_key: Vec<u8>,
    value: Vec<u8>,
}

struct TxOperation {
    tx_id: u64,
    ts: u64,
    op: String,
    pkey: Vec<u8>,
    join_key: Vec<u8>,
    value: Vec<u8>,
}

// For expected join result rows; we only compare pkey and decoded value.
#[derive(Debug, PartialEq, Eq, Clone)]
struct ExpectedRow {
    pkey: String,
    join_key: String,
    value: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ExpectedUpdateRow {
    key: Vec<u8>,
    pkey: Vec<u8>,
    start_ts: u64,
    end_ts: u64,
    value: Vec<u8>,
}

// -----------------------------------------------------------------------------
// 2) Utility Functions
// -----------------------------------------------------------------------------

/// Decode a value string in the form "BASESTRING(TOTAL_LENGTH)".
/// If it matches, it returns a byte vector with the base repeated (without any suffix);
/// otherwise, it returns the raw bytes.
fn decode_base_value(value_str: &str) -> Vec<u8> {
    lazy_static::lazy_static! {
        static ref BASE_VALUE_RE: Regex = Regex::new(r"^(.*)\((\d+)\)$").unwrap();
    }
    if let Some(cap) = BASE_VALUE_RE.captures(value_str) {
        let base_str = &cap[1];
        let total_len_str = &cap[2];
        if let Ok(total_len) = total_len_str.parse::<usize>() {
            let base_bytes = base_str.as_bytes();
            let base_len = base_bytes.len();
            if base_len == 0 {
                return vec![0u8; total_len];
            }
            let repeat_count = total_len / base_len;
            let remainder = total_len % base_len;
            let mut result = Vec::with_capacity(total_len);
            for _ in 0..repeat_count {
                result.extend_from_slice(base_bytes);
            }
            if remainder > 0 {
                result.extend_from_slice(&base_bytes[..remainder]);
            }
            return result;
        }
    }
    value_str.as_bytes().to_vec()
}

/// Decode an expected value string (removes the "(N)" suffix) and returns a String.
fn decode_expected_value(value_str: &str) -> String {
    // We simply decode the value and convert it to a UTF-8 string.
    let decoded: Vec<u8> = decode_base_value(value_str);
    String::from_utf8_lossy(&decoded).to_string()
}

/// Read TX CSV which may have either 4 columns (for join scan logs) or 6 columns.
fn read_ops_csv(filepath: &PathBuf, rp: &RepairType) -> io::Result<Vec<TxOperation>> {
    let file = File::open(filepath)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    let mut ops = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let num_cols = record.len();
        if num_cols == 6 {
            // Format: tx_id, ts, op, pkey, join_key, value
            let tx_id = record[0].parse::<u64>().unwrap_or(0);
            let ts = record[1].parse::<u64>().unwrap_or(0);
            let op = record[2].to_string();
            let pkey = record[3].as_bytes().to_vec();
            let join_key = record[4].as_bytes().to_vec();
            let expanded_value = decode_base_value(&record[5]);
            if rp == &RepairType::WriteRepair && op == "update" {
                ops.push(TxOperation {
                    tx_id,
                    ts,
                    op: String::from("update_write_repair"),
                    pkey,
                    join_key,
                    value: expanded_value,
                })
            } else {
                ops.push(TxOperation {
                    tx_id,
                    ts,
                    op,
                    pkey,
                    join_key,
                    value: expanded_value,
                });
            }
        } else {
            eprintln!(
                "Invalid record with {} columns in {}: {:?}",
                num_cols,
                filepath.display(),
                record
            );
            panic!("invalid record!")
        }
    }
    Ok(ops)
}

fn read_deltas_and_apply(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
    rp: &RepairType,
) -> Result<(), Box<dyn Error>> {
    let ops = read_ops_csv(filepath, rp)?;
    let mut op_count = 0;
    let start = Instant::now();
    let mut delta_cnt = 0;
    for op in &ops {
        match op.op.as_str() {
            "insert" => {
                hash_join_table.insert(
                    op.join_key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
                op_count += 1;
            }
            "update" => {
                hash_join_table.update(
                    op.join_key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
                op_count += 1;
            }
            "delete" => {
                hash_join_table.delete(&op.join_key, &op.pkey, op.ts, op.tx_id)?;
                op_count += 1;
            }
            "get" => {
                let _ = hash_join_table.get(&op.join_key, &op.pkey, op.ts)?;
                op_count += 1;
            }
            "split_delta" => {
                hash_join_table.split_at_ts(op.ts)?;
                delta_cnt += 1;
            }
            "tx_begin" | "tx_commit" => { /* no-op */ }
            "update_write_repair" => {
                hash_join_table.update_write_repair(
                    op.join_key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
                op_count += 1;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }
    let duration = start.elapsed();
    println!(
        "Applied all deltas with op count: {:?} delta count: {:?} from {:?} in {} ns",
        op_count,
        delta_cnt,
        filepath.file_name().unwrap_or_default(),
        duration.as_nanos()
    );
    Ok(())
}

fn read_join_keys(
    scan_key_file: &Path,
) -> Result<Vec<(Vec<u8>, Timestamp)>, Box<dyn std::error::Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(scan_key_file)?;
    let mut key_with_ts = Vec::new();
    for record in rdr.records() {
        let rec = record?;
        if rec.len() > 1 {
            let k = rec[0].as_bytes().to_vec();
            let ts = rec[1].parse::<Timestamp>()?;
            key_with_ts.push((k, ts));
        }
    }
    Ok(key_with_ts)
}

fn read_join_keys_and_apply(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
    test_name: &String,
    rp: &RepairType,
) -> Result<(), Box<dyn Error>> {
    let join_keys = read_join_keys(filepath)?;
    let mut join_key_count = 0;
    let start = Instant::now();
    if rp == &RepairType::ReadRepair {
        for join_key in &join_keys {
            hash_join_table.scan_key_vec_read_repair(&join_key.0, join_key.1)?;
            join_key_count += 1;
        }
    } else {
        for join_key in &join_keys {
            hash_join_table.scan_key_vec(&join_key.0, join_key.1)?;
            join_key_count += 1;
        }
    }
    let duration = start.elapsed();
    println!(
        "[test_name: {:?}] Applied {:?} ops of join_key_scan from {:?} in {} ns",
        test_name,
        join_key_count,
        filepath.file_name().unwrap_or_default(),
        duration.as_nanos()
    );
    Ok(())
}

fn bench_join_keys(
    csv_dir: PathBuf,
    join_keys_csv_name_prefix: &String,
    hash_join_table: &mut BoxMvccIndexMemPool,
    rp: &RepairType,
) -> Result<(), Box<dyn Error>> {
    let join_keys_csv_history_path =
        csv_dir.join(format!("{}{}", join_keys_csv_name_prefix, "_history.csv"));
    let join_keys_csv_recent_path =
        csv_dir.join(format!("{}{}", join_keys_csv_name_prefix, "_recent.csv"));
    read_join_keys_and_apply(
        &join_keys_csv_history_path,
        hash_join_table,
        &format!("history_scan_keys"),
        rp,
    )?;
    read_join_keys_and_apply(
        &join_keys_csv_recent_path,
        hash_join_table,
        &format!("recent_scan_keys"),
        rp,
    )?;
    Ok(())
}

// -----------------------------------------------------------------------------
// 5) Default CSV Directory Resolver
// -----------------------------------------------------------------------------
fn default_csv_path_relative_to_bin() -> PathBuf {
    if let Ok(exe_path) = env::current_exe() {
        let mut exe_dir = exe_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        exe_dir.push("..");
        exe_dir.push("..");
        exe_dir.push("benches");
        exe_dir.push("hash_join");
        exe_dir.push("join_simulation");
        exe_dir.push("csv");
        return exe_dir;
    }
    PathBuf::from("../../benches/hash_join/join_simulation/csv")
}

// -----------------------------------------------------------------------------
// 6) Main Driver
// -----------------------------------------------------------------------------
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    // CLI defaults
    let mut csv_dir = default_csv_path_relative_to_bin();
    let mut deltas_csv_name = String::from("table_deltas.csv");
    let mut join_keys_csv_name_prefix = String::from("join_keys.csv");

    let mut hash_table_t = HashTableType::HeapTable; // default
    let mut repair_t = RepairType::NoRepair;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--csv-dir" => {
                if i + 1 < args.len() {
                    csv_dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --csv-dir requires a path");
                    return Ok(());
                }
            }
            "--deltas" => {
                if i + 1 < args.len() {
                    deltas_csv_name = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --deltas requires a filename");
                    return Ok(());
                }
            }
            "--join-keys_prefix" => {
                if i + 1 < args.len() {
                    join_keys_csv_name_prefix = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --join-keys requires a filename");
                    return Ok(());
                }
            }
            "-t" => {
                if i + 1 < args.len() {
                    let type_name = &args[i + 1];
                    match type_name.as_str() {
                        "chain" => hash_table_t = HashTableType::RecentHistoryChained,
                        "heap" => hash_table_t = HashTableType::HeapTable,
                        "rust" => hash_table_t = HashTableType::RustHashMap,
                        "ts_partition" => hash_table_t = HashTableType::TsPartitionChained,
                        other => eprintln!("Unknown table type: {}", other),
                    }
                    i += 2;
                } else {
                    eprintln!("Error: -t requires a type name");
                    return Ok(());
                }
            }
            "-rp" => {
                if i + 1 < args.len() {
                    let type_name = &args[i + 1];
                    match type_name.as_str() {
                        "no_repair" => repair_t = RepairType::NoRepair,
                        "read_repair" => repair_t = RepairType::ReadRepair,
                        "write_repair" => repair_t = RepairType::WriteRepair,
                        other => eprintln!("Unknown table type: {}", other),
                    }
                    i += 2;
                } else {
                    eprintln!("Error: -rp requires a type name");
                    return Ok(());
                }
            }
            unknown => {
                eprintln!("Unknown arg: {}", unknown);
                i += 1;
            }
        }
    }

    if !csv_dir.exists() {
        eprintln!("CSV directory does not exist: {:?}", csv_dir);
        return Ok(());
    }
    let delta_csv_path = csv_dir.join(&deltas_csv_name);

    println!("Using CSV directory: {:?}", csv_dir);
    println!(
        "delta_csv: {:?}",
        delta_csv_path.file_name().unwrap_or_default()
    );
    println!("join_keys_csv prefix: {:?}", join_keys_csv_name_prefix);
    println!("Hash Table Type: {:?}", hash_table_t);

    // 1) Create the hash join table.
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let mut hash_join_table: BoxMvccIndexMemPool = match hash_table_t {
        HashTableType::RecentHistoryChained => {
            Box::new(ChainedHashTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::HeapTable => {
            Box::new(HeapHashTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::RustHashMap => {
            Box::new(MvccRustHashMap::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::TsPartitionChained => {
            Box::new(TsPartitionedTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
    };

    // // 2) Insert base table rows.
    // println!("\n=== Inserting base table rows ===");
    // read_table_0_and_insert(&delta_csv_path, &mut hash_join_table)?;

    // 3) Apply insert and update deltas.
    println!("\n=== Applying Deltas ===");
    read_deltas_and_apply(&delta_csv_path, &mut hash_join_table, &repair_t)?;

    // 4) Scan keys in hash_join_table
    println!("\n=== Applying Scan Keys ===");
    // TODO: scan delta
    bench_join_keys(
        csv_dir,
        &join_keys_csv_name_prefix,
        &mut hash_join_table,
        &repair_t,
    )?;

    Ok(())
}
