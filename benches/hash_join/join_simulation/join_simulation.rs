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

use fbtree::mvcc_index::hash_heap::hash_heap_table::HashHeapTable;
use fbtree::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hashtable_mu::mvcc_hash_join_table::OpenAddrHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
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
    let decoded = decode_base_value(value_str);
    String::from_utf8_lossy(&decoded).to_string()
}

/// Read table_0.csv into a Vec<TableRow>
fn read_table_0_csv(filepath: &PathBuf) -> io::Result<Vec<TableRow>> {
    let file = File::open(filepath)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    let mut rows = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if record.len() < 3 {
            eprintln!("Invalid record in table_0.csv: {:?}", record);
            continue;
        }
        let pkey = record[0].as_bytes().to_vec();
        let join_key = record[1].as_bytes().to_vec();
        let expanded_value = decode_base_value(&record[2]);
        rows.push(TableRow {
            pkey,
            join_key,
            value: expanded_value,
        });
    }
    Ok(rows)
}

/// Read TX CSV which may have either 4 columns (for join scan logs) or 6 columns.
fn read_txs_csv(filepath: &PathBuf) -> io::Result<Vec<TxOperation>> {
    let file = File::open(filepath)?;
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);
    let mut ops = Vec::new();
    for result in rdr.records() {
        let record = result?;
        let num_cols = record.len();
        if num_cols == 4 {
            // Format: tx_id, ts, op, join_key
            let tx_id = record[0].parse::<u64>().unwrap_or(0);
            let ts = record[1].parse::<u64>().unwrap_or(0);
            let op = record[2].to_string();
            let join_key = record[3].as_bytes().to_vec();
            ops.push(TxOperation {
                tx_id,
                ts,
                op,
                pkey: Vec::new(),
                join_key,
                value: Vec::new(),
            });
        } else if num_cols == 6 {
            // Format: tx_id, ts, op, pkey, join_key, value
            let tx_id = record[0].parse::<u64>().unwrap_or(0);
            let ts = record[1].parse::<u64>().unwrap_or(0);
            let op = record[2].to_string();
            let pkey = record[3].as_bytes().to_vec();
            let join_key = record[4].as_bytes().to_vec();
            let expanded_value = decode_base_value(&record[5]);
            ops.push(TxOperation {
                tx_id,
                ts,
                op,
                pkey,
                join_key,
                value: expanded_value,
            });
        } else {
            eprintln!(
                "Invalid record with {} columns in {}: {:?}",
                num_cols,
                filepath.display(),
                record
            );
        }
    }
    Ok(ops)
}

fn read_expected_update_csv(file: &Path) -> io::Result<Vec<ExpectedUpdateRow>> {
    let mut results = Vec::new();
    let rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file)?;
    for record in rdr.into_records() {
        let record = record?;
        if record.len() < 5 {
            // We expect at least 5 columns: key, pkey, start_ts, end_ts, value
            eprintln!("Invalid row in {}: {:?}", file.display(), record);
            continue;
        }
        let start_str = &record[0]; // e.g. "0"
        let end_str = &record[1]; // e.g. ""
        let pkey_str = &record[2]; // e.g. "44Gt3L6b"
        let join_key_str = &record[3];
        let value_str = &record[4]; // e.g. "dDI1QGTYf3r24HSY(16)"

        // 1) Parse start_ts/end_ts as u64
        let start_ts = start_str.parse::<u64>().unwrap_or(0);
        let end_ts = parse_end_ts(end_str);

        // 2) Convert pkey/join_key to bytes as ASCII
        let pkey_bytes = pkey_str.as_bytes().to_vec();
        let join_key_bytes = join_key_str.as_bytes().to_vec();

        // 3) Decode the value
        let value_bytes = decode_base_value(value_str);

        results.push(ExpectedUpdateRow {
            key: join_key_bytes,
            pkey: pkey_bytes,
            start_ts,
            end_ts,
            value: value_bytes,
        });
    }
    Ok(results)
}

fn read_expected_rows(file: &Path) -> Result<Vec<ExpectedUpdateRow>, Box<dyn std::error::Error>> {
    if !file.exists() {
        // If the file doesn't exist, return an empty vector or an error; your choice
        eprintln!("File {:?} not found; returning empty data set", file);
        return Ok(Vec::new());
    }
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(file)?;
    let mut rows = Vec::new();
    for record in rdr.records() {
        let record = record?;
        if record.len() < 5 {
            // Expecting at least 5 columns: start_ts, end_ts, pkey, join_key, value
            continue;
        }
        let start_ts = record[0].parse::<u64>().unwrap_or(0);
        let end_ts = parse_end_ts(&record[1]);
        let pkey_bytes = record[2].as_bytes().to_vec();
        let key_bytes = record[3].as_bytes().to_vec();
        let value_bytes = decode_base_value(&record[4]); // your existing decoding
        rows.push(ExpectedUpdateRow {
            start_ts,
            end_ts,
            pkey: pkey_bytes,
            key: key_bytes,
            value: value_bytes,
        });
    }
    Ok(rows)
}

/// Return all expected rows for the given `(key, ts)`.
fn expected_visible_for_key_ts(
    data: &[ExpectedUpdateRow],
    key: &[u8],
    ts: u64,
) -> Vec<ExpectedUpdateRow> {
    data.iter()
        .filter(|row| row.key == key && row.start_ts <= ts && ts < row.end_ts)
        .cloned()
        .collect()
}

fn max_timestamp(rows: &[ExpectedUpdateRow]) -> u64 {
    rows.iter()
        .map(|r| r.end_ts)
        .filter(|&t| t != u64::MAX)
        .max()
        .unwrap_or(100)
}

fn load_expected_data(
    recent_file: &Path,
    history_file: &Path,
) -> Result<Vec<ExpectedUpdateRow>, Box<dyn std::error::Error>> {
    let mut combined = Vec::new();
    let recent = read_expected_rows(recent_file)?;
    let history = read_expected_rows(history_file)?;
    combined.extend(recent);
    combined.extend(history);
    Ok(combined)
}

fn mvcc_entry_to_expected_row(e: &MvccEntry) -> ExpectedUpdateRow {
    ExpectedUpdateRow {
        key: e.key.clone(),
        pkey: e.pkey.clone(),
        start_ts: e.start_ts,
        end_ts: e.end_ts,
        value: e.value.clone(),
    }
}

fn parse_end_ts(field: &str) -> u64 {
    if field.is_empty() {
        u64::MAX
    } else {
        field.parse::<u64>().unwrap_or(u64::MAX)
    }
}

fn read_key_pool(pool_file: &Path) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(pool_file)?;
    let mut keys = Vec::new();
    for record in rdr.records() {
        let rec = record?;
        if rec.len() > 0 {
            let k = rec[0].as_bytes().to_vec();
            keys.push(k);
        }
    }
    Ok(keys)
}

// -----------------------------------------------------------------------------
// 3) TX Application Functions (for update and join logs)
// -----------------------------------------------------------------------------
fn read_table_0_and_insert(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<(), Box<dyn Error>> {
    let rows = read_table_0_csv(filepath)?;
    let insert_count = rows.len();
    let start = Instant::now();
    let init_tx_id = 0;
    let init_ts = 0;
    for row in rows {
        hash_join_table.insert(row.join_key, row.pkey, init_ts, init_tx_id, row.value)?;
    }
    let duration = start.elapsed();
    println!(
        "Inserted {} rows from {:?} in {} ns",
        insert_count,
        filepath.file_name().unwrap_or_default(),
        duration.as_nanos()
    );
    Ok(())
}

fn read_txs_and_apply(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<(), Box<dyn Error>> {
    let ops = read_txs_csv(filepath)?;
    let mut op_count = 0;
    let start = Instant::now();
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
            "tx_begin" | "tx_commit" => { /* no-op */ }
            "scan_with_join_key" => {
                // For scan operations, we call scan_key but suppress printing
                let _ = hash_join_table.scan_key(&op.join_key, op.ts)?;
                op_count += 1;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }
    let duration = start.elapsed();
    println!(
        "Applied {} operations from {:?} in {} ns",
        op_count,
        filepath.file_name().unwrap_or_default(),
        duration.as_nanos()
    );
    Ok(())
}

// -----------------------------------------------------------------------------
// 4) Consistency Check Functions for Updates and Joins
// -----------------------------------------------------------------------------

/// Read an expected join result CSV file into a mapping from join_key to (ts, Vec<ExpectedRow>).
fn parse_expected_join_file(
    expected_path: &Path,
) -> io::Result<HashMap<String, (u64, Vec<ExpectedRow>)>> {
    let file = File::open(expected_path)?;
    let reader = BufReader::new(file);

    // Header lines start with: --- Scan for join_key: <JOIN_KEY> at ts=<TS> ---
    let header_regex =
        Regex::new(r"^---\s*Scan for join_key:\s*(\S+)\s+at ts=(\d+)\s*---").unwrap();
    let mut expected_map: HashMap<String, (u64, Vec<ExpectedRow>)> = HashMap::new();

    let mut current_key: Option<String> = None;
    let mut current_ts: u64 = 0;
    let mut current_rows: Vec<ExpectedRow> = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let line_trim = line.trim();
        if line_trim.is_empty() {
            continue;
        }
        if line_trim.starts_with("---") {
            // Save previous block if it exists
            if let Some(ref key) = current_key {
                expected_map.insert(key.clone(), (current_ts, current_rows.clone()));
            }
            // Parse the new header line
            if let Some(caps) = header_regex.captures(line_trim) {
                current_key = Some(caps[1].to_string());
                current_ts = caps[2].parse::<u64>().unwrap_or(0);
                current_rows.clear();
            } else {
                eprintln!("Warning: Unable to parse header line: {}", line_trim);
                current_key = None;
                current_rows.clear();
            }
        } else {
            // Expect CSV row with: start_ts,end_ts,pkey,join_key,value
            let parts: Vec<&str> = line_trim.split(',').collect();
            if parts.len() < 5 {
                continue;
            }
            // For consistency, compare only pkey and the decoded value
            let exp = ExpectedRow {
                pkey: parts[2].trim().to_string(),
                join_key: parts[3].trim().to_string(),
                value: decode_expected_value(parts[4].trim()),
            };
            current_rows.push(exp);
        }
    }
    // Insert the last block if present
    if let Some(ref key) = current_key {
        expected_map.insert(key.clone(), (current_ts, current_rows.clone()));
    }

    Ok(expected_map)
}

/// Checks join consistency for a given join_key and timestamp.
/// Returns `true` if the check passed, `false` if it failed (mismatch).
fn check_full_join_consistency(
    hash_join_table: &BoxMvccIndexMemPool,
    join_key: &str,
    ts: u64,
    expected: &[ExpectedRow],
) -> Result<bool, Box<dyn Error>> {
    // Perform the actual join scan
    let join_key_bytes = join_key.as_bytes().to_vec();
    let scan_iter = hash_join_table.scan_key(&join_key_bytes, ts)?;
    let mut actual: Vec<(String, String)> = scan_iter
        .map(|(pkey, value)| {
            (
                String::from_utf8_lossy(&pkey).to_string(),
                String::from_utf8_lossy(&value).to_string(),
            )
        })
        .collect();

    // Convert expected rows into (pkey, value) pairs
    let mut expected_vec: Vec<(String, String)> = expected
        .iter()
        .map(|r| (r.pkey.clone(), r.value.clone()))
        .collect();

    // Sort before comparing, so ordering differences won't cause a false mismatch
    actual.sort();
    expected_vec.sort();

    if actual == expected_vec {
        // Passed
        Ok(true)
    } else {
        // Failed: print the diff, return false
        println!(
            "[ERROR] Join consistency check FAILED for key '{}' at ts {}.",
            join_key, ts
        );
        println!("Expected:");
        for (p, v) in &expected_vec {
            println!("  pkey: {}, value: {}", p, v);
        }
        println!("Actual:");
        for (p, v) in &actual {
            println!("  pkey: {}, value: {}", p, v);
        }
        println!("---\n");

        Ok(false)
    }
}

/// Reads two CSV files (expected recent & expected history) and does a
/// detailed row‑by‑row comparison against a full `scan_all()` of the index.
/// Prints missing/extra rows if there's a mismatch.
fn full_check_update_consistency(
    hash_join_table: &BoxMvccIndexMemPool,
    expected_recent: &Path,
    expected_history: &Path,
) -> Result<(), Box<dyn Error>> {
    // 1) Full scan of the index
    let scan_iter = hash_join_table.scan_all()?;
    // Convert the scanned MvccEntry items to our ExpectedUpdateRow form
    let actual_rows: Vec<ExpectedUpdateRow> = scan_iter
        .map(|mvcc_e| mvcc_entry_to_expected_row(&mvcc_e))
        .collect();

    // Turn them into a set for easier diff
    let actual_set: HashSet<ExpectedUpdateRow> = actual_rows.into_iter().collect();

    // 2) Read the “expected” rows from the two CSVs
    let expected_r = read_expected_update_csv(expected_recent).unwrap_or_default();
    let expected_h = read_expected_update_csv(expected_history).unwrap_or_default();

    // Combine them into one
    let mut all_expected = Vec::with_capacity(expected_r.len() + expected_h.len());
    all_expected.extend(expected_r);
    all_expected.extend(expected_h);

    let expected_set: HashSet<ExpectedUpdateRow> = all_expected.into_iter().collect();

    // 3) Compare sets
    let missing_in_actual = expected_set.difference(&actual_set).collect::<Vec<_>>();
    let extra_in_actual = actual_set.difference(&expected_set).collect::<Vec<_>>();

    if missing_in_actual.is_empty() && extra_in_actual.is_empty() {
        // Perfect match
        println!("Update consistency check PASSED: actual rows match expected rows exactly.");
    } else {
        println!("[ERROR] Update consistency check FAILED: mismatch in actual vs. expected rows.");
        if !missing_in_actual.is_empty() {
            println!("  Missing from actual (in expected, but not found in scan):");
            for row in &missing_in_actual {
                println!("    {:?}", row);
            }
        }
        if !extra_in_actual.is_empty() {
            println!("  Extra in actual (found in scan, but not in expected):");
            for row in &extra_in_actual {
                println!("    {:?}", row);
            }
        }
    }
    Ok(())
}

fn random_partial_update_consistency(
    hash_join_table: &BoxMvccIndexMemPool,
    recent_csv: &Path,
    history_csv: &Path,
    key_pool_csv: &Path,
    sample_count_ts: usize,  // how many random timestamps to test
    sample_percent_key: f64, // e.g. 0.1 means 10% of keys
) -> Result<(), Box<dyn std::error::Error>> {
    // 1) Load all expected data
    let all_expected = load_expected_data(recent_csv, history_csv)?;
    // 2) Read the key pool
    let key_pool = read_key_pool(key_pool_csv)?;
    if key_pool.is_empty() {
        eprintln!("Key pool is empty, skipping partial check.");
        return Ok(());
    }
    // 3) Find the maximum relevant timestamp in all_expected
    let max_ts = all_expected.iter().map(|r| r.start_ts).max().unwrap_or(100);

    // 4) Setup RNG
    let mut rng = SmallRng::from_entropy();

    let mut checks_done = 0usize;
    let mut fails = 0usize;

    // 5) For each random pick of ts:
    for _ in 0..sample_count_ts {
        // pick a random ts in [0..=max_ts]
        let ts = rng.gen_range(0..=max_ts);

        // pick the subset of keys from the key_pool
        // e.g. 10% means (key_pool.len() as f64 * 0.1).round() as usize
        let sample_size = (key_pool.len() as f64 * sample_percent_key).ceil() as usize;
        if sample_size == 0 {
            continue;
        }
        // shuffle the key_pool or pick random distinct indices
        // simplest approach is to shuffle a clone
        let mut shuffled_keys = key_pool.clone();
        shuffled_keys.shuffle(&mut rng);
        let subset = &shuffled_keys[..sample_size.min(shuffled_keys.len())];

        // 6) For each key in the subset, do scan_key and compare
        for key in subset {
            checks_done += 1;

            // a) get the actual results
            let actual_iter = hash_join_table.scan_key(key, ts)?;
            let mut actual: Vec<(Vec<u8>, Vec<u8>)> = actual_iter.map(|(p, v)| (p, v)).collect();
            actual.sort();

            // b) get the expected results (filter all_expected)
            let expected_rows = expected_visible_for_key_ts(&all_expected, key, ts);
            let mut expected: Vec<(Vec<u8>, Vec<u8>)> = expected_rows
                .into_iter()
                .map(|r| (r.pkey, r.value))
                .collect();
            expected.sort();

            // c) compare them
            if actual != expected {
                fails += 1;
                println!(
                    "[ERROR] Mismatch at ts={} key={:?}",
                    ts,
                    String::from_utf8_lossy(key)
                );
                println!("  Expected:");
                for (p, v) in &expected {
                    println!(
                        "    pkey={:?}, value={:?}",
                        String::from_utf8_lossy(p),
                        String::from_utf8_lossy(v)
                    );
                }
                println!("  Actual:");
                for (p, v) in &actual {
                    println!(
                        "    pkey={:?}, value={:?}",
                        String::from_utf8_lossy(p),
                        String::from_utf8_lossy(v)
                    );
                }
                println!("---");
            }
        }
    }

    println!(
        "Random partial consistency check done: {} checks, {} failures.",
        checks_done, fails
    );
    if fails == 0 {
        println!("All random partial checks passed!");
    }
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
    let mut table0_name = String::from("table_0.csv");
    let mut txs0_name = String::from("txs_u0.1_t0.csv"); // update TX log file
    let mut hash_table_t = HashTableType::HeapTable; // default

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
            "--table0" => {
                if i + 1 < args.len() {
                    table0_name = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --table0 requires a filename");
                    return Ok(());
                }
            }
            "--txs0" => {
                if i + 1 < args.len() {
                    txs0_name = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --txs0 requires a filename");
                    return Ok(());
                }
            }
            "-t" => {
                if i + 1 < args.len() {
                    let type_name = &args[i + 1];
                    match type_name.as_str() {
                        "open_address" => hash_table_t = HashTableType::OpenAddressing,
                        "chain" => hash_table_t = HashTableType::Chained,
                        "heap" => hash_table_t = HashTableType::HeapTable,
                        "rust" => hash_table_t = HashTableType::RustHashMap,
                        other => eprintln!("Unknown table type: {}", other),
                    }
                    i += 2;
                } else {
                    eprintln!("Error: -t requires a type name");
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
    let table0_path = csv_dir.join(&table0_name);
    let txs0_path = csv_dir.join(&txs0_name);

    println!("Using CSV directory: {:?}", csv_dir);
    println!(
        "Base table: {:?}",
        table0_path.file_name().unwrap_or_default()
    );
    println!(
        "Update TX log: {:?}",
        txs0_path.file_name().unwrap_or_default()
    );
    println!("Hash Table Type: {:?}", hash_table_t);

    // Derive update label from txs0 filename.
    let txs0_filename = txs0_path.file_name().unwrap().to_string_lossy();
    let update_label = if txs0_filename.starts_with("txs_") && txs0_filename.ends_with("_t0.csv") {
        &txs0_filename[4..(txs0_filename.len() - 7)]
    } else {
        "default"
    };
    println!("Derived update label: {}", update_label);

    // Expected update result files in "res" subdirectory
    let res_dir = csv_dir.join("res");
    if !res_dir.exists() {
        fs::create_dir_all(&res_dir)?;
        println!("Created res directory: {:?}", res_dir);
    }
    let expected_recent = res_dir.join(format!("res_{}_t0_r.csv", update_label));
    let expected_history = res_dir.join(format!("res_{}_t0_h.csv", update_label));
    println!("Expected update result files:");
    println!("  Recent: {:?}", expected_recent);
    println!("  History: {:?}", expected_history);

    // 1) Create the hash join table.
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let mut hash_join_table: BoxMvccIndexMemPool = match hash_table_t {
        HashTableType::Chained => {
            Box::new(ChainedHashTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::OpenAddressing => {
            Box::new(OpenAddrHashTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::HeapTable => {
            Box::new(HashHeapTable::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
        HashTableType::RustHashMap => {
            Box::new(MvccRustHashMap::create(c_key, mem_pool.clone())?) as BoxMvccIndexMemPool
        }
    };

    // 2) Insert base table rows.
    println!("\n=== Inserting base table rows ===");
    read_table_0_and_insert(&table0_path, &mut hash_join_table)?;

    // 3) Apply update TX log.
    println!("\n=== Applying update TX log ===");
    read_txs_and_apply(&txs0_path, &mut hash_join_table)?;

    // 4) Check update consistency.

    // println!("\n=== Checking full consistency for updated table_0 ===");
    // full_check_update_consistency(&hash_join_table, &expected_recent, &expected_history)?;

    // println!("\n=== Partial random consistency check ===");
    // random_partial_update_consistency(
    //     &hash_join_table,
    //     &expected_recent,
    //     &expected_history,
    //     &csv_dir.join("key_pool.csv"),
    //     10,     // e.g. pick 10 random ts
    //     0.1     // e.g. check 10% of keys each time
    // )?;

    // 5) Discover join TX logs: files matching "txs_join_ts(\d+)_t(\d+).csv"
    println!("\n=== Discovering join TX logs ===");
    let join_pattern = Regex::new(r"^txs_join_ts(\d+)_t(\d+)\.csv$").unwrap();
    let mut join_files = Vec::new();
    for entry in fs::read_dir(&csv_dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let fname_str = fname.to_string_lossy();
        if let Some(capt) = join_pattern.captures(&fname_str) {
            let ts_val: u64 = capt[1].parse().unwrap_or(0);
            let table_idx: u64 = capt[2].parse().unwrap_or(0);
            join_files.push((fname_str.into_owned(), ts_val, table_idx));
        }
    }
    join_files.sort_by_key(|(_, ts, tid)| (*tid, *ts));
    if join_files.is_empty() {
        println!("No join TX logs found.");
    } else {
        println!("Discovered {} join TX log(s):", join_files.len());
        for (fname, ts, tbl) in &join_files {
            println!("  file={}, table_idx={}, ts={}", fname, tbl, ts);
        }
        // For each join TX log, apply it and then perform a join consistency check.
        for (fname, ts_val, table_idx) in join_files {
            let join_log_path = csv_dir.join(&fname);
            println!(
                "\n=== Applying join TX log: {} (table_idx={}, ts={}) ===",
                fname, table_idx, ts_val
            );
            read_txs_and_apply(&join_log_path, &mut hash_join_table)?;
            // Construct expected join result file name:
            // Remove "_t0" from update label and add _ts{ts}_t{table_idx} suffix.

            // let expected_join = res_dir.join(format!("res_join_{}_ts{}_t{}.csv", update_label, ts_val, table_idx));
            // if !expected_join.exists() {
            //     println!("[WARN] Expected join result file {} not found; skipping check.", expected_join.display());
            //     continue;
            // }
            // println!("Checking join consistency using expected file {}", expected_join.display());
            // let expected_map = parse_expected_join_file(&expected_join)?;
            // // For each join key block in expected file, perform check.
            // let mut all_passed = true;
            // for (exp_join_key, (exp_ts, expected_rows)) in expected_map {
            //     let pass = check_full_join_consistency(&hash_join_table, &exp_join_key, exp_ts, &expected_rows)?;
            //     if !pass {
            //         all_passed = false;
            //         // Note: We do *not* break here in case you want to see *all* failures.
            //     }
            // }

            // if all_passed {
            //     // If you want no output on success, remove the next line entirely.
            //     println!("All join checks in file '{}' passed with no mismatches.", expected_join.display());
            // }
        }
    }

    // println!("\nConsistency check completed.");
    Ok(())
}
