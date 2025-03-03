use fbtree::mvcc_index::hash_heap::hash_heap_table::HashHeapTable;
use fbtree::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hashtable_mu::mvcc_hash_join_table::OpenAddrHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccIndex};
use fbtree::prelude::*;

use regex::Regex;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::time::Instant;

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

// -----------------------------------------------------------------------------
// 2) Utility: decode_base_value + read_table_0_and_insert + read_txs_and_apply
// -----------------------------------------------------------------------------

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
    // fallback
    value_str.as_bytes().to_vec()
}

/// Insert all rows from table_0.csv
fn read_table_0_and_insert(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<(), Box<dyn Error>> {
    let rows = read_table_0_csv(filepath)?;
    let start = Instant::now();
    let mut insert_count = 0;

    let init_tx_id = 0;
    let init_ts = 0;

    for row in rows {
        hash_join_table.insert(row.join_key, row.pkey, init_ts, init_tx_id, row.value)?;
        insert_count += 1;
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

/// Apply normal ops + scans from a CSV file (4-col or 6-col lines).
fn read_txs_and_apply(
    filepath: &PathBuf,
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<(), Box<dyn Error>> {
    let ops = read_txs_csv(filepath)?;
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
            }
            "update" => {
                hash_join_table.update(
                    op.join_key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
            }
            "delete" => {
                hash_join_table.delete(&op.join_key, &op.pkey, op.ts, op.tx_id)?;
            }
            "get" => {
                let _ = hash_join_table.get(&op.join_key, &op.pkey, op.ts)?;
            }
            "tx_begin" | "tx_commit" => {
                // no-op
            }
            "scan_with_join_key" => {
                let _ = hash_join_table.scan_key(&op.join_key, op.ts)?;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }

    let duration = start.elapsed();
    println!(
        "Applied {} operations from {:?} in {} ns",
        ops.len(),
        filepath.file_name().unwrap_or_default(),
        duration.as_nanos()
    );
    Ok(())
}

/// Read table_0.csv => (pkey, join_key, value)
fn read_table_0_csv(filepath: &PathBuf) -> io::Result<Vec<TableRow>> {
    let file = File::open(filepath)?;
    let mut rows = Vec::new();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    for result in rdr.records() {
        let record = result?;
        if record.len() < 3 {
            eprintln!("Invalid record in table_0.csv: {:?}", record);
            continue;
        }
        let pkey_str = &record[0];
        let join_key_str = &record[1];
        let value_str = &record[2];

        let pkey = pkey_str.as_bytes().to_vec();
        let join_key = join_key_str.as_bytes().to_vec();
        let expanded_value = decode_base_value(value_str);

        rows.push(TableRow {
            pkey,
            join_key,
            value: expanded_value,
        });
    }
    Ok(rows)
}

/// This reads a CSV that might have 4 columns (scan row) or 6 columns (normal).
fn read_txs_csv(filepath: &PathBuf) -> io::Result<Vec<TxOperation>> {
    let file = File::open(filepath)?;
    let mut ops = Vec::new();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    for result in rdr.records() {
        let record = result?;
        let num_cols = record.len();
        if num_cols == 4 {
            // Format: tx_id, ts, op, join_key
            let tx_id = record[0].parse::<u64>().unwrap_or(0);
            let ts = record[1].parse::<u64>().unwrap_or(0);
            let op = record[2].to_string();
            let join_key_str = &record[3];
            let join_key = join_key_str.as_bytes().to_vec();

            let pkey = Vec::new();
            let value = Vec::new();

            ops.push(TxOperation {
                tx_id,
                ts,
                op,
                pkey,
                join_key,
                value,
            });
        } else if num_cols == 6 {
            // Format: tx_id, ts, op, pkey, join_key, value
            let tx_id = record[0].parse::<u64>().unwrap_or(0);
            let ts = record[1].parse::<u64>().unwrap_or(0);
            let op = record[2].to_string();
            let pkey_str = &record[3];
            let join_key_str = &record[4];
            let value_str = &record[5];

            let pkey = pkey_str.as_bytes().to_vec();
            let join_key = join_key_str.as_bytes().to_vec();
            let expanded_value = decode_base_value(value_str);

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
            continue;
        }
    }
    Ok(ops)
}

/// Finds the default CSV dir relative to the binary
fn default_csv_path_relative_to_bin() -> PathBuf {
    if let Ok(exe_path) = env::current_exe() {
        let mut exe_dir = exe_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
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
// 3) Main
// -----------------------------------------------------------------------------
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    // CLI defaults
    let mut csv_dir = default_csv_path_relative_to_bin();
    let mut table0_name = String::from("table_0.csv");
    let mut txs0_name = String::from("txs_u0.1_t0.csv");
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
                        other => {
                            eprintln!("Unknown table type: {}", other);
                        }
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

    // 1) ensure csv_dir
    if !csv_dir.exists() {
        eprintln!("CSV directory does not exist: {:?}", csv_dir);
        return Ok(());
    }
    let table0_path = csv_dir.join(&table0_name);
    let txs0_path = csv_dir.join(&txs0_name);

    println!("Using CSV directory: {:?}", csv_dir);
    println!("table_0: {:?}", table0_path.file_name().unwrap_or_default());
    println!(
        "txs_table_0: {:?}",
        txs0_path.file_name().unwrap_or_default()
    );
    println!("Hash Table Type: {:?}", hash_table_t);

    // 2) Create the hash join table
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);

    let mut hash_join_table = match hash_table_t {
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

    // 3) Insert table_0
    println!("\n=== Inserting from table_0 ===");
    read_table_0_and_insert(&table0_path, &mut hash_join_table)?;

    // 4) Apply txs0
    println!("\n=== Applying TXS from txs_table_0 ===");
    read_txs_and_apply(&txs0_path, &mut hash_join_table)?;

    // 5) Discover and apply all txs_join_ts(\d+)_t(\d+).csv
    //    We can measure them individually
    println!("\n=== Discovering join logs => apply them ===");

    let pattern = Regex::new(r"^txs_join_ts(\d+)_t(\d+)\.csv$").unwrap();
    let mut discovered = Vec::new();
    // read the directory listing
    for entry in fs::read_dir(&csv_dir)? {
        let entry = entry?;
        let fname = entry.file_name();
        let fname_str = fname.to_string_lossy();
        if let Some(capt) = pattern.captures(&fname_str) {
            let ts_val_str = &capt[1];
            let table_idx_str = &capt[2];
            if let (Ok(ts_val), Ok(tbl_idx)) =
                (ts_val_str.parse::<u64>(), table_idx_str.parse::<u64>())
            {
                discovered.push((fname_str.into_owned(), ts_val, tbl_idx));
            }
        }
    }
    // Sort by (tbl_idx, ts_val)
    discovered.sort_by_key(|(_, tsval, tblidx)| (*tblidx, *tsval));

    if discovered.is_empty() {
        println!("No files matching txs_join_ts(\\d+)_t(\\d+) found. Done.");
    } else {
        println!("Discovered {} join logs:", discovered.len());
        for (fname, tsval, tblidx) in &discovered {
            println!("  file={}, table_idx={}, ts={}", fname, tblidx, tsval);
        }

        for (fname, tsval, tblidx) in discovered {
            let join_path = csv_dir.join(&fname);
            println!(
                "\nApplying join ops => {}, table_idx={}, ts={}",
                fname, tblidx, tsval
            );
            read_txs_and_apply(&join_path, &mut hash_join_table)?;
        }
    }

    println!("\nAll done!");
    Ok(())
}
