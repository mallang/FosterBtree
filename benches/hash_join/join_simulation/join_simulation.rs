use fbtree::mvcc_index::hash_heap::hash_heap_table::HashHeapTable;
use fbtree::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::hashtable_mu::mvcc_hash_join_table::OpenAddrHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccIndex};
use fbtree::prelude::*;

use regex::Regex;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::{Component, PathBuf};
use std::sync::Arc;
use std::time::Instant;

/// A single row from table_0.csv
struct TableRow {
    pkey: Vec<u8>,
    join_key: Vec<u8>,
    value: Vec<u8>,
}

/// A single transaction op from txs_table_0.csv
struct TxOperation {
    tx_id: u64,
    ts: u64,
    op: String,
    pkey: Vec<u8>,
    join_key: Vec<u8>,
    value: Vec<u8>,
}

/// If `value_str` is in the format "BASESTRING(TOTAL_LENGTH)",
/// expand it to a repeated byte vector of length `TOTAL_LENGTH`.
/// Otherwise, just return the raw bytes of `value_str`.
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
                // If base is empty, just produce zero bytes
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
    // Fallback: treat as raw
    value_str.as_bytes().to_vec()
}

/// Read table_0.csv into memory and do `hash_join_table.insert(...)`.
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

/// Read txs_table_0.csv and apply each op
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
                // possible scan if we want to implement
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

/// Actually read table_0.csv: "pkey,join_key,value"
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

/// Actually read txs_table_0.csv: "tx_id,ts,op,pkey,join_key,value"
fn read_txs_csv(filepath: &PathBuf) -> io::Result<Vec<TxOperation>> {
    let file = File::open(filepath)?;
    let mut ops = Vec::new();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(file);

    for result in rdr.records() {
        let record = result?;
        if record.len() < 6 {
            eprintln!("Invalid record in txs_table_0.csv: {:?}", record);
            continue;
        }
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
    }
    Ok(ops)
}

/// Attempt to resolve a path like ../../benches/hash_join/join_simulation/csv
/// relative to the directory of the current executable.
/// This function tries to handle things so you can run the binary from anywhere.
fn default_csv_path_relative_to_bin() -> PathBuf {
    // Suppose we want to go from the binary's directory to ../../benches/hash_join/join_simulation/csv
    // We'll find the binary's dir, then pop/push the relative path.
    if let Ok(exe_path) = env::current_exe() {
        let mut exe_dir = exe_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();
        // We want 2 "pops" to go up 2 directories, then push "benches/hash_join/join_simulation/csv"
        // But we can just do the relative approach:
        exe_dir.push(".."); // pop once
        exe_dir.push(".."); // pop twice
        exe_dir.push("benches");
        exe_dir.push("hash_join");
        exe_dir.push("join_simulation");
        exe_dir.push("csv");
        return exe_dir;
    }
    // fallback if that fails
    PathBuf::from("../../benches/hash_join/join_simulation/csv")
}

/// Main program for testing all 4 hash table types.
/// Usage:
///   cargo run --bin hash_join_test -- [--csv-dir <path>] [--table0 <file>] [--txs0 <file>] -t <chain|open_address|heap|rust>
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    // We'll default to the auto-computed path from the bin location
    let mut csv_dir = default_csv_path_relative_to_bin();
    let mut table0_name = String::from("table_0.csv");
    let mut txs0_name = String::from("txs_table_0.csv");
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

    // 1) Make sure csv_dir exists
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

    // 2) Create the hash table via fbtree
    let mem_pool = get_in_mem_pool(); // from fbtree
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

    // 3) Read table_0.csv => Insert data
    println!("\n=== Inserting from table_0 ===");
    read_table_0_and_insert(&table0_path, &mut hash_join_table)?;

    // 4) Read txs_table_0.csv => Apply ops
    println!("\n=== Applying TXS from txs_table_0 ===");
    read_txs_and_apply(&txs0_path, &mut hash_join_table)?;

    println!("\nAll done!");
    Ok(())
}
