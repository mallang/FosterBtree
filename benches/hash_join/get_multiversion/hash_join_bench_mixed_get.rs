use dashmap::mapref::entry;
use fbtree::mvcc_index::{MvccEntry, MvccIndex};
// use fbtree::{mvcc_index::hash_join::mvcc_hash_join::MvccHashJoinTable, prelude::*};
use fbtree::{mvcc_index::hashtable_mu::mvcc_hash_join_table::MvccHashJoinTable, prelude::*};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::str::from_utf8;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    // Variables to store arguments
    let mut data_file: Option<String> = None;
    let mut ops_file: Option<String> = None;
    let mut recent_data_file: Option<String> = None;
    let mut history_data_file: Option<String> = None;
    // let mut scan_ops_file: Option<String> = None;
    let mut limit_ops: Option<usize> = None;

    // Simple argument parsing loop
    // We expect something like:
    // -df data.csv -of ops.csv -rdf recent_data.csv -hdf history_data.csv -sof scan_ops.csv -n 100
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-df" => {
                if i + 1 < args.len() {
                    data_file = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: -df requires a file path");
                    return Ok(());
                }
            }
            "-of" => {
                if i + 1 < args.len() {
                    ops_file = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Error: -of requires a file path");
                    return Ok(());
                }
            }
            // "-rdf" => {
            //     if i + 1 < args.len() {
            //         recent_data_file = Some(args[i + 1].clone());
            //         i += 2;
            //     } else {
            //         eprintln!("Error: -rdf requires a file path");
            //         return Ok(());
            //     }
            // }
            // "-hdf" => {
            //     if i + 1 < args.len() {
            //         history_data_file = Some(args[i + 1].clone());
            //         i += 2;
            //     } else {
            //         eprintln!("Error: -hdf requires a file path");
            //         return Ok(());
            //     }
            // }
            // "-sof" => {
            //     if i + 1 < args.len() {
            //         scan_ops_file = Some(args[i + 1].clone());
            //         i += 2;
            //     } else {
            //         eprintln!("Error: -sof requires a file path");
            //         return Ok(());
            //     }
            // }
            "-n" => {
                if i + 1 < args.len() {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        limit_ops = Some(n);
                        i += 2;
                    } else {
                        eprintln!("Warning: Invalid number after -n, ignoring...");
                        i += 2;
                    }
                } else {
                    eprintln!("Warning: -n specified without a following number, ignoring...");
                    i += 1;
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                i += 1;
            }
        }
    }

    // Check required arguments
    if data_file.is_none()
        || ops_file.is_none()
        // || recent_data_file.is_none()
        // || history_data_file.is_none()
        // || scan_ops_file.is_none()
    {
        eprintln!("Usage:");
        eprintln!("  {} -df <data_file> -of <ops_file> [-n <num_ops>]", args[0]);
        return Ok(());
    }

    let data_file = data_file.unwrap();
    let ops_file = ops_file.unwrap();
    // let recent_data_file = recent_data_file.unwrap();
    // let history_data_file = history_data_file.unwrap();
    // let scan_ops_file = scan_ops_file.unwrap();

    println!("Data file: {}", data_file);
    println!("Ops file: {}", ops_file);
    // println!("Recent data file: {}", recent_data_file);
    // println!("History data file: {}", history_data_file);
    // println!("Scan operations file: {}", scan_ops_file);
    if let Some(n) = limit_ops {
        println!("Limiting operations to first {} ops", n);
    }
    println!();

    // Read data and operations
    let init_ops = read_ops_file(&data_file)?;
    let mut ops = read_ops_file(&ops_file)?;
    if let Some(n) = limit_ops {
        if n < ops.len() {
            ops.truncate(n);
        }
    }

    let op_num = ops.len();
    

    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = MvccHashJoinTable::create(c_key, mem_pool.clone())?;

    // Initialize Rust's default HashMap
    let mut rust_hash_map: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();

    //
    // Measure and report data loading time for Rust HashMap
    let start_time_hashmap_load = Instant::now();
    // Load data into Rust's HashMap
    for op in &init_ops {
        let key = op.key.clone();
        match op.op_type.as_str() {
            "insert" | "update" => {
                let entries = rust_hash_map.entry(key.clone()).or_insert_with(Vec::new);
                // End the previous version if exists for the same pkey
                if let Some(last_entry) = entries
                    .iter_mut()
                    .rev()
                    .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
                {
                    last_entry.end_ts = op.ts;
                }
                // Add the new version
                let new_entry = MvccEntry::new(
                    key.clone(),
                    op.pkey.clone(),
                    op.value.clone(),
                    op.ts,
                    u64::MAX,
                );
                entries.push(new_entry);
            }
            "delete" => {
                if let Some(entries) = rust_hash_map.get_mut(&key) {
                    if let Some(last_entry) = entries
                        .iter_mut()
                        .rev()
                        .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
                    {
                        last_entry.end_ts = op.ts;
                    }
                }
            }
            "get" => {
                if let Some(entries) = rust_hash_map.get(&key) {
                    let value = entries.iter().rev().find(|entry| {
                        entry.pkey == op.pkey && entry.start_ts <= op.ts && op.ts < entry.end_ts
                    });
                    // Use `value` as needed
                    if let Some(_entry) = value {
                        // println!("Got value: {}", bytes_to_string(&entry.value));
                    } else {
                        // No value found
                    }
                } else {
                    // Key does not exist
                }
            }
            "commit" | "scan" => {
                // No-op for Rust's HashMap
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }
    let duration_hashmap_load = start_time_hashmap_load.elapsed();
    let data_num = rust_hash_map.iter().map(|x| x.1.iter().map(|x| &x.pkey[..]).collect::<HashSet<&[u8]>>().len()).sum::<usize>();
    println!(
        "Loaded {} entries into Rust HashMap in {} ns",
        data_num,
        duration_hashmap_load.as_nanos()
    );

    //
    // Measure and report data loading time for HashJoinTable
    let start_time_hj_load = Instant::now();
    // Load data into the hash join table
    for op in &init_ops {
        match op.op_type.as_str() {
            "insert" => {
                hash_join_table.insert(
                    op.key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
            }
            "update" => {
                hash_join_table.update(
                    op.key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
            }
            "delete" => {
                hash_join_table.delete(&op.key, &op.pkey, op.ts, op.tx_id)?;
            }
            "get" => {
                let _ = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
            }
            "commit" => {
               // ignore
            }
            "scan" => {
                // Impl scan if needed
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }
    let duration_hj_load = start_time_hj_load.elapsed();
    // println!(
    //     "Loaded {} entries into HashJoinTable in {:.2?}",
    //     data_num, duration_hj_load
    // );
    println!(
        "Loaded {} entries into HashJoinTable in {} ns",
        data_num,
        duration_hj_load.as_nanos()
    );

    println!();

    //
    // Start the benchmark for Rust's HashMap
    let start_time_hashmap = Instant::now();

    for op in &ops {
        let key = op.key.clone();
        match op.op_type.as_str() {
            "insert" | "update" => {
                let entries = rust_hash_map.entry(key.clone()).or_insert_with(Vec::new);
                // End the previous version if exists for the same pkey
                if let Some(last_entry) = entries
                    .iter_mut()
                    .rev()
                    .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
                {
                    last_entry.end_ts = op.ts;
                }
                // Add the new version
                let new_entry = MvccEntry::new(
                    key.clone(),
                    op.pkey.clone(),
                    op.value.clone(),
                    op.ts,
                    u64::MAX,
                );
                // let new_entry = MvccEntry {
                //     key: key.clone(),
                //     pkey: op.pkey.clone(),
                //     value: op.value.clone(),
                //     start_ts: op.ts,
                //     end_ts: u64::MAX,
                // };
                entries.push(new_entry);
            }
            "delete" => {
                if let Some(entries) = rust_hash_map.get_mut(&key) {
                    if let Some(last_entry) = entries
                        .iter_mut()
                        .rev()
                        .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
                    {
                        last_entry.end_ts = op.ts;
                    }
                }
            }
            "get" => {
                if let Some(entries) = rust_hash_map.get(&key) {
                    let value = entries.iter().rev().find(|entry| {
                        entry.pkey == op.pkey && entry.start_ts <= op.ts && op.ts < entry.end_ts
                    });
                    // Use `value` as needed
                    if let Some(_entry) = value {
                        // println!("Got value: {}", bytes_to_string(&_entry.value));
                    } else {
                        // No value found
                    }
                } else {
                    // Key does not exist
                }
            }
            "commit" | "scan" => {
                // No-op for Rust's HashMap
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }

    let duration_hashmap = start_time_hashmap.elapsed();
    // println!(
    //     "Rust HashMap: Executed {} operations in {:.2?}",
    //     op_num, duration_hashmap
    // );
    println!(
        "Rust HashMap: Executed {} operations in {} ns",
        op_num,
        duration_hashmap.as_nanos()
    );

    //
    // Start the benchmark for HashJoinTable
    let start_time_hj = Instant::now();

    // Execute operations from ops.csv on HashJoinTable
    let mut commit_cnt = 0;
    for op in &ops {
        match op.op_type.as_str() {
            "insert" => {
                hash_join_table.insert(
                    op.key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
            }
            "update" => {
                hash_join_table.update(
                    op.key.clone(),
                    op.pkey.clone(),
                    op.ts,
                    op.tx_id,
                    op.value.clone(),
                )?;
            }
            "delete" => {
                hash_join_table.delete(&op.key, &op.pkey, op.ts, op.tx_id)?;
            }
            "get" => {
                let _ = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
                // println!("pkey{:?} value{:?} ts{:?}", from_utf8(&op.pkey[..]) ,  from_utf8(&_x.unwrap()[..]), op.ts);
            }
            "commit" => {
                // Implement commit if needed
                // commit_cnt += 1;
                // if commit_cnt == 5000 {
                //     hash_join_table.garbage_collect(Timestamp::MAX)?;
                // }

            }
            "scan" => {
                // Impl scan if needed
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }

    let duration_hj = start_time_hj.elapsed();
    // println!(
    //     "HashJoinTable: Executed {} operations in {:.2?}",
    //     op_num, duration_hj
    // );
    println!(
        "HashJoinTable: Executed {} operations in {} ns",
        op_num,
        duration_hj.as_nanos()
    );

    println!();
    // for _ in 0..5 {
    //     for op in &ops {
    //         match op.op_type.as_str() {
    //             "insert" => {
    //                 hash_join_table.insert(
    //                     op.key.clone(),
    //                     op.pkey.clone(),
    //                     op.ts,
    //                     op.tx_id,
    //                     op.value.clone(),
    //                 )?;
    //             }
    //             "update" => {
    //                 hash_join_table.update(
    //                     op.key.clone(),
    //                     op.pkey.clone(),
    //                     op.ts,
    //                     op.tx_id,
    //                     op.value.clone(),
    //                 )?;
    //             }
    //             "delete" => {
    //                 hash_join_table.delete(&op.key, &op.pkey, op.ts, op.tx_id)?;
    //             }
    //             "get" => {
    //                 let _ = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
    //             }
    //             "commit" => {
    //                 // Implement commit if needed
    //                 // commit_cnt += 1;
    //                 // if commit_cnt == 5000 {
    //                 //     hash_join_table.garbage_collect(Timestamp::MAX)?;
    //                 // }
    
    //             }
    //             "scan" => {
    //                 // Impl scan if needed
    //             }
    //             _ => {
    //                 eprintln!("Unknown operation: {}", op.op_type);
    //             }
    //         }
    //     }
    // }
    

    Ok(())
}

// Define a struct to represent an operation
struct Operation {
    tx_id: u64,
    ts: u64,
    op_type: String,
    key: Vec<u8>,
    pkey: Vec<u8>,
    value: Vec<u8>,
}

// Function to read data.csv
use std::fs::File;
use std::io::{self, BufRead};

// Function to read ops.csv
fn read_ops_file(file_path: &str) -> io::Result<Vec<Operation>> {
    let mut operations = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
        // Use a CSV parser to handle commas in values
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(line.as_bytes());
        for result in rdr.records() {
            let record = result?;
            if record.len() >= 6 {
                let tx_id = record[0].parse::<u64>().unwrap_or(0);
                let ts = record[1].parse::<u64>().unwrap_or(0);
                let op_type = record[2].to_string();
                let key = record[3].as_bytes().to_vec();
                let pkey = record[4].as_bytes().to_vec();
                let value = record[5].as_bytes().to_vec();
                operations.push(Operation {
                    tx_id,
                    ts,
                    op_type,
                    key,
                    pkey,
                    value,
                });
            } else {
                eprintln!("Invalid line (expected 6 fields): {}", line);
            }
        }
    }
    Ok(operations)
}

// Function to convert byte arrays to strings safely
fn bytes_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => bytes
            .iter()
            .map(|b| format!("{:02X}", b))
            .collect::<String>(),
    }
}


fn scan_rust_hash_map(rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>, ts: u64) -> Vec<MvccEntry> {
    let mut results = Vec::new();
    for entries in rust_hash_map.values() {
        for entry in entries {
            if entry.start_ts <= ts && ts < entry.end_ts {
                results.push(entry.clone());
            }
        }
    }
    results
}
