use dashmap::mapref::entry;
use fbtree::mvcc_index::hash_heap::hash_heap_table::HashHeapTable;
use fbtree::mvcc_index::hash_join::chained_hash_bucket_second::{
    HISTORY_GET_COUNT, HISTORY_GET_TOTAL_NS, RECENT_GET_COUNT, RECENT_GET_TOTAL_NS,
};
use fbtree::mvcc_index::hash_join::chained_hash_history_chain::HCHAIN_PAGE_READ_COUNT;
use fbtree::mvcc_index::hash_join::chained_hash_page::HISTORY_SLOT_CMP_CNT;
use fbtree::mvcc_index::hashtable_mu::mvcc_hash_join_table::OpenAddrHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccEntry, MvccIndex};
use fbtree::{mvcc_index::hash_join::chained_hash_table::ChainedHashTable, prelude::*};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::str::from_utf8;
use std::sync::atomic::Ordering;
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
    let mut hash_table_t: HashTableType = HashTableType::HeapTable;

    // Simple argument parsing loop
    // We expect something like:
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
            "-t" => {
                if i + 1 < args.len() {
                    if let Ok(type_name) = args[i + 1].parse::<String>() {
                        match type_name.as_str() {
                            "open_address" => {
                                hash_table_t = HashTableType::OpenAddressing;
                            }
                            "chain" => {
                                hash_table_t = HashTableType::Chained;
                            }
                            "heap" => {
                                hash_table_t = HashTableType::HeapTable;
                            }
                            "rust" => {
                                hash_table_t = HashTableType::RustHashMap;
                            }
                            _ => {
                                eprintln!("Warning: Invalid hash table type, ignoring...");
                            }
                        }
                        i += 2;
                    } else {
                        eprintln!("Warning: Invalid number after -t, ignoring...");
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
    if data_file.is_none() || ops_file.is_none()
    // || recent_data_file.is_none()
    // || history_data_file.is_none()
    // || scan_ops_file.is_none()
    {
        eprintln!("Usage:");
        eprintln!(
            "  {} -df <data_file> -of <ops_file> -t <open_address/chain/heap> [-n <num_ops>]",
            args[0]
        );
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
    let hash_join_table = match hash_table_t {
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

    //
    // Measure and report data loading time for HashJoinTable
    let start_time_hj_load = Instant::now();
    let mut insert_cnt: u64 = 0;
    let mut update_cnt: u64 = 0;
    // Load data into the hash join table
    for load_op in &init_ops {
        match load_op.op_type.as_str() {
            "insert" => {
                insert_cnt += 1;
                hash_join_table.insert(
                    load_op.key.clone(),
                    load_op.pkey.clone(),
                    load_op.ts,
                    load_op.tx_id,
                    load_op.value.clone(),
                )?;
            }
            "update" => {
                update_cnt += 1;
                hash_join_table.update(
                    load_op.key.clone(),
                    load_op.pkey.clone(),
                    load_op.ts,
                    load_op.tx_id,
                    load_op.value.clone(),
                )?;
            }
            "delete" => {
                hash_join_table.delete(&load_op.key, &load_op.pkey, load_op.ts, load_op.tx_id)?;
            }
            "get" => {
                let _ = hash_join_table.get(&load_op.key, &load_op.pkey, load_op.ts)?;
            }
            "commit" => {
                // ignore
            }
            "scan" => {
                // Impl scan if needed
            }
            _ => {
                eprintln!("Unknown operation: {}", load_op.op_type);
            }
        }
    }
    let duration_hj_load = start_time_hj_load.elapsed();
    // println!(
    //     "Loaded {} entries into HashJoinTable in {:.2?}",
    //     data_num, duration_hj_load
    // );
    println!(
        "Loaded {} entries with {} versions into HashJoinTable in {} ns",
        insert_cnt,
        (update_cnt / insert_cnt) + 1,
        duration_hj_load.as_nanos()
    );
    // println!(
    //     "Loaded {} entries into HashJoinTable in {} ns",
    //     data_num,
    //     duration_hj_load.as_nanos()
    // );

    println!();

    //
    // Start the benchmark for HashJoinTable
    let start_time_hj = Instant::now();

    // Execute operations from ops.csv on HashJoinTable
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
            "commit" => {}
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

    // After finishing all operations, print the statistics.
    println!("===STAT_START===");
    if hash_table_t == HashTableType::Chained {
        if let Some(chained_hash_table) = hash_join_table
            .as_any()
            .downcast_ref::<ChainedHashTable<_>>()
        {
            println!(
                "{}",
                ChainedHashTable::<InMemPool>::stat(chained_hash_table)
            );
        }

        println!(
            "Total time spent in recent_chain.get(): {} ns",
            RECENT_GET_TOTAL_NS.load(Ordering::Relaxed)
        );
        let recent_get_count = RECENT_GET_COUNT.load(Ordering::Relaxed);
        println!("Total count of recent_chain.get(): {}", recent_get_count);
        if recent_get_count > 0 {
            println!(
                "Avg time spent in recent_chain.get(): {} ns",
                RECENT_GET_TOTAL_NS.load(Ordering::Relaxed) / recent_get_count
            );
        } else {
            println!("Avg time spent in recent_chain.get(): 0 ns");
        }

        println!(
            "Total time spent in history_chain.get(): {} ns",
            HISTORY_GET_TOTAL_NS.load(Ordering::Relaxed)
        );
        let history_get_count = HISTORY_GET_COUNT.load(Ordering::Relaxed);
        println!("Total count of history_chain.get(): {}", history_get_count);
        if history_get_count > 0 {
            println!(
                "Avg time spent in history_chain.get(): {} ns",
                HISTORY_GET_TOTAL_NS.load(Ordering::Relaxed) / history_get_count
            );
        } else {
            println!("Avg time spent in history_chain.get(): 0 ns");
        }

        let history_page_read_count = HCHAIN_PAGE_READ_COUNT.load(Ordering::Relaxed);
        println!(
            "Total Page Read in history_chain.get(): {}",
            history_page_read_count
        );
        if history_get_count > 0 {
            println!(
                "Avg Page Read in history_chain.get(): {}",
                history_page_read_count / history_get_count
            );
        } else {
            println!("Avg Page Read in history_chain.get(): 0");
        }
        println!(
            "Total slot compare in page when history_chain.get(): {}",
            HISTORY_SLOT_CMP_CNT.load(Ordering::Relaxed)
        );
        if history_page_read_count > 0 {
            println!(
                "Avg slot compare in page when history_chain.get(): {}",
                HISTORY_SLOT_CMP_CNT.load(Ordering::Relaxed) / history_page_read_count
            );
        } else {
            println!("Avg slot compare in page when history_chain.get(): 0");
        }
        println!("===STAT_END===");

        println!();
    }

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
