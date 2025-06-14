use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccEntry, MvccIndex};
use fbtree::{mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable, prelude::*};
use std::collections::{HashMap, HashSet};
use std::env;
use std::error::Error;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    // Variables to store arguments
    let mut data_file_prefix: Option<String> = None;
    let mut ops_file: Option<String> = None;
    let mut limit_ops: Option<usize> = None;
    let mut is_partition = false;
    let mut is_read_repair = false;
    // let mut hash_table_t: HashTableType = HashTableType::HeapTable;

    // Simple argument parsing loop
    // We expect something like:
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-df" => {
                if i + 1 < args.len() {
                    data_file_prefix = Some(args[i + 1].clone());
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
                            "ts_partitioned" => {}
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
            "-p" => {
                is_partition = true;
                i += 1;
            }
            "-rr" => {
                is_read_repair = true;
                i += 1;
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                i += 1;
            }
        }
    }

    // Check required arguments
    if data_file_prefix.is_none() || ops_file.is_none() {
        eprintln!("Usage:");
        eprintln!(
            "  {} -df <data_file_prefix> -of <ops_file> -t <hash_join_type> -p -rr[-n <num_ops>]",
            args[0]
        );
        return Ok(());
    }

    ts_partition_bench_get(
        data_file_prefix,
        ops_file,
        limit_ops,
        is_partition,
        is_read_repair,
    )?;
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

fn ts_partition_bench_get(
    data_file_prefix: Option<String>,
    ops_file: Option<String>,
    limit_ops: Option<usize>,
    is_partition: bool,
    is_read_repair: bool,
) -> Result<(), Box<dyn Error>> {
    let data_file_prefix = data_file_prefix.unwrap();
    let ops_file = ops_file.unwrap();

    println!("Data file prefix: {}", data_file_prefix);
    println!("Ops file: {}", ops_file);

    if let Some(n) = limit_ops {
        println!("Limiting operations to first {} ops", n);
    }
    println!();

    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = TsPartitionedTable::new(c_key, mem_pool);

    // Read data and operations

    let mut partitioned_data_files = vec![];
    // get directory path from prefix
    let directory_path = data_file_prefix
        .rsplit_once('/')
        .map(|(dir, _)| dir)
        .unwrap_or("");
    for entry in std::fs::read_dir(directory_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.to_str().unwrap().starts_with(&data_file_prefix) {
            partitioned_data_files.push(path);
        }
    }
    // println!("Data files: {:?}", partitioned_data_files);
    partitioned_data_files.sort();

    let mut init_ops = vec![];
    for par_file in partitioned_data_files {
        let partition_init_ops = read_ops_file(par_file.to_str().unwrap())?;
        let partition_max_ts = partition_init_ops.last().unwrap().ts;
        init_ops.push((partition_init_ops, partition_max_ts));
    }

    //
    // Measure and report data loading time for HashJoinTable
    let start_time_hj_load = Instant::now();
    let mut insert_cnt: u64 = 0;
    let mut update_cnt: u64 = 0;
    for (partition_init_ops, par_max_ts) in init_ops {
        // Load data into the hash join table
        for load_op in &partition_init_ops {
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
                    hash_join_table.delete(
                        &load_op.key,
                        &load_op.pkey,
                        load_op.ts,
                        load_op.tx_id,
                    )?;
                }
                "get" => {
                    if is_read_repair {
                        // Read repair
                        let _ = hash_join_table._get_read_repair(
                            &load_op.key,
                            &load_op.pkey,
                            load_op.ts,
                        )?;
                    } else {
                        let _ = hash_join_table.get(&load_op.key, &load_op.pkey, load_op.ts)?;
                    }
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
        if is_partition {
            hash_join_table.split_at_ts(par_max_ts)?;
        }

        println!(
            "Loaded {} entries into HashJoinTable with max ts {}",
            partition_init_ops.len(),
            par_max_ts
        );
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

    let ops = read_ops_file(&ops_file)?;
    let op_num = ops.len();

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
                // let _ = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
                if is_read_repair {
                    // Read repair
                    let _ = hash_join_table._get_read_repair(&op.key, &op.pkey, op.ts)?;
                } else {
                    let _ = hash_join_table.get(&op.key, &op.pkey, op.ts)?;
                }
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

    Ok(())
}
