use fbtree::mvcc_index::MvccIndex;
use fbtree::{mvcc_index::hash_join::mvcc_hash_join::HashJoinTable, prelude::*};
// use fbtree::{mvcc_index::hashtable_mu::mvcc_hash_join_cuckoo::HashJoinTable, prelude::*};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!(
            "Usage: {} <data_file> <ops_file> <expected_data_file>",
            args[0]
        );
        return Ok(());
    }
    let data_file = &args[1];
    let ops_file = &args[2];
    let expected_data_file = &args[3];
    println!("Data file: {}", data_file);
    println!("Ops file: {}", ops_file);
    println!("Expected data file: {}", expected_data_file);

    // Read data and operations
    let data = read_data_file(data_file)?;
    let ops = read_ops_file(ops_file)?;
    let op_num = ops.len();
    let data_num = data.len();

    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool(); // You need to implement or import this function
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = HashJoinTable::create(c_key, mem_pool.clone())?;

    // Initialize Rust's default HashMap
    let mut rust_hash_map: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();

    //
    // Measure and report data loading time for Rust HashMap
    let start_time_hashmap_load = Instant::now();
    // Load data into Rust's HashMap
    for (key, pkey, value) in &data {
        rust_hash_map.insert((key.clone(), pkey.clone()), value.clone());
    }
    let duration_hashmap_load = start_time_hashmap_load.elapsed();
    println!(
        "Loaded {} entries into Rust HashMap in {:.2?}",
        data_num, duration_hashmap_load
    );

    //
    // Measure and report data loading time for HashJoinTable
    let start_time_hj_load = Instant::now();
    // Load data into the hash join table
    for (key, pkey, value) in &data {
        hash_join_table.insert(key.clone(), pkey.clone(), 0, 0, value.clone())?;
    }
    let duration_hj_load = start_time_hj_load.elapsed();
    println!(
        "Loaded {} entries into HashJoinTable in {:.2?}",
        data_num, duration_hj_load
    );

    //
    // Start the benchmark for Rust's HashMap
    let start_time_hashmap = Instant::now();

    // Execute operations from ops.csv on Rust's HashMap
    for op in &ops {
        let key_pkey = (op.key.clone(), op.pkey.clone());
        match op.op_type.as_str() {
            "insert" | "update" => {
                rust_hash_map.insert(key_pkey, op.value.clone());
            }
            "delete" => {
                rust_hash_map.remove(&key_pkey);
            }
            "get" => {
                let _ = rust_hash_map.get(&key_pkey);
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }

    let duration_hashmap = start_time_hashmap.elapsed();
    println!(
        "Rust HashMap: Executed {} operations in {:.2?}",
        op_num, duration_hashmap
    );

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
            }
            _ => {
                eprintln!("Unknown operation: {}", op.op_type);
            }
        }
    }

    let duration_hj = start_time_hj.elapsed();
    println!(
        "HashJoinTable: Executed {} operations in {:.2?}",
        op_num, duration_hj
    );

    // Perform consistency check with expected data
    let expected_data = read_expected_data_file(expected_data_file)?;

    // Check HashJoinTable data against expected data
    let is_consistent_hj = check_consistency_hash_join_table(&hash_join_table, &expected_data)?;
    println!(
        "HashJoinTable consistency check: {}",
        if is_consistent_hj { "PASSED" } else { "FAILED" }
    );

    // // Check Rust HashMap data against expected data
    // let is_consistent_hashmap = check_consistency_hash_map(&rust_hash_map, &expected_data);
    // println!(
    //     "Rust HashMap consistency check: {}",
    //     if is_consistent_hashmap { "PASSED" } else { "FAILED" }
    // );

    // Perform consistency check between HashJoinTable and Rust HashMap
    let is_consistent =
        check_consistency_between_hash_join_and_hash_map(&hash_join_table, &rust_hash_map)?;
    println!(
        "Consistency check between HashJoinTable and Rust HashMap: {}",
        if is_consistent { "PASSED" } else { "FAILED" }
    );

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

fn read_data_file(file_path: &str) -> io::Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
    let mut data = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 3 {
            let key = parts[0].as_bytes().to_vec();
            let pkey = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            data.push((key, pkey, value));
        }
    }
    Ok(data)
}

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

// Function to read expected data after ops (data_after_ops.csv)
fn read_expected_data_file(file_path: &str) -> io::Result<HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>> {
    let mut expected_data = HashMap::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 5 {
            let key = parts[2].as_bytes().to_vec();
            let pkey = parts[3].as_bytes().to_vec();
            let value = parts[4].as_bytes().to_vec();
            expected_data.insert((key, pkey), value);
        }
    }
    Ok(expected_data)
}

// Function to check consistency of HashJoinTable with expected data
fn check_consistency_hash_join_table(
    hash_join_table: &HashJoinTable<impl MemPool>,
    expected_data: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
) -> Result<bool, Box<dyn Error>> {
    let mut is_consistent = true;

    // Collect entries from HashJoinTable
    let mut hjt_entries: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
    let scanner = hash_join_table.scan(u64::MAX)?;
    for entry in scanner {
        hjt_entries.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
        // hjt_entries.insert((entry.0, entry.1), entry.2);
    }

    // Compare expected data with HashJoinTable entries
    for ((key, pkey), expected_value) in expected_data {
        match hjt_entries.get(&(key.clone(), pkey.clone())) {
            Some(value) => {
                if value != expected_value {
                    eprintln!(
                        "Mismatch for key '{}', pkey '{}': expected '{}', got '{}'",
                        bytes_to_string(&key),
                        bytes_to_string(&pkey),
                        bytes_to_string(expected_value),
                        bytes_to_string(value)
                    );
                    is_consistent = false;
                }
            }
            None => {
                eprintln!(
                    "Missing entry in HashJoinTable for key '{}', pkey '{}'",
                    bytes_to_string(&key),
                    bytes_to_string(&pkey)
                );
                is_consistent = false;
            }
        }
    }

    // Check for any extra entries in HashJoinTable not present in expected data
    for ((key, pkey), value) in hjt_entries {
        if !expected_data.contains_key(&(key.clone(), pkey.clone())) {
            eprintln!(
                "Extra entry in HashJoinTable: key '{}', pkey '{}', value '{}'",
                bytes_to_string(&key),
                bytes_to_string(&pkey),
                bytes_to_string(&value)
            );
            is_consistent = false;
        }
    }

    Ok(is_consistent)
}

// Function to check consistency of Rust HashMap with expected data
fn check_consistency_hash_map(
    rust_hash_map: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    expected_data: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
) -> bool {
    let mut is_consistent = true;

    // Compare Rust HashMap with expected data
    for ((key, pkey), expected_value) in expected_data {
        match rust_hash_map.get(&(key.clone(), pkey.clone())) {
            Some(value) => {
                if value != expected_value {
                    eprintln!(
                        "Mismatch for key '{}', pkey '{}': expected '{}', got '{}'",
                        bytes_to_string(&key),
                        bytes_to_string(&pkey),
                        bytes_to_string(expected_value),
                        bytes_to_string(value)
                    );
                    is_consistent = false;
                }
            }
            None => {
                eprintln!(
                    "Missing entry in Rust HashMap for key '{}', pkey '{}'",
                    bytes_to_string(&key),
                    bytes_to_string(&pkey)
                );
                is_consistent = false;
            }
        }
    }

    // Check for any extra entries in Rust HashMap not present in expected data
    for ((key, pkey), value) in rust_hash_map {
        if !expected_data.contains_key(&(key.clone(), pkey.clone())) {
            eprintln!(
                "Extra entry in Rust HashMap: key '{}', pkey '{}', value '{}'",
                bytes_to_string(&key),
                bytes_to_string(&pkey),
                bytes_to_string(value)
            );
            is_consistent = false;
        }
    }

    is_consistent
}

fn check_consistency_between_hash_join_and_hash_map(
    hash_join_table: &HashJoinTable<impl MemPool>,
    rust_hash_map: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
) -> Result<bool, Box<dyn Error>> {
    let mut is_consistent = true;

    // Collect entries from HashJoinTable
    let mut hjt_entries: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
    let scanner = hash_join_table.scan(u64::MAX)?;
    for entry in scanner {
        hjt_entries.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
        // hjt_entries.insert((entry.0, entry.1), entry.2);
    }

    // Compare entries in Rust HashMap with entries in HashJoinTable
    for ((key, pkey), expected_value) in rust_hash_map {
        match hjt_entries.get(&(key.clone(), pkey.clone())) {
            Some(value) => {
                if value != expected_value {
                    eprintln!(
                        "Mismatch for key '{}', pkey '{}': expected '{}', got '{}'",
                        bytes_to_string(&key),
                        bytes_to_string(&pkey),
                        bytes_to_string(expected_value),
                        bytes_to_string(value)
                    );
                    is_consistent = false;
                }
            }
            None => {
                eprintln!(
                    "Missing entry in HashJoinTable for key '{}', pkey '{}'",
                    bytes_to_string(&key),
                    bytes_to_string(&pkey)
                );
                is_consistent = false;
            }
        }
    }

    // Check for any extra entries in HashJoinTable not present in Rust HashMap
    for ((key, pkey), value) in hjt_entries {
        if !rust_hash_map.contains_key(&(key.clone(), pkey.clone())) {
            eprintln!(
                "Extra entry in HashJoinTable: key '{}', pkey '{}', value '{}'",
                bytes_to_string(&key),
                bytes_to_string(&pkey),
                bytes_to_string(&value)
            );
            is_consistent = false;
        }
    }

    Ok(is_consistent)
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
