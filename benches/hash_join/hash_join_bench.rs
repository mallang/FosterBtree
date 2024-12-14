use fbtree::mvcc_index::{MvccEntry, MvccIndex, Timestamp};
// use fbtree::{mvcc_index::hash_join::mvcc_hash_join::MvccHashJoinTable, prelude::*};
use fbtree::{mvcc_index::hashtable_mu::mvcc_hash_join_table::MvccHashJoinTable, prelude::*};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "Usage: {} <data_file> <ops_file> <recent_data_file> <history_data_file> <scan_ops_file>",
            args[0]
        );
        return Ok(());
    }
    let data_file = &args[1];
    let ops_file = &args[2];
    let recent_data_file = &args[3];
    let history_data_file = &args[4];
    let scan_ops_file = &args[5];
    println!("Data file: {}", data_file);
    println!("Ops file: {}", ops_file);
    println!("Recent data file: {}", recent_data_file);
    println!("History data file: {}", history_data_file);
    println!("Scan operations file: {}", scan_ops_file);

    // Read data and operations
    let data = read_data_file(data_file)?;
    let ops = read_ops_file(ops_file)?;
    let op_num = ops.len();
    let data_num = data.len();

    // Initialize the hash join table using the MvccIndex trait
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let hash_join_table = MvccHashJoinTable::create(c_key, mem_pool.clone())?;

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
            "commit" => {
                // No-op for Rust's HashMap
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
            "commit" => {
                // Implement commit if needed
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

    // Perform consistency check with expected recent data
    let expected_recent_data = read_expected_data_file(recent_data_file)?;
    let is_consistent_hj =
        check_consistency_hash_join_table(&hash_join_table, &expected_recent_data)?;
    println!(
        "HashJoinTable recent data consistency check: {}",
        if is_consistent_hj { "PASSED" } else { "FAILED" }
    );

    // Perform full consistency check with expected full data
    let expected_full_data = {
        let mut data = read_expected_full_data_file(recent_data_file)?;
        let mut history_data = read_expected_full_data_file(history_data_file)?;
        data.append(&mut history_data);
        data
    };

    let is_full_consistent_hj =
        check_full_consistency_hash_join_table(&hash_join_table, &expected_full_data)?;
    println!(
        "HashJoinTable full consistency check: {}",
        if is_full_consistent_hj {
            "PASSED"
        } else {
            "FAILED"
        }
    );

    // Check Rust HashMap data against expected recent data
    let is_consistent_hashmap = check_consistency_hash_map(&rust_hash_map, &expected_recent_data);
    println!(
        "Rust HashMap consistency check: {}",
        if is_consistent_hashmap {
            "PASSED"
        } else {
            "FAILED"
        }
    );

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

// Function to read expected data after ops (recent_data_after_ops.csv)
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
            let start_ts = parts[0];
            let end_ts = parts[1];
            let key = parts[2].as_bytes().to_vec();
            let pkey = parts[3].as_bytes().to_vec();
            let value = parts[4].as_bytes().to_vec();
            expected_data.insert((key, pkey), value);
        } else {
            eprintln!("Invalid line (expected at least 5 fields): {}", line);
        }
    }
    Ok(expected_data)
}

fn read_expected_full_data_file(
    file_path: &str,
) -> io::Result<Vec<(Timestamp, Timestamp, Vec<u8>, Vec<u8>, Vec<u8>)>> {
    let mut data = Vec::new();
    let file = File::open(file_path)?;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue; // Skip empty lines
        }
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 5 {
            let start_ts = parts[0].parse::<u64>().unwrap_or(0);
            let end_ts = if parts[1] == "-1" {
                u64::MAX
            } else {
                parts[1].parse::<u64>().unwrap_or(u64::MAX)
            };
            let key = parts[2].as_bytes().to_vec();
            let pkey = parts[3].as_bytes().to_vec();
            let value = parts[4].as_bytes().to_vec();
            data.push((start_ts, end_ts, key, pkey, value));
        }
    }
    Ok(data)
}

// Function to check consistency of HashJoinTable with expected data
fn check_consistency_hash_join_table(
    hash_join_table: &MvccHashJoinTable<impl MemPool>,
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
    hash_join_table: &MvccHashJoinTable<impl MemPool>,
    rust_hash_map: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
) -> Result<bool, Box<dyn Error>> {
    let mut is_consistent = true;

    // Collect entries from HashJoinTable valid at ts = u64::MAX
    let mut hjt_entries: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
    let scanner = hash_join_table.scan(u64::MAX)?;
    for entry in scanner {
        hjt_entries.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
    }

    // Compare entries in Rust HashMap with entries in HashJoinTable
    for ((key, pkey), value) in rust_hash_map {
        match hjt_entries.get(&(key.clone(), pkey.clone())) {
            Some(hjt_value) => {
                if hjt_value != value {
                    eprintln!(
                        "Mismatch for key '{}', pkey '{}': Rust HashMap value '{}', HashJoinTable value '{}'",
                        bytes_to_string(key),
                        bytes_to_string(pkey),
                        bytes_to_string(value),
                        bytes_to_string(hjt_value)
                    );
                    is_consistent = false;
                }
            }
            None => {
                eprintln!(
                    "Missing entry in HashJoinTable for key '{}', pkey '{}'",
                    bytes_to_string(key),
                    bytes_to_string(pkey)
                );
                is_consistent = false;
            }
        }
    }

    // Check for any extra entries in HashJoinTable not present in Rust HashMap
    for ((key, pkey), hjt_value) in hjt_entries {
        if !rust_hash_map.contains_key(&(key.clone(), pkey.clone())) {
            eprintln!(
                "Extra entry in HashJoinTable: key '{}', pkey '{}', value '{}'",
                bytes_to_string(&key),
                bytes_to_string(&pkey),
                bytes_to_string(&hjt_value)
            );
            is_consistent = false;
        }
    }

    Ok(is_consistent)
}

fn check_full_consistency_hash_join_table(
    hash_join_table: &MvccHashJoinTable<impl MemPool>,
    expected_data: &Vec<(u64, u64, Vec<u8>, Vec<u8>, Vec<u8>)>,
) -> Result<bool, Box<dyn Error>> {
    let mut is_consistent = true;

    // Collect all entries from HashJoinTable
    let mut hjt_entries: HashSet<MvccEntry> = HashSet::new();
    let scanner = hash_join_table.scan_all()?; // Implement scan_all method
    for entry in scanner {
        hjt_entries.insert(entry);
    }

    // Create a HashSet of expected entries for comparison
    let expected_entries: HashSet<MvccEntry> = expected_data
        .iter()
        .map(|(start_ts, end_ts, key, pkey, value)| MvccEntry {
            start_ts: *start_ts,
            end_ts: *end_ts,
            key: key.clone(),
            pkey: pkey.clone(),
            value: value.clone(),
        })
        .collect();

    // Compare the sets
    if hjt_entries != expected_entries {
        is_consistent = false;
        let missing_entries = expected_entries.difference(&hjt_entries);
        let extra_entries = hjt_entries.difference(&expected_entries);

        for entry in missing_entries {
            eprintln!(
                "Missing entry in HashJoinTable: start_ts '{}', end_ts '{}', key '{}', pkey '{}', value '{}'",
                entry.start_ts,
                if entry.end_ts == u64::MAX { -1 } else { entry.end_ts as i64 },
                bytes_to_string(&entry.key),
                bytes_to_string(&entry.pkey),
                bytes_to_string(&entry.value)
            );
        }

        for entry in extra_entries {
            eprintln!(
                "Extra entry in HashJoinTable: start_ts '{}', end_ts '{}', key '{}', pkey '{}', value '{}'",
                entry.start_ts,
                if entry.end_ts == u64::MAX { -1 } else { entry.end_ts as i64 },
                bytes_to_string(&entry.key),
                bytes_to_string(&entry.pkey),
                bytes_to_string(&entry.value)
            );
        }
    }

    Ok(is_consistent)
}

fn read_scan_ops_file(
    file_path: &str,
) -> Result<Vec<(u64, Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>)>, Box<dyn Error>> {
    let mut scan_operations = Vec::new();

    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut lines = reader.lines();

    while let Some(line) = lines.next() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() == 3 && parts[0] == "scan" && parts[1] == "ts" {
            let ts = parts[2].parse::<u64>()?;
            let mut entries = Vec::new();

            while let Some(entry_line) = lines.next() {
                let entry_line = entry_line?;
                if entry_line.trim().is_empty() {
                    break;
                }
                let entry_parts: Vec<&str> = entry_line.split(',').map(|s| s.trim()).collect();
                if entry_parts.len() >= 3 {
                    let key = entry_parts[0].as_bytes().to_vec();
                    let pkey = entry_parts[1].as_bytes().to_vec();
                    let value = entry_parts[2].as_bytes().to_vec();
                    entries.push((key, pkey, value));
                }
            }
            scan_operations.push((ts, entries));
        }
    }

    Ok(scan_operations)
}

fn perform_scans_and_check(
    hash_join_table: &MvccHashJoinTable<impl MemPool>,
    scan_operations: &[(u64, Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>)],
) -> Result<bool, Box<dyn Error>> {
    let mut is_consistent = true;

    for (ts, expected_entries) in scan_operations {
        let effective_ts = if *ts == u64::MAX || *ts == -1_i64 as u64 {
            u64::MAX
        } else {
            *ts
        };

        // Perform the scan at the specified timestamp
        let scanner = hash_join_table.scan(effective_ts)?;
        let mut scan_results: HashSet<(Vec<u8>, Vec<u8>, Vec<u8>)> = HashSet::new();
        for entry in scanner {
            scan_results.insert((entry.key.clone(), entry.pkey.clone(), entry.value.clone()));
        }

        // Convert expected entries to a set
        let expected_set: HashSet<(Vec<u8>, Vec<u8>, Vec<u8>)> =
            expected_entries.iter().cloned().collect();

        // Compare results
        if scan_results != expected_set {
            is_consistent = false;

            let missing_entries: Vec<_> = expected_set.difference(&scan_results).collect();
            let extra_entries: Vec<_> = scan_results.difference(&expected_set).collect();

            println!("Discrepancies found in scan at timestamp {}:", ts);

            for (key, pkey, value) in missing_entries {
                println!(
                    "Missing entry: key '{}', pkey '{}', value '{}'",
                    bytes_to_string(key),
                    bytes_to_string(pkey),
                    bytes_to_string(value)
                );
            }

            for (key, pkey, value) in extra_entries {
                println!(
                    "Extra entry: key '{}', pkey '{}', value '{}'",
                    bytes_to_string(key),
                    bytes_to_string(pkey),
                    bytes_to_string(value)
                );
            }
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
