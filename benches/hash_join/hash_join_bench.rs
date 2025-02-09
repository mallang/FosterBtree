// use dashmap::mapref::entry;
// use fbtree::mvcc_index::{MvccEntry, MvccIndex, Timestamp};
// use fbtree::{mvcc_index::hash_join::chained_hash_table::ChainedHashTable, prelude::*};
// // use fbtree::{mvcc_index::hashtable_mu::mvcc_hash_join_table::MvccHashJoinTable, prelude::*};
// use std::collections::{HashMap, HashSet};
// use std::env;
use std::error::Error;
// use std::sync::Arc;
// use std::time::Instant;

// fn main() -> Result<(), Box<dyn Error>> {
//     let args: Vec<String> = env::args().collect();

//     // Variables to store arguments
//     let mut data_file: Option<String> = None;
//     let mut ops_file: Option<String> = None;
//     let mut recent_data_file: Option<String> = None;
//     let mut history_data_file: Option<String> = None;
//     // let mut scan_ops_file: Option<String> = None;
//     let mut limit_ops: Option<usize> = None;


//     // Simple argument parsing loop
//     // We expect something like:
//     // -df data.csv -of ops.csv -rdf recent_data.csv -hdf history_data.csv -sof scan_ops.csv -n 100
//     let mut i = 1;
//     while i < args.len() {
//         match args[i].as_str() {
//             "-df" => {
//                 if i + 1 < args.len() {
//                     data_file = Some(args[i + 1].clone());
//                     i += 2;
//                 } else {
//                     eprintln!("Error: -df requires a file path");
//                     return Ok(());
//                 }
//             }
//             "-of" => {
//                 if i + 1 < args.len() {
//                     ops_file = Some(args[i + 1].clone());
//                     i += 2;
//                 } else {
//                     eprintln!("Error: -of requires a file path");
//                     return Ok(());
//                 }
//             }
//             "-rdf" => {
//                 if i + 1 < args.len() {
//                     recent_data_file = Some(args[i + 1].clone());
//                     i += 2;
//                 } else {
//                     eprintln!("Error: -rdf requires a file path");
//                     return Ok(());
//                 }
//             }
//             "-hdf" => {
//                 if i + 1 < args.len() {
//                     history_data_file = Some(args[i + 1].clone());
//                     i += 2;
//                 } else {
//                     eprintln!("Error: -hdf requires a file path");
//                     return Ok(());
//                 }
//             }
//             // "-sof" => {
//             //     if i + 1 < args.len() {
//             //         scan_ops_file = Some(args[i + 1].clone());
//             //         i += 2;
//             //     } else {
//             //         eprintln!("Error: -sof requires a file path");
//             //         return Ok(());
//             //     }
//             // }
//             "-n" => {
//                 if i + 1 < args.len() {
//                     if let Ok(n) = args[i + 1].parse::<usize>() {
//                         limit_ops = Some(n);
//                         i += 2;
//                     } else {
//                         eprintln!("Warning: Invalid number after -n, ignoring...");
//                         i += 2;
//                     }
//                 } else {
//                     eprintln!("Warning: -n specified without a following number, ignoring...");
//                     i += 1;
//                 }
//             }
//             _ => {
//                 eprintln!("Unknown argument: {}", args[i]);
//                 i += 1;
//             }
//         }
//     }

//     // Check required arguments
//     if data_file.is_none()
//         || ops_file.is_none()
//         || recent_data_file.is_none()
//         || history_data_file.is_none()
//     // || scan_ops_file.is_none()
//     {
//         eprintln!("Usage:");
//         eprintln!("  {} -df <data_file> -of <ops_file> -rdf <recent_data_file> -hdf <history_data_file> -sof <scan_ops_file> [-n <num_ops>]", args[0]);
//         return Ok(());
//     }

//     let data_file = data_file.unwrap();
//     let ops_file = ops_file.unwrap();
//     let recent_data_file = recent_data_file.unwrap();
//     let history_data_file = history_data_file.unwrap();
//     // let scan_ops_file = scan_ops_file.unwrap();

//     println!("Data file: {}", data_file);
//     println!("Ops file: {}", ops_file);
//     println!("Recent data file: {}", recent_data_file);
//     println!("History data file: {}", history_data_file);
//     // println!("Scan operations file: {}", scan_ops_file);
//     if let Some(n) = limit_ops {
//         println!("Limiting operations to first {} ops", n);
//     }
//     println!();

//     // Read data and operations
//     let data = read_data_file(&data_file)?;
//     let mut ops = read_ops_file(&ops_file)?;
//     if let Some(n) = limit_ops {
//         if n < ops.len() {
//             ops.truncate(n);
//         }
//     }

//     let op_num = ops.len();
//     let data_num = data.len();

//     // Initialize the hash join table using the MvccIndex trait
//     let mem_pool = get_in_mem_pool();
//     let c_key = ContainerKey::new(0, 0);
//     let hash_join_table = ChainedHashTable::create(c_key, mem_pool.clone())?;

//     // Initialize Rust's default HashMap
//     let mut rust_hash_map: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();

//     //
//     // Measure and report data loading time for Rust HashMap
//     let start_time_hashmap_load = Instant::now();
//     // Load data into Rust's HashMap
//     for (key, pkey, value) in &data {
//         let entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), 0, u64::MAX);
//         rust_hash_map
//             .entry(key.clone())
//             .or_insert_with(Vec::new)
//             .push(entry);
//     }
//     let duration_hashmap_load = start_time_hashmap_load.elapsed();
//     // println!(
//     //     "Loaded {} entries into Rust HashMap in {:.2?}",
//     //     data_num, duration_hashmap_load
//     // );
//     println!(
//         "Loaded {} entries into Rust HashMap in {} ns",
//         data_num,
//         duration_hashmap_load.as_nanos()
//     );

//     //
//     // Measure and report data loading time for HashJoinTable
//     let start_time_hj_load = Instant::now();
//     // Load data into the hash join table
//     for (key, pkey, value) in &data {
//         hash_join_table.insert(key.clone(), pkey.clone(), 0, 0, value.clone())?;
//     }
//     let duration_hj_load = start_time_hj_load.elapsed();
//     // println!(
//     //     "Loaded {} entries into HashJoinTable in {:.2?}",
//     //     data_num, duration_hj_load
//     // );
//     println!(
//         "Loaded {} entries into HashJoinTable in {} ns",
//         data_num,
//         duration_hj_load.as_nanos()
//     );

//     println!();

//     //
//     // Start the benchmark for Rust's HashMap
//     let start_time_hashmap = Instant::now();

//     for op in &ops {
//         let key = op.key.clone();
//         match op.op_type.as_str() {
//             "insert" | "update" => {
//                 let entries = rust_hash_map.entry(key.clone()).or_insert_with(Vec::new);
//                 // End the previous version if exists for the same pkey
//                 if let Some(last_entry) = entries
//                     .iter_mut()
//                     .rev()
//                     .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
//                 {
//                     last_entry.end_ts = op.ts;
//                 }
//                 // Add the new version
//                 let new_entry = MvccEntry::new(
//                     key.clone(),
//                     op.pkey.clone(),
//                     op.value.clone(),
//                     op.ts,
//                     u64::MAX,
//                 );
//                 // let new_entry = MvccEntry {
//                 //     key: key.clone(),
//                 //     pkey: op.pkey.clone(),
//                 //     value: op.value.clone(),
//                 //     start_ts: op.ts,
//                 //     end_ts: u64::MAX,
//                 // };
//                 entries.push(new_entry);
//             }
//             "delete" => {
//                 if let Some(entries) = rust_hash_map.get_mut(&key) {
//                     if let Some(last_entry) = entries
//                         .iter_mut()
//                         .rev()
//                         .find(|e| e.pkey == op.pkey && e.end_ts == u64::MAX)
//                     {
//                         last_entry.end_ts = op.ts;
//                     }
//                 }
//             }
//             "get" => {
//                 if let Some(entries) = rust_hash_map.get(&key) {
//                     let value = entries.iter().rev().find(|entry| {
//                         entry.pkey == op.pkey && entry.start_ts <= op.ts && op.ts < entry.end_ts
//                     });
//                     // Use `value` as needed
//                     if let Some(_entry) = value {
//                         // println!("Got value: {}", bytes_to_string(&entry.value));
//                     } else {
//                         // No value found
//                     }
//                 } else {
//                     // Key does not exist
//                 }
//             }
//             "commit" | "scan" => {
//                 // No-op for Rust's HashMap
//             }
//             _ => {
//                 eprintln!("Unknown operation: {}", op.op_type);
//             }
//         }
//     }

//     let duration_hashmap = start_time_hashmap.elapsed();
//     // println!(
//     //     "Rust HashMap: Executed {} operations in {:.2?}",
//     //     op_num, duration_hashmap
//     // );
//     println!(
//         "Rust HashMap: Executed {} operations in {} ns",
//         op_num,
//         duration_hashmap.as_nanos()
//     );

//     //
//     // Start the benchmark for HashJoinTable
//     let start_time_hj = Instant::now();

//     // Execute operations from ops.csv on HashJoinTable
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
//             "commit" | "scan" => {
//                 // Implement commit if needed
//             }
//             _ => {
//                 eprintln!("Unknown operation: {}", op.op_type);
//             }
//         }
//     }

//     let duration_hj = start_time_hj.elapsed();
//     // println!(
//     //     "HashJoinTable: Executed {} operations in {:.2?}",
//     //     op_num, duration_hj
//     // );
//     println!(
//         "HashJoinTable: Executed {} operations in {} ns",
//         op_num,
//         duration_hj.as_nanos()
//     );

//     println!();

//     // Perform consistency checks
//     let expected_recent_data = read_expected_data_file(&recent_data_file)?;
//     let is_consistent_hj =
//         check_consistency_hash_join_table(&hash_join_table, &expected_recent_data)?;
//     println!(
//         "HashJoinTable recent data consistency check: {}",
//         if is_consistent_hj { "PASSED" } else { "FAILED" }
//     );

//     let expected_full_data = {
//         let mut data = read_expected_full_data_file(&recent_data_file)?;
//         let mut history_data = read_expected_full_data_file(&history_data_file)?;
//         data.append(&mut history_data);
//         data
//     };

//     let is_full_consistent_hj =
//         check_full_consistency_hash_join_table(&hash_join_table, &expected_full_data)?;
//     println!(
//         "HashJoinTable full consistency check: {}",
//         if is_full_consistent_hj {
//             "PASSED"
//         } else {
//             "FAILED"
//         }
//     );

//     let is_consistent_hashmap = check_consistency_hash_map(&rust_hash_map, &expected_recent_data);
//     println!(
//         "Rust HashMap consistency check: {}",
//         if is_consistent_hashmap {
//             "PASSED"
//         } else {
//             "FAILED"
//         }
//     );

//     let is_consistent =
//         check_consistency_between_hash_join_and_hash_map(&hash_join_table, &rust_hash_map)?;
//     println!(
//         "Consistency check between HashJoinTable and Rust HashMap: {}",
//         if is_consistent { "PASSED" } else { "FAILED" }
//     );

//     Ok(())
// }

// // Define a struct to represent an operation
// struct Operation {
//     tx_id: u64,
//     ts: u64,
//     op_type: String,
//     key: Vec<u8>,
//     pkey: Vec<u8>,
//     value: Vec<u8>,
// }

// // Function to read data.csv
// use std::fs::File;
// use std::io::{self, BufRead};

// fn read_data_file(file_path: &str) -> io::Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
//     let mut data = Vec::new();
//     let file = File::open(file_path)?;
//     for line in io::BufReader::new(file).lines() {
//         let line = line?;
//         if line.trim().is_empty() {
//             continue; // Skip empty lines
//         }
//         let parts: Vec<&str> = line.split(',').collect();
//         if parts.len() >= 3 {
//             let key = parts[0].as_bytes().to_vec();
//             let pkey = parts[1].as_bytes().to_vec();
//             let value = parts[2].as_bytes().to_vec();
//             data.push((key, pkey, value));
//         }
//     }
//     Ok(data)
// }

// // Function to read ops.csv
// fn read_ops_file(file_path: &str) -> io::Result<Vec<Operation>> {
//     let mut operations = Vec::new();
//     let file = File::open(file_path)?;
//     for line in io::BufReader::new(file).lines() {
//         let line = line?;
//         if line.trim().is_empty() {
//             continue; // Skip empty lines
//         }
//         // Use a CSV parser to handle commas in values
//         let mut rdr = csv::ReaderBuilder::new()
//             .has_headers(false)
//             .from_reader(line.as_bytes());
//         for result in rdr.records() {
//             let record = result?;
//             if record.len() >= 6 {
//                 let tx_id = record[0].parse::<u64>().unwrap_or(0);
//                 let ts = record[1].parse::<u64>().unwrap_or(0);
//                 let op_type = record[2].to_string();
//                 let key = record[3].as_bytes().to_vec();
//                 let pkey = record[4].as_bytes().to_vec();
//                 let value = record[5].as_bytes().to_vec();
//                 operations.push(Operation {
//                     tx_id,
//                     ts,
//                     op_type,
//                     key,
//                     pkey,
//                     value,
//                 });
//             } else {
//                 eprintln!("Invalid line (expected 6 fields): {}", line);
//             }
//         }
//     }
//     Ok(operations)
// }

// // Function to read expected data after ops (recent_data_after_ops.csv)
// fn read_expected_data_file(file_path: &str) -> io::Result<HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>> {
//     let mut expected_data = HashMap::new();
//     let file = File::open(file_path)?;
//     for line in io::BufReader::new(file).lines() {
//         let line = line?;
//         if line.trim().is_empty() {
//             continue; // Skip empty lines
//         }
//         let parts: Vec<&str> = line.split(',').collect();
//         if parts.len() >= 5 {
//             let start_ts = parts[0];
//             let end_ts = parts[1];
//             let key = parts[2].as_bytes().to_vec();
//             let pkey = parts[3].as_bytes().to_vec();
//             let value = parts[4].as_bytes().to_vec();
//             expected_data.insert((key, pkey), value);
//         } else {
//             eprintln!("Invalid line (expected at least 5 fields): {}", line);
//         }
//     }
//     Ok(expected_data)
// }

// fn read_expected_full_data_file(
//     file_path: &str,
// ) -> io::Result<Vec<(Timestamp, Timestamp, Vec<u8>, Vec<u8>, Vec<u8>)>> {
//     let mut data = Vec::new();
//     let file = File::open(file_path)?;
//     for line in io::BufReader::new(file).lines() {
//         let line = line?;
//         if line.trim().is_empty() {
//             continue; // Skip empty lines
//         }
//         let parts: Vec<&str> = line.split(',').collect();
//         if parts.len() >= 5 {
//             let start_ts = parts[0].parse::<u64>().unwrap_or(0);
//             let end_ts = if parts[1] == "-1" {
//                 u64::MAX
//             } else {
//                 parts[1].parse::<u64>().unwrap_or(u64::MAX)
//             };
//             let key = parts[2].as_bytes().to_vec();
//             let pkey = parts[3].as_bytes().to_vec();
//             let value = parts[4].as_bytes().to_vec();
//             data.push((start_ts, end_ts, key, pkey, value));
//         }
//     }
//     Ok(data)
// }

// // Function to check consistency of HashJoinTable with expected data
// fn check_consistency_hash_join_table(
//     hash_join_table: &ChainedHashTable<impl MemPool>,
//     expected_data: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
// ) -> Result<bool, Box<dyn Error>> {
//     let mut is_consistent = true;

//     // Collect entries from HashJoinTable
//     let mut hjt_entries: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
//     let scanner = hash_join_table.scan(u64::MAX)?;
//     for entry in scanner {
//         // hjt_entries.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
//         hjt_entries.insert((entry.0, entry.1), entry.2);
//     }

//     // Compare expected data with HashJoinTable entries
//     for ((key, pkey), expected_value) in expected_data {
//         match hjt_entries.get(&(key.clone(), pkey.clone())) {
//             Some(value) => {
//                 if value != expected_value {
//                     eprintln!(
//                         "Mismatch for key '{}', pkey '{}': expected '{}', got '{}'",
//                         bytes_to_string(&key),
//                         bytes_to_string(&pkey),
//                         bytes_to_string(expected_value),
//                         bytes_to_string(value)
//                     );
//                     is_consistent = false;
//                 }
//             }
//             None => {
//                 eprintln!(
//                     "Missing entry in HashJoinTable for key '{}', pkey '{}'",
//                     bytes_to_string(&key),
//                     bytes_to_string(&pkey)
//                 );
//                 is_consistent = false;
//             }
//         }
//     }

//     // Check for any extra entries in HashJoinTable not present in expected data
//     for ((key, pkey), value) in hjt_entries {
//         if !expected_data.contains_key(&(key.clone(), pkey.clone())) {
//             eprintln!(
//                 "Extra entry in HashJoinTable: key '{}', pkey '{}', value '{}'",
//                 bytes_to_string(&key),
//                 bytes_to_string(&pkey),
//                 bytes_to_string(&value)
//             );
//             is_consistent = false;
//         }
//     }

//     Ok(is_consistent)
// }

// fn check_consistency_between_hash_join_and_hash_map(
//     hash_join_table: &ChainedHashTable<impl MemPool>,
//     rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>,
// ) -> Result<bool, Box<dyn Error>> {
//     let mut is_consistent = true;

//     // Collect entries from HashJoinTable valid at ts = u64::MAX
//     let mut hjt_entries: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
//     let scanner = hash_join_table.scan(u64::MAX)?;
//     for entry in scanner {
//         // hjt_entries.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
//         hjt_entries.insert((entry.0, entry.1), entry.2);
//     }

//     // Collect current entries from rust_hash_map
//     let rust_entries = get_current_entries_from_rust_hash_map(rust_hash_map);

//     // Compare entries in Rust HashMap with entries in HashJoinTable
//     for ((key, pkey), value) in &rust_entries {
//         match hjt_entries.get(&(key.clone(), pkey.clone())) {
//             Some(hjt_value) => {
//                 if hjt_value != value {
//                     eprintln!(
//                         "Mismatch for key '{}', pkey '{}': Rust HashMap value '{}', HashJoinTable value '{}'",
//                         bytes_to_string(key),
//                         bytes_to_string(pkey),
//                         bytes_to_string(value),
//                         bytes_to_string(hjt_value)
//                     );
//                     is_consistent = false;
//                 }
//             }
//             None => {
//                 eprintln!(
//                     "Missing entry in HashJoinTable for key '{}', pkey '{}'",
//                     bytes_to_string(key),
//                     bytes_to_string(pkey)
//                 );
//                 is_consistent = false;
//             }
//         }
//     }

//     // Check for any extra entries in HashJoinTable not present in Rust HashMap
//     for ((key, pkey), hjt_value) in &hjt_entries {
//         if !rust_entries.contains_key(&(key.clone(), pkey.clone())) {
//             eprintln!(
//                 "Extra entry in HashJoinTable: key '{}', pkey '{}', value '{}'",
//                 bytes_to_string(key),
//                 bytes_to_string(pkey),
//                 bytes_to_string(hjt_value)
//             );
//             is_consistent = false;
//         }
//     }

//     Ok(is_consistent)
// }

// fn check_full_consistency_hash_join_table(
//     hash_join_table: &ChainedHashTable<impl MemPool>,
//     expected_data: &Vec<(u64, u64, Vec<u8>, Vec<u8>, Vec<u8>)>,
// ) -> Result<bool, Box<dyn Error>> {
//     let mut is_consistent = true;

//     // Collect all entries from HashJoinTable
//     let mut hjt_entries: HashSet<MvccEntry> = HashSet::new();
//     let scanner = hash_join_table.scan_all()?; // Implement scan_all method
//     for entry in scanner {
//         hjt_entries.insert(entry);
//     }

//     // Create a HashSet of expected entries for comparison
//     let expected_entries: HashSet<MvccEntry> = expected_data
//         .iter()
//         .map(|(start_ts, end_ts, key, pkey, value)| {
//             MvccEntry::new(key.clone(), pkey.clone(), value.clone(), *start_ts, *end_ts)
//         })
//         .collect();

//     // Compare the sets
//     if hjt_entries != expected_entries {
//         is_consistent = false;
//         let missing_entries = expected_entries.difference(&hjt_entries);
//         let extra_entries = hjt_entries.difference(&expected_entries);

//         for entry in missing_entries {
//             eprintln!(
//                 "Missing entry in HashJoinTable: start_ts '{}', end_ts '{}', key '{}', pkey '{}', value '{}'",
//                 entry.start_ts,
//                 if entry.end_ts == u64::MAX { -1 } else { entry.end_ts as i64 },
//                 bytes_to_string(&entry.key),
//                 bytes_to_string(&entry.pkey),
//                 bytes_to_string(&entry.value)
//             );
//         }

//         for entry in extra_entries {
//             eprintln!(
//                 "Extra entry in HashJoinTable: start_ts '{}', end_ts '{}', key '{}', pkey '{}', value '{}'",
//                 entry.start_ts,
//                 if entry.end_ts == u64::MAX { -1 } else { entry.end_ts as i64 },
//                 bytes_to_string(&entry.key),
//                 bytes_to_string(&entry.pkey),
//                 bytes_to_string(&entry.value)
//             );
//         }
//     }

//     Ok(is_consistent)
// }

// // fn read_scan_ops_file(
// //     file_path: &str,
// // ) -> Result<Vec<(u64, Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>)>, Box<dyn Error>> {
// //     let mut scan_operations = Vec::new();

// //     let file = File::open(file_path)?;
// //     let reader = io::BufReader::new(file);
// //     let mut lines = reader.lines();

// //     while let Some(line) = lines.next() {
// //         let line = line?;
// //         if line.trim().is_empty() {
// //             continue;
// //         }

// //         let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
// //         if parts.len() == 3 && parts[0] == "scan" && parts[1] == "ts" {
// //             let ts = parts[2].parse::<u64>()?;
// //             let mut entries = Vec::new();

// //             while let Some(entry_line) = lines.next() {
// //                 let entry_line = entry_line?;
// //                 if entry_line.trim().is_empty() {
// //                     break;
// //                 }
// //                 let entry_parts: Vec<&str> = entry_line.split(',').map(|s| s.trim()).collect();
// //                 if entry_parts.len() >= 3 {
// //                     let key = entry_parts[0].as_bytes().to_vec();
// //                     let pkey = entry_parts[1].as_bytes().to_vec();
// //                     let value = entry_parts[2].as_bytes().to_vec();
// //                     entries.push((key, pkey, value));
// //                 }
// //             }
// //             scan_operations.push((ts, entries));
// //         }
// //     }

// //     Ok(scan_operations)
// // }

// // Function to convert byte arrays to strings safely
// fn bytes_to_string(bytes: &[u8]) -> String {
//     match std::str::from_utf8(bytes) {
//         Ok(s) => s.to_string(),
//         Err(_) => bytes
//             .iter()
//             .map(|b| format!("{:02X}", b))
//             .collect::<String>(),
//     }
// }

// fn get_current_entries(rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>) -> Vec<MvccEntry> {
//     let mut current_entries = Vec::new();
//     for (key, entries) in rust_hash_map {
//         for entry in entries {
//             if entry.end_ts == u64::MAX {
//                 current_entries.push(entry.clone());
//             }
//         }
//     }
//     current_entries
// }

// fn get_current_entries_from_rust_hash_map(
//     rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>,
// ) -> HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> {
//     let mut current_entries = HashMap::new();
//     for (key, entries) in rust_hash_map {
//         for entry in entries {
//             if entry.end_ts == u64::MAX {
//                 current_entries.insert((key.clone(), entry.pkey.clone()), entry.value.clone());
//             }
//         }
//     }
//     current_entries
// }

// fn scan_rust_hash_map(rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>, ts: u64) -> Vec<MvccEntry> {
//     let mut results = Vec::new();
//     for entries in rust_hash_map.values() {
//         for entry in entries {
//             if entry.start_ts <= ts && ts < entry.end_ts {
//                 results.push(entry.clone());
//             }
//         }
//     }
//     results
// }

// fn check_consistency_hash_map(
//     rust_hash_map: &HashMap<Vec<u8>, Vec<MvccEntry>>,
//     expected_data: &HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
// ) -> bool {
//     let current_entries = get_current_entries(rust_hash_map);
//     let mut is_consistent = true;

//     let mut rust_entries_map: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>> = HashMap::new();
//     for entry in current_entries {
//         rust_entries_map.insert((entry.key.clone(), entry.pkey.clone()), entry.value.clone());
//     }

//     // Compare current entries with expected data
//     for ((key, pkey), expected_value) in expected_data {
//         match rust_entries_map.get(&(key.clone(), pkey.clone())) {
//             Some(value) => {
//                 if value != expected_value {
//                     eprintln!(
//                         "Mismatch for key '{}', pkey '{}': expected '{}', got '{}'",
//                         bytes_to_string(key),
//                         bytes_to_string(pkey),
//                         bytes_to_string(expected_value),
//                         bytes_to_string(value)
//                     );
//                     is_consistent = false;
//                 }
//             }
//             None => {
//                 eprintln!(
//                     "Missing entry in Rust HashMap for key '{}', pkey '{}'",
//                     bytes_to_string(key),
//                     bytes_to_string(pkey)
//                 );
//                 is_consistent = false;
//             }
//         }
//     }

//     // Check for any extra entries in rust_entries_map not present in expected data
//     for ((key, pkey), value) in rust_entries_map {
//         if !expected_data.contains_key(&(key.clone(), pkey.clone())) {
//             eprintln!(
//                 "Extra entry in Rust HashMap: key '{}', pkey '{}', value '{}'",
//                 bytes_to_string(&key),
//                 bytes_to_string(&pkey),
//                 bytes_to_string(&value)
//             );
//             is_consistent = false;
//         }
//     }

//     is_consistent
// }

fn main() -> Result<(), Box<dyn Error>> {
    Ok(())
}
