use anyhow::{Ok, Result};
use clap::{Parser, ValueEnum};
use fbtree::bp::{get_in_mem_pool, ContainerKey};
use fbtree::mvcc_index::hash_heap::hash_heap_table::HeapHashTable;
use fbtree::mvcc_index::hash_join::chained_hash_table::ChainedHashTable;
use fbtree::mvcc_index::linear_hash::linear_hash_table::linear_hash_table::LinearHashTable;
use fbtree::mvcc_index::rust_hash_map::rust_hash_map::MvccRustHashMap;
use fbtree::mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable;
use fbtree::mvcc_index::{BoxMvccIndexMemPool, HashTableType, MvccIndex, TxId};
use fbtree::prelude::Timestamp;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TxOperation {
    pub tx_id: u64,
    pub ts: u64,
    pub op: String,
    pub pkey: Vec<u8>,
    pub join_key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,
    Heap,
    Rust,
    Linear,
    Partition,
}

fn random_bytes(rng: &mut SmallRng, len: usize) -> Vec<u8> {
    let mut buf = vec![0u8; len];
    rng.fill(&mut buf[..]);
    buf
}

pub fn generate_insert_ops(
    row_count: usize,
    num_join_keys: usize,
    join_key_size: usize,
    pkey_size: usize,
    value_size: usize,
    table_map: &mut HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
) -> Vec<TxOperation> {
    let mut rng = SmallRng::from_entropy();

    let mut join_keys = Vec::with_capacity(num_join_keys);
    for _ in 0..num_join_keys {
        let jk = random_bytes(&mut rng, join_key_size);
        join_keys.push(jk);
    }

    let mut used_pkeys = HashSet::with_capacity(row_count);

    let mut ops = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let jk_idx = rng.gen_range(0..num_join_keys);
        let selected_jk = &join_keys[jk_idx];

        let pkey = loop {
            let candidate = random_bytes(&mut rng, pkey_size);
            if !used_pkeys.contains(&candidate) {
                used_pkeys.insert(candidate.clone());
                break candidate;
            }
        };

        let value = random_bytes(&mut rng, value_size);
        table_map.insert(pkey.clone(), (selected_jk.clone(), value.clone()));

        let op = TxOperation {
            tx_id: 0,
            ts: 0,
            op: "insert".to_string(),
            pkey,
            join_key: selected_jk.clone(),
            value,
        };

        ops.push(op);
    }

    ops
}

pub fn generate_update_ops(
    row_count: usize,
    update_ratio: f64,
    value_size: usize,
    start_ts: Timestamp,
    num_tx: usize,
    table_map: &mut HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
) -> Vec<TxOperation> {
    let mut rng = SmallRng::from_entropy();
    let total_updates = ((row_count as f64) * update_ratio).ceil() as usize;
    let ops_per_tx = (total_updates as f64 / num_tx as f64).ceil() as usize;

    let all_keys: Vec<_> = table_map.keys().cloned().collect();
    let mut ops = Vec::with_capacity(total_updates);

    for i in 0..num_tx {
        let ts = start_ts + i as u64;
        let tx_id = ts;

        let sampled_keys = all_keys
            .choose_multiple(&mut rng, ops_per_tx)
            .cloned()
            .collect::<Vec<_>>();

        for pkey in sampled_keys {
            if let Some((join_key, _)) = table_map.get(&pkey) {
                let join_key = join_key.clone();
                let new_val = random_bytes(&mut rng, value_size);
                table_map.insert(pkey.clone(), (join_key.clone(), new_val.clone()));

                ops.push(TxOperation {
                    tx_id,
                    ts,
                    op: "update".to_string(),
                    pkey,
                    join_key,
                    value: new_val,
                });
            }
        }
    }

    ops
}

pub fn generate_get_ops(
    row_count: usize,
    get_ratio: f64,
    recent_get_ratio: f64,
    max_ts: Timestamp,
    tx_id: TxId,
    table_map: &HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>,
) -> Vec<TxOperation> {
    let mut rng = SmallRng::from_entropy();
    let total_gets = ((row_count as f64) * get_ratio).ceil() as usize;
    let recent_gets = ((total_gets as f64) * recent_get_ratio).ceil() as usize;
    let history_gets = total_gets.saturating_sub(recent_gets);

    let all_keys: Vec<_> = table_map.keys().cloned().collect();
    let mut ops = Vec::with_capacity(total_gets);

    // 1) recent reads (max_ts)
    for _ in 0..recent_gets {
        let pkey = all_keys.choose(&mut rng).unwrap().clone();
        let (join_key, _) = table_map.get(&pkey).unwrap();

        ops.push(TxOperation {
            tx_id,
            ts: max_ts,
            op: "get".to_string(),
            pkey,
            join_key: join_key.clone(),
            value: vec![], // not used in get
        });
    }

    // 2) older reads (random ts in 0..max_ts)
    for _ in 0..history_gets {
        let pkey = all_keys.choose(&mut rng).unwrap().clone();
        let (join_key, _) = table_map.get(&pkey).unwrap();
        let ts = rng.gen_range(0..max_ts);

        ops.push(TxOperation {
            tx_id,
            ts,
            op: "get".to_string(),
            pkey,
            join_key: join_key.clone(),
            value: vec![],
        });
    }

    ops
}

fn run_txs_no_repair(
    ops: &[TxOperation],
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<Duration> {
    let start = Instant::now();
    for op in ops {
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
            "tx_begin" | "tx_commit" => { /* no-op */ }
            "scan_with_join_key" => {
                let _ = hash_join_table.scan_key_vec(&op.join_key, op.ts)?;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }
    Ok(start.elapsed())
}

fn run_txs_read_repair(
    ops: &[TxOperation],
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<Duration> {
    let start = Instant::now();
    for op in ops {
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
                let _ = hash_join_table.get_read_repair(&op.join_key, &op.pkey, op.ts)?;
            }
            "tx_begin" | "tx_commit" => { /* no-op */ }
            "scan_with_join_key" => {
                let _ = hash_join_table.scan_key_vec(&op.join_key, op.ts)?;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }
    Ok(start.elapsed())
}

fn run_txs_write_repair(
    ops: &[TxOperation],
    hash_join_table: &mut BoxMvccIndexMemPool,
) -> Result<Duration> {
    let start = Instant::now();
    for op in ops {
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
                hash_join_table.update_write_repair(
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
            "tx_begin" | "tx_commit" => { /* no-op */ }
            "scan_with_join_key" => {
                let _ = hash_join_table.scan_key_vec(&op.join_key, op.ts)?;
            }
            other => {
                eprintln!("Unknown operation: {}", other);
            }
        }
    }
    Ok(start.elapsed())
}

/// Parse human-readable sizes like "1M", "512K", or "100"
fn parse_human_readable_usize(s: &str) -> Result<usize, String> {
    let s = s.trim().to_ascii_lowercase();

    let (num_str, multiplier) = if let Some(stripped) = s.strip_suffix('k') {
        (stripped, 1_000)
    } else if let Some(stripped) = s.strip_suffix('m') {
        (stripped, 1_000_000)
    } else {
        (&s[..], 1)
    };

    let num = num_str
        .replace('_', "")
        .parse::<f64>()
        .map_err(|e| format!("Invalid number '{}': {}", s, e))?;

    let result = (num * (multiplier as f64)).round() as usize;
    Ok(result).map_err(|e| e.to_string())
}

#[derive(Parser, Debug)]
struct Cli {
    /// Row count (number of insert ops for creating the table)
    #[arg(short = 'r', long = "row-count", default_value = "1M", value_parser = parse_human_readable_usize)]
    row_count: usize,

    /// Number of distinct join-keys to generate
    #[arg(long = "num-join-keys")]
    num_join_keys: Option<usize>,

    /// Size (in bytes) of each join-key
    #[arg(short = 'j', long = "join-key-size", default_value = "100", value_parser = parse_human_readable_usize)]
    join_key_size: usize,

    /// Size (in bytes) of each pkey
    #[arg(short = 'p', long = "pkey-size", default_value = "100", value_parser = parse_human_readable_usize)]
    pkey_size: usize,

    /// Size (in bytes) of each value
    #[arg(short = 'v', long = "value-size", default_value = "800", value_parser = parse_human_readable_usize)]
    value_size: usize,

    /// Table type (chain, heap, rust, linear, partition)
    #[arg(short = 't', long = "table-type", default_value = "heap")]
    table_kind: TableType,

    /// Update ratio (0.0 - 1.0)
    #[arg(short = 'u', long = "update-ratio", default_value = "0.1")]
    update_ratio: f64,

    /// number of transactions
    #[arg(short = 'n', long = "num-tx", default_value = "10")]
    num_tx: usize,

    /// Get ratio (0.0 - 1.0)
    #[arg(short = 'g', long = "get-ratio", default_value = "0.5")]
    get_ratio: f64,

    /// Recent get ratio (0.0 - 1.0)
    #[arg(long = "recent-get-ratio")]
    recent_get_ratio: Option<f64>,

    /// Bucket_num
    #[arg(short = 'b', long = "bucket-num")]
    bucket_num: Option<usize>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let pkey_per_join_key = 500;
    let join_key_per_bucket = 2;

    let num_join_keys = cli
        .num_join_keys
        .unwrap_or(cli.row_count.max(1) / pkey_per_join_key)
        .max(1);
    let recent_get_ratio = cli
        .recent_get_ratio
        .unwrap_or(1.0 / (cli.num_tx + 1) as f64);
    let bucket_num = cli
        .bucket_num
        .unwrap_or(num_join_keys.max(1) / join_key_per_bucket)
        .max(1);

    let mut table_map: HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)> = HashMap::new();
    let insert_ops = generate_insert_ops(
        cli.row_count,
        num_join_keys,
        cli.join_key_size,
        cli.pkey_size,
        cli.value_size,
        &mut table_map,
    );
    let update_ops = generate_update_ops(
        cli.row_count,
        cli.update_ratio,
        cli.value_size,
        1,
        cli.num_tx,
        &mut table_map,
    );
    let get_ops = generate_get_ops(
        cli.row_count,
        cli.get_ratio,
        recent_get_ratio,
        cli.num_tx as Timestamp,
        (cli.num_tx + 1) as TxId,
        &table_map,
    );
    let hash_table_t = match cli.table_kind {
        TableType::Chain => HashTableType::RecentHistoryChained,
        TableType::Heap => HashTableType::HeapTable,
        TableType::Rust => HashTableType::RustHashMap,
        TableType::Linear => HashTableType::LinearHashTable,
        TableType::Partition => HashTableType::TsPartitionChained,
    };
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);

    println!("Hash table type: {:?}", hash_table_t);
    println!("Bucket number: {}", bucket_num);
    println!(
        "Pkey per Bucket: {}",
        pkey_per_join_key * join_key_per_bucket
    );
    println!("Join key per Bucket: {}", join_key_per_bucket);
    println!("Pkey per Join key: {}", pkey_per_join_key);
    println!("-----------------------------------------------------------------------");
    println!();
    println!("Row count: {}", cli.row_count);
    println!("Number of distinct join keys: {}", num_join_keys);
    println!("Join key size: {}", cli.join_key_size);
    println!("Pkey size: {}", cli.pkey_size);
    println!("Value size: {}", cli.value_size);
    println!();
    println!("Update ratio: {}", cli.update_ratio);
    println!(
        "Number of transactions (max Timestamp value): {}",
        cli.num_tx
    );
    println!();
    println!("Get ratio: {}", cli.get_ratio);
    println!("Recent get ratio: {}", recent_get_ratio);
    println!("-----------------------------------------------------------------------");
    println!();
    println!("No Repair");
    // no_repair
    {
        let mut table_no_repair = match hash_table_t {
            HashTableType::RecentHistoryChained => Box::new(
                ChainedHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
            HashTableType::HeapTable => Box::new(HeapHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::RustHashMap => Box::new(MvccRustHashMap::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::LinearHashTable => Box::new(LinearHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        let insert_dur = run_txs_no_repair(&insert_ops, &mut table_no_repair)?;
        println!("No repair insert duration: {:?}", insert_dur);
        let update_dur = run_txs_no_repair(&update_ops, &mut table_no_repair)?;
        println!("No repair update duration: {:?}", update_dur);
        let get_dur_1 = run_txs_no_repair(&get_ops, &mut table_no_repair)?;
        println!("No repair get duration 1: {:?}", get_dur_1);
        let get_dur_2 = run_txs_no_repair(&get_ops, &mut table_no_repair)?;
        println!("No repair get duration 2: {:?}", get_dur_2);
        let get_dur_3 = run_txs_no_repair(&get_ops, &mut table_no_repair)?;
        println!("No repair get duration 3: {:?}", get_dur_3);
    }

    println!();
    println!("Read Repair");
    // read_repair
    {
        let mut table_read_repair = match hash_table_t {
            HashTableType::RecentHistoryChained => Box::new(
                ChainedHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
            HashTableType::HeapTable => Box::new(HeapHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::RustHashMap => Box::new(MvccRustHashMap::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::LinearHashTable => Box::new(LinearHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        let insert_dur = run_txs_read_repair(&insert_ops, &mut table_read_repair)?;
        println!("Read repair insert duration: {:?}", insert_dur);
        let update_dur = run_txs_read_repair(&update_ops, &mut table_read_repair)?;
        println!("Read repair update duration: {:?}", update_dur);
        let get_dur_1 = run_txs_read_repair(&get_ops, &mut table_read_repair)?;
        println!("Read repair get duration 1: {:?}", get_dur_1);
        let get_dur_2 = run_txs_read_repair(&get_ops, &mut table_read_repair)?;
        println!("Read repair get duration 2: {:?}", get_dur_2);
        let get_dur_3 = run_txs_read_repair(&get_ops, &mut table_read_repair)?;
        println!("Read repair get duration 3: {:?}", get_dur_3);
    }

    println!();
    println!("Write Repair");
    // write_repair
    {
        let mut table_write_repair = match hash_table_t {
            HashTableType::RecentHistoryChained => Box::new(
                ChainedHashTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
            HashTableType::HeapTable => Box::new(HeapHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::RustHashMap => Box::new(MvccRustHashMap::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::LinearHashTable => Box::new(LinearHashTable::create_with_bucket_num(
                c_key,
                mem_pool.clone(),
                bucket_num,
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        let insert_dur = run_txs_write_repair(&insert_ops, &mut table_write_repair)?;
        println!("Write repair insert duration: {:?}", insert_dur);
        let update_dur = run_txs_write_repair(&update_ops, &mut table_write_repair)?;
        println!("Write repair update duration: {:?}", update_dur);
        let get_dur_1 = run_txs_write_repair(&get_ops, &mut table_write_repair)?;
        println!("Write repair get duration 1: {:?}", get_dur_1);
        let get_dur_2 = run_txs_write_repair(&get_ops, &mut table_write_repair)?;
        println!("Write repair get duration 2: {:?}", get_dur_2);
        let get_dur_3 = run_txs_write_repair(&get_ops, &mut table_write_repair)?;
        println!("Write repair get duration 3: {:?}", get_dur_3);
    }

    Ok(())
}
