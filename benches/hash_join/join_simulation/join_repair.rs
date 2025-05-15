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
use num_format::{Locale, ToFormattedString};
use rand::rngs::{SmallRng, StdRng};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::{Rng, RngCore};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const PKEY_PER_JOIN_KEY: usize = 500;
const JOIN_KEY_PER_BUCKET: usize = 5;
const LINEAR_BUCKET_NUM: usize = 2048;

#[derive(Debug, Clone)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Get,
    ScanKey,
    Scan,
    DeltaScan,
}

#[derive(Debug, Clone)]
pub struct TxOperation {
    pub tx_id: TxId,
    pub tx_ts: Timestamp,
    pub op: OperationType,
    pub read_ts: Timestamp, // for read operations
    pub pkey: Vec<u8>,
    pub join_key: Vec<u8>,
    pub value: Vec<u8>,
    pub gc_ts: Vec<Timestamp>, // ts that we want to gc
}
impl TxOperation {
    pub fn new(
        tx_id: TxId,
        tx_ts: Timestamp,
        op: OperationType,
        read_ts: Timestamp,
        pkey: Vec<u8>,
        join_key: Vec<u8>,
        value: Vec<u8>,
        gc_ts: Vec<Timestamp>,
    ) -> Self {
        Self {
            tx_id,
            tx_ts,
            op,
            read_ts,
            pkey,
            join_key,
            value,
            gc_ts,
        }
    }

    pub fn new_insert(
        tx_id: TxId,
        tx_ts: Timestamp,
        pkey: Vec<u8>,
        join_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Insert,
            0,
            pkey,
            join_key,
            value,
            vec![],
        )
    }

    pub fn new_get(
        tx_id: TxId,
        tx_ts: Timestamp,
        pkey: Vec<u8>,
        join_key: Vec<u8>,
        read_ts: Timestamp,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Get,
            read_ts,
            pkey,
            join_key,
            vec![],
            vec![],
        )
    }

    pub fn new_update(
        tx_id: TxId,
        tx_ts: Timestamp,
        pkey: Vec<u8>,
        join_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Update,
            0,
            pkey,
            join_key,
            value,
            vec![],
        )
    }

    pub fn new_delete(tx_id: TxId, tx_ts: Timestamp, pkey: Vec<u8>, join_key: Vec<u8>) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Delete,
            0,
            pkey,
            join_key,
            vec![],
            vec![],
        )
    }

    pub fn new_scan_key(
        tx_id: TxId,
        tx_ts: Timestamp,
        join_key: Vec<u8>,
        read_ts: Timestamp,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::ScanKey,
            read_ts,
            vec![],
            join_key,
            vec![],
            vec![],
        )
    }

    pub fn new_scan_all(tx_id: TxId, tx_ts: Timestamp, read_ts: Timestamp) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Scan,
            read_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        )
    }

    pub fn new_delta_scan(tx_id: TxId, tx_ts: Timestamp, read_ts: Timestamp) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::DeltaScan,
            read_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        )
    }
}

#[derive(Debug, Clone)]
pub struct Tx {
    pub tx_type: OperationType,
    pub tx_id: TxId,
    pub tx_ts: Timestamp,
    pub ops: Vec<TxOperation>,
}
impl Tx {
    pub fn new(
        tx_type: OperationType,
        tx_id: TxId,
        tx_ts: Timestamp,
        ops: Vec<TxOperation>,
    ) -> Self {
        Self {
            tx_type,
            tx_id,
            tx_ts,
            ops,
        }
    }
    pub fn add_op(&mut self, op: TxOperation) {
        self.ops.push(op);
    }
}

pub struct TxBench {
    pub txs: Vec<Tx>, // txs[0] is the initial insert
    pub read_ts_candidates: Vec<Timestamp>,
    pub read_txs: Vec<Tx>,
    pub tx_count: TxId,
    pub next_ts: Timestamp,

    pub join_keys: Vec<Vec<u8>>,
    pub join_key_set: HashSet<Vec<u8>>,
    pub key_pairs: Vec<(Vec<u8>, Vec<u8>)>, // (pkey, join_key)
    pub pkey_set: HashSet<Vec<u8>>,

    pub table_map: HashMap<Vec<u8>, HashMap<Vec<u8>, Vec<(Timestamp, Vec<u8>)>>>, // key -> pkey -> VersionList(ts, value)

    pub cli: Cli,
    pub rng: SmallRng,

    pub row_count: usize,
}

impl TxBench {
    pub fn new(mut cli: Cli) -> Self {
        let pkey_per_join_key = PKEY_PER_JOIN_KEY;
        let join_key_per_bucket = JOIN_KEY_PER_BUCKET;

        let num_join_keys = cli
            .num_join_keys
            .unwrap_or(cli.row_count.max(1) / pkey_per_join_key)
            .max(1);
        cli.num_join_keys = Some(num_join_keys);
        let recent_get_ratio = cli
            .recent_get_ratio
            .unwrap_or(1.0 / (cli.num_tx + 1) as f64);
        cli.recent_get_ratio = Some(recent_get_ratio);
        let bucket_num = cli
            .bucket_num
            .unwrap_or(num_join_keys.max(1) / join_key_per_bucket)
            .max(1);
        cli.bucket_num = Some(bucket_num);
        let seed = match cli.seed {
            Some(seed) => seed,
            None => {
                let mut entropy = StdRng::from_entropy();
                entropy.next_u64()
            }
        };
        cli.seed = Some(seed);

        let rng = SmallRng::seed_from_u64(cli.seed.unwrap_or(0));
        Self {
            txs: Vec::new(),
            read_ts_candidates: Vec::new(),
            read_txs: Vec::new(),
            tx_count: 0,
            next_ts: 0,

            join_keys: Vec::new(),
            join_key_set: HashSet::new(),
            key_pairs: Vec::new(),
            pkey_set: HashSet::new(),

            table_map: HashMap::new(),

            cli,
            rng,

            row_count: 0,
        }
    }

    pub fn gen_new_tx(&mut self) -> (TxId, Timestamp) {
        let tx_id = self.tx_count;
        let tx_ts = self.next_ts;
        self.tx_count += 1;
        self.next_ts += 1;
        (tx_id, tx_ts)
    }

    pub fn gen_new_ts(&mut self) -> Timestamp {
        let ts = self.next_ts;
        self.next_ts += 1;
        ts
    }

    fn random_bytes(&mut self, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        self.rng.fill(&mut buf[..]);
        buf
    }

    pub fn gen_initial_insert(
        &mut self,
        row_count: usize,
        num_join_keys: usize,
        join_key_size: usize,
        pkey_size: usize,
        value_size: usize,
    ) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        for _ in 0..num_join_keys {
            let jkey = loop {
                let candidate = self.random_bytes(join_key_size);
                if self.join_key_set.insert(candidate.clone()) {
                    break candidate;
                }
            };
            self.join_keys.push(jkey);
        }

        let mut ops = Vec::new();
        for _ in 0..row_count {
            let jkey_idx = self.rng.gen_range(0..num_join_keys);
            let join_key = self.join_keys[jkey_idx].clone();

            let pkey = loop {
                let candidate = self.random_bytes(pkey_size);
                if self.pkey_set.insert(candidate.clone()) {
                    break candidate;
                }
            };
            self.key_pairs.push((pkey.clone(), join_key.clone()));

            let value = self.random_bytes(value_size);
            let pkey_map = self
                .table_map
                .entry(join_key.clone())
                .or_insert_with(HashMap::new);
            let version_list = pkey_map.entry(pkey.clone()).or_insert_with(Vec::new);
            version_list.push((0, value.clone()));

            let op = TxOperation::new_insert(tx_id, tx_ts, pkey, join_key, value);
            ops.push(op);
        }
        self.txs
            .push(Tx::new(OperationType::Insert, tx_id, tx_ts, ops));

        self.row_count = row_count;
    }

    pub fn gen_initial_insert_from_cli(&mut self) {
        self.gen_initial_insert(
            self.cli.row_count,
            self.cli.num_join_keys.unwrap(),
            self.cli.join_key_size,
            self.cli.pkey_size,
            self.cli.value_size,
        );
    }

    pub fn gen_insert_tx(&mut self, insert_count: usize) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        for _ in 0..insert_count {
            let jkey_idx = self.rng.gen_range(0..self.join_keys.len());
            let join_key = self.join_keys[jkey_idx].clone();

            let pkey = loop {
                let candidate = self.random_bytes(self.cli.pkey_size);
                if self.pkey_set.insert(candidate.clone()) {
                    break candidate;
                }
            };
            self.key_pairs.push((pkey.clone(), join_key.clone()));

            let value = self.random_bytes(self.cli.value_size);
            let pkey_map = self
                .table_map
                .entry(join_key.clone())
                .or_insert_with(HashMap::new);
            let version_list = pkey_map.entry(pkey.clone()).or_insert_with(Vec::new);
            version_list.push((tx_ts, value.clone()));

            let op = TxOperation::new_insert(tx_id, tx_ts, pkey, join_key, value);
            self.txs
                .push(Tx::new(OperationType::Insert, tx_id, tx_ts, vec![op]));
        }
    }

    pub fn gen_update_tx(&mut self, update_count: usize) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let sampled_pairs = self
            .key_pairs
            .choose_multiple(&mut self.rng, update_count)
            .cloned()
            .collect::<Vec<_>>();
        let mut ops = Vec::new();
        for (pkey, join_key) in sampled_pairs {
            let new_val = self.random_bytes(self.cli.value_size);

            if let Some(pkey_map) = self.table_map.get_mut(&join_key) {
                let version_list = pkey_map.entry(pkey.clone()).or_insert_with(Vec::new);
                version_list.push((tx_ts, new_val.clone()));

                let op = TxOperation::new_update(tx_id, tx_ts, pkey, join_key, new_val);
                ops.push(op);
            } else {
                panic!("Join_key should found in table map");
            }
        }
        self.txs
            .push(Tx::new(OperationType::Update, tx_id, tx_ts, ops));
    }

    pub fn gen_update_tx_skewed(&mut self, update_count: usize) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let gen_func = |cnt: usize, range: usize, exp: f64| -> Vec<_> {
            use rand::distributions::Distribution;
            let mut rng = rand::thread_rng();
            let zipf = zipf::ZipfDistribution::new(range, exp).unwrap();
            (0..cnt).into_iter().map(|_| {
                let sample_idx = zipf.sample(&mut rng) - 1;
                self.key_pairs[sample_idx].clone()
            })
            .collect()
        };
        let sampled_pairs: Vec<(Vec<u8>, Vec<u8>)> = gen_func(update_count, self.key_pairs.len(), self.cli.update_skew_exp);
        let mut ops = Vec::new();
        for (pkey, join_key) in sampled_pairs {
            let new_val = self.random_bytes(self.cli.value_size);

            if let Some(pkey_map) = self.table_map.get_mut(&join_key) {
                let version_list = pkey_map.entry(pkey.clone()).or_insert_with(Vec::new);
                version_list.push((tx_ts, new_val.clone()));

                let op = TxOperation::new_update(tx_id, tx_ts, pkey, join_key, new_val);
                ops.push(op);
            } else {
                panic!("Join_key should found in table map");
            }
        }
        self.txs
            .push(Tx::new(OperationType::Update, tx_id, tx_ts, ops));
    }

    pub fn gen_delete_tx(&mut self, delete_count: usize) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let sampled_pairs = self
            .key_pairs
            .choose_multiple(&mut self.rng, delete_count)
            .cloned()
            .collect::<Vec<_>>();

        let mut ops = Vec::new();
        for (pkey, join_key) in sampled_pairs {
            if let Some(pkey_map) = self.table_map.get_mut(&join_key) {
                let version_list = pkey_map.entry(pkey.clone()).or_insert_with(Vec::new);
                version_list.push((tx_ts, vec![]));

                let op = TxOperation::new_delete(tx_id, tx_ts, pkey.clone(), join_key.clone());
                ops.push(op);

                // remove key_pairs
                if let Some(pos) = self
                    .key_pairs
                    .iter()
                    .position(|(pk, jkey)| pk == &pkey && jkey == &join_key)
                {
                    self.key_pairs.remove(pos);
                }
                // remove from pkey_set
                self.pkey_set.remove(&pkey);
            } else {
                panic!("Join_key should found in table map");
            }
        }
        self.txs
            .push(Tx::new(OperationType::Delete, tx_id, tx_ts, ops));
        self.row_count -= delete_count;
    }

    pub fn gen_get_tx(&mut self, get_count: usize, recent_get_ratio: f64) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let recent_gets = ((get_count as f64) * recent_get_ratio).ceil() as usize;
        let history_gets = get_count.saturating_sub(recent_gets);

        let mut ops = Vec::new();

        for _ in 0..recent_gets {
            let (pkey, join_key) = self.key_pairs.choose(&mut self.rng).unwrap().clone();
            let op = TxOperation::new_get(tx_id, tx_id, pkey, join_key, tx_id);
            ops.push(op);
        }
        for _ in 0..history_gets {
            let (pkey, join_key) = self.key_pairs.choose(&mut self.rng).unwrap().clone();
            let ts = self.rng.gen_range(0..(tx_ts - 1).max(1));
            let op = TxOperation::new_get(tx_id, ts, pkey, join_key, ts);
            ops.push(op);
        }
        self.txs
            .push(Tx::new(OperationType::Get, tx_id, tx_ts, ops));
    }

    pub fn gen_scan_key_tx(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        self.read_ts_candidates.push(tx_ts);
        let join_key = self.join_keys.choose(&mut self.rng).unwrap().clone();
        let read_ts = self
            .read_ts_candidates
            .choose(&mut self.rng)
            .unwrap()
            .clone();
        let op = TxOperation::new_scan_key(tx_id, tx_ts, join_key.clone(), read_ts);
        let tx = Tx::new(OperationType::ScanKey, tx_id, tx_ts, vec![op]);
        self.read_txs.push(tx.clone());
        self.txs.push(tx);
    }

    pub fn gen_scan_tx(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        self.read_ts_candidates.push(tx_ts);
        let read_ts = self
            .read_ts_candidates
            .choose(&mut self.rng)
            .unwrap()
            .clone();
        let op = TxOperation::new_scan_all(tx_id, tx_ts, read_ts);
        let tx = Tx::new(OperationType::Scan, tx_id, tx_ts, vec![op.clone()]);
        self.read_txs.push(tx.clone());
        self.txs.push(tx);
    }

    pub fn gen_scan_tx_with_ts(&mut self, read_ts: Timestamp) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        self.read_ts_candidates.push(tx_ts);
        let op = TxOperation::new_scan_all(tx_id, tx_ts, read_ts);
        let tx = Tx::new(OperationType::Scan, tx_id, tx_ts, vec![op.clone()]);
        self.read_txs.push(tx.clone());
        self.txs.push(tx);
    }

    pub fn gen_delta_scan_tx(&mut self) {
        let tx_ts = self.gen_new_ts();

        // randomly select tx from read_txs
        let old_tx = self.read_txs.choose(&mut self.rng).unwrap();

        let tx_id = old_tx.tx_id;
        let read_ts = match old_tx.tx_type {
            OperationType::ScanKey => old_tx.ops[0].read_ts,
            OperationType::Scan => old_tx.ops[0].read_ts,
            OperationType::DeltaScan => old_tx.ops[0].tx_ts,
            _ => panic!("Invalid tx_type for delta scan"),
        };

        // remove read_ts from read_ts_candidates
        if let Some(pos) = self.read_ts_candidates.iter().position(|&ts| ts == read_ts) {
            self.read_ts_candidates.remove(pos);
        }
        // TODO: garbage collect the read_ts when pos == 0

        // remove old_tx from read_txs
        if let Some(pos) = self.read_txs.iter().position(|tx| tx.tx_id == old_tx.tx_id) {
            self.read_txs.remove(pos);
        }

        self.read_ts_candidates.push(tx_ts);
        let op = TxOperation::new_delta_scan(tx_id, tx_ts, read_ts);
        let tx = Tx::new(OperationType::DeltaScan, tx_id, tx_ts, vec![op.clone()]);
        self.read_txs.push(tx.clone());
        self.txs.push(tx);
    }

    pub fn gen_delta_scan_tx_with_ts(&mut self, from: Timestamp) {
        let (tx_id, tx_ts) = self.gen_new_tx();

        let read_ts = from;

        let op = TxOperation::new_delta_scan(tx_id, tx_ts, read_ts);
        let tx = Tx::new(OperationType::DeltaScan, tx_id, tx_ts, vec![op.clone()]);
        self.read_txs.push(tx.clone());
        self.txs.push(tx);
    }

    pub fn gen_read_tx_with_ratio(
        &mut self,
        scan_key_ratio: f64,
        scan_all_ratio: f64,
        mut delta_scan_ratio: f64,
    ) {
        if self.read_ts_candidates.is_empty() {
            delta_scan_ratio = 0.0;
        }
        let total_ratio = scan_key_ratio + scan_all_ratio + delta_scan_ratio;
        if total_ratio == 0.0 {
            println!(
                "Total ratio is 0.0, no read tx generated, at least one ratio should be > 0.0"
            );
            return;
        }

        let random_value = self.rng.gen_range(0.0..total_ratio);
        if random_value < scan_key_ratio {
            self.gen_scan_key_tx();
        } else if random_value < scan_key_ratio + scan_all_ratio {
            self.gen_scan_tx();
        } else {
            self.gen_delta_scan_tx();
        }
    }

    pub fn gen_random_read_tx(&mut self) {
        self.gen_read_tx_with_ratio(1.0, 1.0, 1.0)
    }

    pub fn gen_txs_with_ratio(
        &mut self,
        total_tx_count: usize,
        write_tx_ratio: f64,
        insert_tx_ratio: f64,
        insert_tx_count: usize,
        update_tx_ratio: f64,
        update_tx_count: usize,
        delete_tx_ratio: f64,
        delete_tx_count: usize,
        read_tx_ratio: f64,
        scan_key_tx_ratio: f64,
        scan_all_tx_ratio: f64,
        delta_scan_tx_ratio: f64,
    ) {
        let read_write_ratio = write_tx_ratio + read_tx_ratio;
        if read_write_ratio == 0.0 {
            println!("Total ratio is 0.0, no tx generated, at least one ratio should be > 0.0");
            return;
        }
        for _ in 0..total_tx_count {
            let random_read_write = self.rng.gen_range(0.0..read_write_ratio);
            if random_read_write < write_tx_ratio {
                let random_write = self
                    .rng
                    .gen_range(0.0..(insert_tx_ratio + update_tx_ratio + delete_tx_ratio));
                if random_write < insert_tx_ratio {
                    self.gen_insert_tx(insert_tx_count);
                } else if random_write < insert_tx_ratio + update_tx_ratio {
                    self.gen_update_tx(update_tx_count);
                } else {
                    self.gen_delete_tx(delete_tx_count);
                }
            } else {
                self.gen_read_tx_with_ratio(
                    scan_key_tx_ratio,
                    scan_all_tx_ratio,
                    delta_scan_tx_ratio,
                );
            }
        }
    }

    pub fn gen_txs(&mut self) {
        if self.cli.manual_txs.is_none() {
            self.gen_random_txs();
        } else {
            self.gen_manual_txs();
        }
    }

    pub fn gen_random_txs(&mut self) {
        self.gen_initial_insert_from_cli();
        let insert_tx_count =
            ((self.cli.row_count as f64) * self.cli.insert_count_ratio).ceil() as usize;
        let update_tx_count =
            ((self.cli.row_count as f64) * self.cli.update_count_ratio).ceil() as usize;
        let delete_tx_count =
            ((self.cli.row_count as f64) * self.cli.delete_count_ratio).ceil() as usize;
        self.gen_txs_with_ratio(
            self.cli.num_tx,
            self.cli.write_tx_ratio,
            self.cli.insert_tx_ratio,
            insert_tx_count,
            self.cli.update_tx_ratio,
            update_tx_count,
            self.cli.delete_tx_ratio,
            delete_tx_count,
            self.cli.read_tx_ratio,
            self.cli.scan_key_tx_ratio,
            self.cli.scan_all_tx_ratio,
            self.cli.delta_scan_tx_ratio,
        );
    }

    pub fn gen_manual_txs(&mut self) {
        if self.cli.manual_txs.is_none() {
            panic!("manual_txs is not given");
        }
        self.gen_initial_insert_from_cli();

        let row_count = self.cli.row_count;
        let txs_raw = self.cli.manual_txs.clone().unwrap();

        let txs: Vec<String> = txs_raw
            .split_whitespace()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        for tx in txs {
            let parts: Vec<_> = tx.split(',').map(|s| s.trim()).collect();
            if parts.len() != 2 {
                panic!("Invalid manual tx format: '{}'", tx);
            }

            let op = parts[0].to_lowercase();
            let param = parts[1];

            match op.as_str() {
                "i" | "insert" => {
                    let count = (row_count as f64 * param.parse::<f64>().unwrap()) as usize;
                    self.gen_insert_tx(count);
                }
                "u" | "update" => {
                    let count = (row_count as f64 * param.parse::<f64>().unwrap()) as usize;
                    self.gen_update_tx(count);
                }
                "us" | "update_skewed" => {
                    let count = (row_count as f64 * param.parse::<f64>().unwrap()) as usize;
                    self.gen_update_tx_skewed(count);
                }
                "d" | "delete" => {
                    let count = (row_count as f64 * param.parse::<f64>().unwrap()) as usize;
                    self.gen_delete_tx(count);
                }
                "g" | "get" => {
                    let count = (row_count as f64 * param.parse::<f64>().unwrap()) as usize;
                    self.gen_get_tx(count, self.cli.recent_get_ratio.unwrap_or(1.0));
                }
                "scan_key" => {
                    todo!("manual scan_key not implemented");
                }
                "scan" => {
                    let ts = param.parse::<usize>().unwrap();
                    self.gen_scan_tx_with_ts(ts as Timestamp);
                }
                "delta_scan" => {
                    let ts = param.parse::<u64>().unwrap();
                    self.gen_delta_scan_tx_with_ts(ts);
                }
                _ => {
                    panic!("Unknown op: '{}'", op);
                }
            }
        }
    }

    pub fn run_tx_no_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &mut BoxMvccIndexMemPool,
    ) -> Result<Duration> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        match tx.tx_type {
            OperationType::Insert => {
                for op in &tx.ops {
                    hash_join_table
                        .insert(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Update => {
                for op in &tx.ops {
                    hash_join_table
                        .update(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Delete => {
                for op in &tx.ops {
                    hash_join_table
                        .delete(&op.join_key, &op.pkey, op.tx_ts, op.tx_id)
                        .unwrap();
                }
            }
            OperationType::Get => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .get(&op.join_key, &op.pkey, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::ScanKey => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .scan_key_vec(&op.join_key, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::Scan => {
                for op in &tx.ops {
                    let _ = hash_join_table.scan(op.read_ts).unwrap();
                }
            }
            OperationType::DeltaScan => {
                for op in &tx.ops {
                    let _ = hash_join_table.delta_scan(op.read_ts, op.tx_ts).unwrap();
                }
            }
        }
        let elapsed = start.elapsed();
        print!(
            "[No Repair] idx: {:>3}, tx_id: {:>3}, tx_type: {:>10}, duration: {:?}, ",
            txs_idx,
            tx.tx_id,
            format!("{:?}", tx.tx_type),
            elapsed
        );
        match tx.tx_type {
            OperationType::Insert => {
                println!(" Insert count: {:?}", tx.ops.len());
            }
            OperationType::Update => {
                println!(" Update count: {:?}", tx.ops.len());
            }
            OperationType::Delete => {
                println!(" Delete count: {:?}", tx.ops.len());
            }
            OperationType::Get => {
                println!(" Get count: {:?}", tx.ops.len());
            }
            OperationType::ScanKey => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::Scan => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    " from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
        }
        Ok(elapsed)
    }

    pub fn run_all_txs_no_repair(&self, hash_join_table: &mut BoxMvccIndexMemPool) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_no_repair(txs_idx as TxId, hash_join_table);
        }
    }

    pub fn run_tx_read_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &mut BoxMvccIndexMemPool,
    ) -> Result<Duration> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        match tx.tx_type {
            OperationType::Insert => {
                for op in &tx.ops {
                    hash_join_table
                        .insert(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Update => {
                for op in &tx.ops {
                    hash_join_table
                        .update(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Delete => {
                for op in &tx.ops {
                    hash_join_table
                        .delete(&op.join_key, &op.pkey, op.tx_ts, op.tx_id)
                        .unwrap();
                }
            }
            OperationType::Get => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .get_read_repair(&op.join_key, &op.pkey, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::ScanKey => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .scan_key_vec_read_repair(&op.join_key, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::Scan => {
                for op in &tx.ops {
                    let _ = hash_join_table.scan_read_repair(op.read_ts).unwrap();
                }
            }
            OperationType::DeltaScan => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .delta_scan_read_repair(op.read_ts, op.tx_ts)
                        .unwrap();
                }
            }
        }
        let elapsed = start.elapsed();
        print!(
            "[Read Repair] idx: {:>3}, tx_id: {:>3}, tx_type: {:>10}, duration: {:?}, ",
            txs_idx,
            tx.tx_id,
            format!("{:?}", tx.tx_type),
            elapsed
        );
        match tx.tx_type {
            OperationType::Insert => {
                println!(" Insert count: {:?}", tx.ops.len());
            }
            OperationType::Update => {
                println!(" Update count: {:?}", tx.ops.len());
            }
            OperationType::Delete => {
                println!(" Delete count: {:?}", tx.ops.len());
            }
            OperationType::Get => {
                println!(" Get count: {:?}", tx.ops.len());
            }
            OperationType::ScanKey => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::Scan => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    " from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
        }
        Ok(elapsed)
    }

    pub fn run_all_txs_read_repair(&self, hash_join_table: &mut BoxMvccIndexMemPool) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_read_repair(txs_idx as TxId, hash_join_table);
        }
    }

    fn run_tx_write_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &mut BoxMvccIndexMemPool,
    ) -> Result<Duration> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        match tx.tx_type {
            OperationType::Insert => {
                for op in &tx.ops {
                    hash_join_table
                        .insert(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Update => {
                hash_join_table.bulk_update_start().unwrap();
                for op in &tx.ops {
                    hash_join_table
                        .update_write_repair(
                            op.join_key.clone(),
                            op.pkey.clone(),
                            op.tx_ts,
                            op.tx_id,
                            op.value.clone(),
                        )
                        .unwrap();
                }
                hash_join_table.bulk_update_end().unwrap();
                hash_join_table
                    .split_at_ts(tx.ops.first().unwrap().tx_ts + 1)
                    .unwrap();
            }
            OperationType::Delete => {
                for op in &tx.ops {
                    hash_join_table
                        .delete(&op.join_key, &op.pkey, op.tx_ts, op.tx_id)
                        .unwrap();
                }
            }
            OperationType::Get => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .get(&op.join_key, &op.pkey, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::ScanKey => {
                for op in &tx.ops {
                    let _ = hash_join_table
                        .scan_key_vec(&op.join_key, op.read_ts)
                        .unwrap();
                }
            }
            OperationType::Scan => {
                for op in &tx.ops {
                    let _ = hash_join_table.scan(op.read_ts).unwrap();
                }
            }
            OperationType::DeltaScan => {
                for op in &tx.ops {
                    let _ = hash_join_table.delta_scan(op.read_ts, op.tx_ts).unwrap();
                }
            }
        }
        let elapsed = start.elapsed();
        print!(
            "[Write Repair] idx: {:>3}, tx_id: {:>3}, tx_type: {:>10}, duration: {:?}, ",
            txs_idx,
            tx.tx_id,
            format!("{:?}", tx.tx_type),
            elapsed
        );
        match tx.tx_type {
            OperationType::Insert => {
                println!(" Insert count: {:?}", tx.ops.len());
            }
            OperationType::Update => {
                println!(" Update count: {:?}", tx.ops.len());
            }
            OperationType::Delete => {
                println!(" Delete count: {:?}", tx.ops.len());
            }
            OperationType::Get => {
                println!(" Get count: {:?}", tx.ops.len());
            }
            OperationType::ScanKey => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::Scan => {
                println!(" read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    " from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
        }
        Ok(elapsed)
    }

    pub fn run_all_txs_write_repair(&self, hash_join_table: &mut BoxMvccIndexMemPool) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_write_repair(txs_idx as TxId, hash_join_table);
        }
    }

    pub fn print_cli(&self) {
        let cli = &self.cli;

        println!("Hash table type: {:?}", cli.table_type);
        println!("Bucket number: {:?}", cli.bucket_num);
        println!(
            "Pkey per Bucket: {}",
            PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET
        );
        println!("- Join key per Bucket: {}", JOIN_KEY_PER_BUCKET);
        println!("- Pkey per Join key: {}", PKEY_PER_JOIN_KEY);
        println!("Rng seed: {:?}", cli.seed);
        println!("-----------------------------------------------------------------------");
        println!();
        println!(
            "Row count: {}",
            cli.row_count.to_formatted_string(&Locale::en)
        );
        println!("Number of distinct join keys: {:?}", cli.num_join_keys);
        println!("Join key size: {}", cli.join_key_size);
        println!("Pkey size: {}", cli.pkey_size);
        println!("Value size: {}", cli.value_size);
        println!();
        println!("Update ratio: {}", cli.update_tx_ratio);
        println!(
            "Number of transactions (max Timestamp value): {}",
            cli.num_tx
        );
        println!();
        println!("Get ratio: {}", cli.get_count_ratio);
        println!("Recent get ratio: {:?}", cli.recent_get_ratio);
        println!("update skew_exponent: {:?}", cli.update_skew_exp);
        println!("-----------------------------------------------------------------------");
        println!();
    }

    pub fn print_txs(&self) {
        println!("txs:");
        // print idx, tx_id, tx_type
        for (idx, tx) in self.txs.iter().enumerate() {
            print!(
                "idx: {:>3}, tx_id: {:>3}, tx_ts: {:>3}, tx_type: {:>10}, ",
                idx,
                tx.tx_id,
                tx.tx_ts,
                format!("{:?}", tx.tx_type)
            );
            match tx.tx_type {
                OperationType::Insert => {
                    println!(" Insert count: {:?}", tx.ops.len());
                }
                OperationType::Update => {
                    println!(" Update count: {:?}", tx.ops.len());
                }
                OperationType::Delete => {
                    println!(" Delete count: {:?}", tx.ops.len());
                }
                OperationType::Get => {
                    println!(" Get count: {:?}", tx.ops.len());
                }
                OperationType::ScanKey => {
                    println!(" read_ts: {:?}", tx.ops[0].read_ts);
                }
                OperationType::Scan => {
                    println!(" read_ts: {:?}", tx.ops[0].read_ts);
                }
                OperationType::DeltaScan => {
                    println!(
                        " from read_ts: {:?} to tx_ts: {:?}",
                        tx.ops[0].read_ts, tx.ops[0].tx_ts
                    );
                }
            }
        }
        println!("-----------------------------------------------------------------------");
        println!();
        println!("read_txs:");
        for tx in &self.read_txs {
            print!(
                "tx_id: {:>3}, tx_type: {:>10}, ",
                tx.tx_id,
                format!("{:?}", tx.tx_type)
            );
            match tx.tx_type {
                OperationType::ScanKey => {
                    println!(" read_ts: {:>3}", tx.ops[0].read_ts);
                }
                OperationType::Scan => {
                    println!(" read_ts: {:>3}", tx.ops[0].read_ts);
                }
                OperationType::DeltaScan => {
                    println!(
                        " read_ts: {:>3} to tx_ts: {:>3}",
                        tx.ops[0].read_ts, tx.ops[0].tx_ts
                    );
                }
                _ => {}
            }
        }
        println!("-----------------------------------------------------------------------");
        println!();
        print!("read_ts_candidates: ");
        for ts in &self.read_ts_candidates {
            print!("{:?}, ", ts);
        }
        println!();
        println!("-----------------------------------------------------------------------");
        println!();
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TableType {
    Chain,
    Heap,
    Rust,
    Linear,
    Partition,
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
pub struct Cli {
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
    table_type: TableType,

    /// number of transactions
    #[arg(short = 'n', long = "num-tx", default_value = "10")]
    num_tx: usize,

    /// Write tx ratio
    #[arg(short = 'w', long = "write-tx-ratio", default_value = "0.7")]
    write_tx_ratio: f64,

    /// Update tx ratio
    #[arg(short = 'u', long = "update-tx-ratio", default_value = "1.0")]
    update_tx_ratio: f64,

    /// Update count ratio
    #[arg(long = "update-count-ratio", default_value = "0.1")]
    update_count_ratio: f64,

    /// Insert ratio
    #[arg(long = "insert-tx-ratio", default_value = "0.0")]
    insert_tx_ratio: f64,

    /// Insert count ratio
    #[arg(long = "insert-count-ratio", default_value = "0.1")]
    insert_count_ratio: f64,

    /// Delete ratio
    #[arg(long = "delete-tx-ratio", default_value = "0.0")]
    delete_tx_ratio: f64,

    /// Delete count ratio
    #[arg(long = "delete-count-ratio", default_value = "0.1")]
    delete_count_ratio: f64,

    /// Get tx ratio
    #[arg(long = "get-tx-ratio", default_value = "0.0")]
    get_tx_ratio: f64,

    /// Get count ratio
    #[arg(long = "get-count-ratio", default_value = "0.1")]
    get_count_ratio: f64,

    /// Recent get ratio (0.0 - 1.0)
    #[arg(long = "recent-get-ratio")]
    recent_get_ratio: Option<f64>,

    /// Read tx ratio
    #[arg(short = 'r', long = "read-tx-ratio", default_value = "0.3")]
    read_tx_ratio: f64,

    /// Scan key tx ratio
    #[arg(long = "scan-key-tx-ratio", default_value = "1.0")]
    scan_key_tx_ratio: f64,

    /// Scan all tx ratio
    #[arg(long = "scan-all-tx-ratio", default_value = "0.0")]
    scan_all_tx_ratio: f64,

    /// Delta scan tx ratio
    #[arg(long = "delta-scan-tx-ratio", default_value = "0.0")]
    delta_scan_tx_ratio: f64,

    /// Bucket_num
    #[arg(short = 'b', long = "bucket-num")]
    bucket_num: Option<usize>,

    /// Seed for random number generation
    #[arg(short = 's', long = "seed")]
    seed: Option<u64>,

    /// Manually specify txs like "u,0.1 scan,3"
    #[arg(long = "manual-txs")]
    manual_txs: Option<String>,

    #[arg(long = "skew-exponent", default_value = "1.03")]
    update_skew_exp: f64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Generate benchmark data
    let mut bench = TxBench::new(cli);
    bench.gen_txs();
    bench.print_cli();
    bench.print_txs();

    // Create HashJoin table
    let hash_table_t = match bench.cli.table_type {
        TableType::Chain => HashTableType::RecentHistoryChained,
        TableType::Heap => HashTableType::HeapTable,
        TableType::Rust => HashTableType::RustHashMap,
        TableType::Linear => HashTableType::LinearHashTable,
        TableType::Partition => HashTableType::TsPartitionChained,
    };
    let bucket_num = bench.cli.bucket_num.unwrap();

    println!("No Repair");
    // no_repair
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

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
                bucket_num * (PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET / 100),
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        bench.run_all_txs_no_repair(&mut table_no_repair);
    }

    println!();
    println!("No Repair");
    // no_repair
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

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
                bucket_num * (PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET / 100),
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        bench.run_all_txs_no_repair(&mut table_no_repair);
    }

    println!();
    println!("Read Repair");
    // read_repair
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 1);

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
                bucket_num * (PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET / 100),
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        bench.run_all_txs_read_repair(&mut table_read_repair);
    }

    println!();
    println!("Write Repair");
    // write_repair
    {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 2);

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
                bucket_num * (PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET / 100),
            )?) as BoxMvccIndexMemPool,
            HashTableType::TsPartitionChained => Box::new(
                TsPartitionedTable::create_with_bucket_num(c_key, mem_pool.clone(), bucket_num)?,
            ) as BoxMvccIndexMemPool,
        };
        bench.run_all_txs_write_repair(&mut table_write_repair);
    }

    Ok(())
}
