use std::{
    collections::{HashMap, HashSet},
    default,
    time::Instant,
};

use anyhow::Error;
use fbtree::{mvcc_index::TxId, prelude::Timestamp};
use rand::{
    rngs::{SmallRng, StdRng},
    seq::SliceRandom,
    Rng, SeedableRng,
};
use std::time::Duration;

use crate::{
    cli::Cli,
    dbgen::DataSource,
    interface::{BoxMVIndex, OperationType},
};

const PKEY_PER_JOIN_KEY: usize = 500;
const JOIN_KEY_PER_BUCKET: usize = 20;

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

fn clear_cpu_cache() {
    let size = 256 * 1024 * 1024;
    let mut buf = vec![0u8; size];

    for x in buf.iter_mut() {
        *x = x.wrapping_add(1);
    }
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

    pub fn new_probe(
        tx_id: TxId,
        tx_ts: Timestamp,
        pkey: Vec<u8>,
        join_key: Vec<u8>,
        value: Vec<u8>,
        probe_ts: Timestamp,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Probe,
            probe_ts,
            pkey,
            join_key,
            value,
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

    pub fn new_gc(tx_id: TxId, tx_ts: Timestamp, read_ts: Timestamp) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::GbgCollect,
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

    pub cli: Cli,
    pub rng: SmallRng,

    pub data_source: DataSource,
}

impl TxBench {
    pub fn new(mut cli: Cli) -> Self {
        let rng = SmallRng::seed_from_u64(cli.seed);
        Self {
            txs: Vec::new(),
            read_ts_candidates: Vec::new(),
            read_txs: Vec::new(),
            tx_count: 0,
            next_ts: 0,

            cli: cli.clone(),
            rng,

            data_source: DataSource::new(cli),
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

    pub fn gen_initial_insert_from_cli(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        self.data_source.generate_customer_table();
        self.txs
            .push(Tx::new(OperationType::InitLoad, tx_id, tx_ts, vec![]));
    }

    pub fn gen_update_tx(&mut self, update_count: usize) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let mut ops = Vec::new();
        let mut pkey_set = HashSet::new();
        for _ in 0..update_count {
            // remove duplicate
            let mut op = self.data_source.generate_transactional_op();
            loop {
                if pkey_set.contains(&op.pkey) {
                    op = self.data_source.generate_transactional_op();
                    continue;
                } else {
                    pkey_set.insert(op.pkey.clone());
                    break;
                }
            }

            // insert op
            ops.push(TxOperation::new_update(
                tx_id,
                tx_ts,
                op.pkey,
                op.join_key,
                op.value,
            ));
        }
        self.txs
            .push(Tx::new(OperationType::Update, tx_id, tx_ts, ops));
    }

    pub fn gen_probe_tx(&mut self, probe_count: usize, probe_ts: Timestamp) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let mut ops = Vec::new();
        let mut pkey_set = HashSet::new();
        for _ in 0..probe_count {
            // remove duplicate
            let mut op = self.data_source.generate_transactional_op();
            loop {
                if pkey_set.contains(&op.pkey) {
                    op = self.data_source.generate_transactional_op();
                    continue;
                } else {
                    pkey_set.insert(op.pkey.clone());
                    break;
                }
            }

            // insert op
            ops.push(TxOperation::new_probe(
                tx_id,
                tx_ts,
                op.pkey,
                op.join_key,
                op.value,
                probe_ts,
            ));
        }
        self.txs
            .push(Tx::new(OperationType::Probe, tx_id, tx_ts, ops));
    }

    pub fn gen_scan_txs(&mut self, scan_ts: Timestamp) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        assert!(self.read_ts_candidates.contains(&scan_ts));
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::Scan,
            scan_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        );

        ops.push(op);
        let tx = Tx::new(OperationType::Scan, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    pub fn gen_scan_txs_random(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let scan_ts = self
            .read_ts_candidates
            .choose(&mut self.rng)
            .unwrap()
            .to_owned();
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::Scan,
            scan_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        );

        ops.push(op);
        let tx = Tx::new(OperationType::Scan, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    pub fn gen_mark_ts_txs(&mut self, mark_ts: Timestamp) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        assert_eq!(tx_ts, mark_ts, "tx_ts must be equal to mark_ts");
        // add to candidates
        self.read_ts_candidates.push(tx_ts);
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::MarkTs,
            mark_ts,
            vec![], // pkey is not used for marking ts
            vec![], // join_key is not used for marking ts
            vec![],
            vec![],
        );
        ops.push(op);

        let tx = Tx::new(OperationType::MarkTs, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    pub fn gen_delta_scan_tx(&mut self, start_idx: usize, rng: &mut SmallRng) {
        let all_ts = &self.read_ts_candidates[start_idx..];

        if all_ts.len() < 2 {
            panic!("Not enough read_ts candidates for delta scan, at least 2 required");
        }

        // randomly select 2 different timestamps
        let mut selected_ts = all_ts.choose_multiple(rng, 2);
        let read_ts1 = selected_ts.next().unwrap();
        let read_ts2 = selected_ts.next().unwrap();
        assert!(
            read_ts1 != read_ts2,
            "Selected timestamps must be different"
        );
        let from_ts = read_ts1.min(read_ts2);
        let to_ts = read_ts1.max(read_ts2);

        let op = TxOperation::new_delta_scan(0, *to_ts, *from_ts);
        let tx = Tx::new(OperationType::DeltaScan, 0, 0, vec![op]);
        self.txs.push(tx.clone());
        self.read_txs.push(tx);
    }

    pub fn gen_full_delta_scan_tx(&mut self) {
        let all_ts = &self.read_ts_candidates;

        if all_ts.len() < 2 {
            panic!("Not enough read_ts candidates for delta scan, at least 2 required");
        }

        let read_ts1 = all_ts.iter().min().unwrap();
        let read_ts2 = all_ts.iter().max().unwrap();
        assert!(
            read_ts1 != read_ts2,
            "Selected timestamps must be different"
        );
        let from_ts = read_ts1.min(read_ts2);
        let to_ts = read_ts1.max(read_ts2);

        let op = TxOperation::new_delta_scan(0, *to_ts, *from_ts);
        let tx = Tx::new(OperationType::DeltaScan, 0, 0, vec![op]);
        self.txs.push(tx.clone());
        self.read_txs.push(tx);
    }

    pub fn gen_txs(&mut self) {
        if self.cli.manual_txs.is_none() {
            self.gen_random_txs();
        } else {
            unimplemented!()
        }
    }

    pub fn gen_random_txs(&mut self) {
        // generate random transactions based on the cli parameters

        // load init txn
        self.gen_initial_insert_from_cli();

        // load markts txn
        self.gen_mark_ts_txs(1);

        for i in 2..self.cli.txn_count - 2 {
            let tx_type = if self.rng.gen_bool(self.cli.analytical_ratio) {
                if self.rng.gen_bool(0.2) {
                    OperationType::MarkTs
                } else {
                    OperationType::Probe
                }
            } else {
                OperationType::Update
            };

            match tx_type {
                OperationType::Update => {
                    let update_count = (self.cli.op_ratio
                        * self.data_source.get_custoemr_vec().len() as f64)
                        as usize;
                    self.gen_update_tx(update_count);
                }
                OperationType::Probe => {
                    let probe_count = (self.cli.op_ratio
                        * self.data_source.get_custoemr_vec().len() as f64)
                        as usize;
                    let probe_ts = self
                        .read_ts_candidates
                        .choose(&mut self.rng)
                        .unwrap()
                        .to_owned();
                    self.gen_probe_tx(probe_count, probe_ts);
                }
                OperationType::MarkTs => {
                    self.gen_mark_ts_txs(i as Timestamp);
                }
                OperationType::Scan => {
                    self.gen_scan_txs_random();
                }
                _default => {
                    panic!();
                }
            }
        }

        self.gen_mark_ts_txs(self.cli.txn_count as u64 - 2);
        for i in 0..self.cli.scan_count {
            self.gen_scan_txs(self.cli.txn_count as u64 - 2);
        }

        self.gen_full_delta_scan_tx();

        let mut rng = SmallRng::seed_from_u64(2333);
        for i in 0..self.cli.scan_count - 1 {
            self.gen_delta_scan_tx(1, &mut rng);
        }

        // garbage collection
        if self.cli.space_stat.is_none() {
            // only gc when no collecting space stats
            self.gen_gc_txs();
        }

        let mut rng = SmallRng::seed_from_u64(2333);
        for i in 0..self.cli.scan_count - 1 {
            self.gen_delta_scan_tx(0, &mut rng);
        }
    }

    pub fn gen_gc_txs(&mut self) {
        let tss = &mut self.read_ts_candidates;
        assert!(tss.len() >= 2);
        assert_eq!(tss.iter().min(), tss.first());
        let min_ts = tss.iter().min().unwrap().to_owned();
        let (first, right) = tss.split_first().unwrap();
        let new_tss = right.to_owned();
        let new_min_ts = new_tss.iter().min().unwrap().to_owned();
        self.read_ts_candidates = new_tss;

        let (tx_id, tx_ts) = self.gen_new_tx();

        let op = TxOperation::new_gc(tx_id, tx_ts, new_min_ts);
        let tx = Tx::new(OperationType::GbgCollect, tx_id, tx_ts, vec![op]);
        self.txs.push(tx.clone());
    }

    pub fn gen_manual_txs(&mut self) {
        todo!()
    }

    pub fn run_tx_no_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &BoxMVIndex,
    ) -> Result<Duration, Error> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        let mut is_need_scan_warpup = false;
        match tx.tx_type {
            OperationType::InitLoad => {
                hash_join_table.begin_txs(OperationType::InitLoad).unwrap();
                for op in self.data_source.get_custoemr_vec() {
                    hash_join_table.insert(
                        &op.generate_join_key(),
                        &op.generate_pkey(),
                        &op.generate_value(),
                    );
                }
                hash_join_table.end_txs(OperationType::InitLoad).unwrap();
            }
            OperationType::MarkTs => {
                let ts = tx.tx_ts;
                hash_join_table.mark_ts(ts);
                is_need_scan_warpup = true;
            }
            OperationType::Probe => {
                for op in &tx.ops {
                    let _ = hash_join_table.get(&op.join_key, &op.pkey, op.read_ts);
                }
            }
            OperationType::Update => {
                hash_join_table.begin_txs(OperationType::Update).unwrap();
                for op in &tx.ops {
                    hash_join_table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
                }
                hash_join_table.end_txs(OperationType::Update).unwrap();
            }
            OperationType::DeltaScan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.scan_delta(op.read_ts, op.tx_ts, false);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let iter = hash_join_table.scan(op.read_ts, false).unwrap();
                    for entry in iter {
                        assert_eq!(entry.2.len(), 688);
                    }
                }
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.garbage_collect(op.read_ts);
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
            OperationType::InitLoad => {
                println!(
                    "InitialLoad count: {:?}",
                    self.data_source.get_custoemr_vec().len()
                );
            }
            OperationType::Probe => {
                println!(
                    "Probe count: {:?} at read_ts: {:?}",
                    tx.ops.len(),
                    tx.ops[0].read_ts
                )
            }
            OperationType::Update => {
                println!("Update count: {:?}", tx.ops.len());
            }
            OperationType::MarkTs => {
                println!("MarkTs at read_ts: {:?}", tx.ops[0].tx_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    "DeltaScan from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan => {
                println!("Scan read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                println!("Garbage collection read_ts: {:?}", tx.ops[0].read_ts);
            }
        }
        if is_need_scan_warpup {
            hash_join_table.after_mark_ts(tx.tx_ts);
        }
        Ok(elapsed)
    }

    pub fn run_tx_read_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &BoxMVIndex,
    ) -> Result<Duration, Error> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        let mut is_need_scan_warpup = false;

        match tx.tx_type {
            OperationType::InitLoad => {
                for op in self.data_source.get_custoemr_vec() {
                    hash_join_table.insert(
                        &op.generate_join_key(),
                        &op.generate_pkey(),
                        &op.generate_value(),
                    );
                }
            }
            OperationType::Probe => {
                for op in &tx.ops {
                    let _ = hash_join_table.get(&op.join_key, &op.pkey, op.read_ts);
                }
            }
            OperationType::MarkTs => {
                let ts = tx.tx_ts;
                hash_join_table.mark_ts(ts);
                is_need_scan_warpup = true;
            }
            OperationType::Update => {
                hash_join_table.begin_txs(OperationType::Update).unwrap();
                for op in &tx.ops {
                    hash_join_table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
                }
                hash_join_table.end_txs(OperationType::Update).unwrap();
            }
            OperationType::DeltaScan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.scan_delta(op.read_ts, op.tx_ts, true);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let iter = hash_join_table.scan(op.read_ts, true).unwrap();
                    for entry in iter {
                        assert_eq!(entry.2.len(), 688);
                    }
                }
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.garbage_collect(op.read_ts);
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
            OperationType::Probe => {
                println!(
                    "Probe count: {:?} at read_ts: {:?}",
                    tx.ops.len(),
                    tx.ops[0].read_ts
                )
            }
            OperationType::InitLoad => {
                println!(
                    "InitialLoad count: {:?}",
                    self.data_source.get_custoemr_vec().len()
                );
            }
            OperationType::Update => {
                println!("Update count: {:?}", tx.ops.len());
            }
            OperationType::MarkTs => {
                println!("MarkTs at read_ts: {:?}", tx.ops[0].tx_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    "DeltaScan from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan => {
                println!("Scan read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                println!("Garbage collection read_ts: {:?}", tx.ops[0].read_ts);
            }
        }
        if is_need_scan_warpup {
            hash_join_table.after_mark_ts(tx.tx_ts);
        }
        Ok(elapsed)
    }

    pub fn run_tx_write_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &BoxMVIndex,
    ) -> Result<Duration, Error> {
        let tx = &self.txs[txs_idx as usize];
        let start = Instant::now();
        let mut is_need_scan_warmup = false;
        match tx.tx_type {
            OperationType::Probe => {
                for op in &tx.ops {
                    let _ = hash_join_table.get(&op.join_key, &op.pkey, op.read_ts);
                }
            }
            OperationType::InitLoad => {
                for op in self.data_source.get_custoemr_vec() {
                    hash_join_table.insert(
                        &op.generate_join_key(),
                        &op.generate_pkey(),
                        &op.generate_value(),
                    );
                }
            }
            OperationType::MarkTs => {
                let ts = tx.tx_ts;
                hash_join_table.mark_ts(ts);
                is_need_scan_warmup = true;
            }
            OperationType::Update => {
                hash_join_table.begin_txs(OperationType::UpdateWR).unwrap();
                for op in &tx.ops {
                    hash_join_table.update_write_repair(
                        &op.join_key,
                        &op.pkey,
                        &op.value,
                        op.tx_ts,
                    );
                }
                hash_join_table.end_txs(OperationType::UpdateWR).unwrap();
            }
            OperationType::DeltaScan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.scan_delta(op.read_ts, op.tx_ts, false);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let iter = hash_join_table.scan(op.read_ts, false).unwrap();
                    for entry in iter {
                        assert_eq!(entry.2.len(), 688);
                    }
                }
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                for op in &tx.ops {
                    let _ = hash_join_table.garbage_collect(op.read_ts);
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
            OperationType::Probe => {
                println!(
                    "Probe count: {:?} at read_ts: {:?}",
                    tx.ops.len(),
                    tx.ops[0].read_ts
                )
            }
            OperationType::InitLoad => {
                println!(
                    "InitialLoad count: {:?}",
                    self.data_source.get_custoemr_vec().len()
                );
            }
            OperationType::Update => {
                println!("Update count: {:?}", tx.ops.len());
            }
            OperationType::MarkTs => {
                println!("MarkTs at read_ts: {:?}", tx.ops[0].tx_ts);
            }
            OperationType::DeltaScan => {
                println!(
                    "DeltaScan from read_ts: {:?} to tx_ts: {:?}",
                    tx.ops[0].read_ts, tx.ops[0].tx_ts
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan => {
                println!("Scan read_ts: {:?}", tx.ops[0].read_ts);
            }
            OperationType::GbgCollect => {
                assert_eq!(tx.ops.len(), 1);
                println!("Garbage collection read_ts: {:?}", tx.ops[0].read_ts);
            }
        }

        if is_need_scan_warmup {
            hash_join_table.after_mark_ts(tx.tx_ts);
        }
        Ok(elapsed)
    }

    pub fn run_all_txs_no_repair(&self, hash_join_table: &BoxMVIndex) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_no_repair(txs_idx as TxId, hash_join_table);
        }
    }

    pub fn run_all_txs_read_repair(&self, hash_join_table: &BoxMVIndex) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_read_repair(txs_idx as TxId, hash_join_table);
        }
    }

    pub fn run_all_txs_write_repair(&self, hash_join_table: &BoxMVIndex) {
        for txs_idx in 0..self.txs.len() {
            let _ = self.run_tx_write_repair(txs_idx as TxId, hash_join_table);
        }
    }

    pub fn print_cli(&self) {
        let cli = &self.cli;
        println!("-----------------------------------------------------------------------");
        println!("Hash table type: {:?}", cli.table_type);
        // println!("Bucket number: {:?}", cli.bucket_num);
        // println!(
        //     "Pkey per Bucket: {}",
        //     PKEY_PER_JOIN_KEY * JOIN_KEY_PER_BUCKET
        // );
        // println!("- Join key per Bucket: {}", JOIN_KEY_PER_BUCKET);
        // println!("- Pkey per Join key: {}", PKEY_PER_JOIN_KEY);
        println!("Rng seed: {:?}", cli.seed);
        println!("-----------------------------------------------------------------------");
        println!();
        // println!(
        //     "Row count: {}",
        //     cli.row_count.to_formatted_string(&Locale::en)
        // );
        // println!("Number of distinct join keys: {:?}", cli.num_join_keys);
        // println!("Join key size: {}", cli.join_key_size);
        // println!("Pkey size: {}", cli.pkey_size);
        // println!("Value size: {}", cli.value_size);
        // println!();
        println!("Update ratio: {}", cli.op_ratio);
        println!("Analytical ratio: {}", cli.analytical_ratio);
        println!(
            "Number of transactions (max Timestamp value): {}",
            cli.txn_count
        );
        println!("Number of each scan transactions: {}", cli.scan_count);
        println!();
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
                OperationType::Probe => {
                    println!(
                        "Probe count: {:?} at read_ts: {:?}",
                        tx.ops.len(),
                        tx.ops[0].read_ts
                    )
                }
                OperationType::InitLoad => {
                    println!(
                        "Insert count: {:?}",
                        self.data_source.get_custoemr_vec().len()
                    );
                }
                OperationType::Update => {
                    println!("Update count: {:?}", tx.ops.len());
                }
                OperationType::MarkTs => {
                    println!("Mark Ts: {:?}", tx.ops[0].tx_ts);
                }
                OperationType::DeltaScan => {
                    println!(
                        "DeltaScan from read_ts: {:?} to tx_ts: {:?}",
                        tx.ops[0].read_ts, tx.ops[0].tx_ts
                    );
                }
                OperationType::UpdateWR => {
                    panic!();
                }
                OperationType::Scan => {
                    println!("Scan at ts: {:?}", tx.ops[0].read_ts);
                }
                OperationType::GbgCollect => {
                    assert_eq!(tx.ops.len(), 1);
                    println!("Garbage collection read_ts: {:?}", tx.ops[0].read_ts);
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
                OperationType::DeltaScan => {
                    println!(
                        "DeltaScan read_ts: {:>3} to tx_ts: {:>3}",
                        tx.ops[0].read_ts, tx.ops[0].tx_ts
                    );
                }
                OperationType::Scan => {
                    println!("Scan at ts: {:?}", tx.ops[0].read_ts);
                }
                _ => {
                    panic!("no other txn");
                }
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
