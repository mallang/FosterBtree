use std::{
    collections::{HashMap, HashSet},
    default,
    iter::Scan,
    sync::atomic::{AtomicPtr, AtomicUsize},
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
    pub gc_ts: Vec<Timestamp>,                 // ts that we want to gc
    pub delta_scan_ts: (Timestamp, Timestamp), // (from, to)
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
            delta_scan_ts: (0, 0),
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
        join_key: Vec<u8>,
        probe_ts: Timestamp,
    ) -> Self {
        Self::new(
            tx_id,
            tx_ts,
            OperationType::Probe,
            probe_ts,
            vec![],
            join_key,
            vec![],
            vec![],
        )
    }

    pub fn new_delta_scan(tx_id: TxId, tx_ts: Timestamp, from: Timestamp, to: Timestamp) -> Self {
        Self {
            tx_id,
            tx_ts,
            op: OperationType::DeltaScan,
            read_ts: 0,
            pkey: vec![],
            join_key: vec![],
            value: vec![],
            gc_ts: vec![],
            delta_scan_ts: (from, to),
        }
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
    pub recent_ts_candidates: Vec<Timestamp>,
    pub history_ts_candidates: Vec<Timestamp>,
    pub read_txs: Vec<Tx>,
    pub tx_count: TxId,
    pub next_ts: Timestamp,

    pub cli: Cli,
    pub rng: SmallRng,

    pub data_source: DataSource,
    pub updates_since_mark: usize,
}

impl TxBench {
    pub fn new(mut cli: Cli) -> Self {
        let rng = SmallRng::seed_from_u64(cli.seed);
        Self {
            txs: Vec::new(),
            read_ts_candidates: Vec::new(),
            recent_ts_candidates: Vec::new(),
            history_ts_candidates: vec![],
            read_txs: Vec::new(),
            tx_count: 0,
            next_ts: 0,

            cli: cli.clone(),
            rng,

            data_source: DataSource::new(cli),
            updates_since_mark: 0,
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
        self.history_ts_candidates
            .extend_from_slice(&self.recent_ts_candidates);
        self.recent_ts_candidates.clear();
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
        self.updates_since_mark += 1;
        self.maybe_publish_readable_ts(false);
    }

    fn gen_probe_tx_with_read_ts(
        &mut self,
        probe_count: usize,
        read_ts_override: Option<Timestamp>,
    ) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let read_ts = read_ts_override.unwrap_or(tx_ts);
        let mut ops = Vec::new();
        for _ in 0..probe_count {
            // remove duplicate
            let join_key = self.data_source.generate_join_key();

            // insert op
            ops.push(TxOperation::new_probe(
                tx_id,
                tx_ts,
                join_key.to_vec(),
                read_ts,
            ));
        }
        self.txs
            .push(Tx::new(OperationType::Probe, tx_id, tx_ts, ops));
    }

    pub fn gen_probe_tx(&mut self, probe_count: usize, probe_ts: Timestamp) {
        self.gen_probe_tx_with_read_ts(probe_count, Some(probe_ts));
    }

    pub fn gen_probe_tx_latest(&mut self, probe_count: usize) {
        self.gen_probe_tx_with_read_ts(probe_count, None);
    }

    pub fn gen_scan_txs_history(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let scan_ts = if let Some(ts) = self.history_ts_candidates.choose(&mut self.rng) {
            *ts
        } else {
            *self.read_ts_candidates.choose(&mut self.rng).unwrap()
        };
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::HistoryScan,
            scan_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        );

        ops.push(op);
        let tx = Tx::new(OperationType::HistoryScan, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    pub fn gen_scan_txs_latest(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::RecentScan,
            tx_ts,
            vec![],
            vec![],
            vec![],
            vec![],
        );

        ops.push(op);
        let tx = Tx::new(OperationType::RecentScan, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    pub fn gen_mark_ts_txs(&mut self) {
        let (tx_id, tx_ts) = self.gen_new_tx();
        self.read_ts_candidates.push(tx_ts);
        self.recent_ts_candidates.push(tx_ts);
        self.updates_since_mark = 0;
        let mut ops = Vec::new();

        let op = TxOperation::new(
            tx_id,
            tx_ts,
            OperationType::MarkTs,
            tx_ts,
            vec![], // pkey is not used for marking ts
            vec![], // join_key is not used for marking ts
            vec![],
            vec![],
        );
        ops.push(op);

        let tx = Tx::new(OperationType::MarkTs, tx_id, tx_ts, ops);
        self.txs.push(tx);
    }

    fn maybe_publish_readable_ts(&mut self, force: bool) {
        if (*self.cli.analytical_ratio.as_ref().unwrap() - 0.0).abs() <= 1e-6 {
            return;
        }
        if force || self.updates_since_mark >= self.cli.readable_every.max(1) {
            self.gen_mark_ts_txs();
        }
    }

    pub fn gen_delta_scan_tx(&mut self, start_idx: usize) -> bool {
        let all_ts = &self.read_ts_candidates[start_idx..];

        if all_ts.len() < 2 {
            println!("gen failed!!!");
            return false;
        }

        let (tx_id, tx_ts) = self.gen_new_tx();
        let all_ts = &self.read_ts_candidates[start_idx..];
        // randomly select 2 different timestamps
        let mut selected_ts = all_ts.choose_multiple(&mut self.rng, 2);
        let read_ts1 = selected_ts.next().unwrap();
        let read_ts2 = selected_ts.next().unwrap();
        assert!(
            read_ts1 != read_ts2,
            "Selected timestamps must be different"
        );
        let from_ts = read_ts1.min(read_ts2);
        let to_ts = read_ts1.max(read_ts2);

        let op = TxOperation::new_delta_scan(tx_id, tx_ts, *from_ts, *to_ts);
        let tx = Tx::new(OperationType::DeltaScan, tx_id, tx_ts, vec![op]);
        self.txs.push(tx.clone());
        self.read_txs.push(tx);
        return true;
    }

    // pub fn gen_full_delta_scan_tx(&mut self) {
    //     let all_ts = &self.read_ts_candidates;

    //     if all_ts.len() < 2 {
    //         panic!("Not enough read_ts candidates for delta scan, at least 2 required");
    //     }

    //     let read_ts1 = all_ts.iter().min().unwrap();
    //     let read_ts2 = all_ts.iter().max().unwrap();
    //     assert!(
    //         read_ts1 != read_ts2,
    //         "Selected timestamps must be different"
    //     );
    //     let from_ts = read_ts1.min(read_ts2);
    //     let to_ts = read_ts1.max(read_ts2);

    //     let op = TxOperation::new_delta_scan(0, *to_ts, *from_ts);
    //     let tx = Tx::new(OperationType::DeltaScan, 0, 0, vec![op]);
    //     self.txs.push(tx.clone());
    //     self.read_txs.push(tx);
    // }

    pub fn gen_txs(&mut self) {
        if self.cli.manual_txs.is_none() {
            self.gen_random_txs();
        } else {
            unimplemented!()
        }
    }

    fn choose_transaction(
        update_ratio: f64,
        probe_ratio: f64,
        scan_ratio: f64,
        delta_ratio: f64,
        gc_ratio: f64,
        rng: &mut SmallRng,
    ) -> OperationType {
        let x: f64 = rng.gen_range(0.0..1.0); // uniform [0,1)
        assert!(
            (update_ratio + probe_ratio + scan_ratio + delta_ratio + gc_ratio - 1.0f64).abs()
                < 1e-6
        );

        if x < update_ratio {
            OperationType::Update
        } else if x < update_ratio + probe_ratio {
            OperationType::Probe
        } else if x < update_ratio + probe_ratio + scan_ratio {
            OperationType::Scan
        } else if x < update_ratio + probe_ratio + scan_ratio + delta_ratio {
            OperationType::DeltaScan
        } else {
            OperationType::GbgCollect
        }
    }

    fn generate_tx_sequence(
        txn_count: usize,
        update_ratio: f64,
        probe_ratio: f64,
        scan_ratio: f64,
        delta_ratio: f64,
        gc_ratio: f64,
        rng: &mut SmallRng,
        scan_reuse_ratio: f64,
    ) -> Vec<OperationType> {
        assert!(
            (update_ratio + probe_ratio + scan_ratio + delta_ratio + gc_ratio - 1.0).abs() < 1e-6
        );

        let mut ops = Vec::with_capacity(txn_count);

        let update_n = (txn_count as f64 * update_ratio).round() as usize;
        let probe_n = (txn_count as f64 * probe_ratio).round() as usize;
        let delta_n = (txn_count as f64 * delta_ratio).round() as usize;
        let gc_n = (txn_count as f64 * gc_ratio).round() as usize;
        let scan_n = txn_count - update_n - probe_n - gc_n - delta_n;
        let history_scan_n = (scan_n as f64 * scan_reuse_ratio).round() as usize;
        let recent_scan_n = scan_n - history_scan_n;

        ops.extend(std::iter::repeat(OperationType::Update).take(update_n));
        ops.extend(std::iter::repeat(OperationType::Probe).take(probe_n));
        ops.extend(std::iter::repeat(OperationType::RecentScan).take(recent_scan_n));
        ops.extend(std::iter::repeat(OperationType::HistoryScan).take(history_scan_n));
        ops.extend(std::iter::repeat(OperationType::DeltaScan).take(delta_n));
        ops.extend(std::iter::repeat(OperationType::GbgCollect).take(gc_n));

        ops.shuffle(rng);

        ops
    }

    pub fn gen_random_txs(&mut self) {
        // generate random transactions based on the cli parameters

        // load init txn
        self.gen_initial_insert_from_cli();

        // load markts txn
        if (*self.cli.analytical_ratio.as_ref().unwrap() - 0.0).abs() > 1e-6 {
            // not W-ONLY
            self.gen_mark_ts_txs();
        }

        let update_count =
            (self.cli.update_ratio * self.data_source.get_custoemr_vec().len() as f64) as usize;
        self.gen_update_tx(update_count);

        if ((*self.cli.analytical_ratio.as_ref().unwrap() - 0.0).abs() > 1e-6) {
            // Seed at least two readable timestamps before the randomized phase.
            self.maybe_publish_readable_ts(true);
            if self.cli.txn_scan_ratio.as_ref().unwrap().to_owned() > 0.02
                && self.cli.scan_reuse_ratio < 0.999
            {
                self.gen_scan_txs_latest();
            } else {
                self.gen_scan_txs_history();
            }
        }

        if (*self.cli.analytical_ratio.as_ref().unwrap() - 1.0).abs() > 1e-6 {
            // NOT R-ONLY
            let update_count =
                (self.cli.update_ratio * self.data_source.get_custoemr_vec().len() as f64) as usize;
            self.gen_update_tx(update_count);

            if ((*self.cli.analytical_ratio.as_ref().unwrap() - 0.0).abs() > 1e-6) {
                self.maybe_publish_readable_ts(false);
                if self.cli.txn_scan_ratio.as_ref().unwrap().to_owned() > 0.02
                    && self.cli.scan_reuse_ratio < 0.999
                {
                    self.gen_scan_txs_latest();
                } else {
                    self.gen_scan_txs_history();
                }
            }
        }

        let ops = Self::generate_tx_sequence(
            self.cli.txn_count,
            self.cli.txn_update_ratio.as_ref().unwrap().to_owned(),
            self.cli.txn_probe_ratio.as_ref().unwrap().to_owned(),
            self.cli.txn_scan_ratio.as_ref().unwrap().to_owned(),
            self.cli.txn_delta_ratio.as_ref().unwrap().to_owned(),
            self.cli.txn_gc_ratio.as_ref().unwrap().to_owned(),
            &mut self.rng,
            self.cli.scan_reuse_ratio.to_owned(),
        );
        for i in 0..ops.len() {
            let tx_type = &ops[i];

            match tx_type {
                OperationType::Update => {
                    let update_count = (self.cli.update_ratio
                        * self.data_source.get_custoemr_vec().len() as f64)
                        as usize;
                    self.gen_update_tx(update_count);
                }
                OperationType::Probe => {
                    let probe_count = (self.cli.probe_ratio
                        * self.data_source.get_custoemr_vec().len() as f64)
                        as usize;
                    let probe_ts = if let Some(history_ratio) = self.cli.probe_history_ratio {
                        let use_history = history_ratio > 0.0
                            && !self.history_ts_candidates.is_empty()
                            && self.rng.gen::<f64>() < history_ratio;
                        if use_history {
                            Some(*self.history_ts_candidates.choose(&mut self.rng).unwrap())
                        } else {
                            None
                        }
                    } else {
                        Some(
                            self.read_ts_candidates
                            .choose(&mut self.rng)
                            .unwrap()
                            .to_owned(),
                        )
                    };
                    match probe_ts {
                        Some(probe_ts) => self.gen_probe_tx(probe_count, probe_ts),
                        None => self.gen_probe_tx_latest(probe_count),
                    }
                }
                OperationType::DeltaScan => {
                    self.gen_delta_scan_tx(0);
                }
                OperationType::RecentScan => {
                    self.gen_scan_txs_latest();
                }
                OperationType::HistoryScan => {
                    self.gen_scan_txs_history();
                }
                OperationType::GbgCollect => {
                    if (*self.cli.analytical_ratio.as_ref().unwrap() - 0.0).abs() > 1e-6 // NOT W-ONLY
                        &&  *self.cli.analytical_ratio.as_ref().unwrap() < 0.985
                    // NOT R-ONLY
                    {
                        self.gen_gc_txs();
                    }
                }
                _default => {
                    panic!();
                }
            }
        }
    }

    pub fn gen_gc_txs(&mut self) {
        let tss = &mut self.read_ts_candidates;
        if tss.len() < 3 {
            return;
        }
        assert_eq!(tss.iter().min(), tss.first());
        let (tx_id, tx_ts) = self.gen_new_tx();
        let tss = &mut self.read_ts_candidates;
        let min_ts = tss.iter().min().unwrap().to_owned();
        let (_first, right) = tss.split_first().unwrap();
        let new_tss = right.to_owned();
        self.read_ts_candidates = new_tss;
        self.recent_ts_candidates.retain(|&ts| ts > min_ts);
        self.history_ts_candidates.retain(|&ts| ts > min_ts);

        let op = TxOperation::new_gc(tx_id, tx_ts, min_ts);
        let tx = Tx::new(OperationType::GbgCollect, tx_id, tx_ts, vec![op]);
        self.txs.push(tx);
    }

    pub fn gen_manual_txs(&mut self) {
        todo!()
    }

    fn prepare_tx_untimed(&self, tx: &Tx, hash_join_table: &BoxMVIndex) {
        match tx.tx_type {
            OperationType::InitLoad => {
                for op in self.data_source.get_custoemr_vec() {
                    hash_join_table.prepare_insert(
                        &op.generate_join_key(),
                        &op.generate_pkey(),
                        &op.generate_value(),
                    );
                }
            }
            OperationType::Update => {
                for op in &tx.ops {
                    hash_join_table.prepare_update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
                }
            }
            _ => {}
        }
    }

    fn emit_build_snap_if_needed(
        &self,
        phase: &str,
        txs_idx: TxId,
        tx: &Tx,
        hash_join_table: &BoxMVIndex,
    ) {
        let mut targets = Vec::new();
        match tx.tx_type {
            OperationType::Probe | OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
                if let Some(op) = tx.ops.first() {
                    targets.push(op.read_ts);
                }
            }
            OperationType::DeltaScan => {
                if let Some(op) = tx.ops.first() {
                    targets.push(op.delta_scan_ts.0);
                    targets.push(op.delta_scan_ts.1);
                }
            }
            _ => {}
        }

        targets.sort_unstable();
        targets.dedup();
        for ts in targets {
            let duration = hash_join_table.ensure_snapshot_materialized(ts);
            if duration > Duration::default() {
                print!(
                    "[{}] idx: {:>3}, tx_id: {:>3}, tx_type: {:>10}, duration: {:?}, ",
                    phase,
                    txs_idx,
                    tx.tx_id,
                    "MarkTs",
                    duration
                );
                println!(
                    "MarkTs at read_ts: {:?}, build_for: {}",
                    ts,
                    Self::build_snap_reason(&tx.tx_type)
                );
            }
        }
    }

    fn build_snap_reason(tx_type: &OperationType) -> &'static str {
        match tx_type {
            OperationType::Probe => "Probe",
            OperationType::DeltaScan => "DeltaScan",
            OperationType::HistoryScan => "HistoryScan",
            OperationType::RecentScan => "RecentScan",
            OperationType::Scan => "Scan",
            _ => "Other",
        }
    }

    pub fn run_tx_no_repair(
        &self,
        txs_idx: TxId,
        hash_join_table: &BoxMVIndex,
    ) -> Result<Duration, Error> {
        let tx = &self.txs[txs_idx as usize];
        self.prepare_tx_untimed(tx, hash_join_table);
        self.emit_build_snap_if_needed("No Repair", txs_idx, tx, hash_join_table);
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
                    let _ = hash_join_table.probe(&op.join_key, op.read_ts);
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
                    let _ =
                        hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, false);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
                    tx.ops[0].delta_scan_ts.0, tx.ops[0].delta_scan_ts.1
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
        self.prepare_tx_untimed(tx, hash_join_table);
        self.emit_build_snap_if_needed("Read Repair", txs_idx, tx, hash_join_table);
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
                    let _ = hash_join_table.probe(&op.join_key, op.read_ts);
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
                    let _ =
                        hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, true);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
                    tx.ops[0].delta_scan_ts.0, tx.ops[0].delta_scan_ts.1
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
        self.prepare_tx_untimed(tx, hash_join_table);
        self.emit_build_snap_if_needed("Write Repair", txs_idx, tx, hash_join_table);
        let start = Instant::now();
        let mut is_need_scan_warmup = false;
        match tx.tx_type {
            OperationType::Probe => {
                for op in &tx.ops {
                    let _ = hash_join_table.probe(&op.join_key, op.read_ts);
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
                    let _ =
                        hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, false);
                }
            }
            OperationType::UpdateWR => {
                panic!("should not exist");
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
                    tx.ops[0].delta_scan_ts.0, tx.ops[0].delta_scan_ts.1
                );
            }
            OperationType::UpdateWR => {
                panic!();
            }
            OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
        println!("Bucket number: {:?}", cli.bucket_num);
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
        println!("Update ratio: {}", cli.update_ratio);
        println!("probe ratio: {}", cli.probe_ratio);
        println!("Analytical ratio: {:?}", cli.analytical_ratio);
        println!(
            "Number of transactions (max Timestamp value): {}",
            cli.txn_count
        );
        println!("Number of each scan transactions: {}", cli.scan_count);
        println!();
        println!("-----------------------------------------------------------------------");
        println!("Transactions Update ratio: {:?}", cli.txn_update_ratio);
        println!("Transactions Probe ratio: {:?}", cli.txn_probe_ratio);
        println!("Transactions Scan ratio: {:?}", cli.txn_scan_ratio);
        println!("Transactions Delta Scan ratio: {:?}", cli.txn_delta_ratio);
        println!("Transactions GC ratio: {:?}", cli.txn_gc_ratio);
        println!("Transactions Scan Reuse ratio: {:?}", cli.scan_reuse_ratio);
        println!("Readable timestamp cadence: every {} update txs", cli.readable_every);
        println!("-----------------------------------------------------------------------");
        println!();
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
                        tx.ops[0].delta_scan_ts.0, tx.ops[0].delta_scan_ts.1
                    );
                }
                OperationType::UpdateWR => {
                    panic!();
                }
                OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
                        tx.ops[0].delta_scan_ts.0, tx.ops[0].delta_scan_ts.1
                    );
                }
                OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
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
