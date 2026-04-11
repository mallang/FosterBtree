#[path = "../htap_simulation/cli.rs"]
mod cli;
#[path = "../htap_simulation/dbgen.rs"]
mod dbgen;
#[path = "../htap_simulation/interface.rs"]
mod interface;
#[path = "../htap_simulation/txs.rs"]
mod txs;

use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use cli::{Cli, TableType};
use fbtree::{
    bp::{get_in_mem_pool, ContainerKey},
    mvcc_index::{
        dual_heap_hash::chained_hash_table::ChainedHashTable,
        hash_heap::hash_heap_table::HeapHashTable,
        ts_partitioned::ts_partitioned_table::TsPartitionedTable,
    },
    prelude::{AccessMethodError, Timestamp},
};
use interface::{BoxMVIndex, OperationType};
use txs::{Tx, TxBench, TxOperation};

#[derive(Clone, Copy, PartialEq)]
enum RepairType {
    ReadRepair,
    WriteRepair,
    NoRepair,
}

impl RepairType {
    fn label(self) -> &'static str {
        match self {
            RepairType::NoRepair => "No Repair",
            RepairType::ReadRepair => "Read Repair",
            RepairType::WriteRepair => "Write Repair",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum SweepType {
    History,
    Delta,
}

#[derive(Parser, Debug, Clone)]
struct ScanMixCli {
    #[command(flatten)]
    base: Cli,

    #[arg(long = "measured-updates", default_value = "20")]
    measured_updates: usize,

    #[arg(long = "scans-per-update", default_value = "5")]
    scans_per_update: usize,

    #[arg(long = "sweep-type", value_enum, default_value = "history")]
    sweep_type: SweepType,

    #[arg(long = "sweep-ratio", default_value = "0.0")]
    sweep_ratio: f64,
}

fn build_table(cli: &Cli, c_key: ContainerKey) -> BoxMVIndex {
    let mem_pool = get_in_mem_pool();
    match cli.table_type {
        TableType::Chain => {
            Box::new(ChainedHashTable::new_with_bucket_num(c_key, mem_pool, cli.bucket_num))
                as BoxMVIndex
        }
        TableType::Heap => {
            Box::new(HeapHashTable::new_with_bucket_num(c_key, mem_pool, cli.bucket_num))
                as BoxMVIndex
        }
        TableType::Par => {
            Box::new(TsPartitionedTable::new_with_bucket_num(c_key, mem_pool, cli.bucket_num))
                as BoxMVIndex
        }
        TableType::Naive => Box::new(
            fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                cli.bucket_num,
            ),
        ) as BoxMVIndex,
        TableType::Ivmh => Box::new(fbtree::naive_hash_index::IvmHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            cli.bucket_num,
        )) as BoxMVIndex,
    }
}

fn push_recent_scan(bench: &mut TxBench) {
    let (tx_id, tx_ts) = bench.gen_new_tx();
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
    bench.txs
        .push(Tx::new(OperationType::RecentScan, tx_id, tx_ts, vec![op]));
}

fn push_history_scan(bench: &mut TxBench, read_ts: Timestamp) {
    let (tx_id, tx_ts) = bench.gen_new_tx();
    let op = TxOperation::new(
        tx_id,
        tx_ts,
        OperationType::HistoryScan,
        read_ts,
        vec![],
        vec![],
        vec![],
        vec![],
    );
    bench.txs
        .push(Tx::new(OperationType::HistoryScan, tx_id, tx_ts, vec![op]));
}

fn push_delta_scan(bench: &mut TxBench, from_ts: Timestamp, to_ts: Timestamp) {
    let (tx_id, tx_ts) = bench.gen_new_tx();
    let op = TxOperation::new_delta_scan(tx_id, tx_ts, from_ts, to_ts);
    bench.txs
        .push(Tx::new(OperationType::DeltaScan, tx_id, tx_ts, vec![op]));
}

fn timed_tx_count(cli: &ScanMixCli) -> usize {
    let marks_from_updates = cli.measured_updates / cli.base.readable_every.max(1);
    1 + 1 + cli.measured_updates + marks_from_updates + cli.measured_updates * cli.scans_per_update
}

fn build_scan_mix_bench(cli: &ScanMixCli) -> (TxBench, usize, usize) {
    assert!(
        (0.0..=1.0).contains(&cli.sweep_ratio),
        "sweep-ratio must be within [0, 1]"
    );

    let mut base = cli.base.clone();
    base.analytical_ratio = Some(1.0);
    base.txn_update_ratio = None;
    base.txn_probe_ratio = None;
    base.txn_scan_ratio = None;
    base.txn_delta_ratio = None;
    base.txn_gc_ratio = None;
    base.manual_txs = None;

    let mut bench = TxBench::new(base);
    let total_scans = cli.measured_updates * cli.scans_per_update;
    let special_scan_n = (total_scans as f64 * cli.sweep_ratio).round() as usize;
    let max_special_slots = cli.measured_updates / bench.cli.readable_every.max(1);
    assert!(
        special_scan_n <= max_special_slots,
        "requested {} special scans but only {} readable windows exist",
        special_scan_n,
        max_special_slots
    );

    bench.gen_initial_insert_from_cli();
    let update_count = (bench.cli.update_ratio * bench.data_source.get_custoemr_vec().len() as f64)
        as usize;
    bench.gen_mark_ts_txs(); // seed readable ts for delta pairing

    let mut last_readable_ts = *bench
        .read_ts_candidates
        .last()
        .expect("seed readable ts must exist");
    let mut history_targets = Vec::new();
    let mut delta_targets = Vec::new();
    let mut special_consumed = 0usize;

    for update_idx in 1..=cli.measured_updates {
        let before_readables = bench.read_ts_candidates.len();
        bench.gen_update_tx(update_count);
        let published_new_readable = bench.read_ts_candidates.len() > before_readables;
        if published_new_readable {
            let new_ts = *bench.read_ts_candidates.last().unwrap();
            history_targets.push(new_ts);
            delta_targets.push((last_readable_ts, new_ts));
            last_readable_ts = new_ts;
        }

        for scan_idx_in_block in 0..cli.scans_per_update {
            let use_special = published_new_readable
                && scan_idx_in_block == 0
                && special_consumed < special_scan_n;
            if use_special {
                match cli.sweep_type {
                    SweepType::History => {
                        push_history_scan(&mut bench, history_targets[special_consumed])
                    }
                    SweepType::Delta => {
                        let (from_ts, to_ts) = delta_targets[special_consumed];
                        push_delta_scan(&mut bench, from_ts, to_ts);
                    }
                }
                special_consumed += 1;
            } else {
                push_recent_scan(&mut bench);
            }
        }
    }

    assert_eq!(
        special_consumed, special_scan_n,
        "failed to place all requested special scans"
    );

    (bench, timed_tx_count(cli), total_scans)
}

fn prepare_tx_timed(bench: &TxBench, tx: &Tx, hash_join_table: &BoxMVIndex) {
    match tx.tx_type {
        OperationType::InitLoad => {
            for op in bench.data_source.get_custoemr_vec() {
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

fn materialize_snapshots_timed(tx: &Tx, hash_join_table: &BoxMVIndex) {
    let mut targets = Vec::new();
    match tx.tx_type {
        OperationType::Probe
        | OperationType::Scan
        | OperationType::HistoryScan
        | OperationType::RecentScan => {
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
        let _ = hash_join_table.ensure_snapshot_materialized(ts);
    }
}

fn run_tx_no_repair_inclusive(
    bench: &TxBench,
    txs_idx: usize,
    hash_join_table: &BoxMVIndex,
) -> Result<Duration, AccessMethodError> {
    let tx = &bench.txs[txs_idx];
    let start = Instant::now();
    prepare_tx_timed(bench, tx, hash_join_table);
    materialize_snapshots_timed(tx, hash_join_table);
    match tx.tx_type {
        OperationType::InitLoad => {
            hash_join_table.begin_txs(OperationType::InitLoad)?;
            for op in bench.data_source.get_custoemr_vec() {
                hash_join_table.insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
            }
            hash_join_table.end_txs(OperationType::InitLoad)?;
        }
        OperationType::MarkTs => {
            hash_join_table.mark_ts(tx.tx_ts);
        }
        OperationType::Probe => {
            for op in &tx.ops {
                let _ = hash_join_table.probe(&op.join_key, op.read_ts);
            }
        }
        OperationType::Update => {
            hash_join_table.begin_txs(OperationType::Update)?;
            for op in &tx.ops {
                hash_join_table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
            }
            hash_join_table.end_txs(OperationType::Update)?;
        }
        OperationType::DeltaScan => {
            for op in &tx.ops {
                let _ =
                    hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, false);
            }
        }
        OperationType::UpdateWR => unreachable!(),
        OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
            for op in &tx.ops {
                let iter = hash_join_table.scan(op.read_ts, false)?;
                for entry in iter {
                    assert_eq!(entry.2.len(), 688);
                }
            }
        }
        OperationType::GbgCollect => {
            for op in &tx.ops {
                hash_join_table.garbage_collect(op.read_ts);
            }
        }
    }
    Ok(start.elapsed())
}

fn run_tx_read_repair_inclusive(
    bench: &TxBench,
    txs_idx: usize,
    hash_join_table: &BoxMVIndex,
) -> Result<Duration, AccessMethodError> {
    let tx = &bench.txs[txs_idx];
    let start = Instant::now();
    prepare_tx_timed(bench, tx, hash_join_table);
    materialize_snapshots_timed(tx, hash_join_table);
    match tx.tx_type {
        OperationType::InitLoad => {
            for op in bench.data_source.get_custoemr_vec() {
                hash_join_table.insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
            }
        }
        OperationType::MarkTs => {
            hash_join_table.mark_ts(tx.tx_ts);
        }
        OperationType::Probe => {
            for op in &tx.ops {
                let _ = hash_join_table.probe(&op.join_key, op.read_ts);
            }
        }
        OperationType::Update => {
            hash_join_table.begin_txs(OperationType::Update)?;
            for op in &tx.ops {
                hash_join_table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
            }
            hash_join_table.end_txs(OperationType::Update)?;
        }
        OperationType::DeltaScan => {
            for op in &tx.ops {
                let _ =
                    hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, true);
            }
        }
        OperationType::UpdateWR => unreachable!(),
        OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
            for op in &tx.ops {
                let iter = hash_join_table.scan(op.read_ts, true)?;
                for entry in iter {
                    assert_eq!(entry.2.len(), 688);
                }
            }
        }
        OperationType::GbgCollect => {
            for op in &tx.ops {
                hash_join_table.garbage_collect(op.read_ts);
            }
        }
    }
    Ok(start.elapsed())
}

fn run_tx_write_repair_inclusive(
    bench: &TxBench,
    txs_idx: usize,
    hash_join_table: &BoxMVIndex,
) -> Result<Duration, AccessMethodError> {
    let tx = &bench.txs[txs_idx];
    let start = Instant::now();
    prepare_tx_timed(bench, tx, hash_join_table);
    materialize_snapshots_timed(tx, hash_join_table);
    match tx.tx_type {
        OperationType::InitLoad => {
            for op in bench.data_source.get_custoemr_vec() {
                hash_join_table.insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
            }
        }
        OperationType::MarkTs => {
            hash_join_table.mark_ts(tx.tx_ts);
        }
        OperationType::Probe => {
            for op in &tx.ops {
                let _ = hash_join_table.probe(&op.join_key, op.read_ts);
            }
        }
        OperationType::Update => {
            hash_join_table.begin_txs(OperationType::UpdateWR)?;
            for op in &tx.ops {
                hash_join_table.update_write_repair(
                    &op.join_key,
                    &op.pkey,
                    &op.value,
                    op.tx_ts,
                );
            }
            hash_join_table.end_txs(OperationType::UpdateWR)?;
        }
        OperationType::DeltaScan => {
            for op in &tx.ops {
                let _ =
                    hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, false);
            }
        }
        OperationType::UpdateWR => unreachable!(),
        OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
            for op in &tx.ops {
                let iter = hash_join_table.scan(op.read_ts, false)?;
                for entry in iter {
                    assert_eq!(entry.2.len(), 688);
                }
            }
        }
        OperationType::GbgCollect => {
            for op in &tx.ops {
                hash_join_table.garbage_collect(op.read_ts);
            }
        }
    }
    Ok(start.elapsed())
}

fn print_tx_result(phase: &str, bench: &TxBench, txs_idx: usize, elapsed: Duration) {
    let tx = &bench.txs[txs_idx];
    print!(
        "[{}] idx: {:>3}, tx_id: {:>3}, tx_type: {:>10}, duration: {:?}, ",
        phase,
        txs_idx,
        tx.tx_id,
        format!("{:?}", tx.tx_type),
        elapsed
    );
    match tx.tx_type {
        OperationType::InitLoad => {
            println!(
                "InitialLoad count: {:?}",
                bench.data_source.get_custoemr_vec().len()
            );
        }
        OperationType::Probe => {
            println!(
                "Probe count: {:?} at read_ts: {:?}",
                tx.ops.len(),
                tx.ops[0].read_ts
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
        OperationType::UpdateWR => unreachable!(),
        OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
            println!("Scan read_ts: {:?}", tx.ops[0].read_ts);
        }
        OperationType::GbgCollect => {
            println!("Garbage collection read_ts: {:?}", tx.ops[0].read_ts);
        }
    }
}

fn run_repair(bench: &TxBench, cli: &Cli, repair: RepairType, c_key: ContainerKey) {
    println!();
    println!("{}", repair.label());
    let table = build_table(cli, c_key);
    for txs_idx in 0..bench.txs.len() {
        let elapsed = match repair {
            RepairType::NoRepair => run_tx_no_repair_inclusive(bench, txs_idx, &table).unwrap(),
            RepairType::ReadRepair => run_tx_read_repair_inclusive(bench, txs_idx, &table).unwrap(),
            RepairType::WriteRepair => {
                run_tx_write_repair_inclusive(bench, txs_idx, &table).unwrap()
            }
        };
        print_tx_result(repair.label(), bench, txs_idx, elapsed);
    }
}

fn run_no_repair(bench: &TxBench, cli: &Cli) {
    run_repair(bench, cli, RepairType::NoRepair, ContainerKey::new(0, 0));
}

fn run_three_repairs(bench: &TxBench, cli: &Cli) {
    run_repair(bench, cli, RepairType::NoRepair, ContainerKey::new(0, 0));
    run_repair(bench, cli, RepairType::ReadRepair, ContainerKey::new(0, 1));
    run_repair(bench, cli, RepairType::WriteRepair, ContainerKey::new(0, 2));
}

fn main() {
    let cli = ScanMixCli::parse();
    let (bench, timed_txs, total_scans) = build_scan_mix_bench(&cli);

    println!(
        "scan-mix trace: updates={}, scans={}, timed_txs={}, readable_every={}, sweep_type={:?}, sweep_ratio={:.4}",
        cli.measured_updates,
        total_scans,
        timed_txs,
        cli.base.readable_every,
        cli.sweep_type,
        cli.sweep_ratio
    );
    println!(
        "scan-mix generated txs: {}, readables: {}",
        bench.txs.len(),
        bench.read_ts_candidates.len()
    );

    match cli.base.table_type {
        TableType::Naive | TableType::Ivmh => run_no_repair(&bench, &cli.base),
        TableType::Heap | TableType::Chain | TableType::Par => run_three_repairs(&bench, &cli.base),
    }
}
