#[path = "../../common/rpmalloc_global.rs"]
mod rpmalloc_global;

#[path = "../htap_simulation/cli.rs"]
mod cli;
#[path = "../htap_simulation/dbgen.rs"]
mod dbgen;
#[path = "../htap_simulation/interface.rs"]
mod interface;
#[path = "../htap_simulation/txs.rs"]
mod txs;

use clap::{Parser, ValueEnum};
use cli::{Cli, TableType};
use fbtree::{
    bp::{get_in_mem_pool, ContainerKey},
    mvcc_index::{
        dual_heap_hash::chained_hash_table::ChainedHashTable,
        hash_heap::hash_heap_table::HeapHashTable,
        ts_partitioned::ts_partitioned_table::TsPartitionedTable,
    },
    naive_hash_index::{IvmHashTable, NaiveMvHashTable},
};
use interface::{BoxMVIndex, OperationType};
use txs::{Tx, TxBench, TxOperation};

const PRIME_TXS_AFTER_INITLOAD: usize = 4;

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum RepairMode {
    NoRepair,
    WriteRepair,
}

impl RepairMode {
    fn label(self) -> &'static str {
        match self {
            RepairMode::NoRepair => "No Repair",
            RepairMode::WriteRepair => "Write Repair",
        }
    }
}

#[derive(Parser, Debug, Clone)]
struct IvmhHistoryBreakdownCli {
    #[command(flatten)]
    base: Cli,

    #[arg(long = "repair-mode", value_enum, default_value = "no-repair")]
    repair_mode: RepairMode,

    #[arg(long = "blocks", default_value = "5")]
    blocks: usize,

    #[arg(long = "scans-per-block", default_value = "5")]
    scans_per_block: usize,

    #[arg(long = "history-scans", default_value = "0")]
    history_scans: usize,
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

fn push_history_scan(bench: &mut TxBench, read_ts: u64) {
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

fn build_bench(cli: &IvmhHistoryBreakdownCli) -> TxBench {
    assert!(cli.blocks > 0, "blocks must be positive");
    assert!(
        cli.history_scans <= cli.blocks / 2,
        "history-scans must be at most floor(blocks / 2)"
    );

    let mut base = cli.base.clone();
    base.analytical_ratio = Some(1.0);
    base.manual_txs = None;
    base.txn_update_ratio = None;
    base.txn_probe_ratio = None;
    base.txn_scan_ratio = None;
    base.txn_delta_ratio = None;
    base.txn_gc_ratio = None;
    // Disable implicit publication; this benchmark publishes one readable ts
    // explicitly after every measured update.
    base.readable_every = usize::MAX;

    let mut bench = TxBench::new(base);
    bench.gen_initial_insert_from_cli();

    let update_count =
        (bench.cli.update_ratio * bench.data_source.get_custoemr_vec().len() as f64) as usize;
    let mut published_readables = Vec::with_capacity(cli.blocks);

    // Untimed prime after the measured InitLoad:
    // RecentScan -> MarkTs -> Update -> HistoryScan(reading the pre-update readable ts)
    push_recent_scan(&mut bench);
    bench.gen_mark_ts_txs();
    let prime_history_ts = *bench
        .read_ts_candidates
        .last()
        .expect("prime mark_ts must publish a readable timestamp");
    bench.gen_update_tx(update_count);
    push_history_scan(&mut bench, prime_history_ts);

    for block_idx in 0..cli.blocks {
        let history_slot_idx = block_idx / 2;
        let should_insert_history =
            block_idx % 2 == 1 && history_slot_idx < cli.history_scans;
        if should_insert_history {
            let read_ts = published_readables[block_idx - 1];
            push_history_scan(&mut bench, read_ts);
            for _ in 1..cli.scans_per_block {
                push_recent_scan(&mut bench);
            }
        } else {
            for _ in 0..cli.scans_per_block {
                push_recent_scan(&mut bench);
            }
        }

        bench.gen_mark_ts_txs();
        let readable_ts = *bench
            .read_ts_candidates
            .last()
            .expect("mark_ts must publish a readable timestamp");
        published_readables.push(readable_ts);
        bench.gen_update_tx(update_count);
    }

    bench
}

fn run_tx_untimed(bench: &TxBench, txs_idx: usize, hash_join_table: &BoxMVIndex, repair_mode: RepairMode) {
    let tx = &bench.txs[txs_idx];

    match tx.tx_type {
        OperationType::InitLoad => {
            for op in bench.data_source.get_custoemr_vec() {
                hash_join_table.prepare_insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
            }
            hash_join_table.begin_txs(OperationType::InitLoad).unwrap();
            for op in bench.data_source.get_custoemr_vec() {
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
        }
        OperationType::Update => {
            for op in &tx.ops {
                hash_join_table.prepare_update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
            }
            match repair_mode {
                RepairMode::NoRepair => {
                    hash_join_table.begin_txs(OperationType::Update).unwrap();
                    for op in &tx.ops {
                        hash_join_table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
                    }
                    hash_join_table.end_txs(OperationType::Update).unwrap();
                }
                RepairMode::WriteRepair => {
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
            }
        }
        OperationType::Probe => {
            for op in &tx.ops {
                let _ = hash_join_table.probe(&op.join_key, op.read_ts);
            }
        }
        OperationType::DeltaScan => {
            for op in &tx.ops {
                let _ = hash_join_table.ensure_snapshot_materialized(op.delta_scan_ts.0);
                let _ = hash_join_table.ensure_snapshot_materialized(op.delta_scan_ts.1);
                let _ = hash_join_table.scan_delta(op.delta_scan_ts.0, op.delta_scan_ts.1, false);
            }
        }
        OperationType::Scan | OperationType::HistoryScan | OperationType::RecentScan => {
            for op in &tx.ops {
                let _ = hash_join_table.ensure_snapshot_materialized(op.read_ts);
                let iter = hash_join_table.scan(op.read_ts, false).unwrap();
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
        OperationType::UpdateWR => {
            panic!("should not exist");
        }
    }
}

fn build_table(cli: &IvmhHistoryBreakdownCli) -> BoxMVIndex {
    let mem_pool = get_in_mem_pool();
    match cli.base.table_type {
        TableType::Naive => Box::new(NaiveMvHashTable::new_with_bucket_num(
            ContainerKey::new(0, 0),
            mem_pool,
            cli.base.bucket_num,
        )) as BoxMVIndex,
        TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
            ContainerKey::new(0, 0),
            mem_pool,
            cli.base.bucket_num,
        )) as BoxMVIndex,
        TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
            ContainerKey::new(0, 0),
            mem_pool,
            cli.base.bucket_num,
        )) as BoxMVIndex,
        TableType::Ivmh => Box::new(IvmHashTable::new_with_bucket_num(
            ContainerKey::new(0, 0),
            mem_pool,
            cli.base.bucket_num,
        )) as BoxMVIndex,
        TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
            ContainerKey::new(0, 0),
            mem_pool,
            cli.base.bucket_num,
        )) as BoxMVIndex,
    }
}

fn main() {
    let cli = IvmhHistoryBreakdownCli::parse();
    let bench = build_bench(&cli);
    let table = build_table(&cli);

    println!(
        "history-breakdown trace: table={:?}, repair={}, wc={}, blocks={}, scans_per_block={}, history_scans={}, update_ratio={:.6}, bucket_num={}",
        cli.base.table_type,
        cli.repair_mode.label(),
        cli.base.warehouse_count,
        cli.blocks,
        cli.scans_per_block,
        cli.history_scans,
        cli.base.update_ratio,
        cli.base.bucket_num,
    );
    println!(
        "generated txs: {}, readables: {}",
        bench.txs.len(),
        bench.read_ts_candidates.len()
    );

    println!();
    println!("{}", cli.repair_mode.label());
    if !bench.txs.is_empty() {
        match cli.repair_mode {
            RepairMode::NoRepair => {
                let _ = bench.run_tx_no_repair(0, &table).unwrap();
            }
            RepairMode::WriteRepair => {
                let _ = bench.run_tx_write_repair(0, &table).unwrap();
            }
        }
    }

    for txs_idx in 1..bench.txs.len().min(1 + PRIME_TXS_AFTER_INITLOAD) {
        run_tx_untimed(&bench, txs_idx, &table, cli.repair_mode);
    }

    for txs_idx in (1 + PRIME_TXS_AFTER_INITLOAD)..bench.txs.len() {
        match cli.repair_mode {
            RepairMode::NoRepair => {
                let _ = bench.run_tx_no_repair(txs_idx as u64, &table).unwrap();
            }
            RepairMode::WriteRepair => {
                let _ = bench.run_tx_write_repair(txs_idx as u64, &table).unwrap();
            }
        }
    }
}
