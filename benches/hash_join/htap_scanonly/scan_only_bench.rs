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
    prelude::AccessMethodError,
};
use interface::{BoxMVIndex, OperationType};
use txs::{Tx, TxBench};

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
struct ScanOnlyCli {
    #[command(flatten)]
    base: Cli,

    #[arg(long = "setup-readable-ts", default_value = "12")]
    setup_readable_ts: usize,

    #[arg(long = "measured-scan-txs", default_value = "100")]
    measured_scan_txs: usize,

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

fn evenly_spaced_positions(total: usize, replace_n: usize) -> Vec<usize> {
    if replace_n == 0 {
        return Vec::new();
    }
    (0..replace_n)
        .map(|i| ((2 * i + 1) * total) / (2 * replace_n))
        .collect()
}

fn measured_ops(sweep_type: SweepType, sweep_ratio: f64, measured_scan_txs: usize) -> Vec<char> {
    assert!(
        (0.0..=1.0).contains(&sweep_ratio),
        "sweep-ratio must be within [0, 1]"
    );
    let replace_n = (measured_scan_txs as f64 * sweep_ratio).round() as usize;
    let mut ops = vec!['R'; measured_scan_txs];
    let replacement = match sweep_type {
        SweepType::History => 'S',
        SweepType::Delta => 'D',
    };
    for idx in evenly_spaced_positions(measured_scan_txs, replace_n) {
        ops[idx] = replacement;
    }
    ops
}

fn apply_setup_tx(
    bench: &TxBench,
    tx: &Tx,
    table: &BoxMVIndex,
    repair: RepairType,
) -> Result<(), AccessMethodError> {
    match &tx.tx_type {
        OperationType::InitLoad => {
            for op in bench.data_source.get_custoemr_vec() {
                table.prepare_insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
                table.insert(
                    &op.generate_join_key(),
                    &op.generate_pkey(),
                    &op.generate_value(),
                );
            }
        }
        OperationType::MarkTs => {
            table.mark_ts(tx.tx_ts);
        }
        OperationType::Update => {
            let update_op = match repair {
                RepairType::WriteRepair => OperationType::UpdateWR,
                RepairType::NoRepair | RepairType::ReadRepair => OperationType::Update,
            };
            table.begin_txs(update_op.clone())?;
            for op in &tx.ops {
                table.prepare_update(&op.join_key, &op.pkey, &op.value, op.tx_ts);
                match repair {
                    RepairType::WriteRepair => {
                        table.update_write_repair(&op.join_key, &op.pkey, &op.value, op.tx_ts)
                    }
                    RepairType::NoRepair | RepairType::ReadRepair => {
                        table.update(&op.join_key, &op.pkey, &op.value, op.tx_ts)
                    }
                }
            }
            table.end_txs(update_op)?;
        }
        other => panic!("scan-only setup contains unexpected tx type: {:?}", other),
    }
    Ok(())
}

fn setup_update_count(bench: &TxBench) -> usize {
    (bench.cli.update_ratio * bench.data_source.get_custoemr_vec().len() as f64) as usize
}

fn build_scan_only_bench(cli: &ScanOnlyCli) -> (TxBench, usize) {
    let mut base = cli.base.clone();
    base.analytical_ratio = Some(1.0);
    base.txn_update_ratio = None;
    base.txn_probe_ratio = None;
    base.txn_scan_ratio = None;
    base.txn_delta_ratio = None;
    base.txn_gc_ratio = None;
    base.manual_txs = None;

    let mut bench = TxBench::new(base);
    bench.gen_initial_insert_from_cli();
    bench.gen_mark_ts_txs();

    let update_count = setup_update_count(&bench);
    while bench.read_ts_candidates.len() < cli.setup_readable_ts {
        bench.gen_update_tx(update_count);
    }

    let measured_start = bench.txs.len();
    for op in measured_ops(cli.sweep_type, cli.sweep_ratio, cli.measured_scan_txs) {
        match op {
            'R' => bench.gen_scan_txs_latest(),
            'S' => bench.gen_scan_txs_history(),
            'D' => {
                let generated = bench.gen_delta_scan_tx(0);
                assert!(generated, "failed to generate delta scan");
            }
            _ => unreachable!(),
        }
    }
    (bench, measured_start)
}

fn run_repair(
    bench: &TxBench,
    cli: &Cli,
    repair: RepairType,
    setup_end: usize,
    c_key: ContainerKey,
) {
    println!();
    println!("{}", repair.label());
    let table = build_table(cli, c_key);

    for idx in 0..setup_end {
        apply_setup_tx(bench, &bench.txs[idx], &table, repair).unwrap();
    }

    for idx in setup_end..bench.txs.len() {
        match repair {
            RepairType::NoRepair => {
                let _ = bench.run_tx_no_repair(idx as u64, &table);
            }
            RepairType::ReadRepair => {
                let _ = bench.run_tx_read_repair(idx as u64, &table);
            }
            RepairType::WriteRepair => {
                let _ = bench.run_tx_write_repair(idx as u64, &table);
            }
        }
    }
}

fn run_no_repair(bench: &TxBench, cli: &Cli, setup_end: usize) {
    run_repair(bench, cli, RepairType::NoRepair, setup_end, ContainerKey::new(0, 0));
}

fn run_three_repairs(bench: &TxBench, cli: &Cli, setup_end: usize) {
    run_repair(bench, cli, RepairType::NoRepair, setup_end, ContainerKey::new(0, 0));
    run_repair(
        bench,
        cli,
        RepairType::ReadRepair,
        setup_end,
        ContainerKey::new(0, 1),
    );
    run_repair(
        bench,
        cli,
        RepairType::WriteRepair,
        setup_end,
        ContainerKey::new(0, 2),
    );
}

fn main() {
    let cli = ScanOnlyCli::parse();
    let (bench, setup_end) = build_scan_only_bench(&cli);

    println!(
        "scan-only setup: readable_ts={}, measured_scan_txs={}, sweep_type={:?}, sweep_ratio={:.4}",
        bench.read_ts_candidates.len(),
        cli.measured_scan_txs,
        cli.sweep_type,
        cli.sweep_ratio
    );
    println!(
        "scan-only setup txs: {}, measured txs: {}",
        setup_end,
        bench.txs.len() - setup_end
    );

    match cli.base.table_type {
        TableType::Naive | TableType::Ivmh => run_no_repair(&bench, &cli.base, setup_end),
        TableType::Heap | TableType::Chain | TableType::Par => {
            run_three_repairs(&bench, &cli.base, setup_end)
        }
    }
}
