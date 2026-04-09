mod cli;
mod dbgen;
mod interface;
mod txs;

use std::{fs::File, io::Write, path::Path};

use clap::Parser;
use cli::{Cli, TableType};
use fbtree::{
    bp::{get_in_mem_pool, ContainerKey},
    mvcc_index::{
        dual_heap_hash::chained_hash_table::ChainedHashTable,
        hash_common::StatCollector,
        hash_heap::hash_heap_table::HeapHashTable,
        ts_partitioned::ts_partitioned_table::TsPartitionedTable,
    },
    naive_hash_index::SnapshotStat,
};
use interface::BoxMVIndex;

use crate::txs::TxBench;

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

struct SpaceStatRow {
    table_type: String,
    repair_type: String,
    total_space: usize,
    all_versions_space: usize,
    valid_space: usize,
    current_space: usize,
    history_space: usize,
    metadata_space: usize,
}

struct SnapshotStatRow {
    table_type: String,
    repair_type: String,
    readable_timestamps_published: usize,
    retained_snapshots: usize,
    snapshots_built_total: usize,
    snapshot_reads_total: usize,
    snapshot_cache_hits_total: usize,
    snapshot_cache_misses_total: usize,
    current_reads_total: usize,
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

fn run_repair(
    bench: &TxBench,
    cli: &Cli,
    repair: RepairType,
    c_key: ContainerKey,
) -> (StatCollector, Option<SnapshotStat>) {
    println!();
    println!("{}", repair.label());
    let table = build_table(cli, c_key);
    match repair {
        RepairType::NoRepair => bench.run_all_txs_no_repair(&table),
        RepairType::ReadRepair => bench.run_all_txs_read_repair(&table),
        RepairType::WriteRepair => bench.run_all_txs_write_repair(&table),
    }
    (table.collect_space_stat(), table.collect_snapshot_stat())
}

fn run_no_repair(bench: &TxBench, cli: &Cli) {
    let _ = run_repair(bench, cli, RepairType::NoRepair, ContainerKey::new(0, 0));
}

fn run_three_repairs(bench: &TxBench, cli: &Cli) {
    let _ = run_repair(bench, cli, RepairType::NoRepair, ContainerKey::new(0, 0));
    let _ = run_repair(bench, cli, RepairType::ReadRepair, ContainerKey::new(0, 1));
    let _ = run_repair(bench, cli, RepairType::WriteRepair, ContainerKey::new(0, 2));
}

fn stat_row(cli: &Cli, repair: RepairType, stat: StatCollector) -> SpaceStatRow {
    SpaceStatRow {
        table_type: format!("{:?}", cli.table_type),
        repair_type: repair.label().to_string(),
        total_space: stat.total_space(),
        all_versions_space: stat.all_versions_space(),
        valid_space: stat.valid_space(),
        current_space: stat.valid_space(),
        history_space: stat.history_space(),
        metadata_space: stat.metadata_space(),
    }
}

fn write_space_stats(path: &Path, rows: &[SpaceStatRow]) {
    let mut file = File::create(path).expect("failed to create space-stat CSV");
    writeln!(
        file,
        "table_type,repair_type,total_space,all_versions_space,valid_space,current_space,history_space,metadata_space"
    )
    .expect("failed to write space-stat header");
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{}",
            row.table_type,
            row.repair_type,
            row.total_space,
            row.all_versions_space,
            row.valid_space,
            row.current_space,
            row.history_space,
            row.metadata_space
        )
        .expect("failed to write space-stat row");
    }
}

fn snapshot_stat_row(cli: &Cli, repair: RepairType, stat: SnapshotStat) -> SnapshotStatRow {
    SnapshotStatRow {
        table_type: format!("{:?}", cli.table_type),
        repair_type: repair.label().to_string(),
        readable_timestamps_published: stat.readable_timestamps_published,
        retained_snapshots: stat.retained_snapshots,
        snapshots_built_total: stat.snapshots_built_total,
        snapshot_reads_total: stat.snapshot_reads_total,
        snapshot_cache_hits_total: stat.snapshot_cache_hits_total,
        snapshot_cache_misses_total: stat.snapshot_cache_misses_total,
        current_reads_total: stat.current_reads_total,
    }
}

fn write_snapshot_stats(path: &Path, rows: &[SnapshotStatRow]) {
    let mut file = File::create(path).expect("failed to create snapshot-stat CSV");
    writeln!(
        file,
        "table_type,repair_type,readable_timestamps_published,retained_snapshots,snapshots_built_total,snapshot_reads_total,snapshot_cache_hits_total,snapshot_cache_misses_total,current_reads_total"
    )
    .expect("failed to write snapshot-stat header");
    for row in rows {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{}",
            row.table_type,
            row.repair_type,
            row.readable_timestamps_published,
            row.retained_snapshots,
            row.snapshots_built_total,
            row.snapshot_reads_total,
            row.snapshot_cache_hits_total,
            row.snapshot_cache_misses_total,
            row.current_reads_total
        )
        .expect("failed to write snapshot-stat row");
    }
}

fn run_and_collect_stat(bench: &TxBench, cli: &Cli, output_path: &Path) {
    let repairs: &[RepairType] = match cli.table_type {
        TableType::Naive | TableType::Ivmh => &[RepairType::NoRepair],
        TableType::Heap | TableType::Chain | TableType::Par => &[
            RepairType::NoRepair,
            RepairType::ReadRepair,
            RepairType::WriteRepair,
        ],
    };
    let mut rows = Vec::new();
    for (idx, repair) in repairs.iter().copied().enumerate() {
        let (stat, _) = run_repair(bench, cli, repair, ContainerKey::new(0, idx as u16));
        rows.push(stat_row(cli, repair, stat));
    }
    write_space_stats(output_path, &rows);
}

fn run_and_collect_snapshot_stat(bench: &TxBench, cli: &Cli, output_path: &Path) {
    let repairs: &[RepairType] = match cli.table_type {
        TableType::Naive | TableType::Ivmh => &[RepairType::NoRepair],
        TableType::Heap | TableType::Chain | TableType::Par => &[
            RepairType::NoRepair,
            RepairType::ReadRepair,
            RepairType::WriteRepair,
        ],
    };
    let mut rows = Vec::new();
    for (idx, repair) in repairs.iter().copied().enumerate() {
        let (_, stat) = run_repair(bench, cli, repair, ContainerKey::new(0, idx as u16));
        if let Some(stat) = stat {
            rows.push(snapshot_stat_row(cli, repair, stat));
        }
    }
    write_snapshot_stats(output_path, &rows);
}

fn main() {
    let mut cli = Cli::parse();

    assert!(cli.txn_gc_ratio.is_some());
    if cli.analytical_ratio.is_some() {
        assert!(cli.txn_update_ratio.is_none());
        assert!(cli.txn_probe_ratio.is_none());
        assert!(cli.txn_scan_ratio.is_none());
        assert!(cli.txn_delta_ratio.is_none());
        let analytical_ratio = *cli.analytical_ratio.as_ref().unwrap();
        let gc_ratio = *cli.txn_gc_ratio.as_ref().unwrap();
        let update_and_analytical = 1.0 - gc_ratio;

        cli.txn_update_ratio = Some(update_and_analytical * (1.0 - analytical_ratio));

        let real_analytical_ratio = update_and_analytical * analytical_ratio;
        if cli.analytical_uniform.is_some() {
            cli.txn_probe_ratio = Some(real_analytical_ratio * 0.25);
            cli.txn_scan_ratio = Some(real_analytical_ratio * 0.50);
            cli.txn_delta_ratio = Some(real_analytical_ratio * (1.0 - 0.75));
        } else {
            cli.txn_probe_ratio = Some(real_analytical_ratio * 0.4);
            cli.txn_scan_ratio = Some(real_analytical_ratio * 0.4);
            cli.txn_delta_ratio = Some(real_analytical_ratio * (1.0 - 0.8));
        }
    } else if cli.txn_scan_ratio.is_some() {
        assert!(cli.txn_probe_ratio.is_some());
        assert!(cli.txn_update_ratio.is_some());
        assert!(cli.txn_delta_ratio.is_some());
        assert!(cli.txn_gc_ratio.is_some());
        let total = *cli.txn_scan_ratio.as_ref().unwrap()
            + *cli.txn_probe_ratio.as_ref().unwrap()
            + *cli.txn_update_ratio.as_ref().unwrap()
            + *cli.txn_delta_ratio.as_ref().unwrap();
        assert!(
            (total - 1.0).abs() < 1e-6,
            "The sum of txn ratios must be 1.0, but got {}",
            total
        );
        let gc_ratio = *cli.txn_gc_ratio.as_ref().unwrap();
        let update_ratio = *cli.txn_update_ratio.as_ref().unwrap();
        let scan_ratio = *cli.txn_scan_ratio.as_ref().unwrap();
        let delta_ratio = *cli.txn_delta_ratio.as_ref().unwrap();
        let probe_ratio = *cli.txn_probe_ratio.as_ref().unwrap();
        let analytical_and_update = 1.0 - gc_ratio;

        cli.analytical_ratio = Some(analytical_and_update * (1.0 - update_ratio));
        cli.txn_update_ratio = Some(analytical_and_update * update_ratio);
        cli.txn_scan_ratio = Some(analytical_and_update * scan_ratio);
        cli.txn_delta_ratio = Some(analytical_and_update * delta_ratio);
        cli.txn_probe_ratio = Some(analytical_and_update * probe_ratio);
    } else {
        panic!(
            "Either analytical-ratio or all of txn-update-ratio, txn-probe-ratio, txn-scan-ratio, txn-delta-ratio must be set"
        );
    }

    let mut bench = TxBench::new(cli.clone());
    bench.print_cli();
    bench.gen_random_txs();
    bench.print_txs();

    if let Some(path) = cli.space_stat.as_ref() {
        run_and_collect_stat(&bench, &cli, Path::new(path));
    } else if let Some(path) = cli.snapshot_stat.as_ref() {
        run_and_collect_snapshot_stat(&bench, &cli, Path::new(path));
    } else {
        run_no_repair(&bench, &cli);
        run_three_repairs(&bench, &cli);
    }
}
