mod cli;
mod dbgen;
mod interface;
mod txs;

use core::num;

use clap::Parser;
use cli::{Cli, TableType};
use dbgen::DataSource;
use fbtree::{
    bp::{get_in_mem_pool, ContainerKey},
    mvcc_index::dual_heap_hash::chained_hash_table::ChainedHashTable,
    mvcc_index::hash_heap::hash_heap_table::HeapHashTable,
    mvcc_index::ts_partitioned::ts_partitioned_table::TsPartitionedTable,
    prelude::Timestamp,
};
use interface::BoxMVIndex;

use crate::txs::TxBench;

const NUM_BUCKETS: usize = 128;

#[derive(PartialEq)]
enum RepairType {
    ReadRepair,
    WriteRepair,
    NoRepair,
}

fn run_no_repair(bench: &TxBench, cli: &Cli) {
    println!();
    println!("No Repair");
    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let table: BoxMVIndex = match cli.table_type {
        cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            NUM_BUCKETS,
        )) as BoxMVIndex,
        cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            NUM_BUCKETS,
        )) as BoxMVIndex,
        cli::TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
            c_key,
            mem_pool,
            NUM_BUCKETS,
        )) as BoxMVIndex,
        cli::TableType::Naive => Box::new(
            fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            ),
        ) as BoxMVIndex,
    };

    bench.run_all_txs_no_repair(&table);
}

fn run_three_repairs(bench: &TxBench, cli: &Cli) {
    {
        println!();
        println!("No Repair");
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let table: BoxMVIndex = match cli.table_type {
            cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Naive => Box::new(
                fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                    c_key,
                    mem_pool,
                    NUM_BUCKETS,
                ),
            ) as BoxMVIndex,
        };

        bench.run_all_txs_no_repair(&table);
    }

    {
        println!();
        println!("Read Repair");
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 1);
        let table: BoxMVIndex = match cli.table_type {
            cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Naive => Box::new(
                fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                    c_key,
                    mem_pool,
                    NUM_BUCKETS,
                ),
            ) as BoxMVIndex,
        };

        bench.run_all_txs_read_repair(&table);
    }

    {
        println!();
        println!("Write Repair");
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 2);
        let table: BoxMVIndex = match cli.table_type {
            cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Naive => Box::new(
                fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                    c_key,
                    mem_pool,
                    NUM_BUCKETS,
                ),
            ) as BoxMVIndex,
        };

        bench.run_all_txs_write_repair(&table);
    }
}

fn run_and_collect_stat(bench: &TxBench, cli: &Cli) {
    {
        println!();
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 1);
        let table: BoxMVIndex = match cli.table_type {
            cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Par => Box::new(TsPartitionedTable::new_with_bucket_num(
                c_key,
                mem_pool,
                NUM_BUCKETS,
            )) as BoxMVIndex,
            cli::TableType::Naive => Box::new(
                fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                    c_key,
                    mem_pool,
                    NUM_BUCKETS,
                ),
            ) as BoxMVIndex,
        };

        bench.run_all_txs_no_repair(&table);
        let stat = table.collect_space_stat();
        println!("{:?}", stat);
    }
}

fn main() {
    let cli = Cli::parse();

    let mut bench = TxBench::new(cli.clone());
    bench.print_cli();
    bench.gen_random_txs();
    bench.print_txs();

    if cli.space_stat.is_some() {
        // space stat
        run_and_collect_stat(&bench, &cli);
    } else {
        run_no_repair(&bench, &cli);
        run_three_repairs(&bench, &cli);
    }
}
