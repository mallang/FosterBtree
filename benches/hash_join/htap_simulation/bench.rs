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

fn main() {
    let cli = Cli::parse();

    let mem_pool = get_in_mem_pool();
    let c_key = ContainerKey::new(0, 0);
    let num_buckets = 10;

    let table: BoxMVIndex = match cli.table_type {
        cli::TableType::Chain => Box::new(ChainedHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            num_buckets,
        )) as BoxMVIndex,
        cli::TableType::Heap => Box::new(HeapHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            num_buckets,
        )) as BoxMVIndex,
        cli::TableType::Partition => Box::new(TsPartitionedTable::new_with_bucket_num(
            c_key,
            mem_pool,
            num_buckets,
        )) as BoxMVIndex,
        cli::TableType::Naive => Box::new(
            fbtree::naive_hash_index::NaiveMvHashTable::new_with_bucket_num(
                c_key,
                mem_pool,
                num_buckets,
            ),
        ) as BoxMVIndex,
    };

    let mut bench = TxBench::new(cli);
    bench.print_cli();
    bench.gen_random_txs();
    bench.print_txs();

    bench.run_all_txs_no_repair(&table);
}
