use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum TableType {
    Chain,
    Heap,
    Naive,
    TsPartitioned,
}

#[derive(Parser, Debug, Clone)]
#[command(about = "JOIN benchmark for SIGMOD workloads")]
pub struct Cli {
    #[arg(short = 't', long = "table-type", default_value = "chain")]
    pub table_type: TableType,

    #[arg(
        short = 'l',
        long = "lineitem-file",
        default_value = "benches/sigmod/tpch_data/lineitem_sf0.1.tbl"
    )]
    pub lineitem_file: String,

    #[arg(
        short = 'p',
        long = "part-file",
        default_value = "benches/sigmod/tpch_data/part_sf0.1.tbl"
    )]
    pub part_file: String,

    #[arg(
        short = 'u',
        long = "part-updates-file",
        default_value = "benches/sigmod/tpch_data/part_updates_sf0.1_1pct_uniform.tbl"
    )]
    pub part_updates_file: String,

    #[arg(
        short = 'd',
        long = "part-updated-file",
        default_value = "benches/sigmod/tpch_data/part_updated_sf0.1_1pct_uniform.tbl"
    )]
    pub part_updated_file: String,

    #[arg(short = 'b', long = "num-buckets", default_value = "128")]
    pub num_buckets: usize,

    #[arg(short = 's', long = "seed", default_value = "42")]
    pub seed: u64,
}
