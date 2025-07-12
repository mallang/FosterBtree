use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TableType {
    Chain,
    Heap,
    Partition,
    Naive,
}

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    #[arg(short = 'w', long = "warehouse-count", default_value = "10")]
    pub warehouse_count: usize,

    #[arg(short = 's', long = "seed", default_value = "233333")]
    pub seed: u64,

    #[arg(short = 't', long = "table-type", default_value = "naive")]
    pub table_type: TableType,

    #[arg(long = "manual-txs")]
    pub manual_txs: Option<String>,

    #[arg(long = "txn-count", default_value = "10")]
    pub txn_count: usize,

    #[arg(long = "analytical-ratio", default_value = "0.2")]
    pub analytical_ratio: f64,

    #[arg(long = "op-ratio", default_value = "0.3")]
    pub op_ratio: f64,

    #[arg(long = "delta-count", default_value = "3")]
    pub delta_count: usize,
}
