use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum TableType {
    Naive,
    Chain,
    Heap,
    Par,
}

#[derive(Parser, Debug, Clone)]
pub struct Cli {
    #[arg(short = 'w', long = "warehouse-count", default_value = "10")]
    pub warehouse_count: usize,

    #[arg(short = 's', long = "seed", default_value = "232323223")]
    pub seed: u64,

    #[arg(short = 't', long = "table-type", default_value = "chain")]
    pub table_type: TableType,

    #[arg(long = "manual-txs")]
    pub manual_txs: Option<String>,

    #[arg(long = "txn-count", default_value = "10")]
    pub txn_count: usize,

    #[arg(long = "analytical-ratio", default_value = "0.4")]
    pub analytical_ratio: f64,

    #[arg(long = "op-ratio", default_value = "0.01")]
    pub op_ratio: f64,

    #[arg(long = "scan-count", default_value = "3")]
    pub scan_count: usize,

    #[arg(long = "space-stat")]
    pub space_stat: Option<String>,
}
