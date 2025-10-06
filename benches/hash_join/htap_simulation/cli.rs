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

    #[arg(long = "analytical-ratio")]
    pub analytical_ratio: Option<f64>,

    #[arg(long = "update-ratio", default_value = "0.01")]
    pub update_ratio: f64,

    #[arg(long = "probe-ratio", default_value = "0.01")]
    pub probe_ratio: f64,

    #[arg(long = "scan-count", default_value = "3")]
    pub scan_count: usize,

    #[arg(long = "delta-count")]
    pub delta_count: Option<usize>,

    #[arg(long = "space-stat")]
    pub space_stat: Option<String>,

    #[arg(long = "txn-scan-ratio")]
    pub txn_scan_ratio: Option<f64>,

    #[arg(long = "txn-probe-ratio")]
    pub txn_probe_ratio: Option<f64>,

    #[arg(long = "txn-update-ratio")]
    pub txn_update_ratio: Option<f64>,

    #[arg(long = "txn-delta-ratio")]
    pub txn_delta_ratio: Option<f64>,
    
    #[arg(long = "txn-gc-ratio")]
    pub txn_gc_ratio: Option<f64>,

    #[arg(long = "scan-reuse-ratio", default_value = "0.8")]
    pub scan_reuse_ratio: f64,
}
