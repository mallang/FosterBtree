pub mod chained_hash_bucket_first;
pub mod chained_hash_bucket_second;
pub mod chained_hash_history_chain;
pub mod chained_hash_recent_chain;
pub mod chained_hash_table;

use serde::{Deserialize, Serialize};

use super::{Timestamp, TxId, TxInfo};

use clap::Parser;
use std::cell::UnsafeCell;

#[derive(Clone, Parser, Debug)]
pub struct Parameters {
    #[arg(long, default_value = "false")]
    pub sorted: String,
}

static mut PARAMS: UnsafeCell<Option<Parameters>> = UnsafeCell::new(None);

pub fn get_params() -> Parameters {
    unsafe { PARAMS.get().as_ref().unwrap().clone().unwrap() }
}
