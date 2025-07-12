use std::{any::Any, error::Error, fmt::Debug, sync::Arc};

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{MvccEntry, TxId},
};

pub mod hash_join_chain;
mod hash_join_page;
pub mod hash_join_table;

pub trait SingleTsHashTable<T: MemPool>: Send + Sync + Any {
    type Key: Clone + PartialEq<[u8]> + Eq + std::hash::Hash + Debug + Send + Sync + AsRef<[u8]>;
    type PKey: Clone + PartialEq<[u8]> + Eq + std::hash::Hash + Debug + Send + Sync + AsRef<[u8]>;
    type Value: Clone + Debug + Send + Sync + AsRef<[u8]>;
    type Error: Error + Debug + Send + Sync + 'static;

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error>;

    fn get(&self, key: &[u8], pkey: &[u8]) -> Result<Option<Self::Value>, Self::Error>;

    fn create_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized;
}
