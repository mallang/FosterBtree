pub mod hash_join;
pub mod hashtable_mu;
pub type Timestamp = u64;
pub type TxId = u64; // Transaction ID

use std::{error::Error, fmt::Debug, sync::Arc};

use crate::bp::{ContainerKey, MemPool};

pub trait MvccIndex {
    type Key: Clone + PartialEq + Eq + std::hash::Hash + Debug + Send + Sync;
    type PKey: Clone + PartialEq + Eq + std::hash::Hash + Debug + Send + Sync;
    type Value: Clone + Debug + Send + Sync;
    type Error: Error + Debug + Send + Sync;
    type MemPoolType: MemPool;
    type Iter: Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send;
    type DeltaIter: Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send;
    type ScanKeyIter: Iterator<Item = (Self::PKey, Self::Value)> + Send;

    /// Creates a new instance of the index.
    fn create(c_key: ContainerKey, mem_pool: Arc<Self::MemPoolType>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    /// Inserts a key-primary key-value tuple with a timestamp.
    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error>;

    /// Retrieves the value associated with the key and primary key at the given timestamp.
    /// Returns `None` if no matching record is found at that timestamp.
    fn get(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error>;

    /// Retrieves all values associated with the key at the given timestamp.
    /// Useful when multiple rows share the same key.
    fn get_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error>;

    /// Updates the value associated with the key and primary key at the given timestamp.
    /// Returns an error if the key-primary key combination does not exist.
    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error>;

    /// Deletes the key-primary key tuple at the given timestamp.
    fn delete(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error>;

    /// Scans the index and returns an iterator over key-primary key-value tuples valid at the given timestamp.
    fn scan(&self, ts: Timestamp) -> Result<Self::Iter, Self::Error>;

    /// Scans all entries with the given key at the specified timestamp.
    /// Returns an iterator over primary key and value pairs.
    fn scan_key(&self, key: &Self::Key, ts: Timestamp) -> Result<Self::ScanKeyIter, Self::Error>;

    /// Delta scan between two timestamps.
    /// Returns an iterator over key-primary key and the delta (change) that occurred between `from_ts` and `to_ts`.
    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<Self::DeltaIter, Self::Error>;

    /// Performs garbage collection for entries up to the specified timestamp.
    /// This should remove entries that are no longer needed due to transaction commits.
    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error>;
}

/// Represents a change (delta) in the value of a key-primary key tuple.
#[derive(Clone, Debug)]
pub enum Delta<V> {
    Inserted(V),
    Updated(V),
    Deleted,
}
