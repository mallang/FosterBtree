pub mod hash_common;
pub mod hash_heap;
pub mod hash_join;
pub mod hash_join_page;
// pub mod hybrid_hash;
pub mod hash_join_heap_chain;
pub mod linear_hash;
pub mod rust_hash_map;
pub mod txn_handle;

mod hash_join_unittest;

pub mod ts_partitioned;
pub type TxId = u64; // Transaction ID

use crate::{
    bp::{ContainerKey, InMemPool, MemPool},
    prelude::{AccessMethodError, Timestamp},
};
use serde::{Deserialize, Serialize};
use std::{
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Committed(Timestamp),
    Aborted,
    Active,
}

pub struct TxInfo {
    pub tx_id: TxId,
    pub ts: Timestamp,
    pub status: TxStatus,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MvccEntry {
    pub key: Vec<u8>,
    pub pkey: Vec<u8>,
    pub value: Vec<u8>,

    pub tx_id: TxId,
    pub start_ts: Timestamp,
    pub end_ts: Timestamp,
    // pub page_id: PageId,
    // pub slot_id: SlotId,
}

impl Debug for MvccEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccEntry")
            .field("key", &String::from_utf8(self.key.clone()).unwrap())
            .field("pkey", &String::from_utf8(self.pkey.clone()).unwrap())
            .field("value", &String::from_utf8(self.value.clone()).unwrap())
            .field("tx_id", &self.tx_id)
            .field("start_ts", &self.start_ts)
            .field("end_ts", &self.end_ts)
            .finish()
    }
}

impl MvccEntry {
    pub fn new(
        key: Vec<u8>,
        pkey: Vec<u8>,
        value: Vec<u8>,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Self {
        Self {
            key,
            pkey,
            value,
            tx_id: 0,
            start_ts,
            end_ts,
        }
    }
    pub fn new_with_tx_id(
        key: Vec<u8>,
        pkey: Vec<u8>,
        value: Vec<u8>,
        start_ts: Timestamp,
        end_ts: Timestamp,
        tx_id: TxId,
    ) -> Self {
        Self {
            key,
            pkey,
            value,
            tx_id,
            start_ts,
            end_ts,
        }
    }
    pub fn key(&self) -> &[u8] {
        &self.key
    }
    pub fn pkey(&self) -> &[u8] {
        &self.pkey
    }
    pub fn value(&self) -> &[u8] {
        &self.value
    }
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }
    pub fn end_ts(&self) -> Timestamp {
        self.end_ts
    }
    pub fn set_end_ts(&mut self, end_ts: &Timestamp) {
        self.end_ts = *end_ts;
    }
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }
    pub fn set_tx_id(&mut self, tx_id: TxId) {
        self.tx_id = tx_id;
    }
    pub fn search_key(&self) -> &[u8] {
        &self.pkey
    }
}

impl PartialEq for MvccEntry {
    fn eq(&self, other: &Self) -> bool {
        self.start_ts == other.start_ts
            && self.end_ts == other.end_ts
            && self.key == other.key
            && self.pkey == other.pkey
            && self.value == other.value
    }
}

impl Eq for MvccEntry {}

impl Hash for MvccEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.start_ts.hash(state);
        self.end_ts.hash(state);
        self.key.hash(state);
        self.pkey.hash(state);
        self.value.hash(state);
    }
}
use std::any::Any;

pub trait MvccIndex<T: MemPool>: Send + Sync + Any {
    type Key: Clone + PartialEq<[u8]> + Eq + std::hash::Hash + Debug + Send + Sync + AsRef<[u8]>;
    type PKey: Clone + PartialEq<[u8]> + Eq + std::hash::Hash + Debug + Send + Sync + AsRef<[u8]>;
    type Value: Clone + Debug + Send + Sync + AsRef<[u8]>;
    type Error: Error + Debug + Send + Sync + 'static;
    // type MemPoolType: MemPool;
    // type Iter: Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send;
    // type DeltaIter: Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send;
    // type ScanKeyIter: Iterator<Item = (Self::PKey, Self::Value)> + Send;
    // type ScanAllIter: Iterator<Item = MvccEntry> + Send;

    /// Creates a new instance of the index.
    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, Self::Error>
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
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error>;

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
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error>;

    /// Scans the index and returns an iterator over key-primary key-value tuples valid at the given timestamp.
    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>;

    /// Scans all entries with the given key at the specified timestamp.
    /// Returns an iterator over primary key and value pairs.
    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error>;

    /// Scans all entries with the given key at the specified timestamp.
    /// Returns a vec over primary key and value pairs.
    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error>;

    /// Delta scan between two timestamps.
    /// Returns an iterator over key-primary key and the delta (change) that occurred between `from_ts` and `to_ts`.
    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    >;

    /// Performs garbage collection for entries up to the specified timestamp.
    /// This should remove entries that are no longer needed due to transaction commits.
    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error>;

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error>;

    fn as_any(&self) -> &dyn Any;
}

/// Represents a change (delta) in the value of a key-primary key tuple.
#[derive(Clone, Debug)]
pub enum Delta<V> {
    Inserted(V),
    Updated(V),
    Deleted,
}

/// Represents a change (delta) in the value of a key-primary key tuple.
#[derive(Clone, Debug)]
pub struct DeltaEntry<V> {
    pub value_delta: Delta<V>,
    pub key: Vec<u8>,
    pub pkey: Vec<u8>,
}

impl<V> DeltaEntry<V> {
    pub fn new(key: Vec<u8>, pkey: Vec<u8>, value_delta: Delta<V>) -> Self {
        Self {
            value_delta,
            key,
            pkey,
        }
    }
}

pub type BoxMvccIndexMemPool = Box<
    dyn MvccIndex<
        InMemPool,
        Key = Vec<u8>,
        PKey = Vec<u8>,
        Value = Vec<u8>,
        Error = AccessMethodError,
    >,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashTableType {
    Chained,
    HeapTable,
    RustHashMap,
    LinearHashTable,
}
