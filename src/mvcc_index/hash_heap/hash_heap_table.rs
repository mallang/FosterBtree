use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{Delta, MvccEntry, MvccIndex, TxId},
    page::{Page, PageId},
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
    vec::IntoIter,
};

use dashmap::mapref::entry;
use rand::seq::index;
use serde::{Deserialize, Serialize};

use super::hash_heap_chain::ChainedHashHeapChain;

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const DEAFAULT_FIRST_BUCKET_NUM: usize = 128;

pub struct HashHeapTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<ChainedHashHeapChain<T>>>,
    // tx_status: HashMap<TxId, TxInfo>, // Neet to written down to disk later...
}

impl<T: MemPool + 'static> HashHeapTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_FIRST_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());

        let mut bucket_entries: Vec<Arc<ChainedHashHeapChain<T>>> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let second_table = ChainedHashHeapChain::new(c_key, mem_pool.clone());
            bucket_entries.push(Arc::new(second_table));
        }
        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            bucket_count: num_buckets,
            bucket_entries,
        }
    }

    /// Creates a new hash join table using a scanner to populate the table.
    pub fn new_with_scanner(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        scanner: impl Iterator<Item = (Vec<u8>, Timestamp, Vec<u8>)>,
        tx_id: TxId,
    ) -> Self {
        Self::new_with_scanner_and_bucket_num(
            c_key,
            mem_pool,
            DEAFAULT_FIRST_BUCKET_NUM,
            scanner,
            tx_id,
        )
    }

    /// Creates a new hash join table with a specified number of buckets, using a scanner.
    pub fn new_with_scanner_and_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
        scanner: impl Iterator<Item = (Vec<u8>, Timestamp, Vec<u8>)>,
        tx_id: TxId,
    ) -> Self {
        let table = Self::new_with_bucket_num(c_key, mem_pool, num_buckets);
        for (key, ts, val) in scanner {
            let pkey = key.clone();
            let entry = MvccEntry::new_with_tx_id(key, pkey, val, ts, u64::MAX, tx_id);
            table.insert(&entry).unwrap();
        }
        table
    }

    /// Inserts a key-value pair with new pkey into the hash join table.
    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(entry.key());
        let second_table = &self.bucket_entries[index];

        second_table.insert(entry)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let second_table = &self.bucket_entries[index];

        second_table.get(pkey, ts)
    }

    /// Updates an existing key-value pair in the hash join table.
    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let second_table = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        second_table.update(pkey, entry)
    }

    /// Deletes a key-value pair from the hash join table.
    // pub fn delete(&self, key: &[u8], pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
    //     let index = self.get_bucket_index(key);
    //     let second_table = &self.bucket_entries[index];

    //     second_table.delete(pkey, ts)
    // }

    pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.garbage_collect(ts)?;
        }
        Ok(())
    }

    /// Read page with given PageFrameKey
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }
}

impl<T: MemPool + 'static> MvccIndex<T> for HashHeapTable<T> {
    type Error = AccessMethodError;
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry = MvccEntry::new_with_tx_id(key.clone(), pkey, value, ts, u64::MAX, tx_id);
        self.insert(&entry)
    }
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let entry = self.get(key, pkey, &ts)?;
        Ok(Some(entry.value))
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry = MvccEntry::new_with_tx_id(key.clone(), pkey, value, ts, u64::MAX, tx_id);
        self.update(&entry.key(), &entry.pkey(), &entry)
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error> {
        let entry =
            MvccEntry::new_with_tx_id(key.to_vec(), pkey.to_vec(), vec![], ts, u64::MAX, tx_id);
        self.insert(&entry)
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            let iter = bucket.scan(ts)?;
            result.extend(iter);
        }
        Ok(Box::new(
            result.into_iter().map(|e| (e.key, e.pkey, e.value)),
        ))
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            let iter = bucket.scan_all()?;
            result.extend(iter);
        }
        Ok(Box::new(result.into_iter()))
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        todo!("Implement delta_scan for HashHeapTable")
    }
    fn get_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        todo!("Implement get_key for HashHeapTable")
    }
    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let bucket = &self.bucket_entries[idx];
        let iter = bucket.scan(ts)?;
        Ok(Box::new(iter.map(|e| (e.pkey, e.value))))
    }
    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        todo!("Implement garbage_collect for HashHeapTable")
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
    }
}
