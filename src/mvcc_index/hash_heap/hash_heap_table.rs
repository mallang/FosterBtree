use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{hash_common::MvccEntryLoc, Delta, MvccEntry, MvccIndex, TxId},
    page::{Page, PageId},
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
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

use super::super::hash_join_heap_chain::HeapHashChain;

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const DEAFAULT_FIRST_BUCKET_NUM: usize = 128;

pub struct HeapHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<HeapHashChain<T>>>,
    // tx_status: HashMap<TxId, TxInfo>, // Neet to written down to disk later...
}

impl<T: MemPool + 'static> HeapHashTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_FIRST_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());

        let mut bucket_entries: Vec<Arc<HeapHashChain<T>>> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let heap_chain = HeapHashChain::new(c_key, mem_pool.clone());
            bucket_entries.push(Arc::new(heap_chain));
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
        let heap_chain = &self.bucket_entries[index];

        heap_chain.insert(entry)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];

        heap_chain.get_no_repair(pkey, ts)
    }

    pub fn get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];
        let mut versions: BTreeMap<u64, MvccEntryLoc> = BTreeMap::new();

        let res = heap_chain.get_read_repair(pkey, ts, &mut versions);
        heap_chain.read_repair(&mut versions);
        return res;
    }

    /// Updates an existing key-value pair in the hash join table.
    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        heap_chain.update_no_repair(pkey, entry)
    }

    pub fn update_write_reapair(
        &self,
        key: &[u8],
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        heap_chain.update_write_repair(entry)
    }

    /// Deletes a key-value pair from the hash join table.
    // pub fn delete(&self, key: &[u8], pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
    //     let index = self.get_bucket_index(key);
    //     let heap_chain = &self.bucket_entries[index];

    //     heap_chain.delete(pkey, ts)
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

    // pub fn scan_key(&self, key: &[u8], ts: Timestamp) -> Vec<MvccEntry> {
    //     let index = self.get_bucket_index(key);
    //     let bucket = &self.bucket_entries[index];
    //     bucket.scan_key(key, &ts)
    // }
}

impl<T: MemPool + 'static> MvccIndex<T> for HeapHashTable<T> {
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
        let v = self.get(key, pkey, &ts).map_or(None, |e| {
            // log_warn!("get entry: {:?}", e);
            if e.value().is_empty() {
                None
            } else {
                Some(e.value().to_vec())
            }
        });
        Ok(v)
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

    fn update_write_repair(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry =
            MvccEntry::new_with_tx_id(key.clone(), pkey.clone(), value, ts, u64::MAX, tx_id);
        self.update_write_reapair(&key, &pkey, &entry)
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

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            let iter = bucket.scan_unique(ts)?;
            result.extend(iter);
        }
        Ok(Box::new(
            result.into_iter().map(|e| (e.key, e.pkey, e.value)),
        ))
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let chain = &self.bucket_entries[idx];

        let pk_v = chain.scan_key_vec(key, &ts);

        let mapped = pk_v.into_iter();

        Ok(Box::new(mapped))
    }

    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let chain = &self.bucket_entries[idx];

        let mvccs = chain.scan_key_vec(key, &ts);

        Ok(mvccs)
    }

    fn scan_key_vec_read_repair(
            &self,
            key: &Self::Key,
            ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let chain = &self.bucket_entries[idx];
        
        let mvccs = chain.scan_key_vec_read_repair(key, &ts);
        Ok(mvccs)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            let iter = bucket.scan_all()?;
            result.extend(iter);
        }
        Ok(Box::new(result.into_iter()))
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

    fn split_at_ts(&self, ts: Timestamp) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn test_scan() {
    use crate::mvcc_index::MvccIndex;

    use crate::bp::InMemPool;

    let mem_pool = Arc::new(InMemPool::new());
    let table = HeapHashTable::new(ContainerKey::new(0, 0), mem_pool.clone());

    let key = vec![1, 2, 3];
    let pkey = vec![4, 5, 6];
    let ts = 1;
    let value = vec![7, 8, 9];
    let new_value = vec![7, 8, 9, 10];
    <HeapHashTable<_> as MvccIndex<_>>::insert(&table, key.clone(), pkey.clone(), ts, 0, value)
        .unwrap();
    <HeapHashTable<_> as MvccIndex<_>>::update(
        &table,
        key.clone(),
        pkey.clone(),
        ts + 1,
        0,
        new_value.clone(),
    )
    .unwrap();

    let result = <HeapHashTable<_> as MvccIndex<_>>::scan(&table, 10)
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, key);
    assert_eq!(result[0].1, pkey);
    assert_eq!(result[0].2, new_value);

    let result = <HeapHashTable<_> as MvccIndex<_>>::scan_all(&table)
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(result.len(), 2);
}
