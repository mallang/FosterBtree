use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{hash_common::DEFAULT_BUCKET_NUM, Delta, MvccEntry, MvccIndex, TxId},
    page::{Page, PageId},
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{atomic::AtomicU32, Arc, RwLock},
    time::Duration,
    vec::IntoIter,
};

use dashmap::mapref::entry;
use rand::seq::index;
use serde::{Deserialize, Serialize};

use super::ts_partitioned_chain::TimestampPartitionCollection;

pub struct TsPartitionedTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    bucket_count: usize,
    bucket_entries: Vec<Arc<RwLock<TimestampPartitionCollection<T>>>>,
}

impl<T: MemPool + 'static> TsPartitionedTable<T> {
    pub fn split_at_ts(&self, ts: Timestamp) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.write().unwrap().split_last_partition_at(ts)?;
        }

        Ok(())
    }

    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEFAULT_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut bucket_entries = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let second_table = TimestampPartitionCollection::new(c_key, mem_pool.clone());
            bucket_entries.push(Arc::new(RwLock::new(second_table)));
        }

        Self {
            mem_pool,
            c_key,
            bucket_count: num_buckets,
            bucket_entries,
        }
    }

    /// Inserts a key-value pair with new pkey into the hash join table.
    pub fn _insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(entry.key());
        let ts_partitions = &self.bucket_entries[index];

        ts_partitions
            .read()
            .unwrap()
            .insert(entry.start_ts(), entry)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn _get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let ts_partitions = &self.bucket_entries[index];

        ts_partitions.read().unwrap().get_no_repair(pkey, *ts)
    }

    pub fn _get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let ts_partitions = &self.bucket_entries[index];

        ts_partitions.read().unwrap().get_read_repair(pkey, ts)
    }

    /// Updates an existing key-value pair in the hash join table.
    pub fn _update(
        &self,
        key: &[u8],
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let ts_partitions = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        ts_partitions
            .read()
            .unwrap()
            .update(entry.start_ts(), entry)
    }

    // Deletes a key-value pair from the hash join table.
    pub fn _delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let ts_partitions = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        ts_partitions.read().unwrap().delete(*ts, pkey)
    }

    // pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
    //     for bucket in &self.bucket_entries {
    //         bucket.garbage_collect(ts)?;
    //     }
    //     Ok(())
    // }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    fn _scan_unique(&self, ts: &Timestamp) -> Result<Vec<MvccEntry>, AccessMethodError> {
        let mut unique_keys = Vec::new();
        for bucket in &self.bucket_entries {
            let keys = bucket.read().unwrap().scan_unique(*ts)?;
            unique_keys.extend(keys);
        }
        Ok(unique_keys)
    }

    fn _scan_all(&self) -> Result<Vec<MvccEntry>, AccessMethodError> {
        let mut all_entries = Vec::new();
        for bucket in &self.bucket_entries {
            let entries = bucket.read().unwrap().scan_all()?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }
}

impl<T: MemPool + 'static> MvccIndex<T> for TsPartitionedTable<T> {
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
        self._insert(&entry)
    }
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let v = self._get(key, pkey, &ts).map_or(None, |e| {
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
        self._update(&entry.key(), &entry.pkey(), &entry)
    }

    fn update_write_repair(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry = MvccEntry::new_with_tx_id(key.clone(), pkey, value, ts, u64::MAX, tx_id);
        self._update(&entry.key(), &entry.pkey(), &entry)
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error> {
        // let entry =
        //     MvccEntry::new_with_tx_id(key.to_vec(), pkey.to_vec(), vec![], ts, u64::MAX, tx_id);
        // self._insert(&entry)
        unimplemented!("delete is not implemented yet")
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        todo!("Implement delta_scan for TsPartitionedTable")
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        Ok(Box::new(self._scan_unique(&ts)?.into_iter().map(|entry| {
            (
                entry.key().to_vec(),
                entry.pkey().to_vec(),
                entry.value().to_vec(),
            )
        })))
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let partitions = &self.bucket_entries[idx];

        let mvccs = partitions.read().unwrap().scan_with_key(ts, key)?;

        Ok(Box::new(mvccs.into_iter()))
    }

    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        Ok(self
            .scan_key(key, ts)
            .map(|iter| iter.collect::<Vec<_>>())?)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        Ok(Box::new(self._scan_all()?.into_iter()))
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        todo!("Implement garbage_collect for TsPartitionedTable")
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
