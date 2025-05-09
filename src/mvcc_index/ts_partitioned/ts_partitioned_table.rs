use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{
        hash_common::{read_repair_vec, BulkUpdate, DEFAULT_BUCKET_NUM},
        Delta, MvccEntry, MvccIndex, TxId,
    },
    page::{Page, PageId},
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
    vec::IntoIter,
};

use dashmap::mapref::entry;
use rand::seq::index;
use serde::{Deserialize, Serialize};

use super::ts_partitioned_collection::TimestampPartitionCollection;

pub struct TsPartitionedTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    bucket_count: usize,
    bucket_entries: Vec<Arc<RwLock<TimestampPartitionCollection<T>>>>,
    repair_ts: AtomicU64,
    // bulk update
    bulk_update: BulkUpdate,
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
            repair_ts: AtomicU64::new(0),
            bulk_update: BulkUpdate::new(num_buckets),
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
    fn _update(&self, key: &[u8], pkey: &[u8], entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let ts_partitions = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        ts_partitions
            .read()
            .unwrap()
            .update(entry.start_ts(), entry)
    }

    /// Updates an existing key-value pair in the hash join table.
    fn _update_write_repair(
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
            .update_write_repair(entry.start_ts(), entry)
    }

    fn _bulk_update(&self) -> Result<(), AccessMethodError> {
        for (idx, bulk_repair) in self.bulk_update.get_updated_pkeys().iter_mut().enumerate() {
            let bucket = &self.bucket_entries[idx];
            let partition_collection = bucket.read().unwrap();
            // let mut idx = 0;
            for p in partition_collection.partitions().iter() {
                p.chain().traverse_to_endofchain_for_bulk_update(bulk_repair);
            }
            // repair
            for versions in bulk_repair.values() {
                read_repair_vec(&self.mem_pool, versions, self.c_key);
            }
            bulk_repair.clear();
        }
        Ok(())
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

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
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
    fn get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let v = self._get_read_repair(key, pkey, ts).map_or(None, |e| {
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
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
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
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        if self.bulk_update.get_flag() {
            self.bulk_update.put_updated_pkeys(entry.pkey(), self.get_bucket_index(entry.key()));
            self._update(&entry.key(), &entry.pkey(), &entry)
        } else {
            self._update_write_repair(&entry.key(), &entry.pkey(), &entry)
        }
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
        let mut all_entries = Vec::new();
        for bucket in &self.bucket_entries {
            let entries = bucket.read().unwrap().scan_delta(from_ts, to_ts);
            all_entries.extend(entries);
        }
        Ok(Box::new(all_entries.into_iter()))
    }

    fn delta_scan_read_repair(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        let repair_ts = self.repair_ts.load(Ordering::SeqCst);
        let is_need_repair = if to_ts > repair_ts {
            self.repair_ts.store(to_ts, Ordering::SeqCst);
            true
        } else {
            false
        };

        let mut all_entries = Vec::new();
        for bucket in &self.bucket_entries {
            let entries = if is_need_repair {
                bucket
                    .read()
                    .unwrap()
                    .scan_delta_read_repair(from_ts, to_ts)
            } else {
                bucket.read().unwrap().scan_delta(from_ts, to_ts)
            };

            all_entries.extend(entries);
        }
        Ok(Box::new(all_entries.into_iter()))
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            let partition_collection = bucket.read().unwrap();
            let mut best_candidates = HashMap::new();
            // let mut idx = 0;
            for p in partition_collection.partitions().iter() {
                if ts >= p.get_range().0 && ts < p.get_range().1 {
                    p.chain().scan_unique(ts, &mut best_candidates).unwrap();
                }
            }

            result.extend(best_candidates.into_values());
        }
        Ok(Box::new(
            result.into_iter().map(|e| (e.key, e.pkey, e.value)),
        ))
    }

    fn scan_read_repair(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let repair_ts = self.repair_ts.load(Ordering::SeqCst);
        let is_need_repair = if ts > repair_ts {
            self.repair_ts.store(ts, Ordering::SeqCst);
            true
        } else {
            false
        };
        Ok(if is_need_repair {
            Box::new({
                let mut result = vec![];
                for bucket in &self.bucket_entries {
                    let partition_collection = bucket.read().unwrap();
                    let mut best_candidates = HashMap::new();
                    let mut versions_map = HashMap::new();
                    for p in partition_collection.partitions().iter() {
                        if ts >= p.get_range().0 && ts < p.get_range().1 {
                            p.chain()
                                .scan_unique_read_repair(
                                    ts,
                                    &mut best_candidates,
                                    &mut versions_map,
                                )
                                .unwrap();
                        }
                    }

                    for versions in versions_map.into_values() {
                        read_repair_vec(&self.mem_pool, &versions, self.c_key);
                    }

                    result.extend(best_candidates.into_values());
                }

                result.into_iter().map(|e| (e.key, e.pkey, e.value))
            })
        } else {
            self.scan(ts)?
        })
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

    fn scan_key_vec_read_repair(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        let repair_ts = self.repair_ts.load(Ordering::SeqCst);
        let is_need_repair = if ts > repair_ts { true } else { false };
        let idx = self.get_bucket_index(key);
        let partitions = &self.bucket_entries[idx];

        let mvccs = if is_need_repair {
            partitions
                .read()
                .unwrap()
                .scan_with_key_read_repair(ts, key)?
        } else {
            partitions.read().unwrap().scan_with_key(ts, key)?
        };
        Ok(mvccs)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        Ok(Box::new(self._scan_all()?.into_iter()))
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        for chain_bucket in &self.bucket_entries {
            chain_bucket.read().unwrap().garbage_collect(safe_ts)?;
        }
        Ok(())
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
        self.split_at_ts(ts)
    }

    fn create_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        bucket_num: usize,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new_with_bucket_num(c_key, mem_pool, bucket_num))
    }


    fn bulk_update_start(&self) -> Result<(), Self::Error> {
        self.bulk_update.set_flag();
        Ok(())
    }
    fn bulk_update_end(&self) -> Result<(), Self::Error> {
        self._bulk_update()?;
        self.bulk_update.reset_flag();
        Ok(())
    }
}
