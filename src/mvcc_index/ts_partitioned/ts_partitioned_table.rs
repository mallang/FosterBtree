use crate::{
    bp::{ContainerKey, MemPool},
    log_warn,
    mvcc_index::{
        hash_common::{read_repair_vec, BulkUpdate, StatCollector, DEFAULT_BUCKET_NUM},
        Delta, MvccEntry, MvccIndex, TxId,
    },
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    cell::UnsafeCell,
    collections::{hash_map::DefaultHasher, HashMap},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use super::ts_partitioned_collection::TimestampPartitionCollection;

pub struct TsPartitionedTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    bucket_count: usize,
    bucket_entries: Vec<UnsafeCell<TimestampPartitionCollection<T>>>,
    // read repair
    read_repair_ts: AtomicU64,
    latest_update_ts: AtomicU64,
    // bulk update
    bulk_update: BulkUpdate,

    // write repair
    is_write_repair: AtomicBool,
}

// SAFETY: split_at_ts is called between phases, never concurrent with reads.
// All read paths use shared references only.
unsafe impl<T: MemPool + 'static> Sync for TsPartitionedTable<T> {}
unsafe impl<T: MemPool + 'static> Send for TsPartitionedTable<T> {}

impl<T: MemPool + 'static> TsPartitionedTable<T> {
    /// Zero-cost access to a bucket (no locking).
    #[inline]
    fn bucket(&self, idx: usize) -> &TimestampPartitionCollection<T> {
        unsafe { &*self.bucket_entries[idx].get() }
    }

    pub fn split_at_ts(&self, ts: Timestamp) -> Result<(), AccessMethodError> {
        // Bulk-allocate pages for new partitions (one per bucket)
        let pages = self.mem_pool.create_new_pages_for_write(self.c_key, self.bucket_entries.len()).unwrap();
        for (bucket, page) in self.bucket_entries.iter().zip(pages) {
            // SAFETY: split_at_ts is called between phases, never concurrent with reads.
            unsafe { &mut *bucket.get() }.split_last_partition_at_with_page(ts, page)?;
        }
        Ok(())
    }

    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEFAULT_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        // Bulk-allocate all chain pages in one latch acquisition
        let pages = mem_pool.create_new_pages_for_write(c_key, num_buckets).unwrap();
        let mut bucket_entries = Vec::with_capacity(num_buckets);
        for page in pages {
            let second_table = TimestampPartitionCollection::new_from_page(c_key, mem_pool.clone(), page);
            bucket_entries.push(UnsafeCell::new(second_table));
        }

        Self {
            mem_pool,
            c_key,
            bucket_count: num_buckets,
            bucket_entries,
            read_repair_ts: AtomicU64::new(0),
            latest_update_ts: AtomicU64::new(0),
            bulk_update: BulkUpdate::new(num_buckets),
            is_write_repair: AtomicBool::new(false),
        }
    }

    /// Inserts a key-value pair with new pkey into the hash join table.
    pub fn _insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(entry.key());
        self.bucket(index).insert(entry.start_ts(), entry)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn _get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        self.bucket(index).get_no_repair(pkey, *ts)
    }

    pub fn _get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(key);
        self.bucket(index).get_read_repair(pkey, ts)
    }

    /// Updates an existing key-value pair in the hash join table.
    fn _update(&self, key: &[u8], pkey: &[u8], entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        self.bucket(index).update(entry.start_ts(), entry)
    }

    /// Updates an existing key-value pair in the hash join table.
    fn _update_write_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        self.bucket(index).update_write_repair(entry.start_ts(), entry)
    }

    fn _bulk_update(&self) -> Result<(), AccessMethodError> {
        for (idx, bulk_repair) in self.bulk_update.get_updated_pkeys().iter_mut().enumerate() {
            let partition_collection = self.bucket(idx);
            for p in partition_collection.partitions().iter() {
                p.chain().heap_bulk_update_collect(bulk_repair)?;
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
        self.bucket(index).delete(*ts, pkey)
    }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    fn _scan_all(&self) -> Result<Vec<MvccEntry>, AccessMethodError> {
        let mut all_entries = Vec::new();
        for idx in 0..self.bucket_entries.len() {
            let entries = self.bucket(idx).scan_all()?;
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
        self.latest_update_ts.store(ts, Ordering::SeqCst);
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
        self.latest_update_ts.store(ts, Ordering::SeqCst);
        self.is_write_repair.store(true, Ordering::SeqCst);
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        if self.bulk_update.get_flag() {
            self.bulk_update
                .put_updated_pkeys(entry.pkey(), self.get_bucket_index(entry.key()));
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
        for idx in 0..self.bucket_entries.len() {
            let entries = self.bucket(idx).scan_delta(from_ts, to_ts);
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
        let repair_ts = self.read_repair_ts.load(Ordering::SeqCst);
        let latest_update_ts = self.latest_update_ts.load(Ordering::SeqCst);
        let is_need_repair = if to_ts > repair_ts && repair_ts < latest_update_ts {
            let new_repair_ts = std::cmp::min(to_ts, latest_update_ts);
            self.read_repair_ts.store(new_repair_ts, Ordering::SeqCst);
            true
        } else {
            false
        };

        let mut all_entries = Vec::new();
        for idx in 0..self.bucket_entries.len() {
            let entries = if is_need_repair {
                self.bucket(idx).scan_delta_read_repair(from_ts, to_ts)
            } else {
                self.bucket(idx).scan_delta(from_ts, to_ts)
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
        let latest_update_ts = self.latest_update_ts.load(Ordering::SeqCst);
        for idx in 0..self.bucket_entries.len() {
            let partition_collection = self.bucket(idx);
            if self.is_write_repair.load(Ordering::SeqCst) {
                // write repair -> no need to use map to track best candidates
                for p in partition_collection.partitions().iter() {
                    if ts >= p.get_range().0 && !p.chain().is_empty() {
                        p.chain().scan_unique_write_repair(ts, &mut result).unwrap();
                    }
                }
            } else if self.read_repair_ts.load(Ordering::SeqCst) >= ts.min(latest_update_ts) {
                // read repair ts > scan_ts -> no need ...
                for p in partition_collection.partitions().iter() {
                    if ts >= p.get_range().0 && !p.chain().is_empty() {
                        p.chain().scan_unique_write_repair(ts, &mut result).unwrap();
                    }
                }
            } else {
                let mut best_candidates = HashMap::new();
                for p in partition_collection.partitions().iter() {
                    if ts >= p.get_range().0 && !p.chain().is_empty() {
                        p.chain().scan_unique(ts, &mut best_candidates).unwrap();
                    }
                }

                result.extend(best_candidates.into_values());
            }
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
        let repair_ts = self.read_repair_ts.load(Ordering::SeqCst);
        let latest_update_ts = self.latest_update_ts.load(Ordering::SeqCst);
        let is_need_repair = if ts > repair_ts && repair_ts < latest_update_ts {
            let new_repair_ts = std::cmp::min(ts, latest_update_ts);
            self.read_repair_ts.store(new_repair_ts, Ordering::SeqCst);
            true
        } else {
            false
        };
        Ok(if is_need_repair {
            Box::new({
                let mut result = vec![];
                for idx in 0..self.bucket_entries.len() {
                    let partition_collection = self.bucket(idx);
                    let mut best_candidates = HashMap::new();
                    let mut versions_map = HashMap::new();
                    for p in partition_collection.partitions().iter() {
                        if ts >= p.get_range().0 && !p.chain().is_empty() {
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
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let idx = self.get_bucket_index(key);
        let partitions = self.bucket(idx);

        let mvccs = if self.is_write_repair.load(Ordering::SeqCst) {
            // write repair done → no dedup needed, use vector-based fast path
            partitions.scan_with_key_write_repair(ts, key)?
        } else {
            let latest_update_ts = self.latest_update_ts.load(Ordering::SeqCst);
            if self.read_repair_ts.load(Ordering::SeqCst) >= ts.min(latest_update_ts) {
                // read repair completed for this ts → same fast path
                partitions.scan_with_key_write_repair(ts, key)?
            } else {
                // no repair → need HashMap dedup
                partitions.scan_with_key(ts, key)?
            }
        };

        Ok(Box::new(mvccs.into_iter()))
    }

    fn scan_key_vec(
        &self,
        key: &[u8],
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
        let repair_ts = self.read_repair_ts.load(Ordering::SeqCst);
        let latest_update_ts = self.latest_update_ts.load(Ordering::SeqCst);
        let is_need_repair = if ts > repair_ts && repair_ts < latest_update_ts {
            true
        } else {
            false
        };
        let idx = self.get_bucket_index(key);
        let partitions = self.bucket(idx);

        let mvccs = if is_need_repair {
            partitions.scan_with_key_read_repair(ts, key)?
        } else if self.is_write_repair.load(Ordering::SeqCst)
            || repair_ts >= ts.min(latest_update_ts)
        {
            // repair already done → no dedup needed
            partitions.scan_with_key_write_repair(ts, key)?
        } else {
            partitions.scan_with_key(ts, key)?
        };
        Ok(mvccs)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        Ok(Box::new(self._scan_all()?.into_iter()))
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        for idx in 0..self.bucket_entries.len() {
            self.bucket(idx).garbage_collect(safe_ts)?;
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

    fn collect_space_stat(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        for idx in 0..self.bucket_entries.len() {
            self.bucket(idx).collect_space_stat(&mut stat);
        }

        let max_ts = self.latest_update_ts.load(Ordering::Relaxed);
        let valid_space = self
            .scan(max_ts)
            .unwrap()
            .map(|entry| entry.0.len() + entry.1.len() + entry.2.len())
            .sum::<usize>();
        stat.inc_valid_space(valid_space);

        stat
    }
}
