use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::hash_join_page::ChainedHashMetaPage,
    mvcc_index::{Delta, MvccEntry, MvccIndex},
    page::{Page, PageId},
    prelude::AccessMethodError,
};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    result,
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
    vec::IntoIter,
};

use super::{
    chained_hash_bucket_first::FirstBucket,
    chained_hash_history_chain::{ChainedHashHistoryChain, ChainedHashHistoryChainScanner},
    chained_hash_recent_chain::{ChainedHashRecentChain, ChainedHashRecentChainScanner},
    Timestamp, TxId, TxInfo,
};

use dashmap::mapref::entry;
use rand::seq::index;
use serde::{Deserialize, Serialize};

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const DEAFAULT_FIRST_BUCKET_NUM: usize = 128;

pub struct ChainedHashTable<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<FirstBucket<T>>>,
    // tx_status: HashMap<TxId, TxInfo>, // Neet to written down to disk later...
}

impl<T: MemPool> ChainedHashTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_FIRST_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        ChainedHashMetaPage::init(&mut *meta_page, num_buckets);
        ChainedHashMetaPage::set_bucket_num(&mut *meta_page, num_buckets);

        let mut bucket_entries: Vec<Arc<FirstBucket<T>>> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let second_table = FirstBucket::new(c_key, mem_pool.clone());
            // MvccHashJoinMetaPage::set_bucket_entry(&mut *meta_page, i, &entry);
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
    pub fn delete(&self, key: &[u8], pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let second_table = &self.bucket_entries[index];

        second_table.delete(pkey, ts)
    }

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

    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub fn bucket_entries(&self, idx: usize) -> &Arc<FirstBucket<T>> {
        &self.bucket_entries[idx]
    }

    pub fn scan(
        self: &Arc<Self>,
        ts: Timestamp,
    ) -> Result<ChainedHashTableScanner<T>, AccessMethodError> {
        Ok(ChainedHashTableScanner::new(self, ts))
    }

    pub fn scan_all(self: &Arc<Self>) -> Result<ChainedHashTableScanner<T>, AccessMethodError> {
        Ok(ChainedHashTableScanner::new_full_scan(self))
    }

    pub fn scan_key(
        self: &Arc<Self>,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<ChainedHashTableScanner<T>, AccessMethodError> {
        Ok(ChainedHashTableScanner::new_scan_key(self, key, ts))
    }

    pub fn scan_key_vec(self: &Arc<Self>, key: &[u8], ts: Timestamp) -> Vec<MvccEntry> {
        let mut results = Vec::new();
        let idx = self.get_bucket_index(key);
        let first_bucket = &self.bucket_entries[idx];
        first_bucket.scan_key_into(key, &ts, &mut results);
        results
    }

    pub fn scan_into_vec(
        &self,
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.scan_into_vec(&ts, results);
        }
        Ok(())
    }
    /// Returns a human‑readable status string for the ChainedHashTable.
    ///
    /// This aggregates statistics across:
    ///  - First buckets (total, unused),
    ///  - Second buckets (total count and average per first bucket),
    ///  - Chain lengths (average number of pages per second bucket, for both recent and history),
    ///  - Average page usage (in %) per page,
    ///  - And average number of key–value pairs per page.
    pub fn stat(&self) -> String {
        let mut total_first_buckets = self.bucket_entries.len();
        let mut unused_first_buckets = 0;
        let mut total_second_buckets = 0;

        // Global accumulators for the recent chain.
        let mut total_recent_pages = 0;
        let mut total_recent_kv_count = 0;
        let mut total_recent_usage = 0.0;
        // Global accumulators for the history chain.
        let mut total_history_pages = 0;
        let mut total_history_kv_count = 0;
        let mut total_history_usage = 0.0;

        // Iterate over every first bucket.
        for first_bucket in &self.bucket_entries {
            // Each first bucket is itself a hashmap of second buckets.
            let second_buckets = first_bucket.bucket_count();

            // Local accumulators for this first bucket.
            let mut bucket_recent_pages = 0;
            let mut bucket_recent_kv_count = 0;
            let mut bucket_recent_usage = 0.0;

            let mut bucket_history_pages = 0;
            let mut bucket_history_kv_count = 0;
            let mut bucket_history_usage = 0.0;

            // Process every second bucket inside the first bucket.
            for i in 0..second_buckets {
                let second_bucket = first_bucket.bucket_entries(i);
                // Get summary metrics for the recent chain.
                let (r_pages, r_kvs, r_usage, _r_max, _r_min) =
                    second_bucket.recent_chain().summary_metrics();
                // Get summary metrics for the history chain.
                let (h_pages, h_kvs, h_usage, _h_max, _h_min) =
                    second_bucket.history_chain().summary_metrics();

                bucket_recent_pages += r_pages;
                bucket_recent_kv_count += r_kvs;
                bucket_recent_usage += r_usage;

                bucket_history_pages += h_pages;
                bucket_history_kv_count += h_kvs;
                bucket_history_usage += h_usage;
            }

            // If this first bucket has no pages in both chains, mark it as unused.
            if bucket_recent_kv_count == 0 && bucket_history_kv_count == 0 {
                unused_first_buckets += 1;
                continue;
            }

            total_second_buckets += second_buckets;

            total_recent_pages += bucket_recent_pages;
            total_recent_kv_count += bucket_recent_kv_count;
            total_recent_usage += bucket_recent_usage;

            total_history_pages += bucket_history_pages;
            total_history_kv_count += bucket_history_kv_count;
            total_history_usage += bucket_history_usage;
        }

        total_first_buckets -= unused_first_buckets;

        // Compute average number of second buckets per first bucket.
        let avg_second_buckets_per_first = if total_first_buckets > 0 {
            total_second_buckets as f64 / total_first_buckets as f64
        } else {
            0.0
        };

        // Now, we want the average chain length per second bucket, not per first bucket.
        let avg_recent_chain_len = if total_second_buckets > 0 {
            total_recent_pages as f64 / total_second_buckets as f64
        } else {
            0.0
        };

        let avg_history_chain_len = if total_second_buckets > 0 {
            total_history_pages as f64 / total_second_buckets as f64
        } else {
            0.0
        };

        // Compute the average page usage (in percent) per page.
        let avg_recent_page_usage = if total_recent_pages > 0 {
            total_recent_usage / total_recent_pages as f64
        } else {
            0.0
        };

        let avg_history_page_usage = if total_history_pages > 0 {
            total_history_usage / total_history_pages as f64
        } else {
            0.0
        };

        // Compute the average number of key–value pairs per page.
        let avg_recent_kv_per_page = if total_recent_pages > 0 {
            total_recent_kv_count as f64 / total_recent_pages as f64
        } else {
            0.0
        };

        let avg_history_kv_per_page = if total_history_pages > 0 {
            total_history_kv_count as f64 / total_history_pages as f64
        } else {
            0.0
        };

        // Compose the final status string.
        let mut stat_str = String::new();
        stat_str.push_str("=== ChainedHashTable Stats ===\n\n");
        stat_str.push_str(&format!("Total first buckets: {}\n", total_first_buckets));
        stat_str.push_str(&format!(
            "Unused first buckets: {}\n\n",
            unused_first_buckets
        ));
        stat_str.push_str(&format!("Total second buckets: {}\n", total_second_buckets));
        stat_str.push_str(&format!(
            "Average second buckets per first bucket: {:.2}\n\n",
            avg_second_buckets_per_first
        ));
        stat_str.push_str("Average chain length (in pages) per second bucket:\n");
        stat_str.push_str(&format!("  Recent chain: {:.2}\n", avg_recent_chain_len));
        stat_str.push_str(&format!(
            "  History chain: {:.2}\n\n",
            avg_history_chain_len
        ));
        stat_str.push_str("Average page usage (in %) per page:\n");
        stat_str.push_str(&format!("  Recent chain: {:.2}\n", avg_recent_page_usage));
        stat_str.push_str(&format!(
            "  History chain: {:.2}\n\n",
            avg_history_page_usage
        ));
        stat_str.push_str("Average number of key–value pairs per page:\n");
        stat_str.push_str(&format!("  Recent chain: {:.2}\n", avg_recent_kv_per_page));
        stat_str.push_str(&format!(
            "  History chain: {:.2}\n",
            avg_history_kv_per_page
        ));

        stat_str
    }
}

impl<T: MemPool> Clone for ChainedHashTable<T> {
    fn clone(&self) -> Self {
        Self {
            mem_pool: Arc::clone(&self.mem_pool),
            c_key: self.c_key,
            meta_page_id: self.meta_page_id,
            meta_frame_id: AtomicU32::new(
                self.meta_frame_id
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
            bucket_count: self.bucket_count,
            bucket_entries: self.bucket_entries.clone(),
        }
    }
}

impl<T: MemPool + 'static> MvccIndex<T> for ChainedHashTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = AccessMethodError;

    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
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

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        // self.insert(key, pkey, ts, tx_id, value)
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        ChainedHashTable::insert(self, &entry)
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        match ChainedHashTable::get(self, key.as_ref(), pkey.as_ref(), &ts) {
            Ok(entry) => Ok(Some(entry.value().to_vec())),
            Err(AccessMethodError::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        match ChainedHashTable::get(self, key.as_ref(), pkey.as_ref(), &ts) {
            Ok(entry) => Ok(Some(entry.value().to_vec())),
            Err(AccessMethodError::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
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
        ChainedHashTable::update(self, entry.key(), entry.pkey(), &entry)
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
        ChainedHashTable::update(self, entry.key(), entry.pkey(), &entry)
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), Self::Error> {
        ChainedHashTable::delete(self, key.as_ref(), pkey.as_ref(), &ts)
    }

    // fn scan(
    //     &self,
    //     ts: Timestamp,
    // ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    // {
    //     let chained_scanner = ChainedHashTable::scan(&Arc::new(self.clone()), ts)?; // This returns `ChainedHashTableScanner`, which yields MvccEntry
    //     let iter = chained_scanner.map(|entry| {
    //         (
    //             entry.key().to_vec(),
    //             entry.pkey().to_vec(),
    //             entry.value().to_vec(),
    //         )
    //     });
    //     Ok(Box::new(iter))
    // }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let mut results = Vec::new();
        ChainedHashTable::scan_into_vec(self, &ts, &mut results)?;
        let iter = results.into_iter().map(|entry| {
            (
                entry.key().to_vec(),
                entry.pkey().to_vec(),
                entry.value().to_vec(),
            )
        });
        Ok(Box::new(iter))
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let chained_scanner = ChainedHashTable::scan_key(&Arc::new(self.clone()), key, ts)?;
        let iter = chained_scanner.map(|entry| (entry.pkey().to_vec(), entry.value().to_vec()));
        Ok(Box::new(iter))
    }

    fn scan_read_repair(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        self.scan(ts)
    }

    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        Ok(
            ChainedHashTable::scan_key_vec(&Arc::new(self.clone()), key, ts)
                .into_iter()
                .map(|entry| (entry.pkey().to_vec(), entry.value().to_vec()))
                .collect(),
        )
    }

    fn scan_key_vec_read_repair(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        self.scan_key_vec(key, ts)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        let mut map = BTreeMap::<Vec<u8>, (Vec<u8>, Delta<Vec<u8>>)>::new();
        let to = self.scan(to_ts)?;
        for entry in to {
            map.insert(entry.1, (entry.0, Delta::Inserted(entry.2)));
        }

        let from = self.scan(from_ts)?;
        for entry in from {
            log_warn!(
                "from ts : {} get entry: {:?}",
                from_ts,
                String::from_utf8(entry.0.clone())
            );
            let e = map.get_mut(&entry.1);
            if let Some(map_entry) = e {
                if map_entry.1.get_value().unwrap() == &entry.2 {
                    map.remove(&entry.1);
                } else {
                    map_entry.1 = Delta::Updated(map_entry.1.get_value().unwrap().to_vec());
                }
            } else {
                map.insert(entry.1, (entry.0, Delta::Deleted));
            }
        }
        Ok(Box::new(map.into_iter().map(|(pk, kv)| (kv.0, pk, kv.1))))
    }

    fn delta_scan_read_repair(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        self.delta_scan(from_ts, to_ts)
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        self.garbage_collect(&safe_ts)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        Ok(Box::new(ChainedHashTableScanner::new_full_scan(&Arc::new(
            self.clone(),
        ))))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn split_at_ts(&self, ts: Timestamp) -> Result<(), Self::Error> {
        Ok(())
    }
}

pub struct ChainedHashTableScanner<T: MemPool> {
    table: Arc<ChainedHashTable<T>>,
    ts: Timestamp,
    filter_by_ts: bool,
    filter_by_key: Option<Vec<u8>>,

    // Current index into the top-level (first) buckets
    current_first_bucket_idx: usize,
    // Current index into the second-level buckets
    current_second_bucket_idx: usize,

    // State of scanning: we scan "recent" chain first, then history chain.
    scanning_recent: bool,

    // The currently active recent-chain scanner
    recent_scanner: Option<ChainedHashRecentChainScanner<T>>,
    // The currently active history-chain scanner
    history_scanner: Option<ChainedHashHistoryChainScanner<T>>,
}

impl<T: MemPool> ChainedHashTableScanner<T> {
    /// Creates a new scanner for the entire table, optionally filtering by `ts`.
    /// For example, you might want only entries where `entry.start_ts <= ts < entry.end_ts`.
    pub fn new(table: &Arc<ChainedHashTable<T>>, ts: Timestamp) -> Self {
        ChainedHashTableScanner {
            table: table.clone(),
            ts,
            filter_by_ts: true,
            filter_by_key: None,
            current_first_bucket_idx: 0,
            current_second_bucket_idx: 0,
            scanning_recent: true,
            recent_scanner: None,
            history_scanner: None,
        }
    }

    pub fn new_full_scan(table: &Arc<ChainedHashTable<T>>) -> Self {
        ChainedHashTableScanner {
            table: table.clone(),
            ts: 0,
            filter_by_ts: false,
            filter_by_key: None,
            current_first_bucket_idx: 0,
            current_second_bucket_idx: 0,
            scanning_recent: true,
            recent_scanner: None,
            history_scanner: None,
        }
    }

    pub fn new_scan_key(table: &Arc<ChainedHashTable<T>>, key: &[u8], ts: Timestamp) -> Self {
        let mut scanner = ChainedHashTableScanner {
            table: table.clone(),
            ts,
            filter_by_ts: true,
            filter_by_key: Some(key.to_vec()),
            current_first_bucket_idx: 0,
            current_second_bucket_idx: 0,
            scanning_recent: true,
            recent_scanner: None,
            history_scanner: None,
        };

        let idx = scanner.table.get_bucket_index(key);
        scanner.current_first_bucket_idx = idx;

        scanner
    }

    /// Move to the next bucket pair (recent + history).
    /// Returns `true` if we successfully move to a valid bucket, `false` if we’re out of buckets.
    fn advance_to_next_bucket(&mut self) -> bool {
        // Increment second-level bucket index
        self.current_second_bucket_idx += 1;

        let current_first_bucket = &self.table.bucket_entries(self.current_first_bucket_idx);
        if self.current_second_bucket_idx >= current_first_bucket.bucket_count() {
            // If we are scanning for a single key, do NOT advance to the next first bucket.
            if self.filter_by_key.is_some() {
                // That means we are done scanning, because there's only one bucket for that key.
                return false;
            }
            // Move to the next first-level bucket
            self.current_first_bucket_idx += 1;
            self.current_second_bucket_idx = 0;
        }

        // Check if we ran out of first buckets
        if self.current_first_bucket_idx >= self.table.bucket_count() {
            // No more buckets
            return false;
        }

        // Reset scanning phase to "recent" for the new bucket
        self.scanning_recent = true;
        self.recent_scanner = None;
        self.history_scanner = None;
        true
    }

    /// Attempt to initialize a RecentChain scanner or a HistoryChain scanner for the current bucket.
    /// If we’re scanning the “recent” chain, we instantiate `MvccHashJoinRecentChainScanner`.
    /// Otherwise, if we’re scanning “history”, we instantiate `MvccHashJoinHistoryChainScanner`.
    fn initialize_current_chain_scanner(&mut self) {
        let first_bucket = &self.table.bucket_entries(self.current_first_bucket_idx);

        if self.scanning_recent {
            // Create a RecentChain scanner
            let scanner = if self.filter_by_ts {
                ChainedHashRecentChainScanner::new(
                    &first_bucket.recent_chain(self.current_second_bucket_idx),
                    self.ts,
                )
            } else {
                ChainedHashRecentChainScanner::new_full_scan(
                    &first_bucket.recent_chain(self.current_second_bucket_idx),
                )
            };
            self.recent_scanner = Some(scanner);
        } else {
            // Create a HistoryChain scanner
            // If you want to filter by timestamp, use `new(&chain, ts)`.
            // If you want a full scan, use `new_full_scan(&chain)`.
            let scanner = if self.filter_by_ts {
                ChainedHashHistoryChainScanner::new(
                    &first_bucket.history_chain(self.current_second_bucket_idx),
                    self.ts,
                )
            } else {
                ChainedHashHistoryChainScanner::new_full_scan(
                    &first_bucket.history_chain(self.current_second_bucket_idx),
                )
            };
            self.history_scanner = Some(scanner);
        }
    }
}

impl<T: MemPool> Iterator for ChainedHashTableScanner<T> {
    type Item = MvccEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we've exhausted all first buckets, we are done.
            if self.current_first_bucket_idx >= self.table.bucket_count() {
                return None;
            }

            // If we have no current second bucket or are out of range, move to the next bucket.
            let current_first_bucket = &self.table.bucket_entries(self.current_first_bucket_idx);
            if self.current_second_bucket_idx >= current_first_bucket.bucket_count() {
                if !self.advance_to_next_bucket() {
                    return None;
                }
                continue;
            }

            // Initialize the appropriate chain scanner if needed.
            if self.scanning_recent {
                if self.recent_scanner.is_none() {
                    self.initialize_current_chain_scanner();
                }
                // Attempt to fetch next from the recent chain.
                if let Some(ref mut scanner) = self.recent_scanner {
                    if let Some(entry) = scanner.next() {
                        // if we are filtering by key, check if the key matches
                        if let Some(ref key) = self.filter_by_key {
                            if entry.key() != key {
                                continue;
                            }
                        }
                        return Some(entry);
                    }
                }
                // If we got here, the recent chain is exhausted, so switch to history chain.
                self.scanning_recent = false;
                self.recent_scanner = None;
                // Move on to loop again to pick up from the history chain.
            } else {
                // History chain:
                if self.history_scanner.is_none() {
                    self.initialize_current_chain_scanner();
                }
                if let Some(ref mut scanner) = self.history_scanner {
                    if let Some(entry) = scanner.next() {
                        // if we are filtering by key, check if the key matches
                        if let Some(ref key) = self.filter_by_key {
                            if entry.key() != key {
                                continue;
                            }
                        }
                        return Some(entry);
                    }
                }
                // History chain exhausted, advance to the next bucket.
                self.history_scanner = None;
                if !self.advance_to_next_bucket() {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::{get_in_mem_pool, InMemPool};

    fn test_basic_index_ops<I>(index: &I) -> Result<(), I::Error>
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        // 1) Insert some entries (key, pkey, ts, tx_id, value)
        index.insert(
            b"key1".to_vec(),
            b"pkey1".to_vec(),
            100,
            1,
            b"value1".to_vec(),
        )?;
        index.insert(
            b"key2".to_vec(),
            b"pkey2".to_vec(),
            100,
            1,
            b"value2".to_vec(),
        )?;
        index.insert(
            b"key1".to_vec(),
            b"pkey3".to_vec(),
            150,
            1,
            b"value3".to_vec(),
        )?;

        // 2) Get an entry at a specific timestamp
        let got = index.get(b"key1", b"pkey1", 100)?;
        assert_eq!(
            got,
            Some(b"value1".to_vec()),
            "Should find value1 at ts=100"
        );

        // 3) Update an existing entry
        index.update(
            b"key1".to_vec(),
            b"pkey3".to_vec(),
            150,
            2, // new transaction ID
            b"value3_updated".to_vec(),
        )?;

        // 4) Delete an entry
        index.delete(b"key2", b"pkey2", 100, 2)?;

        // 5) Now scan at ts=200
        let mut scan_iter = index.scan(200)?;
        let mut scanned = Vec::new();
        while let Some((key, pkey, value)) = scan_iter.next() {
            println!("Scanned: key={:?}, pkey={:?}, value={:?}", key, pkey, value);
            scanned.push((key, pkey, value));
        }

        // 6) Verify we see "value1" for key1/pkey1, "value3_updated" for key1/pkey3,
        //    and do *not* see key2/pkey2.
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey1" && v == b"value1"),
            "Should still have key1/pkey1/value1"
        );
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey3" && v == b"value3_updated"),
            "Should see updated value3 for key1/pkey3"
        );
        assert!(
            !scanned
                .iter()
                .any(|(k, pk, _)| k == b"key2" && pk == b"pkey2"),
            "Deleted key2/pkey2 should not appear at ts=200"
        );

        // Done
        Ok(())
    }

    #[test]
    fn test_chained_hash_table() -> Result<(), AccessMethodError> {
        // 1) Create your mem_pool (adjust for your environment)
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(1, 1);

        // 2) Build a ChainedHashTable<InMemPool> using the trait’s `create` method
        let index =
            <ChainedHashTable<InMemPool> as MvccIndex<InMemPool>>::create(c_key, mem_pool.clone())?;

        // 3) Pass the index to the generic test function
        test_basic_index_ops(&index)
    }
}
