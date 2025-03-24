use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::hash_join_page::ChainedHashMetaPage,
    mvcc_index::{Delta, MvccEntry, MvccIndex},
    page::{Page, PageId},
    prelude::AccessMethodError,
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

    fn get_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        self.get_key(key, ts)
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

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), Self::Error> {
        ChainedHashTable::delete(self, key.as_ref(), pkey.as_ref(), &ts)
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let chained_scanner = ChainedHashTable::scan(&Arc::new(self.clone()), ts)?; // This returns `ChainedHashTableScanner`, which yields MvccEntry
        let iter = chained_scanner.map(|entry| {
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

    fn delta_scan(
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

    //     #[test]
    //     fn test_meta_page_init() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = 10;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
    //         let stored_num_buckets = page.get_bucket_num();
    //         assert_eq!(stored_num_buckets, num_buckets);
    //     }

    //     #[test]
    //     fn test_meta_page_set_and_get_bucket_num() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = 15;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
    //         page.set_bucket_num(num_buckets);
    //         let stored_num_buckets = page.get_bucket_num();
    //         assert_eq!(stored_num_buckets, num_buckets);
    //     }

    //     #[test]
    //     fn test_meta_page_set_and_get_bucket_entry() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = 5;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

    //         for index in 0..num_buckets {
    //             let entry = BucketEntry {
    //                 first_recent_pid: index as u32 + 100,
    //                 first_history_pid: index as u32 + 200,
    //             };
    //             page.set_bucket_entry(index, &entry);
    //         }

    //         for index in 0..num_buckets {
    //             let entry = page.get_bucket_entry(index);
    //             assert_eq!(entry.first_recent_pid, index as u32 + 100);
    //             assert_eq!(entry.first_history_pid, index as u32 + 200);
    //         }
    //     }

    //     #[test]
    //     fn test_meta_page_read_and_write_all_entries() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = 8;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

    //         let mut entries = Vec::new();
    //         for index in 0..num_buckets {
    //             let entry = BucketEntry {
    //                 first_recent_pid: index as u32 + 500,
    //                 first_history_pid: index as u32 + 600,
    //             };
    //             entries.push(entry);
    //         }

    //         // Write all entries
    //         page.write_all_entries(&entries);

    //         // Read all entries
    //         let read_entries = page.read_all_entries();

    //         assert_eq!(entries, read_entries);
    //     }

    //     #[test]
    //     #[should_panic(expected = "Bucket index out of bounds")]
    //     fn test_meta_page_get_bucket_entry_out_of_bounds() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = 3;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

    //         // This should panic because index is equal to num_buckets
    //         let _entry = page.get_bucket_entry(num_buckets);
    //     }

    //     #[test]
    //     #[should_panic(expected = "Page size is insufficient for the number of buckets")]
    //     fn test_meta_page_init_too_many_buckets() {
    //         let mut page = Page::new_empty();
    //         let num_buckets = (AVAILABLE_PAGE_SIZE - BUCKET_NUM_SIZE) / BUCKET_ENTRY_SIZE + 1;
    //         <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
    //     }

    //     #[test]
    //     fn test_hash_join_table_insert_and_get_multiple_records() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(1, 1);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert multiple records
    //         let num_records = 100;
    //         for i in 0..num_records {
    //             let key = format!("key{}", i).into_bytes();
    //             let pkey = format!("pkey{}", i).into_bytes();
    //             let val = format!("value{}", i).into_bytes();
    //             let ts = 100 + i as u64;
    //             hash_table
    //                 .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
    //                 .unwrap();

    //             // Retrieve the record immediately
    //             let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
    //             assert_eq!(retrieved_val, val);
    //         }

    //         // Verify that all records are retrievable
    //         for i in 0..num_records {
    //             let key = format!("key{}", i).into_bytes();
    //             let pkey = format!("pkey{}", i).into_bytes();
    //             let val = format!("value{}", i).into_bytes();
    //             let ts = 100 + i as u64;
    //             let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
    //             assert_eq!(retrieved_val, val);
    //         }
    //     }

    //     #[test]
    //     fn test_hash_join_table_update_records() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(2, 2);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert and update records
    //         let key = b"key_update".to_vec();
    //         let pkey = b"pkey_update".to_vec();
    //         let val1 = b"value1".to_vec();
    //         let val2 = b"value2".to_vec();
    //         let ts_insert = 100;
    //         let ts_update = 200;

    //         // Insert the record
    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, 0, val1.clone())
    //             .unwrap();

    //         // Update the record
    //         hash_table
    //             .update(key.clone(), pkey.clone(), ts_update, 0, val2.clone())
    //             .unwrap();

    //         // Retrieve the updated record
    //         let retrieved_val = hash_table.get(&key, &pkey, ts_update).unwrap();
    //         assert_eq!(retrieved_val, val2);

    //         // Retrieve the old record from history
    //         let retrieved_val = hash_table.get(&key, &pkey, ts_insert + 50).unwrap();
    //         assert_eq!(retrieved_val, val1);
    //     }

    //     #[test]
    //     fn test_hash_join_table_insert_large_values() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(3, 3);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Generate a large value close to page size
    //         let large_val = vec![b'x'; (AVAILABLE_PAGE_SIZE / 2) as usize];

    //         let key = b"key_large".to_vec();
    //         let pkey = b"pkey_large".to_vec();
    //         let ts = 100;

    //         // Insert the record
    //         let result = hash_table.insert(key.clone(), pkey.clone(), ts, 0, large_val.clone());
    //         assert!(result.is_ok());

    //         // Retrieve the record
    //         let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
    //         assert_eq!(retrieved_val, large_val);
    //     }

    //     #[test]
    //     fn test_hash_join_table_insert_oversized_value() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(4, 4);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Generate an oversized value exceeding page size
    //         let oversized_val = vec![b'x'; (AVAILABLE_PAGE_SIZE + 1) as usize];

    //         let key = b"key_oversized".to_vec();
    //         let pkey = b"pkey_oversized".to_vec();
    //         let ts = 100;

    //         // Attempt to insert the oversized record
    //         let result = hash_table.insert(key.clone(), pkey.clone(), ts, 0, oversized_val);
    //         assert!(matches!(result, Err(AccessMethodError::RecordTooLarge)));
    //     }

    //     #[test]
    //     fn test_hash_join_table_empty_key_and_value() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(5, 5);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let key = b"".to_vec();
    //         let pkey = b"pkey_empty".to_vec();
    //         let val = b"".to_vec();
    //         let ts = 100;

    //         // Insert the record with empty key and value
    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
    //             .unwrap();

    //         // Retrieve the record
    //         let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
    //         assert_eq!(retrieved_val, val);
    //     }

    //     #[test]
    //     fn test_hash_join_table_bucket_distribution() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(6, 6);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let num_records = 10000;
    //         let mut bucket_counts = vec![0; num_buckets];

    //         // Insert records and count bucket distribution
    //         for i in 0..num_records {
    //             let key = format!("key{}", i).into_bytes();
    //             let pkey = format!("pkey{}", i).into_bytes();
    //             let val = format!("value{}", i).into_bytes();
    //             let ts = 100 + i as u64;

    //             let index = hash_table.get_bucket_index(&key);
    //             bucket_counts[index] += 1;

    //             hash_table
    //                 .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
    //                 .unwrap();
    //         }

    //         // Check that records are distributed across buckets
    //         for (i, count) in bucket_counts.iter().enumerate() {
    //             println!("Bucket {}: {} records", i, count);
    //             assert!(*count > 0);
    //         }
    //     }

    //     #[test]
    //     fn test_hash_join_table_delete_non_existent_after_deletion() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(9, 9);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let key = b"key_test".to_vec();
    //         let pkey = b"pkey_test".to_vec();
    //         let val = b"value_test".to_vec();
    //         let ts_insert = 100;
    //         let ts_delete = 200;

    //         // Insert the record
    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
    //             .unwrap();

    //         // Delete the record
    //         hash_table.delete(&key, &pkey, ts_delete, 0).unwrap();

    //         // Attempt to delete again
    //         let result = hash_table.delete(&key, &pkey, ts_delete + 50, 0);
    //         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    //     }

    //     #[test]
    //     fn test_hash_join_table_update_non_existent_record() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(10, 10);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let key = b"key_nonexistent".to_vec();
    //         let pkey = b"pkey_nonexistent".to_vec();
    //         let val = b"value".to_vec();
    //         let ts = 100;

    //         // Attempt to update a non-existent record
    //         let result = hash_table.update(key.clone(), pkey.clone(), ts, 0, val.clone());
    //         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    //     }

    //     #[test]
    //     fn test_hash_join_table_get_with_future_timestamp() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(11, 11);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let key = b"key_future".to_vec();
    //         let pkey = b"pkey_future".to_vec();
    //         let val = b"value_future".to_vec();
    //         let ts_insert = 100;
    //         let ts_future = 1_000_000;

    //         // Insert the record
    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
    //             .unwrap();

    //         // Attempt to get the record with a future timestamp
    //         let retrieved_val = hash_table.get(&key, &pkey, ts_future).unwrap();
    //         assert_eq!(retrieved_val, val);
    //     }

    //     #[test]
    //     fn test_hash_join_table_get_with_past_timestamp() {
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(12, 12);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let key = b"key_past".to_vec();
    //         let pkey = b"pkey_past".to_vec();
    //         let val = b"value_past".to_vec();
    //         let ts_insert = 100;
    //         let ts_past = 50;

    //         // Insert the record
    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
    //             .unwrap();

    //         // Attempt to get the record with a past timestamp
    //         let result = hash_table.get(&key, &pkey, ts_past);
    //         assert!(matches!(
    //             result,
    //             Err(AccessMethodError::KeyFoundButInvalidTimestamp)
    //                 | Err(AccessMethodError::KeyNotFound)
    //         ));
    //     }

    //     #[test]
    //     fn test_hash_join_table_random_operations_no_duplicate_pkeys() {
    //         use rand::prelude::*;
    //         use std::collections::HashMap;

    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(13, 13);
    //         let num_buckets = 16;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Define the number of operations
    //         let num_operations = 10_000;

    //         // Define possible operations
    //         enum Operation {
    //             Insert,
    //             Get,
    //             Update,
    //             Delete,
    //         }

    //         // Create a random number generator
    //         let mut rng = rand::thread_rng();

    //         // HashMap to keep track of the expected state
    //         // Key: (key, pkey), Value: (ts, value)
    //         let mut expected_state: HashMap<(Vec<u8>, Vec<u8>), (Timestamp, Vec<u8>)> = HashMap::new();

    //         // Set to keep track of inserted pkeys to avoid duplicates
    //         let mut inserted_pkeys: std::collections::HashSet<Vec<u8>> =
    //             std::collections::HashSet::new();

    //         // Possible keys, pkeys, and values
    //         let keys: Vec<Vec<u8>> = (0..100).map(|i| format!("key{}", i).into_bytes()).collect();
    //         let pkeys: Vec<Vec<u8>> = (0..1000) // Increase the range to have enough unique pkeys
    //             .map(|i| format!("pkey{}", i).into_bytes())
    //             .collect();
    //         let values: Vec<Vec<u8>> = (0..100)
    //             .map(|i| format!("value{}", i).into_bytes())
    //             .collect();

    //         // Perform random operations
    //         for _ in 0..num_operations {
    //             let op = match rng.gen_range(0..4) {
    //                 0 => Operation::Insert,
    //                 1 => Operation::Get,
    //                 2 => Operation::Update,
    //                 3 => Operation::Delete,
    //                 _ => unreachable!(),
    //             };

    //             // Randomly select key, pkey, value, and timestamp
    //             let key = keys.choose(&mut rng).unwrap().clone();
    //             let pkey = pkeys.choose(&mut rng).unwrap().clone();
    //             let value = values.choose(&mut rng).unwrap().clone();
    //             let ts: Timestamp = rng.gen_range(1..1_000_000);

    //             match op {
    //                 Operation::Insert => {
    //                     // Insert operation
    //                     if inserted_pkeys.contains(&pkey) {
    //                         // Skip insertion if pkey already exists
    //                         continue;
    //                     }
    //                     let res = hash_table.insert(key.clone(), pkey.clone(), ts, 0, value.clone());
    //                     if res.is_ok() {
    //                         expected_state.insert((key.clone(), pkey.clone()), (ts, value.clone()));
    //                         inserted_pkeys.insert(pkey.clone());
    //                     } else {
    //                         assert!(matches!(
    //                             res,
    //                             Err(AccessMethodError::OutOfSpace)
    //                                 | Err(AccessMethodError::RecordTooLarge)
    //                         ));
    //                     }
    //                 }
    //                 Operation::Get => {
    //                     // Get operation
    //                     let res = hash_table.get(&key, &pkey, ts);
    //                     match expected_state.get(&(key.clone(), pkey.clone())) {
    //                         Some(&(stored_ts, ref stored_value)) if stored_ts <= ts => {
    //                             // The entry should be retrievable
    //                             let retrieved_value = res.unwrap();
    //                             assert_eq!(&retrieved_value, stored_value);
    //                         }
    //                         _ => {
    //                             // The entry should not be found
    //                             assert!(matches!(
    //                                 res,
    //                                 Err(AccessMethodError::KeyNotFound)
    //                                     | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
    //                             ));
    //                         }
    //                     }
    //                 }
    //                 Operation::Update => {
    //                     // Update operation
    //                     if !inserted_pkeys.contains(&pkey) {
    //                         // Skip update if pkey does not exist
    //                         continue;
    //                     }
    //                     let res = hash_table.update(key.clone(), pkey.clone(), ts, 0, value.clone());
    //                     match expected_state.get_mut(&(key.clone(), pkey.clone())) {
    //                         Some((stored_ts, stored_value)) if *stored_ts <= ts => {
    //                             // The update should succeed
    //                             // let (old_ts, old_val) = res.unwrap();
    //                             // assert_eq!(old_ts, *stored_ts);
    //                             // assert_eq!(old_val, stored_value.clone());
    //                             // *stored_ts = ts;
    //                             // *stored_value = value.clone();
    //                         }
    //                         _ => {
    //                             // The update should fail
    //                             assert!(matches!(
    //                                 res,
    //                                 Err(AccessMethodError::KeyFoundButInvalidTimestamp)
    //                                     | Err(AccessMethodError::KeyNotFound)
    //                             ));
    //                         }
    //                     }
    //                 }
    //                 Operation::Delete => {
    //                     // Delete operation
    //                     if !inserted_pkeys.contains(&pkey) {
    //                         // Skip deletion if pkey does not exist
    //                         continue;
    //                     }
    //                     let res = hash_table.delete(&key, &pkey, ts, 0);
    //                     match expected_state.remove(&(key.clone(), pkey.clone())) {
    //                         Some((stored_ts, stored_value)) if stored_ts <= ts => {
    //                             // The deletion should succeed
    //                             // let (old_ts, old_val) = res.unwrap();
    //                             // assert_eq!(old_ts, stored_ts);
    //                             // assert_eq!(old_val, stored_value);
    //                             // inserted_pkeys.remove(&pkey);
    //                         }
    //                         Some((stored_ts, stored_value)) => {
    //                             // The deletion should fail due to invalid timestamp
    //                             expected_state
    //                                 .insert((key.clone(), pkey.clone()), (stored_ts, stored_value));
    //                             assert!(matches!(
    //                                 res,
    //                                 Err(AccessMethodError::KeyFoundButInvalidTimestamp)
    //                             ));
    //                         }
    //                         None => {
    //                             // This should not happen as we checked inserted_pkeys
    //                             // panic!("Expected pkey to exist in expected_state");
    //                         }
    //                     }
    //                 }
    //             }
    //         }

    //         // After all operations, verify the final state
    //         for ((key, pkey), (stored_ts, stored_value)) in &expected_state {
    //             let res = hash_table.get(key, pkey, *stored_ts);
    //             if res.is_err() {
    //                 println!("Error for key: {:?}, pkey: {:?}", key, pkey);
    //                 println!("stored_ts: {}, stored_value: {:?}", stored_ts, stored_value);
    //             }
    //             let retrieved_value = res.unwrap();
    //             assert_eq!(&retrieved_value, stored_value);
    //         }

    //         // Optionally, perform additional verification for timestamps beyond the stored timestamp
    //         for ((key, pkey), (stored_ts, stored_value)) in &expected_state {
    //             let ts_future = stored_ts + 1000;
    //             let res = hash_table.get(key, pkey, ts_future);
    //             let retrieved_value = res.unwrap();
    //             assert_eq!(&retrieved_value, stored_value);
    //         }

    //         // Verify that entries are not retrievable with timestamps before they were inserted
    //         for ((key, pkey), (stored_ts, _)) in &expected_state {
    //             let ts_past = if *stored_ts > 1 { stored_ts - 1 } else { 0 };
    //             let res = hash_table.get(key, pkey, ts_past);
    //             assert!(matches!(
    //                 res,
    //                 Err(AccessMethodError::KeyNotFound)
    //                     | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
    //             ));
    //         }
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_basic() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(100, 100);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert entries
    //         let entries = vec![
    //             (
    //                 b"key1".to_vec(),
    //                 b"pkey1".to_vec(),
    //                 b"value1".to_vec(),
    //                 10u64,
    //             ),
    //             (
    //                 b"key2".to_vec(),
    //                 b"pkey2".to_vec(),
    //                 b"value2".to_vec(),
    //                 20u64,
    //             ),
    //             (
    //                 b"key3".to_vec(),
    //                 b"pkey3".to_vec(),
    //                 b"value3".to_vec(),
    //                 30u64,
    //             ),
    //         ];

    //         let tx_id = 1;

    //         for (key, pkey, value, ts) in &entries {
    //             hash_table
    //                 .insert(key.clone(), pkey.clone(), *ts, tx_id, value.clone())
    //                 .unwrap();
    //         }

    //         // Scan the table at timestamp after insertions
    //         let scan_ts = 40u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let mut results: Vec<_> = scanner.collect();

    //         // Verify that all entries are returned
    //         assert_eq!(
    //             results.len(),
    //             entries.len(),
    //             "Expected {} entries",
    //             entries.len()
    //         );

    //         // Create a map for easier verification
    //         let mut result_map = std::collections::HashMap::new();
    //         for entry in results {
    //             result_map.insert((entry.key.clone(), entry.pkey.clone()), entry);
    //         }

    //         for (key, pkey, value, ts) in &entries {
    //             let entry = result_map.get(&(key.clone(), pkey.clone())).unwrap();
    //             assert_eq!(&entry.value, value);
    //             assert_eq!(entry.start_ts, *ts);
    //             assert_eq!(entry.end_ts, u64::MAX);
    //         }
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_after_updates() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(101, 101);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert an entry
    //         let key = b"key1".to_vec();
    //         let pkey = b"pkey1".to_vec();
    //         let value1 = b"value1".to_vec();
    //         let ts_insert = 10u64;
    //         let tx_id = 1;

    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, tx_id, value1.clone())
    //             .unwrap();

    //         // Update the entry
    //         let value2 = b"value2".to_vec();
    //         let ts_update = 20u64;

    //         hash_table
    //             .update(key.clone(), pkey.clone(), ts_update, tx_id, value2.clone())
    //             .unwrap();

    //         // Scan at timestamp after update
    //         let scan_ts = 30u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that the scanner returns the updated entry
    //         assert_eq!(results.len(), 1, "Expected 1 entry");

    //         let entry = &results[0];
    //         assert_eq!(entry.key, key);
    //         assert_eq!(entry.pkey, pkey);
    //         assert_eq!(entry.value, value2);
    //         assert_eq!(entry.start_ts, ts_update);
    //         assert_eq!(entry.end_ts, u64::MAX);

    //         // Scan at timestamp before update
    //         let scan_ts = 15u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that the scanner returns the original entry
    //         assert_eq!(results.len(), 1, "Expected 1 entry");

    //         let entry = &results[0];
    //         assert_eq!(entry.key, key);
    //         assert_eq!(entry.pkey, pkey);
    //         assert_eq!(entry.value, value1);
    //         assert_eq!(entry.start_ts, ts_insert);
    //         assert_eq!(entry.end_ts, ts_update);
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_after_deletions() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(102, 102);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert an entry
    //         let key = b"key1".to_vec();
    //         let pkey = b"pkey1".to_vec();
    //         let value = b"value1".to_vec();
    //         let ts_insert = 10u64;
    //         let tx_id = 1;

    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, tx_id, value.clone())
    //             .unwrap();

    //         // Delete the entry
    //         let ts_delete = 20u64;
    //         hash_table.delete(&key, &pkey, ts_delete, tx_id).unwrap();

    //         // Scan at timestamp after deletion
    //         let scan_ts = 30u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that no entries are returned
    //         assert_eq!(results.len(), 0, "Expected no entries");

    //         // Scan at timestamp before deletion
    //         let scan_ts = 15u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that the entry is returned
    //         assert_eq!(results.len(), 1, "Expected 1 entry");

    //         let entry = &results[0];
    //         assert_eq!(entry.key, key);
    //         assert_eq!(entry.pkey, pkey);
    //         assert_eq!(entry.value, value);
    //         assert_eq!(entry.start_ts, ts_insert);
    //         assert_eq!(entry.end_ts, ts_delete);
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_recent_and_history() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(103, 103);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Insert entries
    //         let entries = vec![
    //             // Entry that will be updated
    //             (
    //                 b"key1".to_vec(),
    //                 b"pkey1".to_vec(),
    //                 b"value1".to_vec(),
    //                 10u64,
    //             ),
    //             // Entry that will remain in recent chain
    //             (
    //                 b"key2".to_vec(),
    //                 b"pkey2".to_vec(),
    //                 b"value2".to_vec(),
    //                 15u64,
    //             ),
    //         ];

    //         let tx_id = 1;

    //         // Insert entries
    //         for (key, pkey, value, ts) in &entries {
    //             hash_table
    //                 .insert(key.clone(), pkey.clone(), *ts, tx_id, value.clone())
    //                 .unwrap();
    //         }

    //         // Update one entry
    //         let key_to_update = b"key1".to_vec();
    //         let pkey_to_update = b"pkey1".to_vec();
    //         let value_updated = b"value1_updated".to_vec();
    //         let ts_update = 20u64;

    //         hash_table
    //             .update(
    //                 key_to_update.clone(),
    //                 pkey_to_update.clone(),
    //                 ts_update,
    //                 tx_id,
    //                 value_updated.clone(),
    //             )
    //             .unwrap();

    //         // Scan at timestamp after update
    //         let scan_ts = 25u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let mut results: Vec<_> = scanner.collect();

    //         // Verify that both entries are returned
    //         assert_eq!(results.len(), 2, "Expected 2 entries");

    //         // Create a map for easier verification
    //         let mut result_map = std::collections::HashMap::new();
    //         for entry in results {
    //             result_map.insert((entry.key.clone(), entry.pkey.clone()), entry);
    //         }

    //         // Check updated entry
    //         let entry = result_map
    //             .get(&(key_to_update.clone(), pkey_to_update.clone()))
    //             .unwrap();
    //         assert_eq!(entry.value, value_updated);
    //         assert_eq!(entry.start_ts, ts_update);
    //         assert_eq!(entry.end_ts, u64::MAX);

    //         // Check the other entry
    //         let key_other = b"key2".to_vec();
    //         let pkey_other = b"pkey2".to_vec();
    //         let value_other = b"value2".to_vec();

    //         let entry = result_map
    //             .get(&(key_other.clone(), pkey_other.clone()))
    //             .unwrap();
    //         assert_eq!(entry.value, value_other);
    //         assert_eq!(entry.start_ts, 15u64);
    //         assert_eq!(entry.end_ts, u64::MAX);
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_empty_table() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(104, 104);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         // Scan the empty table
    //         let scan_ts = 10u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that no entries are returned
    //         assert_eq!(results.len(), 0, "Expected no entries");
    //     }

    //     #[test]
    //     fn test_hash_join_table_scanner_no_duplicates() {
    //         // Initialize mem_pool and container key
    //         let mem_pool = get_in_mem_pool();
    //         let c_key = ContainerKey::new(105, 105);
    //         let num_buckets = 8;
    //         let hash_table =
    //             MvccHashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //         let tx_id = 1;

    //         // Insert an entry
    //         let key = b"key1".to_vec();
    //         let pkey = b"pkey1".to_vec();
    //         let value1 = b"value1".to_vec();
    //         let ts_insert = 10u64;

    //         hash_table
    //             .insert(key.clone(), pkey.clone(), ts_insert, tx_id, value1.clone())
    //             .unwrap();

    //         // Update the entry multiple times
    //         let value2 = b"value2".to_vec();
    //         let ts_update1 = 20u64;

    //         hash_table
    //             .update(key.clone(), pkey.clone(), ts_update1, tx_id, value2.clone())
    //             .unwrap();

    //         let value3 = b"value3".to_vec();
    //         let ts_update2 = 30u64;

    //         hash_table
    //             .update(key.clone(), pkey.clone(), ts_update2, tx_id, value3.clone())
    //             .unwrap();

    //         // Scan at timestamp after updates
    //         let scan_ts = 40u64;
    //         let scanner = hash_table.scan(scan_ts).unwrap();
    //         let results: Vec<_> = scanner.collect();

    //         // Verify that only one entry is returned
    //         assert_eq!(results.len(), 1, "Expected 1 entry");

    //         let entry = &results[0];
    //         assert_eq!(entry.key, key);
    //         assert_eq!(entry.pkey, pkey);
    //         assert_eq!(entry.value, value3);
    //         assert_eq!(entry.start_ts, ts_update2);
    //         assert_eq!(entry.end_ts, u64::MAX);
    //     }
    #[test]
    fn test_scan_key_functionality() -> Result<(), AccessMethodError> {
        // Create a mem_pool and a container key.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(1, 1);
        let num_buckets = 8;
        // Wrap the table in an Arc since scan_key requires &Arc<Self>
        let table = Arc::new(ChainedHashTable::new_with_bucket_num(
            c_key,
            mem_pool,
            num_buckets,
        ));

        let tx_id = 1;

        // Insert entries:
        // Two entries with key "test_key" and one with a different key.
        let entry1 = MvccEntry::new_with_tx_id(
            b"test_key".to_vec(),
            b"pkey1".to_vec(),
            b"value1".to_vec(),
            100,      // timestamp
            u64::MAX, // end_ts
            tx_id,
        );
        let entry2 = MvccEntry::new_with_tx_id(
            b"test_key".to_vec(),
            b"pkey2".to_vec(),
            b"value2".to_vec(),
            200, // later timestamp
            u64::MAX,
            tx_id,
        );
        let entry3 = MvccEntry::new_with_tx_id(
            b"other_key".to_vec(),
            b"pkey3".to_vec(),
            b"value3".to_vec(),
            150,
            u64::MAX,
            tx_id,
        );

        table.insert(&entry1)?;
        table.insert(&entry2)?;
        table.insert(&entry3)?;

        // Now scan for entries with key "test_key" at timestamp 250.
        let scanner = table.scan_key(b"test_key", 250)?;
        let results: Vec<MvccEntry> = scanner.collect();

        // We expect only the entries for "test_key" to be returned.
        assert_eq!(results.len(), 2, "Expected two entries for 'test_key'");
        for entry in results.iter() {
            assert_eq!(entry.key(), b"test_key");
        }

        // Optionally, verify the order (if your implementation preserves insertion order)
        // Here we expect pkey1 (ts=100) to appear before pkey2 (ts=200)
        assert_eq!(results[0].pkey(), b"pkey1");
        assert_eq!(results[1].pkey(), b"pkey2");

        Ok(())
    }
}
