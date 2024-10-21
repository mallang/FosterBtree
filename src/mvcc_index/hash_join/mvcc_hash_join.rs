use crate::{
    access_method::fbt::FosterBtreeRangeScanner,
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_debug, log_trace, log_warn,
    mvcc_index::{Delta, MvccIndex},
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
    prelude::AccessMethodError,
};
use std::{
    collections::hash_map::DefaultHasher,
    error::Error,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use super::{
    mvcc_hash_join_history_chain::MvccHashJoinHistoryChain,
    mvcc_hash_join_history_page::MvccHashJoinHistoryPage,
    mvcc_hash_join_recent_chain::MvccHashJoinRecentChain,
    mvcc_hash_join_recent_page::MvccHashJoinRecentPage, Timestamp, TxId,
};

use rand::seq::index;
use serde::{Deserialize, Serialize};

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const BUCKET_ENTRY_SIZE: usize = std::mem::size_of::<BucketEntry>();
pub const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>(); // Size of bucket_num (u64)

pub const DEAFAULT_NUM_BUCKETS: usize = 16;

pub struct HashJoinTable<T: MemPool> {
    // txid: u64,
    // ts: Timestamp,
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    num_buckets: usize,
    bucket_entries: Vec<(
        Arc<MvccHashJoinRecentChain<T>>,
        Arc<MvccHashJoinHistoryChain<T>>,
    )>,
}

impl<T: MemPool> MvccIndex for HashJoinTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = AccessMethodError;
    type MemPoolType = T;
    type Iter = Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>;
    type DeltaIter = Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>;
    type ScanKeyIter = Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>;

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
        HashJoinTable::insert(self, key, pkey, ts, tx_id, value)
    }

    fn get(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        match HashJoinTable::get(self, key, pkey, ts) {
            Ok(val) => Ok(Some(val)),
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
        HashJoinTable::update(self, key, pkey, ts, tx_id, value)
    }

    fn delete(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
    ) -> Result<(), Self::Error> {
        self.delete(key, pkey, ts, tx_id)
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Self::Iter, Self::Error> {
        self.scan(ts)
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Self::ScanKeyIter, Self::Error> {
        self.scan_key(key, ts)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<Self::DeltaIter, Self::Error> {
        self.delta_scan(from_ts, to_ts)
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        self.garbage_collect(safe_ts)
    }
}

impl<T: MemPool> HashJoinTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_NUM_BUCKETS)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        MvccHashJoinMetaPage::init(&mut *meta_page, num_buckets);
        MvccHashJoinMetaPage::set_bucket_num(&mut *meta_page, num_buckets);

        let mut bucket_entries: Vec<(
            Arc<MvccHashJoinRecentChain<T>>,
            Arc<MvccHashJoinHistoryChain<T>>,
        )> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let recent_chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());
            let recent_page_id = recent_chain.first_page_id();
            // let recent_frame_id = recent_chain.first_frame_id();

            let history_chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());
            let history_page_id = history_chain.first_page_id();
            // let history_frame_id = history_chain.first_frame_id();

            let entry = BucketEntry::new(recent_page_id, history_page_id);
            MvccHashJoinMetaPage::set_bucket_entry(&mut *meta_page, i, &entry);
            bucket_entries.push((Arc::new(recent_chain), Arc::new(history_chain)));
        }
        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            num_buckets,
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
        Self::new_with_scanner_and_bucket_num(c_key, mem_pool, DEAFAULT_NUM_BUCKETS, scanner, tx_id)
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
            table.insert(key, pkey, ts, tx_id, val).unwrap();
        }
        table
    }

    /// Constructs a hash join table from an existing meta page.
    pub fn new_from_page(c_key: ContainerKey, mem_pool: Arc<T>, meta_pid: PageId) -> Self {
        let temp_table = HashJoinTable {
            mem_pool: mem_pool.clone(),
            c_key,
            meta_page_id: meta_pid,
            meta_frame_id: AtomicU32::new(0),
            num_buckets: 0,
            bucket_entries: Vec::new(),
        };

        let page_key = PageFrameKey::new(c_key, meta_pid);
        let meta_page = temp_table.read_page(page_key);

        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        let num_buckets = meta_page.get_bucket_num();

        let mut bucket_entries = Vec::with_capacity(num_buckets);

        for i in 0..num_buckets {
            let entry = meta_page.get_bucket_entry(i);

            let recent_chain = MvccHashJoinRecentChain::new_from_page(
                c_key,
                mem_pool.clone(),
                entry.first_recent_pid,
            );
            let history_chain = MvccHashJoinHistoryChain::new_from_page(
                c_key,
                mem_pool.clone(),
                entry.first_history_pid,
            );

            bucket_entries.push((Arc::new(recent_chain), Arc::new(history_chain)));
        }
        drop(meta_page);

        HashJoinTable {
            mem_pool,
            c_key,
            meta_page_id: meta_pid,
            meta_frame_id,
            num_buckets,
            bucket_entries,
        }
    }

    /// Inserts a key-value pair with new pkey into the hash join table.
    pub fn insert(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: Timestamp,
        _tx_id: TxId,
        val: Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(&key);
        let (recent_chian, _) = &self.bucket_entries[index];

        recent_chian.insert(&key, &pkey, ts, &val)
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let (recent_chain, history_chain) = &self.bucket_entries[index];

        let recent_val = recent_chain.get(key, pkey, ts);
        match recent_val {
            Ok(val) => Ok(val),
            Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
            Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_val = history_chain.get(key, pkey, ts);
                match history_val {
                    Ok(val) => Ok(val),
                    Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Updates an existing key-value pair in the hash join table.
    pub fn update(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: Timestamp,
        _tx_id: TxId,
        val: Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(&key);
        let (recent_chain, history_chain) = &self.bucket_entries[index];

        let old_result = recent_chain.update(&key, &pkey, ts, &val);
        match old_result {
            Ok((old_ts, old_val)) => history_chain.insert(&key, &pkey, old_ts, ts, &old_val),
            Err(e) => Err(e),
        }
    }

    /// Deletes a key-value pair from the hash join table.
    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(&key);
        let (recent_chain, history_chain) = &self.bucket_entries[index];

        let old_result = recent_chain.delete(&key, &pkey, ts);
        match old_result {
            Ok((old_ts, old_val)) => {
                // Insert the old record into the history chain
                history_chain.insert(&key, &pkey, old_ts, ts, &old_val)
            }
            Err(e) => Err(e),
        }
    }

    /// Flushes the in-memory bucket entries back to the meta page.
    fn flush_bucket_entries(&self) -> Result<(), AccessMethodError> {
        todo!()
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
        (hasher.finish() as usize) % self.num_buckets
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BucketEntry {
    first_recent_pid: PageId,
    first_history_pid: PageId,
}

impl BucketEntry {
    pub fn new(first_recent_pid: PageId, first_history_pid: PageId) -> Self {
        Self {
            first_recent_pid,
            first_history_pid,
        }
    }
}
pub trait MvccHashJoinMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);

    /// Retrieves the number of buckets from the meta page.
    fn get_bucket_num(&self) -> usize;

    /// Sets the number of buckets in the meta page.
    fn set_bucket_num(&mut self, num_buckets: usize);

    /// Gets the bucket entry at the specified index.
    fn get_bucket_entry(&self, index: usize) -> BucketEntry;

    /// Sets the bucket entry at the specified index.
    fn set_bucket_entry(&mut self, index: usize, entry: &BucketEntry);

    /// Reads all bucket entries from the meta page.
    fn read_all_entries(&self) -> Vec<BucketEntry>;

    /// Writes all bucket entries to the meta page.
    fn write_all_entries(&mut self, entries: &[BucketEntry]);
}

impl MvccHashJoinMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE + num_buckets * BUCKET_ENTRY_SIZE;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "Page size is insufficient for the number of buckets"
        );

        self.set_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn get_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn set_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn get_bucket_entry(&self, index: usize) -> BucketEntry {
        let num_buckets = self.get_bucket_num();
        assert!(index < num_buckets, "Bucket index out of bounds");

        let offset = BUCKET_NUM_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let first_recent_pid = PageId::from_be_bytes(bytes[0..PAGE_ID_SIZE].try_into().unwrap());
        let first_history_pid =
            PageId::from_be_bytes(bytes[PAGE_ID_SIZE..2 * PAGE_ID_SIZE].try_into().unwrap());
        BucketEntry {
            first_recent_pid,
            first_history_pid,
        }
    }

    fn set_bucket_entry(&mut self, index: usize, entry: &BucketEntry) {
        let num_buckets = self.get_bucket_num();
        assert!(index < num_buckets, "Bucket index out of bounds");

        let offset = BUCKET_NUM_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE].copy_from_slice(&entry.first_recent_pid.to_be_bytes());
        bytes[PAGE_ID_SIZE..2 * PAGE_ID_SIZE]
            .copy_from_slice(&entry.first_history_pid.to_be_bytes());
    }

    fn read_all_entries(&self) -> Vec<BucketEntry> {
        let num_buckets = self.get_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries(&mut self, entries: &[BucketEntry]) {
        let num_buckets = self.get_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_bucket_entry(index, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bp::get_in_mem_pool;

    use super::*;

    #[test]
    fn test_meta_page_init() {
        let mut page = Page::new_empty();
        let num_buckets = 10;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        let stored_num_buckets = page.get_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_num() {
        let mut page = Page::new_empty();
        let num_buckets = 15;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
        page.set_bucket_num(num_buckets);
        let stored_num_buckets = page.get_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_entry() {
        let mut page = Page::new_empty();
        let num_buckets = 5;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

        for index in 0..num_buckets {
            let entry = BucketEntry {
                first_recent_pid: index as u32 + 100,
                first_history_pid: index as u32 + 200,
            };
            page.set_bucket_entry(index, &entry);
        }

        for index in 0..num_buckets {
            let entry = page.get_bucket_entry(index);
            assert_eq!(entry.first_recent_pid, index as u32 + 100);
            assert_eq!(entry.first_history_pid, index as u32 + 200);
        }
    }

    #[test]
    fn test_meta_page_read_and_write_all_entries() {
        let mut page = Page::new_empty();
        let num_buckets = 8;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

        let mut entries = Vec::new();
        for index in 0..num_buckets {
            let entry = BucketEntry {
                first_recent_pid: index as u32 + 500,
                first_history_pid: index as u32 + 600,
            };
            entries.push(entry);
        }

        // Write all entries
        page.write_all_entries(&entries);

        // Read all entries
        let read_entries = page.read_all_entries();

        assert_eq!(entries, read_entries);
    }

    #[test]
    #[should_panic(expected = "Bucket index out of bounds")]
    fn test_meta_page_get_bucket_entry_out_of_bounds() {
        let mut page = Page::new_empty();
        let num_buckets = 3;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);

        // This should panic because index is equal to num_buckets
        let _entry = page.get_bucket_entry(num_buckets);
    }

    #[test]
    #[should_panic(expected = "Page size is insufficient for the number of buckets")]
    fn test_meta_page_init_too_many_buckets() {
        let mut page = Page::new_empty();
        let num_buckets = (AVAILABLE_PAGE_SIZE - BUCKET_NUM_SIZE) / BUCKET_ENTRY_SIZE + 1;
        <Page as MvccHashJoinMetaPage>::init(&mut page, num_buckets);
    }

    #[test]
    fn test_hash_join_table_insert_and_get_multiple_records() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(1, 1);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        // Insert multiple records
        let num_records = 100;
        for i in 0..num_records {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let val = format!("value{}", i).into_bytes();
            let ts = 100 + i as u64;
            hash_table
                .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
                .unwrap();

            // Retrieve the record immediately
            let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
            assert_eq!(retrieved_val, val);
        }

        // Verify that all records are retrievable
        for i in 0..num_records {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let val = format!("value{}", i).into_bytes();
            let ts = 100 + i as u64;
            let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
            assert_eq!(retrieved_val, val);
        }
    }
    
    #[test]
    fn test_hash_join_table_update_records() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(2, 2);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        // Insert and update records
        let key = b"key_update".to_vec();
        let pkey = b"pkey_update".to_vec();
        let val1 = b"value1".to_vec();
        let val2 = b"value2".to_vec();
        let ts_insert = 100;
        let ts_update = 200;

        // Insert the record
        hash_table
            .insert(key.clone(), pkey.clone(), ts_insert, 0, val1.clone())
            .unwrap();

        // Update the record
        hash_table
            .update(key.clone(), pkey.clone(), ts_update, 0, val2.clone())
            .unwrap();

        // Retrieve the updated record
        let retrieved_val = hash_table.get(&key, &pkey, ts_update).unwrap();
        assert_eq!(retrieved_val, val2);

        // Retrieve the old record from history
        let retrieved_val = hash_table.get(&key, &pkey, ts_insert + 50).unwrap();
        assert_eq!(retrieved_val, val1);
    }

    #[test]
    fn test_hash_join_table_insert_large_values() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(3, 3);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        // Generate a large value close to page size
        let large_val = vec![b'x'; (AVAILABLE_PAGE_SIZE / 2) as usize];

        let key = b"key_large".to_vec();
        let pkey = b"pkey_large".to_vec();
        let ts = 100;

        // Insert the record
        let result = hash_table.insert(key.clone(), pkey.clone(), ts, 0, large_val.clone());
        assert!(result.is_ok());

        // Retrieve the record
        let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
        assert_eq!(retrieved_val, large_val);
    }

    #[test]
    fn test_hash_join_table_insert_oversized_value() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(4, 4);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        // Generate an oversized value exceeding page size
        let oversized_val = vec![b'x'; (AVAILABLE_PAGE_SIZE + 1) as usize];

        let key = b"key_oversized".to_vec();
        let pkey = b"pkey_oversized".to_vec();
        let ts = 100;

        // Attempt to insert the oversized record
        let result = hash_table.insert(key.clone(), pkey.clone(), ts, 0, oversized_val);
        assert!(matches!(result, Err(AccessMethodError::RecordTooLarge)));
    }

    #[test]
    fn test_hash_join_table_empty_key_and_value() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(5, 5);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let key = b"".to_vec();
        let pkey = b"pkey_empty".to_vec();
        let val = b"".to_vec();
        let ts = 100;

        // Insert the record with empty key and value
        hash_table
            .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
            .unwrap();

        // Retrieve the record
        let retrieved_val = hash_table.get(&key, &pkey, ts).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_hash_join_table_bucket_distribution() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(6, 6);
        let num_buckets = 8;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let num_records = 10000;
        let mut bucket_counts = vec![0; num_buckets];

        // Insert records and count bucket distribution
        for i in 0..num_records {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let val = format!("value{}", i).into_bytes();
            let ts = 100 + i as u64;

            let index = hash_table.get_bucket_index(&key);
            bucket_counts[index] += 1;

            hash_table
                .insert(key.clone(), pkey.clone(), ts, 0, val.clone())
                .unwrap();
        }

        // Check that records are distributed across buckets
        for (i, count) in bucket_counts.iter().enumerate() {
            println!("Bucket {}: {} records", i, count);
            assert!(*count > 0);
        }
    }

    // #[test]
    // fn test_hash_join_table_concurrent_operations() {
    //     use std::thread;

    //     let mem_pool = get_in_mem_pool();
    //     let c_key = ContainerKey::new(7, 7);
    //     let num_buckets = 16;
    //     let hash_table = Arc::new(HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets));

    //     let num_threads = 4;
    //     let num_operations = 1000;

    //     let mut handles = Vec::new();

    //     for thread_id in 0..num_threads {
    //         let hash_table_clone = hash_table.clone();
    //         let handle = thread::spawn(move || {
    //             for i in 0..num_operations {
    //                 let key = format!("key{}_{}", thread_id, i).into_bytes();
    //                 let pkey = format!("pkey{}_{}", thread_id, i).into_bytes();
    //                 let val = format!("value{}_{}", thread_id, i).into_bytes();
    //                 let ts = 100 + i as u64;

    //                 // Randomly choose an operation
    //                 let op = i % 4;
    //                 match op {
    //                     0 => {
    //                         // Insert
    //                         let _ = hash_table_clone
    //                             .insert(key.clone(), pkey.clone(), ts, 0, val.clone());
    //                     }
    //                     1 => {
    //                         // Get
    //                         let _ = hash_table_clone.get(&key, &pkey, ts);
    //                     }
    //                     2 => {
    //                         // Update
    //                         let new_val = format!("new_value{}_{}", thread_id, i).into_bytes();
    //                         let _ = hash_table_clone.update(
    //                             key.clone(),
    //                             pkey.clone(),
    //                             ts + 50,
    //                             0,
    //                             new_val,
    //                         );
    //                     }
    //                     3 => {
    //                         // Delete
    //                         let _ = hash_table_clone.delete(&key, &pkey, ts + 100, 0);
    //                     }
    //                     _ => {}
    //                 }
    //             }
    //         });
    //         handles.push(handle);
    //     }

    //     for handle in handles {
    //         handle.join().unwrap();
    //     }

    //     // Optionally, verify some entries
    //     for thread_id in 0..num_threads {
    //         for i in 0..num_operations {
    //             let key = format!("key{}_{}", thread_id, i).into_bytes();
    //             let pkey = format!("pkey{}_{}", thread_id, i).into_bytes();
    //             let ts = 100 + i as u64;

    //             // Attempt to get the record
    //             let _ = hash_table.get(&key, &pkey, ts);
    //         }
    //     }
    // }

    // we assume theres no duplicate pkey
    // #[test]
    // fn test_hash_join_table_insert_duplicate_pkey() {
    //     let mem_pool = get_in_mem_pool();
    //     let c_key = ContainerKey::new(8, 8);
    //     let num_buckets = 16;
    //     let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

    //     let key1 = b"key_duplicate".to_vec();
    //     let pkey = b"pkey_duplicate".to_vec();
    //     let val1 = b"value1".to_vec();
    //     let val2 = b"value2".to_vec();
    //     let ts1 = 100;
    //     let ts2 = 200;

    //     // Insert the first record
    //     hash_table
    //         .insert(key1.clone(), pkey.clone(), ts1, 0, val1.clone())
    //         .unwrap();

    //     // Attempt to insert another record with the same pkey
    //     let result = hash_table.insert(key1.clone(), pkey.clone(), ts2, 0, val2.clone());

    //     // Since duplicate pkeys are not allowed, this should return an error
    //     assert!(matches!(result, Err(AccessMethodError::KeyDuplicate)));

    //     // Verify that the original record is still retrievable
    //     let retrieved_val = hash_table.get(&key1, &pkey, ts1).unwrap();
    //     assert_eq!(retrieved_val, val1);
    // }

    #[test]
    fn test_hash_join_table_delete_non_existent_after_deletion() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(9, 9);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let key = b"key_test".to_vec();
        let pkey = b"pkey_test".to_vec();
        let val = b"value_test".to_vec();
        let ts_insert = 100;
        let ts_delete = 200;

        // Insert the record
        hash_table
            .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
            .unwrap();

        // Delete the record
        hash_table
            .delete(&key, &pkey, ts_delete, 0)
            .unwrap();

        // Attempt to delete again
        let result = hash_table.delete(&key, &pkey, ts_delete + 50, 0);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_hash_join_table_update_non_existent_record() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(10, 10);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let key = b"key_nonexistent".to_vec();
        let pkey = b"pkey_nonexistent".to_vec();
        let val = b"value".to_vec();
        let ts = 100;

        // Attempt to update a non-existent record
        let result = hash_table.update(key.clone(), pkey.clone(), ts, 0, val.clone());
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_hash_join_table_get_with_future_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(11, 11);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let key = b"key_future".to_vec();
        let pkey = b"pkey_future".to_vec();
        let val = b"value_future".to_vec();
        let ts_insert = 100;
        let ts_future = 1_000_000;

        // Insert the record
        hash_table
            .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
            .unwrap();

        // Attempt to get the record with a future timestamp
        let retrieved_val = hash_table.get(&key, &pkey, ts_future).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_hash_join_table_get_with_past_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(12, 12);
        let num_buckets = 16;
        let hash_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let key = b"key_past".to_vec();
        let pkey = b"pkey_past".to_vec();
        let val = b"value_past".to_vec();
        let ts_insert = 100;
        let ts_past = 50;

        // Insert the record
        hash_table
            .insert(key.clone(), pkey.clone(), ts_insert, 0, val.clone())
            .unwrap();

        // Attempt to get the record with a past timestamp
        let result = hash_table.get(&key, &pkey, ts_past);
        assert!(matches!(
            result,
            Err(AccessMethodError::KeyFoundButInvalidTimestamp) | Err(AccessMethodError::KeyNotFound)
        ));
    }
}
