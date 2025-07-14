use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{hash_join_page::ChainedHashMetaPage, Delta, MvccEntry, MvccIndex},
    page::{Page, PageId},
    prelude::AccessMethodError,
};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64},
        Arc, Mutex,
    },
};

use super::{chained_hash_bucket_second::SecondBucket, Timestamp, TxId, TxInfo};

pub const DEAFAULT_FIRST_BUCKET_NUM: usize = 128;

pub struct ChainedHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<SecondBucket<T>>>,
    // tx_status: HashMap<TxId, TxInfo>, // Neet to written down to disk later...

    // used in recent scan
    largest_txn_ts: AtomicU64,
    is_bulk_update: AtomicBool,
}

impl<T: MemPool + 'static> ChainedHashTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_FIRST_BUCKET_NUM)
    }

    fn set_largest_txn_ts(&self, ts: Timestamp) {
        self.largest_txn_ts
            .store(ts, std::sync::atomic::Ordering::SeqCst);
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        ChainedHashMetaPage::init(&mut *meta_page, num_buckets);
        ChainedHashMetaPage::set_bucket_num(&mut *meta_page, num_buckets);

        let mut bucket_entries = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let second_table = SecondBucket::new(c_key, mem_pool.clone());
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
            largest_txn_ts: AtomicU64::new(0),
            is_bulk_update: AtomicBool::new(false),
        }
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
        is_bulk_update: bool,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let second_table = &self.bucket_entries[index];

        // TODO: (JUN) now assume key is not changed, need to handle key change later
        second_table.update(pkey, entry, is_bulk_update)?;
        Ok(())
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

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub fn scan_into_vec(
        &self,
        ts: Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.scan_into_vec(ts, results);
        }
        Ok(())
    }

    fn delta_scan_into_vec(
        &self,
        from: Timestamp,
        to: Timestamp,
        results: &mut Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>,
    ) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.delta_scan(from, to, results)?;
        }
        Ok(())
    }

    pub fn scan_into_vec_recent_ignore_ts(
        &self,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.scan_into_vec_recent_ignore_ts(results).unwrap();
            // println!("{}", bucket.stat().as_str());
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
        let mut res = String::new();
        for bucket in &self.bucket_entries {
            res.push_str(bucket.stat().as_str());
        }
        // stat_str
        res
    }

    fn bulk_update(&self) -> Result<(), AccessMethodError> {
        let new_start_ts = self
            .largest_txn_ts
            .load(std::sync::atomic::Ordering::Acquire);
        for bucket in &self.bucket_entries {
            bucket.do_bulk_update(new_start_ts)?;
        }
        Ok(())
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
            largest_txn_ts: AtomicU64::new(
                self.largest_txn_ts
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
            is_bulk_update: AtomicBool::new(
                self.is_bulk_update
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
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
        self.set_largest_txn_ts(ts);
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
        self.set_largest_txn_ts(ts);
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        ChainedHashTable::update(
            self,
            entry.key(),
            entry.pkey(),
            &entry,
            self.is_bulk_update
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn update_write_repair(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.set_largest_txn_ts(ts);
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        ChainedHashTable::update(
            self,
            entry.key(),
            entry.pkey(),
            &entry,
            self.is_bulk_update
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), Self::Error> {
        self.set_largest_txn_ts(ts);
        ChainedHashTable::delete(self, key.as_ref(), pkey.as_ref(), &ts)
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let mut results = Vec::new();
        // log_warn!(
        //     "[chain scan] max txn ts: {:?}",
        //     self.largest_txn_ts
        //         .load(std::sync::atomic::Ordering::Acquire)
        // );
        if ts
            >= self
                .largest_txn_ts
                .load(std::sync::atomic::Ordering::Acquire)
        {
            ChainedHashTable::scan_into_vec_recent_ignore_ts(self, &mut results)?;
        } else {
            ChainedHashTable::scan_into_vec(self, ts, &mut results)?;
        }


        Ok(Box::new(results.into_iter().map(|entry| {
            (
                entry.key,
                entry.pkey,
                entry.value,
            )
        })))
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let mut results = Vec::new();
        for bucket in &self.bucket_entries {
            bucket.scan_key_into(key, &ts, &mut results);
        }
        Ok(Box::new(results.into_iter()))
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
        let mut results = Vec::new();
        for bucket in &self.bucket_entries {
            bucket.scan_key_into(key, &ts, &mut results);
        }
        Ok(results)
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
        let mut results = Vec::new();
        ChainedHashTable::delta_scan_into_vec(self, from_ts, to_ts, &mut results)?;
        Ok(Box::new(results.into_iter()))
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
        ChainedHashTable::garbage_collect(&self, &safe_ts)
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let mut results = Vec::new();
        for bucket in &self.bucket_entries {
            bucket.scan_all(&mut results)?;
        }
        Ok(Box::new(results.into_iter()))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn split_at_ts(&self, _ts: Timestamp) -> Result<(), Self::Error> {
        Ok(())
    }

    fn bulk_update_end(&self) -> Result<(), Self::Error> {
        self.is_bulk_update
            .store(false, std::sync::atomic::Ordering::Release);
        self.bulk_update()?;
        Ok(())
    }
    fn bulk_update_start(&self) -> Result<(), Self::Error> {
        self.is_bulk_update
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    fn collect_page_num(&self) -> usize {
        let mut total_page_num = 0;
        for bucket in &self.bucket_entries {
            total_page_num += bucket.collect_page_num();
        }
        total_page_num
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
            // println!("Scanned: key={:?}, pkey={:?}, value={:?}", key, pkey, value);
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
