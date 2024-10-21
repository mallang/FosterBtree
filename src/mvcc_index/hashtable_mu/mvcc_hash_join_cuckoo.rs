use core::num;
use std::{hash::{DefaultHasher, Hash, Hasher, SipHasher}, sync::{atomic::AtomicU32, Arc}};

use crate::{bp::{ContainerKey, MemPool}, mvcc_index::MvccIndex, page::{Page, PageId, AVAILABLE_PAGE_SIZE}, prelude::AccessMethodError, rwlatch::RwLatch};

use super::{mvcc_hash_join_cuckoo_common::CuckooAccessMethodError, mvcc_hash_join_cuckoo_history_table::CuckooHistoryHashTable, mvcc_hash_join_cuckoo_table:: CuckooHashTable};

pub const HASHER_KEYS: [(u64, u64); 2] = [(0, 0), (1, 1)];
pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>();
pub const BUCKET_ENTRY_SIZE: usize = PAGE_ID_SIZE;
pub const DEFAULT_NUM_BUCKETS: usize = 16;

pub struct HashJoinTable<T: MemPool> {
    mem_pool: Arc<T>,  // TODO: check may be deleted
    c_key: ContainerKey,

    meta_page_id: PageId,  // fixed
    meta_frame_id: AtomicU32,
    
    recent_hash_table: CuckooHashTable<T>,
    history_hash_table: CuckooHistoryHashTable<T>,

}

impl<T: MemPool> MvccIndex for HashJoinTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = CuckooAccessMethodError;
    type MemPoolType = T;


    fn create(
            c_key: ContainerKey,
            mem_pool: Arc<Self::MemPoolType>,
        ) -> Result<Self, Self::Error>
        where
            Self: Sized {
        Ok(Self::new(c_key, mem_pool))
    }

    fn insert(
            &self,
            key: Self::Key,
            pkey: Self::PKey,
            ts: crate::mvcc_index::Timestamp,
            tx_id: crate::mvcc_index::TxId,
            value: Self::Value,
        ) -> Result<(), Self::Error> {
        self.recent_hash_table.insert(&key, &pkey, ts, &value)
    }

    fn get(
            &self,
            key: &Self::Key,
            pkey: &Self::PKey,
            ts: crate::mvcc_index::Timestamp,
        ) -> Result<Option<Self::Value>, Self::Error> {
        let recent_val = self.recent_hash_table.get(key, pkey, ts);
        match recent_val {
            Ok(val) => Ok(Some(val)),
            Err(CuckooAccessMethodError::KeyNotFound) => {
                Ok(None)
            },
            Err(e) => Err(e),
        }
    }

    fn get_all(
            &self,
            key: &Self::Key,
            ts: crate::mvcc_index::Timestamp,
        ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        todo!()
    }

    fn scan(
            &self,
            ts: crate::mvcc_index::Timestamp,
        ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error> {
        todo!()
    }

    fn update(
            &self,
            key: Self::Key,
            pkey: Self::PKey,
            ts: crate::mvcc_index::Timestamp,
            tx_id: crate::mvcc_index::TxId,
            value: Self::Value,
        ) -> Result<(), Self::Error> {
        let old_result = self.recent_hash_table.update(&key, &pkey, ts, &value);
        match old_result {
            Ok((old_ts, old_val)) => {
                // insert to history table
                self.history_hash_table.insert(&key, &pkey, old_ts, ts, &old_val)
            }
            Err(e) => Err(e),
        }
    }

    fn delete(
            &self,
            key: &Self::Key,
            pkey: &Self::PKey,
            ts: crate::mvcc_index::Timestamp,
            tx_id: crate::mvcc_index::TxId,
        ) -> Result<(), Self::Error> {
            let old_result = self.recent_hash_table.delete(&key, &pkey, ts);
            match old_result {
                Ok((old_ts, old_val)) => {
                    // insert to history table
                    self.history_hash_table.insert(&key, &pkey, old_ts, ts, &old_val)
                }
                Err(e) => Err(e),
            }
    }

    fn delta_scan(
            &self,
            from_ts: crate::mvcc_index::Timestamp,
            to_ts: crate::mvcc_index::Timestamp,
        ) -> Result<
            Box<dyn Iterator<Item = (Self::Key, Self::PKey, crate::mvcc_index::Delta<Self::Value>)> + Send>,
            Self::Error,
        > {
        todo!()
    }

    fn garbage_collect(
            &self,
            safe_ts: crate::mvcc_index::Timestamp,
        ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<T: MemPool> HashJoinTable<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEFAULT_NUM_BUCKETS)
    }

    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        MvccHashJoinCuckooMetaPage::init(&mut *meta_page, num_buckets);

        let recent_table = CuckooHashTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);
        let history_table = CuckooHistoryHashTable::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);
        
        let recent_pages = recent_table.get_all_bucket_pages_for_init();
        let history_pages = history_table.get_all_bucket_pages_for_init();

        MvccHashJoinCuckooMetaPage::write_all_entries_recent(&mut *meta_page, &recent_pages);
        MvccHashJoinCuckooMetaPage::write_all_entries_history(&mut *meta_page, &history_pages);

        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            recent_hash_table: recent_table,
            history_hash_table: history_table,
        }
    }
}


/*
    <Recent Bucket Num> <History Bucket Num> [Recent Page Id ...] [History Page Id...]

*/
pub trait MvccHashJoinCuckooMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);
    fn set_history_bucket_num(&mut self, num_buckets: usize);
    fn set_recent_bucket_num(&mut self, num_buckets: usize);
    fn get_recent_bucket_num(&self) -> usize;
    fn get_history_bucket_num(&self) -> usize;

    fn get_recent_bucket_entry(&self, index: usize) -> PageId;
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId);
    fn get_history_bucket_entry(&self, index: usize) -> PageId;
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId);

    fn read_all_entries_recent(&self) -> Vec<PageId>;
    fn write_all_entries_recent(&mut self, entries: &[PageId]);

    fn read_all_entries_history(&self) -> Vec<PageId>;
    fn write_all_entries_history(&mut self, entries: &[PageId]);
}


impl MvccHashJoinCuckooMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE * 2 + (num_buckets * BUCKET_ENTRY_SIZE) * 2;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "page size is insufficient for the number of buckets",
        );
        self.set_recent_bucket_num(num_buckets);
        self.set_history_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn set_recent_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn set_history_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn get_recent_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_history_bucket_num(&self) -> usize {
        let bytes = &self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_recent_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let recent_pid = PageId::from_be_bytes(
            bytes[0..PAGE_ID_SIZE].try_into().unwrap(),
        );

        recent_pid
    }
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE]
            .copy_from_slice(&entry.to_be_bytes());
    }
    fn get_history_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + recent_num_buckets * BUCKET_ENTRY_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let history_pid = PageId::from_be_bytes(
            bytes[0..PAGE_ID_SIZE].try_into().unwrap(),
        );

        history_pid
    }
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + recent_num_buckets * BUCKET_ENTRY_SIZE + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE]
            .copy_from_slice(&entry.to_be_bytes());
    }



    fn read_all_entries_recent(&self) -> Vec<PageId> {
        let num_buckets = self.get_recent_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_recent_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_recent(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_recent_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_recent_bucket_entry(index, entry);
        }
    }

    fn read_all_entries_history(&self) -> Vec<PageId> {
        let num_buckets = self.get_history_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_history_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_history(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_history_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_history_bucket_entry(index, entry);
        }
    }

}


