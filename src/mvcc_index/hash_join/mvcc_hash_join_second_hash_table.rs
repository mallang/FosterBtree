use super::{
    mvcc_hash_join::MvccHashJoinMetaPage, mvcc_hash_join_history_chain::MvccHashJoinHistoryChain,
    mvcc_hash_join_recent_chain::MvccHashJoinRecentChain,
    mvcc_hash_join_second_bucket::SecondTableBucket, Timestamp,
};

use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::TxId,
    page::PageId,
    prelude::AccessMethodError,
};

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

pub const DEAFAULT_SECOND_BUCKET_NUM: usize = 1024;

pub struct SecondHashJoinTable<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    pub num_buckets: usize,
    bucket_entries: Vec<Arc<SecondTableBucket<T>>>,
}

impl<T: MemPool> SecondHashJoinTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_SECOND_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        // TODO: (JUN) Need to do something with the meta page
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        MvccHashJoinMetaPage::init(&mut *meta_page, num_buckets);
        MvccHashJoinMetaPage::set_bucket_num(&mut *meta_page, num_buckets);

        let mut bucket_entries: Vec<Arc<SecondTableBucket<T>>> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let bucket_entry = SecondTableBucket::new(c_key, mem_pool.clone());
            // MvccHashJoinMetaPage::set_bucket_entry(&mut *meta_page, i, &entry);
            bucket_entries.push(Arc::new(bucket_entry));
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

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.insert(key, pkey, ts, tx_id, val)
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.get(key, pkey, ts)
    }

    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.update(key, pkey, ts, tx_id, val)
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.delete(key, pkey, ts, tx_id)
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

    pub fn get_recent_chain(&self, idx: usize) -> Arc<MvccHashJoinRecentChain<T>> {
        self.bucket_entries[idx].get_recent_chain()
    }

    pub fn get_history_chain(&self, idx: usize) -> Arc<MvccHashJoinHistoryChain<T>> {
        self.bucket_entries[idx].get_history_chain()
    }
    // Additional methods as needed...
}
