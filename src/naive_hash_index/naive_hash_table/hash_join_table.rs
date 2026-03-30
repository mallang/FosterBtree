use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{
        hash_common::{read_repair_btree, read_repair_vec, StatCollector},
        hash_join_page::record::RecordRef,
        Delta, MvccEntry, MvccIndex, TxId,
    },
    naive_hash_index::naive_hash_table::{hash_join_chain::HeapHashChain, SingleTsHashTable},
    page::{Page, PageId},
    prelude::{AccessMethodError, Timestamp},
};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const DEAFAULT_FIRST_BUCKET_NUM: usize = 128;

pub struct NaiveHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<HeapHashChain<T>>>,
}

impl<T: MemPool + 'static> NaiveHashTable<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_FIRST_BUCKET_NUM)
    }

    pub fn get_chain(&self, index: usize) -> Arc<HeapHashChain<T>> {
        self.bucket_entries[index].clone()
    }

    pub fn collect_page_num(&self) -> usize {
        let mut page_num = 0;
        for chain in &self.bucket_entries {
            page_num += chain.collect_page_num();
        }
        page_num
    }

    pub fn collect_space_stat(&self, stat: &mut StatCollector) {
        for bucket in &self.bucket_entries {
            bucket.collect_space_stat(stat);
        }
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

    /// Inserts a key-value pair with new pkey into the hash join table.
    pub fn insert(&self, rec: RecordRef) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(rec.key());
        let heap_chain = &self.bucket_entries[index];

        heap_chain.insert(&rec)
    }

    /// Update the value of an existing record in-place (same value size) or via
    /// delete + reinsert (different value size).  Delegates to the bucket's chain.
    pub fn update(&self, rec: RecordRef) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(rec.key());
        self.bucket_entries[index].update_value(rec.key(), rec.pkey(), rec.val())
    }

    /// Retrieves a value associated with the given key and primary key at a specific timestamp.
    pub fn get(&self, key: &[u8], pkey: &[u8]) -> Result<Option<Vec<u8>>, AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];

        Ok(heap_chain.get_no_repair(pkey).unwrap_or(None))
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        results: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(key);
        let heap_chain = &self.bucket_entries[index];

        heap_chain.chain_scan_key(key, results)?;
        Ok(())
    }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    pub fn scan(
        &self,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let mut result = vec![];
        for bucket in &self.bucket_entries {
            bucket.scan_into_vec(&mut result)?;
        }
        Ok(Box::new(
            result.into_iter().map(|e| (e.key, e.pkey, e.value)),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, AccessMethodError>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
    }
    fn split_at_ts(&self, ts: Timestamp) -> Result<(), AccessMethodError> {
        Ok(())
    }
}
