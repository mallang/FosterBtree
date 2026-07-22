use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    thread,
    time::Duration,
};

use crate::{
    access_method::{hash_leaf_page::HashLeafPage, AccessMethodError, FilterType, UniqueKeyIndex},
    bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    page::{PageId, AVAILABLE_PAGE_SIZE},
};

pub mod prelude {
    pub use super::{
        encode_hash_key, hash_key, PagedHashChainV1, PagedHashChainV1Iter, PagedHashChainV1Stats,
    };
}

const HASH_BYTES: usize = std::mem::size_of::<u64>();

pub fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

pub fn encode_hash_key(key: &[u8]) -> Vec<u8> {
    let hash_value = hash_key(key);
    let mut encoded = Vec::with_capacity(HASH_BYTES + key.len());
    encoded.extend_from_slice(&hash_value.to_be_bytes());
    encoded.extend_from_slice(key);
    encoded
}

fn original_key(encoded_key: &[u8]) -> &[u8] {
    &encoded_key[HASH_BYTES..]
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PagedHashChainV1Stats {
    pub bucket_count: usize,
    pub total_pages: usize,
    pub overflow_pages: usize,
    pub max_chain_len: usize,
    pub total_records: usize,
    pub max_bucket_records: usize,
}

impl PagedHashChainV1Stats {
    pub fn avg_chain_len(&self) -> f64 {
        if self.bucket_count == 0 {
            0.0
        } else {
            self.total_pages as f64 / self.bucket_count as f64
        }
    }

    pub fn avg_bucket_records(&self) -> f64 {
        if self.bucket_count == 0 {
            0.0
        } else {
            self.total_records as f64 / self.bucket_count as f64
        }
    }
}

pub type PagedHashChainV1Iter = std::vec::IntoIter<(Vec<u8>, Vec<u8>)>;

pub struct PagedHashChainV1<T: MemPool> {
    pub mem_pool: Arc<T>,
    c_key: ContainerKey,
    num_buckets: usize,
    meta_page_id: PageId,
    buckets: Vec<PageFrameKey>,
}

impl<T: MemPool> PagedHashChainV1<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        if num_buckets == 0 {
            panic!("Number of buckets cannot be 0");
        }
        if num_buckets * std::mem::size_of::<PageId>() + std::mem::size_of::<usize>()
            > AVAILABLE_PAGE_SIZE
        {
            panic!("Number of buckets too large to fit in the meta page");
        }

        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let mut offset = 0;
        meta_page[offset..offset + std::mem::size_of::<usize>()]
            .copy_from_slice(&num_buckets.to_be_bytes());
        offset += std::mem::size_of::<usize>();

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            let mut bucket_page = mem_pool.create_new_page_for_write(c_key).unwrap();
            bucket_page.init_hash_leaf();

            let page_id = bucket_page.get_id();
            let frame_id = bucket_page.frame_id();
            meta_page[offset..offset + std::mem::size_of::<PageId>()]
                .copy_from_slice(&page_id.to_be_bytes());
            offset += std::mem::size_of::<PageId>();

            buckets.push(PageFrameKey::new_with_frame_id(c_key, page_id, frame_id));
        }

        Self {
            mem_pool,
            c_key,
            num_buckets,
            meta_page_id: meta_page.get_id(),
            buckets,
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, meta_page_id: PageId) -> Self {
        let meta_page = mem_pool
            .get_page_for_read(PageFrameKey::new(c_key, meta_page_id))
            .unwrap();

        let mut offset = 0;
        let num_buckets = usize::from_be_bytes(
            meta_page[offset..offset + std::mem::size_of::<usize>()]
                .try_into()
                .unwrap(),
        );
        offset += std::mem::size_of::<usize>();

        let mut buckets = Vec::with_capacity(num_buckets);
        for _ in 0..num_buckets {
            let page_id = PageId::from_be_bytes(
                meta_page[offset..offset + std::mem::size_of::<PageId>()]
                    .try_into()
                    .unwrap(),
            );
            offset += std::mem::size_of::<PageId>();
            buckets.push(PageFrameKey::new(c_key, page_id));
        }

        Self {
            mem_pool,
            c_key,
            num_buckets,
            meta_page_id,
            buckets,
        }
    }

    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    pub fn page_stats(&self) -> Result<PagedHashChainV1Stats, AccessMethodError> {
        let mut stats = PagedHashChainV1Stats {
            bucket_count: self.num_buckets,
            ..PagedHashChainV1Stats::default()
        };

        for bucket in &self.buckets {
            let mut chain_len = 0;
            let mut bucket_records = 0usize;
            let mut current_key = *bucket;
            loop {
                let page = self.read_page(current_key)?;
                chain_len += 1;
                stats.total_pages += 1;
                let page_records = page.hash_leaf_record_count() as usize;
                stats.total_records += page_records;
                bucket_records += page_records;

                if let Some((next_page_id, next_frame_id)) = page.hash_leaf_next_page() {
                    current_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                } else {
                    break;
                }
            }
            stats.max_chain_len = stats.max_chain_len.max(chain_len);
            stats.max_bucket_records = stats.max_bucket_records.max(bucket_records);
            stats.overflow_pages += chain_len.saturating_sub(1);
        }

        Ok(stats)
    }

    fn bucket_key(&self, hash_value: u64) -> PageFrameKey {
        self.buckets[hash_value as usize % self.num_buckets]
    }

    fn read_page(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameReadGuard<T::EP>, AccessMethodError> {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_read(page_key) {
                Ok(page) => return Ok(page),
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    attempts += 1;
                    backoff(attempts);
                }
                Err(MemPoolStatus::CannotEvictPage) => thread::sleep(Duration::from_millis(1)),
                Err(status) => {
                    return Err(AccessMethodError::Other(format!(
                        "failed to read hash leaf page {}: {}",
                        page_key, status
                    )));
                }
            }
        }
    }

    fn write_page(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameWriteGuard<T::EP>, AccessMethodError> {
        let mut attempts = 0;
        loop {
            match self.mem_pool.get_page_for_write(page_key) {
                Ok(page) => return Ok(page),
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    attempts += 1;
                    backoff(attempts);
                }
                Err(MemPoolStatus::CannotEvictPage) => thread::sleep(Duration::from_millis(1)),
                Err(status) => {
                    return Err(AccessMethodError::Other(format!(
                        "failed to write hash leaf page {}: {}",
                        page_key, status
                    )));
                }
            }
        }
    }

    fn collect_records(
        &self,
        filter: Option<&FilterType>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut records = Vec::new();
        for bucket in &self.buckets {
            let mut current_key = *bucket;
            loop {
                let page = self.read_page(current_key)?;
                for slot_id in 1..page.hash_leaf_slot_count().saturating_sub(1) {
                    if page.hash_leaf_is_ghost(slot_id) {
                        continue;
                    }
                    let encoded_key = page.hash_leaf_get_raw_key(slot_id);
                    let key = original_key(encoded_key);
                    let value = page.hash_leaf_get_val(slot_id);
                    if filter.map(|f| f(key, value)).unwrap_or(true) {
                        records.push((key.to_vec(), value.to_vec()));
                    }
                }

                if let Some((next_page_id, next_frame_id)) = page.hash_leaf_next_page() {
                    current_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                } else {
                    break;
                }
            }
        }
        Ok(records)
    }
}

impl<T: MemPool> UniqueKeyIndex for PagedHashChainV1<T> {
    type Iter = PagedHashChainV1Iter;

    fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        let hash_value = hash_key(key);
        let encoded_key = encode_hash_key(key);
        let mut current_key = self.bucket_key(hash_value);

        loop {
            let mut page = self.write_page(current_key)?;
            if page.hash_leaf_get(&encoded_key).is_ok() {
                return Err(AccessMethodError::KeyDuplicate);
            }

            if let Some((next_page_id, next_frame_id)) = page.hash_leaf_next_page() {
                current_key =
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                continue;
            }

            match page.hash_leaf_insert(&encoded_key, value) {
                Ok(()) => return Ok(()),
                Err(AccessMethodError::OutOfSpace) => {
                    let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key)?;
                    new_page.init_hash_leaf();
                    page.hash_leaf_set_next_page(new_page.get_id(), new_page.frame_id());
                    return new_page.hash_leaf_insert(&encoded_key, value);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>, AccessMethodError> {
        let hash_value = hash_key(key);
        let encoded_key = encode_hash_key(key);
        let mut current_key = self.bucket_key(hash_value);

        loop {
            let page = self.read_page(current_key)?;
            match page.hash_leaf_get(&encoded_key) {
                Ok(value) => return Ok(value.to_vec()),
                Err(AccessMethodError::KeyNotFound) => {
                    if let Some((next_page_id, next_frame_id)) = page.hash_leaf_next_page() {
                        current_key = PageFrameKey::new_with_frame_id(
                            self.c_key,
                            next_page_id,
                            next_frame_id,
                        );
                    } else {
                        return Err(AccessMethodError::KeyNotFound);
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn delete(&self, _key: &[u8]) -> Result<(), AccessMethodError> {
        Err(AccessMethodError::Other(
            "PagedHashChainV1 delete is not implemented".to_string(),
        ))
    }

    fn update(&self, _key: &[u8], _value: &[u8]) -> Result<(), AccessMethodError> {
        Err(AccessMethodError::Other(
            "PagedHashChainV1 update is not implemented".to_string(),
        ))
    }

    fn upsert(&self, _key: &[u8], _value: &[u8]) -> Result<(), AccessMethodError> {
        Err(AccessMethodError::Other(
            "PagedHashChainV1 upsert is not implemented".to_string(),
        ))
    }

    fn upsert_with_merge(
        &self,
        _key: &[u8],
        _value: &[u8],
        _merge_fn: impl Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> Result<(), AccessMethodError> {
        Err(AccessMethodError::Other(
            "PagedHashChainV1 upsert_with_merge is not implemented".to_string(),
        ))
    }

    fn scan(self: &Arc<Self>) -> Self::Iter {
        self.collect_records(None).unwrap().into_iter()
    }

    fn scan_with_filter(self: &Arc<Self>, filter: FilterType) -> Self::Iter {
        self.collect_records(Some(&filter)).unwrap().into_iter()
    }
}

fn backoff(attempts: u32) {
    let nanos = 2_u64.saturating_pow(attempts.min(20));
    thread::sleep(Duration::from_nanos(nanos));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{bp::get_in_mem_pool, prelude::UniqueKeyIndex};

    fn setup() -> Arc<PagedHashChainV1<crate::bp::InMemPool>> {
        Arc::new(PagedHashChainV1::new(
            ContainerKey::new(0, 0),
            get_in_mem_pool(),
            4,
        ))
    }

    fn u64_key(value: u64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    fn paged_hash_chain_v1_inserts_and_gets() {
        let index = setup();
        let value = b"value".to_vec();

        index.insert(&u64_key(7), &value).unwrap();

        assert_eq!(index.get(&u64_key(7)).unwrap(), value);
        assert_eq!(index.get(&u64_key(8)), Err(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn paged_hash_chain_v1_rejects_duplicate_key() {
        let index = setup();
        let key = u64_key(7);

        index.insert(&key, b"value").unwrap();

        assert_eq!(
            index.insert(&key, b"other"),
            Err(AccessMethodError::KeyDuplicate)
        );
    }

    #[test]
    fn paged_hash_chain_v1_uses_overflow_pages() {
        let index = Arc::new(PagedHashChainV1::new(
            ContainerKey::new(0, 0),
            get_in_mem_pool(),
            1,
        ));
        let value = vec![9_u8; 1024];

        for key in 0..40_u64 {
            index.insert(&u64_key(key), &value).unwrap();
        }

        for key in 0..40_u64 {
            assert_eq!(index.get(&u64_key(key)).unwrap(), value);
        }

        let stats = index.page_stats().unwrap();
        assert_eq!(stats.total_records, 40);
        assert!(stats.overflow_pages > 0);
        assert!(stats.max_chain_len > 1);
    }

    #[test]
    fn paged_hash_chain_v1_scan_returns_original_keys() {
        let index = setup();
        index.insert(b"b", b"2").unwrap();
        index.insert(b"a", b"1").unwrap();

        let mut records = index.scan().collect::<Vec<_>>();
        records.sort();

        assert_eq!(
            records,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
    }
}
