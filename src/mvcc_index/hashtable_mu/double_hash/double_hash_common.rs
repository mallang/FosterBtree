use core::fmt;
use std::{
    hash::{Hash, Hasher, SipHasher},
    sync::{atomic::AtomicU32, Arc, Mutex},
};

use crate::{bp::MemPoolStatus, log_warn, page::PageId};

pub(crate) const HASHER_SEED: u32 = 233;
pub const MAX_CUCKOO_ITERATE_COUNT: usize = 3;

pub struct BucketEntry {
    page_id: PageId,
    frame_id: AtomicU32, // changed when normal case, except REHASH
}

impl BucketEntry {
    pub fn new(pid: PageId) -> Self {
        Self {
            page_id: pid,
            frame_id: AtomicU32::new(u32::MAX),
        }
    }
    pub fn new_with_frame_id(pid: PageId, frame_id: u32) -> Self {
        Self {
            page_id: pid,
            frame_id: AtomicU32::new(frame_id),
        }
    }
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
    pub fn frame_id(&self) -> u32 {
        self.frame_id.load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
pub enum SwapChainStatus {
    Swap((u32, Vec<u8>, u32)), // swap at slot: $1, swapped key: $2, swapped space_need: $3
    Insert(u32),               // can be inserted with space_need: $2
}

#[derive(Debug, Clone)]
pub struct ChainStatusEntry(pub usize, pub SwapChainStatus);

pub struct Buckets {
    pub num_buckets: u32,
    // with length = `num_buckets`
    pub buckets: Vec<BucketEntry>,
}

impl Buckets {
    pub fn new(num_buckets: u32, buckets: Vec<BucketEntry>) -> Self {
        Self {
            num_buckets,
            buckets,
        }
    }
    pub fn get_bucket_num(&self) -> u32 {
        self.num_buckets
    }

    pub fn get_bucket_entry(&self, entry_idx: usize) -> &BucketEntry {
        &self.buckets[entry_idx]
    }

    pub fn get_bucket_index(&self, key: &[u8]) -> usize {
        let num_buckets = self.num_buckets;

        (farmhash::hash32_with_seed(key, HASHER_SEED) % num_buckets) as usize
    }

    // maybe NOT exist -> None
    pub fn get_a_second_bucket_index(&self, key: &[u8], first_idx: usize) -> Option<usize> {
        let num_buckets = self.num_buckets;

        let second_idx =
            (farmhash::hash32_with_seed(key, HASHER_SEED) % (num_buckets * 2)) as usize;
        if second_idx != first_idx {
            return Some(second_idx);
        }

        None
    }

    pub fn get_all_bucket_index(&self, key: &[u8]) -> Vec<usize> {
        let num_buckets = self.num_buckets;
        let mut bucket_idxs =
            vec![(farmhash::hash32_with_seed(key, HASHER_SEED) % num_buckets) as usize];

        bucket_idxs
    }
}

pub fn get_first_hash_idx(key: &[u8], total_nums: u32) -> usize {
    (farmhash::hash32_with_seed(key, HASHER_SEED) % total_nums) as usize
}
