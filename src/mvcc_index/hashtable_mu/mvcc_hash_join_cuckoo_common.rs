use core::fmt;
use std::{hash::{Hash, Hasher, SipHasher}, sync::atomic::AtomicU32};

use crate::{bp::MemPoolStatus, page::PageId};

use super::mvcc_hash_join_cuckoo::HASHER_KEYS;

pub const MAX_CUCKOO_ITERATE_COUNT: usize = 3;

pub struct BucketEntry {
    pub page_id: PageId,  
    pub frame_id: AtomicU32, // changed when normal case, except REHASH
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
}

#[derive(Debug, Clone)]
pub enum SwapChainStatus {
    Swap((u32, Vec<u8>, u32, )), // swap at slot: $1, swapped key: $2, swapped space_need: $3 
    Insert(u32), // can be inserted with space_need: $2
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

    // assert have had a latch
    pub fn get_bucket_index(&self, key: &[u8], hasher_idx: usize) ->  usize {
        let num_buckets = self.num_buckets;
        let mut hasher = SipHasher::new_with_keys(
            HASHER_KEYS[hasher_idx].0, 
            HASHER_KEYS[hasher_idx].1,
        );
        key.hash(&mut hasher);
        (hasher.finish() as usize) % num_buckets as usize
    }

    // maybe NOT exist -> None
    pub fn get_a_second_bucket_index(&self, key: &[u8], first_idx: usize) -> Option<usize> {
        let num_buckets = self.num_buckets;
        let bucket_idxs = HASHER_KEYS.iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(
                    *key0, 
                    *key1,
                );
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_buckets as usize
            })
            .collect::<Vec<_>>();

        for idx in bucket_idxs {
            if idx != first_idx {
                return Some(idx);
            }
        }
        
        None
    }

    pub fn get_all_bucket_index(&self, key: &[u8]) -> Vec<usize> {
        let num_buckets = self.num_buckets;
        let mut bucket_idxs = HASHER_KEYS.iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(
                    *key0, 
                    *key1,
                );
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_buckets as usize
            })
            .collect::<Vec<_>>();

        bucket_idxs.sort();
        bucket_idxs.dedup();
        bucket_idxs
    }
}


#[derive(Debug, PartialEq)]
pub enum CuckooAccessMethodError {
    KeyNotFound,
    KeyFoundButInvalidTimestamp, // For MVCC
    KeyDuplicate,
    KeyNotInPageRange, // For Btree
    PageReadLatchFailed,
    PageWriteLatchFailed,
    RecordTooLarge,
    MemPoolStatus(MemPoolStatus),
    OutOfSpace, // For ReadOptimizedPage
    OutOfSpaceForUpdate(Vec<u8>),
    NeedToUpdateMVCC(u64, Vec<u8>), // For MVCC
    InvalidTimestamp, // For MVCC
    CuckooIterateFailed(u32), // new_hash_size
    Other(String),
}

impl fmt::Display for CuckooAccessMethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CuckooAccessMethodError::KeyNotFound => write!(f, "Key not found"),
            CuckooAccessMethodError::KeyFoundButInvalidTimestamp => write!(f, "Key found but invalid timestamp"),
            CuckooAccessMethodError::KeyDuplicate => write!(f, "Key duplicate"),
            CuckooAccessMethodError::KeyNotInPageRange => write!(f, "Key not in page range"),
            CuckooAccessMethodError::PageReadLatchFailed => write!(f, "Page read latch failed"),
            CuckooAccessMethodError::PageWriteLatchFailed => write!(f, "Page write latch failed"),
            CuckooAccessMethodError::RecordTooLarge => write!(f, "Record too large"),
            CuckooAccessMethodError::MemPoolStatus(status) => write!(f, "MemPool status: {:?}", status),
            CuckooAccessMethodError::OutOfSpace => write!(f, "Out of space"),
            CuckooAccessMethodError::OutOfSpaceForUpdate(key) => write!(f, "Out of space for update: {:?}", key),
            CuckooAccessMethodError::NeedToUpdateMVCC(ts, val) => write!(f, "Need to update MVCC: ts: {}, val: {:?}", ts, val),
            CuckooAccessMethodError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            CuckooAccessMethodError::Other(msg) => write!(f, "{}", msg),
            CuckooAccessMethodError::CuckooIterateFailed(u32) => write!(f, "cuckoo iterate failed!"),
        }
    }
}

impl std::error::Error for CuckooAccessMethodError {}