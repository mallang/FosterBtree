use core::fmt;
use std::{
    hash::{Hash, Hasher, SipHasher},
    sync::{atomic::AtomicU32, Arc, Mutex},
};

use crate::{
    bp::MemPoolStatus, log_warn, mvcc_index::hashtable_mu::mvcc_hash_join_cuckoo::HASHER_KEYS,
    page::PageId,
};

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

    // assert have had a latch
    fn get_bucket_index(&self, key: &[u8], hasher_idx: usize) -> usize {
        let num_buckets = self.num_buckets;
        let mut hasher =
            SipHasher::new_with_keys(HASHER_KEYS[hasher_idx].0, HASHER_KEYS[hasher_idx].1);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % num_buckets as usize
    }

    pub fn get_bucket_index_random(&self, key: &[u8]) -> usize {
        let hasher_idx = (rand::random::<u8>() % 2) as usize;
        let num_buckets = self.num_buckets;
        let mut hasher =
            SipHasher::new_with_keys(HASHER_KEYS[hasher_idx].0, HASHER_KEYS[hasher_idx].1);
        key.hash(&mut hasher);
        (hasher.finish() as usize) % num_buckets as usize
    }

    // maybe NOT exist -> None
    pub fn get_a_second_bucket_index(&self, key: &[u8], first_idx: usize) -> Option<usize> {
        let num_buckets = self.num_buckets as usize;
        let bucket_idxs = HASHER_KEYS
            .iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(*key0, *key1);
                key.hash(&mut hasher);
                let hasher_result = hasher.finish() as usize;
                hasher_result
            })
            .collect::<Vec<_>>();

        // log_warn!("hash_result: {:?}, bucket_hash_result: {:?}", bucket_idxs, bucket_idxs.iter().map(|x| (x % num_buckets, x % (num_buckets * 2))).collect::<Vec<_>>() );

        for idx in bucket_idxs {
            if (idx % num_buckets) == first_idx {
                let second_idx = idx % (num_buckets * 2);
                if second_idx != first_idx {
                    return Some(second_idx);
                }
            }
        }

        None
    }

    pub fn get_all_bucket_index(&self, key: &[u8]) -> Vec<usize> {
        let num_buckets = self.num_buckets;
        let mut bucket_idxs = HASHER_KEYS
            .iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(*key0, *key1);
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
    InvalidTimestamp,               // For MVCC
    CuckooOutOfSpace(u32),          // new_hash_size
    AcquireLockFailed,              // for multi-page acq
    Other(String),
}

impl fmt::Display for CuckooAccessMethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CuckooAccessMethodError::KeyNotFound => write!(f, "Key not found"),
            CuckooAccessMethodError::KeyFoundButInvalidTimestamp => {
                write!(f, "Key found but invalid timestamp")
            }
            CuckooAccessMethodError::KeyDuplicate => write!(f, "Key duplicate"),
            CuckooAccessMethodError::KeyNotInPageRange => write!(f, "Key not in page range"),
            CuckooAccessMethodError::PageReadLatchFailed => write!(f, "Page read latch failed"),
            CuckooAccessMethodError::PageWriteLatchFailed => write!(f, "Page write latch failed"),
            CuckooAccessMethodError::RecordTooLarge => write!(f, "Record too large"),
            CuckooAccessMethodError::MemPoolStatus(status) => {
                write!(f, "MemPool status: {:?}", status)
            }
            CuckooAccessMethodError::OutOfSpace => write!(f, "Out of space"),
            CuckooAccessMethodError::OutOfSpaceForUpdate(key) => {
                write!(f, "Out of space for update: {:?}", key)
            }
            CuckooAccessMethodError::NeedToUpdateMVCC(ts, val) => {
                write!(f, "Need to update MVCC: ts: {}, val: {:?}", ts, val)
            }
            CuckooAccessMethodError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            CuckooAccessMethodError::Other(msg) => write!(f, "{}", msg),
            CuckooAccessMethodError::CuckooOutOfSpace(u32) => {
                write!(f, "cuckoo iterate failed!")
            }
            CuckooAccessMethodError::AcquireLockFailed => {
                write!(f, "cuckoo acquire page lock failed")
            }
        }
    }
}

impl std::error::Error for CuckooAccessMethodError {}

pub mod arcrwlock {
    use lock_api::GuardSend;
    use std::{
        ops::Deref,
        sync::{self, atomic::AtomicI16},
    };

    use crate::rwlatch::RwLatch;

    pub struct RawRwLock(RwLatch);
    impl Deref for RawRwLock {
        type Target = RwLatch;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    unsafe impl lock_api::RawRwLock for RawRwLock {
        type GuardMarker = GuardSend;
        const INIT: Self = RawRwLock(RwLatch {
            cnt: AtomicI16::new(0),
        });
        fn try_lock_exclusive(&self) -> bool {
            self.try_exclusive()
        }
        fn try_lock_shared(&self) -> bool {
            self.try_shared()
        }
        fn lock_exclusive(&self) {
            self.exclusive();
        }
        fn lock_shared(&self) {
            self.shared();
        }
        unsafe fn unlock_exclusive(&self) {
            self.release_exclusive();
        }
        unsafe fn unlock_shared(&self) {
            self.release_shared();
        }
    }

    pub type ArcRwlock<T> = sync::Arc<lock_api::RwLock<RawRwLock, T>>;
    pub type ArcRwlockReadGuard<T> = lock_api::ArcRwLockReadGuard<RawRwLock, T>;
    pub type ArcRwlockWriteGuard<T> = lock_api::ArcRwLockWriteGuard<RawRwLock, T>;

    pub fn new_arc_rw_lock<T>(instance: T) -> ArcRwlock<T> {
        return sync::Arc::new(lock_api::RwLock::<RawRwLock, T>::new(instance));
    }

    #[test]
    fn arc_rwlock_simple_test() {
        struct FinalStruct {
            buckets: ArcRwlock<i32>,
            buckets_read: ArcRwlockReadGuard<i32>,
        }

        impl FinalStruct {
            pub fn new(buckets: ArcRwlock<i32>) -> Self {
                let read = buckets.read_arc();
                return Self {
                    buckets,
                    buckets_read: read,
                };
            }
        }

        fn is_send<T: Send>() {}
        is_send::<ArcRwlockReadGuard<i32>>();
        let a = new_arc_rw_lock(1_i32);
        let c = a.clone();
        let b = FinalStruct::new(a);

        let handle = std::thread::spawn(move || {
            let try_result = c.try_write();
            assert!(try_result.is_none());
        });
        handle.join().unwrap();
        drop(b);
    }
}

// // guard is a non-Send version of RAII of LockManager
// // TODO: may optimize with FrameGuard
// pub struct LockManagerGuard {
//     lock_manager: Arc<Mutex<LockManager>>,
//     tid: TransactionId,
//     pid: ValueId,
// }

// impl LockManagerGuard {
//     pub fn new(lock_manager: &Arc<Mutex<LockManager>>, tid: TransactionId, pid: ValueId) -> Self {
//         Self {
//             lock_manager: lock_manager.clone(),
//             tid,
//             pid,
//         }
//     }
// }

// impl Drop for LockManagerGuard {
//     fn drop(&mut self) {
//         self.lock_manager
//             .lock()
//             .unwrap()
//             .release_lock(self.tid, self.pid)
//             .unwrap();
//     }
// }
