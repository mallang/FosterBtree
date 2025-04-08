use core::fmt;
use std::{
    hash::{Hash, Hasher, SipHasher},
    sync::{atomic::AtomicU32, Arc, Mutex},
    time::Duration,
};

use crate::{
    bp::{FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    page::PageId,
};

pub(crate) const SUBTABLE_HASHER_SEED: u32 = 233;

pub(crate) const DEFAULT_BUCKET_NUM: usize = 128;
pub fn get_hashed_bucket_index(key: &[u8], total_size: u32) -> usize {
    (farmhash::hash32_with_seed(key, SUBTABLE_HASHER_SEED) % total_size) as usize
}

pub struct MvccEntryLoc(PageId, u32);

impl MvccEntryLoc {
    pub fn new(page_id: PageId, slot_id: u32) -> Self {
        Self(page_id, slot_id)
    }
    pub fn page_id(&self) -> PageId {
        self.0
    }
    pub fn slot_id(&self) -> u32 {
        self.1
    }
}

#[derive(Default)]
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

// helper function
pub fn write_page<T: MemPool>(mem_pool: &T, page_key: PageFrameKey) -> FrameWriteGuard {
    loop {
        let page = mem_pool.get_page_for_write(page_key);
        // protected by LockManager, only acceptable error is "CannotEvictPage"
        match page {
            Ok(page) => {
                return page;
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                std::hint::spin_loop();
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
// helper function
pub fn try_write_page<T: MemPool + 'static>(
    mem_pool: &T,
    page_key: PageFrameKey,
) -> Option<FrameWriteGuard> {
    let page = mem_pool.get_page_for_write(page_key);
    match page {
        Ok(page) => return Some(page),
        Err(MemPoolStatus::CannotEvictPage) => {
            std::thread::sleep(Duration::from_millis(1));
            return None;
        }
        Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
            return None;
        }
        Err(e) => {
            panic!("Unexpected error: {:?}", e);
        }
    }
}
// helper function
pub fn read_page<T: MemPool + 'static>(mem_pool: &T, page_key: PageFrameKey) -> FrameReadGuard {
    loop {
        let page = mem_pool.get_page_for_read(page_key);
        match page {
            Ok(page) => {
                return page;
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                std::hint::spin_loop();
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
// helper function
pub fn try_read_page<T: MemPool + 'static>(
    mem_pool: &T,
    page_key: PageFrameKey,
) -> Option<FrameReadGuard> {
    let page = mem_pool.get_page_for_read(page_key);
    match page {
        Ok(page) => {
            return Some(page);
        }
        Err(MemPoolStatus::CannotEvictPage) => {
            std::thread::sleep(Duration::from_millis(1));
            return None;
        }
        Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
            std::hint::spin_loop();
            return None;
        }
        Err(e) => {
            panic!("Unexpected error: {:?}", e);
        }
    }
}
