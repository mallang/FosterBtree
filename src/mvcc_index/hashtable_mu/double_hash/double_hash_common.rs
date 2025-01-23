use core::fmt;
use std::{
    hash::{Hash, Hasher, SipHasher},
    sync::{atomic::AtomicU32, Arc, Mutex},
};

use crate::{bp::MemPoolStatus, log_warn, page::PageId};

pub(crate) const SUBTABLE_HASHER_SEED: u32 = 233;

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


