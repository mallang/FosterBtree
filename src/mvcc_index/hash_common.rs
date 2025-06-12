use core::fmt;
use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher, SipHasher},
    ops::Bound::{Excluded, Unbounded},
    sync::{
        atomic::{AtomicBool, AtomicU32},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};

use crate::{
    bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_debug, log_warn,
    page::{Page, PageId},
    prelude::Timestamp,
};

use super::hash_join_page::HashJoinPage;

pub(crate) const SUBTABLE_HASHER_SEED: u32 = 233;

pub(crate) const DEFAULT_BUCKET_NUM: usize = 128;
pub fn get_hashed_bucket_index(key: &[u8], total_size: u32) -> usize {
    (farmhash::hash32_with_seed(key, SUBTABLE_HASHER_SEED) % total_size) as usize
}

#[derive(Clone, Debug, PartialEq)]
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

/*
   tools used in scan_delta
*/
#[derive(Clone, Default, PartialEq, Debug)]
pub struct KVWithTs {
    k: Vec<u8>,
    v: Vec<u8>,
    start_ts: Timestamp,
}

impl KVWithTs {
    pub fn get_k(&self) -> &[u8] {
        &self.k[..]
    }
    pub fn get_v(&self) -> &[u8] {
        &self.v[..]
    }
    pub fn get_start_ts(&self) -> Timestamp {
        self.start_ts
    }

    pub fn cmp_and_swap(&mut self, new_st: Timestamp, new_k: &[u8], new_v: &[u8]) {
        if new_st >= self.start_ts {
            self.k = new_k.to_vec();
            self.v = new_v.to_vec();
            self.start_ts = new_st;
        }
    }

    pub fn set(&mut self, new_st: Timestamp, new_k: &[u8], new_v: &[u8]) {
        self.k = new_k.to_vec();
        self.v = new_v.to_vec();
        self.start_ts = new_st;
    }
}

#[derive(Clone, Default, PartialEq, Debug)]
pub struct RowDelta {
    from: KVWithTs,
    to: KVWithTs,
}

impl RowDelta {
    pub fn new() -> Self {
        Self {
            from: KVWithTs::default(),
            to: KVWithTs::default(),
        }
    }

    pub fn from(&mut self) -> &mut KVWithTs {
        &mut self.from
    }

    pub fn to(&mut self) -> &mut KVWithTs {
        &mut self.to
    }

    pub fn split(self) -> (KVWithTs, KVWithTs) {
        (self.from, self.to)
    }
}

pub fn read_repair_btree(
    mem_pool: &Arc<impl MemPool>,
    versions: &BTreeMap<Timestamp, (MvccEntryLoc, bool)>,
    c_key: ContainerKey,
) {
    for (ts, loc_and_is_need_repair) in versions.iter() {
        let (loc, is_need_repair) = loc_and_is_need_repair;
        if !*is_need_repair {
            continue;
        }
        let next_entry = versions.range((Excluded(*ts), Unbounded)).next();
        if let Some((next_ts, _)) = next_entry {
            let page_key = PageFrameKey::new(c_key, loc.page_id());
            let mut current_page = write_page(&**mem_pool, page_key);
            let slot =
                <Page as HashJoinPage>::unsafe_slot_mut(&mut current_page, loc.slot_id() as usize);
            slot.set_end_ts(*next_ts);
            let header = <Page as HashJoinPage>::unsafe_header_mut(&current_page);
            header.try_set_page_max_end_ts(*next_ts);
            // log_warn!(
            //     "[DEC slot: {}] page_id: {}, slot_cnt: {}",
            //     loc.slot_id(),
            //     loc.page_id(),
            //     header.recent_slot_cnt()
            // );
            header.dec_recent_slot_cnt();
        }
    }
}

pub fn read_repair_vec(
    mem_pool: &Arc<impl MemPool>,
    versions: &Vec<(Timestamp, MvccEntryLoc, bool)>,
    c_key: ContainerKey,
) {
    for idx in 0..versions.len() - 1 {
        let (_ts, loc_and_need_repair, is_need_repair) = &versions[idx];
        if !is_need_repair {
            continue;
        }
        let next_entry = &versions[idx + 1];
        let (next_ts, _, _) = next_entry;
        let page_key = PageFrameKey::new(c_key, loc_and_need_repair.page_id());
        let mut current_page = write_page(&**mem_pool, page_key);
        let slot = <Page as HashJoinPage>::unsafe_slot_mut(
            &mut current_page,
            loc_and_need_repair.slot_id() as usize,
        );
        assert!(slot.end_ts() == Timestamp::MAX);
        slot.set_end_ts(*next_ts);
        let header = <Page as HashJoinPage>::unsafe_header_mut(&current_page);
        header.try_set_page_max_end_ts(*next_ts);
        // log_warn!(
        //     "[DEC slot: {}] page_id: {}, slot_cnt: {}",
        //     loc_and_need_repair.slot_id(),
        //     loc_and_need_repair.page_id(),
        //     header.recent_slot_cnt()
        // );
        header.dec_recent_slot_cnt();
    }
}

pub struct BulkUpdate {
    flag: AtomicBool,
    updated_keys: Mutex<Vec<HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>>>,
}

impl BulkUpdate {
    pub fn new(bucket_num: usize) -> Self {
        let mut updated_keys_collection = Vec::new();
        for _ in 0..bucket_num {
            updated_keys_collection.push(HashMap::new());
        }
        Self {
            flag: AtomicBool::new(false),
            updated_keys: Mutex::new(updated_keys_collection),
        }
    }
    pub fn put_updated_pkeys(&self, pk: &[u8], idx: usize) {
        let mut x: MutexGuard<'_, Vec<HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>>> =
            self.updated_keys.lock().unwrap();
        x[idx].insert(pk.to_vec(), vec![]);
    }
    pub fn get_updated_pkeys(
        &self,
    ) -> MutexGuard<'_, Vec<HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>>> {
        self.updated_keys.lock().unwrap()
    }
    pub fn set_flag(&self) {
        self.flag.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn reset_flag(&self) {
        self.flag.store(false, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn get_flag(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Opportunistically try to fix the next page frame id
pub fn fix_frame_id<'a>(
    this: FrameReadGuard<'a>,
    new_pid: PageId,
    new_fid: u32,
) -> FrameReadGuard<'a> {
    log_debug!("Frame of the next page has been changed. Trying to fix the frame id");
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            write_guard.set_next_page(new_pid, new_fid);
            log_debug!("Fixed frame id of the next page");
            write_guard.downgrade()
        }
        Err(read_guard) => {
            log_debug!("Failed to fix frame id of the next page");
            read_guard
        }
    }
}

/// Opportunistically try to fix the next page frame id
pub fn fix_frame_id2<'a>(this: &mut FrameWriteGuard<'a>, new_frame_key: &PageFrameKey) {
    this.set_next_page(new_frame_key.p_key().page_id, new_frame_key.frame_id());
}