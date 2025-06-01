use core::panic;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{self, AtomicU32, AtomicU64},
        Arc,
    },
    time::Duration,
    vec::IntoIter,
};

use dashmap::mapref::entry;

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_info, log_trace, log_warn,
    mvcc_index::{
        hash_join_page::{record::RecordRef, HashJoinPage},
        MvccEntry, TxId,
    },
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::{chained_hash_bucket_first::ChainBucketBulkUpdate, Timestamp};

pub struct ChainedHashRecentChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    first_page_id: PageId,
    first_frame_id: AtomicU32,

    last_page_id: AtomicU32,
    last_frame_id: AtomicU32,
}

impl<T: MemPool> ChainedHashRecentChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let first_page_id = page.get_id();
        let first_frame_id = page.frame_id();

        HashJoinPage::init(&mut *page);
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id,
            first_frame_id: AtomicU32::new(first_frame_id),
            last_page_id: AtomicU32::new(first_page_id),
            last_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, first_page_id: PageId) -> Self {
        Self {
            mem_pool,
            c_key,
            first_page_id,
            first_frame_id: AtomicU32::new(u32::MAX),
            last_page_id: AtomicU32::new(u32::MAX),
            last_frame_id: AtomicU32::new(u32::MAX),
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let space_need = <Page as HashJoinPage>::require_space(&entry);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        let last_page_id = self.last_page_id.load(atomic::Ordering::Acquire);
        let last_frame_id = self.last_frame_id.load(atomic::Ordering::Acquire);
        let last_page_frame_key =
            PageFrameKey::new_with_frame_id(self.c_key, last_page_id, last_frame_id);
        let mut last_page = self.traverse_until_endofchain_for_write(last_page_frame_key)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        match last_page.insert_recent_history(&rec, entry.start_ts(), entry.end_ts()) {
            Ok(_) => {
                if self.last_page_id.load(atomic::Ordering::Acquire) != last_page.get_id() {
                    self.last_page_id
                        .store(last_page.get_id(), atomic::Ordering::Release);
                    self.last_frame_id
                        .store(last_page.frame_id(), atomic::Ordering::Release);
                } else if self.last_frame_id.load(atomic::Ordering::Acquire) != last_page.frame_id()
                {
                    log_debug!(
                        "Frame of the last page has been changed. Trying to fix the frame id"
                    );
                    self.last_frame_id
                        .store(last_page.frame_id(), atomic::Ordering::Release);
                }
                Ok(())
            }
            Err(AccessMethodError::OutOfSpace) => {
                log_debug!(
                    "Not enough space in page {}. Creating a new page.",
                    last_page.get_id()
                );
                let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                new_page.init();
                last_page.set_next_page(new_page.get_id(), new_page.frame_id());
                log_trace!(
                    "Linked last page {} -> new page {}",
                    last_page.get_id(),
                    new_page.get_id()
                );
                self.last_page_id
                    .store(new_page.get_id(), atomic::Ordering::Release);
                self.last_frame_id
                    .store(new_page.frame_id(), atomic::Ordering::Release);
                match new_page.insert_recent_history(&rec, entry.start_ts(), entry.end_ts()) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn traverse_until_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let last_page = self.try_traverse_until_endofchain_for_write(page_key);
            match last_page {
                Ok(last_page) => {
                    return Ok(last_page);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_trace!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn try_traverse_until_endofchain_for_write(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                match current_page.try_upgrade(true) {
                    Ok(upgraded_page) => {
                        return Ok(upgraded_page);
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
        }
    }

    // pub fn insert_with_check(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
    //     let space_need = <Page as HashJoinPage>::require_space(&entry);
    //     if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
    //         return Err(AccessMethodError::RecordTooLarge);
    //     }
    //     let mut last_page = self.traverse_until_endofchain_for_insert(self.first_key(), entry)?;
    //     log_trace!("Acquired write lock for page {}", last_page.get_id());
    //     match last_page.insert(entry) {
    //         Ok(_) => Ok(()),
    //         Err(AccessMethodError::OutOfSpace) => {
    //             log_debug!(
    //                 "Not enough space in page {}. Creating a new page.",
    //                 last_page.get_id()
    //             );
    //             let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
    //             new_page.init();
    //             last_page.set_next_page(new_page.get_id(), new_page.frame_id());
    //             log_trace!(
    //                 "Linked last page {} -> new page {}",
    //                 last_page.get_id(),
    //                 new_page.get_id()
    //             );
    //             match new_page.insert(entry) {
    //                 Ok(_) => Ok(()),
    //                 Err(e) => Err(e),
    //             }
    //         }
    //         Err(e) => Err(e),
    //     }
    // }

    // fn traverse_until_endofchain_for_insert(
    //     &self,
    //     page_key: PageFrameKey,
    //     entry: &MvccEntry,
    // ) -> Result<FrameWriteGuard, AccessMethodError> {
    //     let base = 2;
    //     let mut attempts = 0;
    //     loop {
    //         let last_page = self.try_traverse_until_endofchain_for_insert(page_key, entry);
    //         match last_page {
    //             Ok(last_page) => {
    //                 return Ok(last_page);
    //             }
    //             Err(AccessMethodError::PageWriteLatchFailed) => {
    //                 attempts += 1;
    //                 log_trace!(
    //                     "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
    //                     attempts,
    //                     u64::pow(base, attempts)
    //                 );
    //                 std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
    //             }
    //             Err(AccessMethodError::KeyDuplicate) => {
    //                 return Err(AccessMethodError::KeyDuplicate);
    //             }
    //             Err(e) => {
    //                 panic!("Unexpected error: {:?}", e);
    //             }
    //         }
    //     }
    // }

    // fn try_traverse_until_endofchain_for_insert(
    //     &self,
    //     page_key: PageFrameKey,
    //     entry: &MvccEntry,
    // ) -> Result<FrameWriteGuard, AccessMethodError> {
    //     let mut current_page = self.read_page(page_key);
    //     loop {
    //         if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
    //             if current_page.binary_search(entry.search_key()).0 {
    //                 return Err(AccessMethodError::KeyDuplicate);
    //             }

    //             // TODO: check free space may can insert here later.
    //             let next_page = self.read_page(PageFrameKey::new_with_frame_id(
    //                 self.c_key,
    //                 next_page_id,
    //                 next_frame_id,
    //             ));
    //             if next_page.frame_id() != next_frame_id {
    //                 log_debug!(
    //                     "Frame of the next page has been changed. Trying to fix the frame id"
    //                 );
    //                 let new_frame_key = PageFrameKey::new_with_frame_id(
    //                     self.c_key,
    //                     next_page_id,
    //                     next_page.frame_id(),
    //                 );
    //                 let _ = fix_frame_id(current_page, &new_frame_key);
    //             }
    //             current_page = next_page;
    //         } else {
    //             // TODO: check key to avoid write lock in case of duplicate key
    //             match current_page.try_upgrade(true) {
    //                 Ok(upgraded_page) => {
    //                     return Ok(upgraded_page);
    //                 }
    //                 Err(_) => {
    //                     log_debug!("Failed to upgrade the page. Will retry");
    //                     return Err(AccessMethodError::PageWriteLatchFailed);
    //                 }
    //             }
    //         }
    //     }
    // }

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            match current_page.get(pkey, ts) {
                Ok(entry) => {
                    return Ok(entry);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                        let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                            self.c_key,
                            next_page_id,
                            next_frame_id,
                        ));
                        if next_page.frame_id() != next_frame_id {
                            log_debug!("Frame of the next page has been changed. Trying to fix the frame id");
                            let new_frame_key = PageFrameKey::new_with_frame_id(
                                self.c_key,
                                next_page_id,
                                next_page.frame_id(),
                            );
                            let _ = fix_frame_id(current_page, &new_frame_key);
                        }
                        current_page = next_page;
                    } else {
                        return Err(AccessMethodError::KeyNotFound);
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    pub fn update(&self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError> {
        match self.traverse_to_endofchain_for_update(self.first_key(), pkey, entry) {
            Ok(old_entry) => {
                return Ok(old_entry);
            }
            Err(AccessMethodError::KeyNotFound) => {
                return Err(AccessMethodError::KeyNotFound);
            }
            Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                self.insert(entry).expect("Insert should succeed");
                return Ok(old_entry);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    pub fn do_bulk_update(&self, bulk: &mut ChainBucketBulkUpdate, new_start_ts: Timestamp) -> Result<(), AccessMethodError> {
        match self.traverse_to_endofchain_for_bulk_update(self.first_key(), bulk, new_start_ts) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    fn try_traverse_to_endofchain_for_bulk_update(
        &self,
        page_key: PageFrameKey,
        bulk: &mut ChainBucketBulkUpdate,
        new_start_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.write_page(page_key);
        loop {
            current_page.chain_bulk_update_slots_recent(bulk, new_start_ts);

            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.write_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id2(&mut current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                return Ok(());
            }
        }
    }

    fn traverse_to_endofchain_for_bulk_update(
        &self,
        page_key: PageFrameKey,
        bulk: &mut ChainBucketBulkUpdate,
        new_start_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page = self.try_traverse_to_endofchain_for_bulk_update(page_key, bulk, new_start_ts);
            match find_page {
                Ok(_) => {
                    return Ok(());
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_info!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                    log_debug!(
                        "Should not happen in YCSB workload. key({}) old_value({})",
                        pkey,
                        old_val
                    );
                    return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                }
                Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                    return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
                }
                Err(e) => {
                    log_debug!("Error while traverse for upadate: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    fn traverse_to_endofchain_for_update(
        &self,
        page_key: PageFrameKey,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<MvccEntry, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page = self.try_traverse_to_endofchain_for_update(page_key, pkey, entry);
            match find_page {
                Ok(old_entry) => {
                    return Ok(old_entry);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_info!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                    log_debug!(
                        "Should not happen in YCSB workload. key({}) old_value({})",
                        pkey,
                        old_val
                    );
                    return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                }
                Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                    return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
                }
                Err(e) => {
                    log_debug!("Error while traverse for upadate: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    fn try_traverse_to_endofchain_for_update(
        &self,
        page_key: PageFrameKey,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        loop {
            let (found, slot_id) = current_page.search_slot(pkey);
            if found {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => {
                        match upgraded_page.update_at_slot_id(
                            &rec,
                            entry.start_ts(),
                            entry.end_ts(),
                            slot_id,
                        ) {
                            Ok(old_entry) => {
                                return Ok(old_entry);
                            }
                            Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                                log_debug!("Not enough space in page {}. Delete the key({}) and old_value({}), then insert updated key to next page", upgraded_page.get_id(), pkey, old_val);
                                return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                            }
                            Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                                log_debug!("Not enough space in page {}. Delete the old entry({}) in the page, then insert updated new entry({}) to next page", upgraded_page.get_id(), old_entry, entry);
                                return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
                            }
                            Err(e) => {
                                return Err(e);
                            }
                        }
                    }
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                log_debug!("Key({}) not found for update.", pkey);
                return Err(AccessMethodError::KeyNotFound);
            }
        }
    }

    pub fn delete(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        self.traverse_to_endofchain_for_delete(self.first_key(), pkey, ts)
    }

    fn traverse_to_endofchain_for_delete(
        &self,
        page_key: PageFrameKey,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page = self.try_traverse_to_endofchain_for_delete(page_key, pkey, ts);
            match find_page {
                Ok(old_entry) => {
                    return Ok(old_entry);
                }
                Err(AccessMethodError::PageWriteLatchFailed) => {
                    attempts += 1;
                    log_info!(
                        "Failed to acquire write lock (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                }
                Err(AccessMethodError::OutOfSpaceForUpdate(old_val)) => {
                    log_debug!(
                        "Should not happen in YCSB workload. key({}) old_value({})",
                        pkey,
                        old_val
                    );
                    return Err(AccessMethodError::OutOfSpaceForUpdate(old_val));
                }
                Err(e) => {
                    log_debug!("Error while traverse for upadate: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    fn try_traverse_to_endofchain_for_delete(
        &self,
        page_key: PageFrameKey,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            let (found, slot_id) = current_page.search_slot(pkey);
            if found {
                match current_page.try_upgrade(true) {
                    Ok(mut upgraded_page) => match upgraded_page.delete_at_slot_id(ts, slot_id) {
                        Ok(old_entry) => {
                            return Ok(old_entry);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    },
                    Err(_) => {
                        log_debug!("Failed to upgrade the page. Will retry");
                        return Err(AccessMethodError::PageWriteLatchFailed);
                    }
                }
            }
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
                if next_page.frame_id() != next_frame_id {
                    log_debug!(
                        "Frame of the next page has been changed. Trying to fix the frame id"
                    );
                    let new_frame_key = PageFrameKey::new_with_frame_id(
                        self.c_key,
                        next_page_id,
                        next_page.frame_id(),
                    );
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                log_debug!("Key({}) not found for update.", pkey);
                return Err(AccessMethodError::KeyNotFound);
            }
        }
    }

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id
    }

    pub fn first_frame_id(&self) -> u32 {
        self.first_frame_id.load(atomic::Ordering::Acquire)
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        let base = 2;
        let mut attempts = 0;
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    attempts += 1;
                    log_info!(
                        "Failed to acquire read latch (#attempt {}). Sleeping for {:?}",
                        attempts,
                        u64::pow(base, attempts)
                    );
                    std::thread::sleep(Duration::from_nanos(u64::pow(base, attempts)));
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.mem_pool.get_page_for_write(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    log_warn!(
                        "Exclusive page latch grant failed: {:?}. Will retry",
                        page_key
                    );
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to write the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    pub fn first_key(&self) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(
            self.c_key,
            self.first_page_id,
            self.first_frame_id
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    fn first_page(&self) -> FrameReadGuard {
        let first_frame_id = self
            .first_frame_id
            .load(std::sync::atomic::Ordering::Acquire);
        let first_page = self.read_page(PageFrameKey::new_with_frame_id(
            self.c_key,
            self.first_page_id,
            first_frame_id,
        ));
        if first_page.frame_id() != first_frame_id {
            log_debug!("Frame of the first page has been changed. Trying to fix the frame id");
            self.first_frame_id
                .store(first_page.frame_id(), std::sync::atomic::Ordering::Release);
        }
        first_page
    }

    // pub fn scan(
    //     &self,
    //     ts: Timestamp,
    // ) -> Result<MvccHashJoinRecentChainScanner<T>, AccessMethodError> {
    //     Ok(MvccHashJoinRecentChainScanner::new(
    //         Arc::new(self.clone()),
    //         ts,
    //     ))
    // }
    pub fn scan(self: &Arc<Self>, ts: Timestamp) -> ChainedHashRecentChainScanner<T> {
        ChainedHashRecentChainScanner::new(self, ts)
    }

    pub fn scan_all(self: &Arc<Self>) -> ChainedHashRecentChainScanner<T> {
        ChainedHashRecentChainScanner::new_full_scan(self)
    }

    /// Scan the entire chain for MvccEntries whose logical key == `search_key`
    /// that are visible at time `ts`.
    /// We call each page’s `scan_key_recent` method to do the local scan,
    /// gather the results, and move on to the next page.
    pub fn scan_key_into(&self, search_key: &[u8], ts: &Timestamp, results: &mut Vec<MvccEntry>) {
        let mut current_page = self.first_page();

        loop {
            current_page.scan_key_recent_into(search_key, ts, results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key, next_pid, next_fid,
                ));
                if next_page.frame_id() != next_fid {
                    let new_frame_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_page.frame_id());
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                break;
            }
        }
    }

    pub fn scan_into_vec(
        &self,
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            current_page.scan_recent_into(ts, results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key, next_pid, next_fid,
                ));
                if next_page.frame_id() != next_fid {
                    let new_frame_key =
                        PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_page.frame_id());
                    let _ = fix_frame_id(current_page, &new_frame_key);
                }
                current_page = next_page;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Traverse the chain and return a human‑readable status string.
    ///
    /// The report includes, for each page:
    /// - The page ID.
    /// - The number of slots (key–value pairs) on the page.
    /// - The page’s own statistics (e.g. usage, free space before compaction, etc.).
    ///
    /// Finally, a summary with the total number of pages and total kv count is appended.
    pub fn stat(&self) -> String {
        let mut stat_str = String::new();
        let mut page_count = 0;
        let mut total_kvs = 0;

        // Start with the first page.
        let mut current_page = self.first_page();
        loop {
            page_count += 1;
            let kv_count = current_page.slot_count();
            total_kvs += kv_count;
            // Here, current_page.stat() is assumed to return a human‑readable string for that page.
            stat_str.push_str(&format!("{}\n", current_page.stat()));

            // Traverse to the next page if available.
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                current_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key,
                    next_page_id,
                    next_frame_id,
                ));
            } else {
                break;
            }
        }
        stat_str.push_str(&format!(
            "Total pages: {}, total kv count: {}\n",
            page_count, total_kvs
        ));
        stat_str
    }

    /// Returns a tuple: (page_count, total_kv_count, usage_sum, max_usage, min_usage)
    pub fn summary_metrics(&self) -> (usize, usize, f64, f64, f64) {
        let mut page_count = 0;
        let mut total_kv_count = 0;
        let mut usage_sum = 0.0;
        let mut max_usage: f64 = 0.0;
        let mut min_usage = f64::MAX;
        let mut current_page = self.first_page();
        loop {
            page_count += 1;
            let kv = current_page.slot_count();
            total_kv_count += kv;
            let used_bytes = current_page.unsafe_header().total_bytes_used();
            let usage = (used_bytes as f64 / AVAILABLE_PAGE_SIZE as f64) * 100.0;
            usage_sum += usage;
            max_usage = max_usage.max(usage as f64);
            min_usage = min_usage.min(usage as f64);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                current_page = self.read_page(PageFrameKey::new_with_frame_id(
                    self.c_key, next_pid, next_fid,
                ));
            } else {
                break;
            }
        }
        (page_count, total_kv_count, usage_sum, max_usage, min_usage)
    }
}

/// Opportunistically try to fix the next page frame id
fn fix_frame_id<'a>(this: FrameReadGuard<'a>, new_frame_key: &PageFrameKey) -> FrameReadGuard<'a> {
    match this.try_upgrade(true) {
        Ok(mut write_guard) => {
            write_guard.set_next_page(new_frame_key.p_key().page_id, new_frame_key.frame_id());
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
fn fix_frame_id2<'a>(this: &mut FrameWriteGuard<'a>, new_frame_key: &PageFrameKey) {
    this.set_next_page(new_frame_key.p_key().page_id, new_frame_key.frame_id());
}

// Implement Clone for MvccHashJoinRecentChain to allow cloning
impl<T: MemPool> Clone for ChainedHashRecentChain<T> {
    fn clone(&self) -> Self {
        Self {
            mem_pool: Arc::clone(&self.mem_pool),
            c_key: self.c_key,
            first_page_id: self.first_page_id,
            first_frame_id: AtomicU32::new(
                self.first_frame_id
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
            last_page_id: AtomicU32::new(
                self.last_page_id.load(std::sync::atomic::Ordering::Acquire),
            ),
            last_frame_id: AtomicU32::new(
                self.last_frame_id
                    .load(std::sync::atomic::Ordering::Acquire),
            ),
        }
    }
}

pub struct ChainedHashRecentChainScanner<T: MemPool> {
    chain: Arc<ChainedHashRecentChain<T>>,
    ts: Timestamp,

    current_page: Option<FrameReadGuard<'static>>,
    current_slot_id: usize,

    initialized: bool,
    finished: bool,
}

impl<T: MemPool> ChainedHashRecentChainScanner<T> {
    pub fn new(chain: &Arc<ChainedHashRecentChain<T>>, ts: Timestamp) -> Self {
        Self {
            chain: chain.clone(),
            ts,
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
        }
    }

    pub fn new_full_scan(chain: &Arc<ChainedHashRecentChain<T>>) -> Self {
        Self {
            chain: chain.clone(),
            ts: u64::MAX - 1,
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
        }
    }

    fn initialize(&mut self) {
        let first_page = self.chain.first_page();
        let first_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(first_page) };
        self.current_page = Some(first_page);
        self.current_slot_id = 0;
        self.initialized = true;
    }

    fn finish(&mut self) {
        self.finished = true;
        self.current_page = None;
    }
}

impl<T: MemPool> Iterator for ChainedHashRecentChainScanner<T> {
    type Item = MvccEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized {
            self.initialize();
        }

        if self.finished {
            return None;
        }

        loop {
            if self.current_page.is_none() {
                self.finish();
                return None;
            }

            let current_page = self.current_page.as_ref().unwrap();
            if self.current_slot_id < current_page.slot_count() {
                match current_page.get_entry_at_slot_id(self.current_slot_id) {
                    Ok(entry) => {
                        self.current_slot_id += 1;
                        if self.ts < entry.start_ts || entry.end_ts <= self.ts {
                            continue;
                        }
                        return Some(entry);
                    }
                    Err(_) => {
                        panic!("Unexpected error while reading the entry");
                    }
                }
            } else if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page: FrameReadGuard<'_> = self.chain.read_page(
                    PageFrameKey::new_with_frame_id(self.chain.c_key, next_pid, next_fid),
                );
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(next_page)
                };
                self.current_page = Some(next_page);
                self.current_slot_id = 0;
                continue;
            } else {
                self.finish();
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
    };

    /// Generates `n` random MvccEntry instances.
    /// - `n`: number of entries to generate.
    /// - `key_size`: size in bytes of the key.
    /// - `pkey_size`: size in bytes of the primary key (pkey).  
    ///               This implementation ensures that each pkey is unique by using the index.
    /// - `value_size`: nominal size in bytes of the value; actual size will vary by ±20%.
    pub fn generate_random_recent_mvcc_entries(
        n: usize,
        key_size: usize,
        pkey_size: usize,
        value_size: usize,
    ) -> Vec<MvccEntry> {
        let mut rng = thread_rng();
        let mut entries = Vec::with_capacity(n);

        for i in 0..n {
            // Generate key as a random printable ASCII string of the specified length.
            let key: Vec<u8> = (0..key_size).map(|_| rng.gen_range(33u8..127u8)).collect();

            // Generate a unique primary key.
            // We use a fixed prefix ("pkey-") and append the index, formatted to fill the remainder.
            let prefix = "pkey-";
            let prefix_len = prefix.len();
            let num_width = if pkey_size > prefix_len {
                pkey_size - prefix_len
            } else {
                0
            };
            // If there is room for digits, format the index with leading zeros.
            let pkey_str = if num_width > 0 {
                format!("{}{:0>width$}", prefix, i, width = num_width)
            } else {
                // If pkey_size is too small to include the prefix and any digits, just use the prefix.
                prefix.to_string()
            };
            let pkey = pkey_str.into_bytes();

            // Determine the actual value size within ±20% of the nominal value_size.
            let lower = (value_size as f64 * 0.8).ceil() as usize;
            let upper = (value_size as f64 * 1.2).floor() as usize;
            let actual_value_size = rng.gen_range(lower..=upper);
            let value: Vec<u8> = (0..actual_value_size)
                .map(|_| rng.gen_range(33u8..127u8))
                .collect();

            // Generate timestamps: start_ts in [1000, 10000) and a random duration in [1, 100)
            let start_ts: u64 = rng.gen_range(1000..10000);
            let duration: u64 = rng.gen_range(1..100);
            // let end_ts = start_ts + duration;
            let end_ts = u64::MAX;

            let entry = MvccEntry::new(key, pkey, value, start_ts, end_ts);
            entries.push(entry);
        }

        entries
    }

    #[test]
    fn test_recent_chain_insert_and_get() {
        // Create a new chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Create an entry.
        let key = b"test-key".to_vec();
        let pkey = key.clone();
        let value = b"initial".to_vec();
        let entry = MvccEntry::new(key, pkey.clone(), value.clone(), 100, 200);

        // Insert the entry.
        chain.insert(&entry).expect("Insert should succeed");

        // Get the entry back.
        let fetched = chain.get(&pkey, &100).expect("Get should succeed");
        assert_eq!(
            fetched.value(),
            value.as_slice(),
            "Fetched value should equal inserted value"
        );
    }

    #[test]
    fn test_recent_chain_update() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(1, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        let key = b"update-key".to_vec();
        let pkey = key.clone();
        let initial_value = b"old-value".to_vec();
        let entry = MvccEntry::new(key, pkey.clone(), initial_value.clone(), 100, 200);

        // Insert the original entry.
        chain.insert(&entry).expect("Insert should succeed");

        // Update the entry with a new value.
        let new_value = b"new-value".to_vec();
        let updated_entry = MvccEntry::new(pkey.clone(), pkey.clone(), new_value.clone(), 100, 200);
        let old_entry = chain
            .update(&pkey, &updated_entry)
            .expect("Update should succeed");

        // The update method returns the previous version.
        assert_eq!(
            old_entry.value(),
            initial_value.as_slice(),
            "Old value should be returned on update"
        );

        // Verify that get now returns the updated value.
        let fetched = chain.get(&pkey, &100).expect("Get should succeed");
        assert_eq!(
            fetched.value(),
            new_value.as_slice(),
            "Fetched value should equal updated value"
        );
    }

    #[test]
    fn test_recent_chain_delete() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(2, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        let key = b"delete-key".to_vec();
        let pkey = key.clone();
        let value = b"to-delete".to_vec();
        let entry = MvccEntry::new(key, pkey.clone(), value.clone(), 100, 200);

        chain.insert(&entry).expect("Insert should succeed");

        // Delete the entry.
        let deleted = chain.delete(&pkey, &100).expect("Delete should succeed");
        assert_eq!(
            deleted.value(),
            value.as_slice(),
            "Deleted entry should match the original"
        );

        // Verify that a subsequent get returns an error.
        let res = chain.get(&pkey, &100);
        assert!(res.is_err(), "After deletion, get should return an error");
    }

    /// Test that duplicate primary keys are not allowed.
    #[ignore = "reason"]
    #[test]
    fn test_recent_chain_duplicate_pkey_insertion() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(4, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Insert an entry with a given pkey.
        let key1 = b"key-A".to_vec();
        let pkey = b"unique-pkey".to_vec();
        let entry1 = MvccEntry::new(key1, pkey.clone(), b"value-A".to_vec(), 100, 200);
        chain.insert(&entry1).expect("First insert should succeed");

        // Attempt to insert another entry with a different key but the same primary key.
        let key2 = b"key-B".to_vec();
        let entry2 = MvccEntry::new(key2, pkey.clone(), b"value-B".to_vec(), 100, 200);
        let res = chain.insert(&entry2);
        assert!(
            res.is_err(),
            "Duplicate pkey insertion should fail, but it succeeded"
        );
        if let Err(e) = res {
            // Here we expect the error to be KeyDuplicate.
            assert_eq!(
                e,
                AccessMethodError::KeyDuplicate,
                "Expected KeyDuplicate error for duplicate pkey"
            );
        }
    }

    /// Test that duplicate keys with different primary keys are allowed.
    #[test]
    fn test_recent_chain_duplicate_key_allowed() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(5, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Insert first entry using key "same-key" but with pkey "pkey-A".
        let key = b"same-key".to_vec();
        let pkey1 = b"pkey-A".to_vec();
        let entry1 = MvccEntry::new(key.clone(), pkey1.clone(), b"value-1".to_vec(), 100, 200);
        chain.insert(&entry1).expect("First insert should succeed");

        // Insert second entry with the same key but a different primary key "pkey-B".
        let pkey2 = b"pkey-B".to_vec();
        let entry2 = MvccEntry::new(key, pkey2.clone(), b"value-2".to_vec(), 100, 200);
        chain
            .insert(&entry2)
            .expect("Insert with duplicate key but different pkey should succeed");

        // Verify that each record can be retrieved using its respective primary key.
        let fetched1 = chain
            .get(&pkey1, &100)
            .expect("Get should succeed for primary key pkey-A");
        assert_eq!(
            fetched1.value(),
            b"value-1".as_ref(),
            "Fetched value for pkey-A does not match"
        );
        let fetched2 = chain
            .get(&pkey2, &100)
            .expect("Get should succeed for primary key pkey-B");
        assert_eq!(
            fetched2.value(),
            b"value-2".as_ref(),
            "Fetched value for pkey-B does not match"
        );
    }

    /// Additional tests for basic recent chain operations (insert, get, update, delete, and scanning).
    #[test]
    fn test_recent_chain_basic_operations() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Insert an entry.
        let key = b"test-key".to_vec();
        let pkey = key.clone();
        let value = b"initial".to_vec();
        let entry = MvccEntry::new(key, pkey.clone(), value.clone(), 100, 200);
        chain.insert(&entry).expect("Insert should succeed");

        // Get the entry.
        let fetched = chain.get(&pkey, &100).expect("Get should succeed");
        assert_eq!(
            fetched.value(),
            value.as_slice(),
            "Fetched value should equal inserted value"
        );

        // Update the entry.
        let new_value = b"updated".to_vec();
        let updated_entry = MvccEntry::new(pkey.clone(), pkey.clone(), new_value.clone(), 100, 200);
        let old_entry = chain
            .update(&pkey, &updated_entry)
            .expect("Update should succeed");
        assert_eq!(
            old_entry.value(),
            value.as_slice(),
            "Update should return the old value"
        );
        let fetched_updated = chain
            .get(&pkey, &100)
            .expect("Get should succeed after update");
        assert_eq!(
            fetched_updated.value(),
            new_value.as_slice(),
            "Fetched value should equal updated value"
        );

        // Delete the entry.
        let deleted = chain.delete(&pkey, &100).expect("Delete should succeed");
        assert_eq!(
            deleted.value(),
            new_value.as_slice(),
            "Deleted entry should equal the updated value"
        );
        let get_after_delete = chain.get(&pkey, &100);
        assert!(
            get_after_delete.is_err(),
            "After deletion, get should return an error"
        );
    }

    /// Test that many random entries can be inserted into the recent chain,
    /// that the chain spans multiple pages, and that every inserted entry is retrievable.
    #[test]
    fn test_chain_multiple_random_insert_get() {
        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(10, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Generate a number of random entries.
        // Adjust NUM_ENTRIES to force the chain to span multiple pages.
        const NUM_ENTRIES: usize = 100;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert every generated entry.
        // In the recent chain, duplicate primary keys are not allowed.
        // (This generator produces random pkeys; collisions are extremely unlikely.)
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }

        // Print chain statistics.
        // let stat_str = chain.stat();
        // println!("Chain statistics:\n{}", stat_str);
        // assert!(
        //     stat_str.contains("Total pages:"),
        //     "Chain stat() should include total pages info"
        // );

        // For each inserted entry, try to retrieve it using a timestamp
        // that is in its valid range (here we use the mid-point of [start_ts, end_ts)).
        for (i, entry) in entries.iter().enumerate() {
            let query_ts = entry.start_ts();
            let fetched = chain
                .get(entry.pkey(), &query_ts)
                .expect(&format!("Get failed for entry {}", i));
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for entry {}",
                i
            );
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for entry {}",
                i
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for entry {}",
                i
            );
        }
    }

    /// Test that many random entries can be inserted into the recent chain,
    /// then updated so that each entry's value becomes 10 times bigger,
    /// and finally verified via get().
    #[test]
    fn test_chain_multiple_random_insert_update_get() {
        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(10, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Generate a number of random entries.
        // Increase NUM_ENTRIES to force the chain to span multiple pages.
        const NUM_ENTRIES: usize = 100;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert every generated entry.
        // In the recent chain, duplicate primary keys are not allowed.
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }

        // let stat_str = chain.stat();
        // println!("Chain statistics after insert and before update:\n{}", stat_str);
        // assert!(
        //     stat_str.contains("Total pages:"),
        //     "Chain stat() should include total pages info"
        // );

        // Now update every inserted entry: new value will be 10 times bigger.
        for (i, entry) in entries.iter().enumerate() {
            // Create a new value that is 10 times bigger than the original.
            let new_value = entry.value().repeat(10);
            // Construct the updated entry with the same key, same primary key, and same timestamps.
            let updated_entry = MvccEntry::new(
                entry.key().to_vec(),
                entry.pkey().to_vec(),
                new_value.clone(),
                entry.start_ts(),
                entry.end_ts(),
            );
            // Call update on the chain.
            let old_entry = chain
                .update(entry.pkey(), &updated_entry)
                .expect(&format!("Update failed for entry {}", i));
            // Verify that the update returns the old value.
            assert_eq!(
                old_entry.value(),
                entry.value(),
                "Old value mismatch for entry {}",
                i
            );
        }

        // Finally, verify that all entries have been updated.
        for (i, entry) in entries.iter().enumerate() {
            // Choose a query timestamp that lies in the valid range.
            let query_ts = entry.start_ts();
            let fetched = chain
                .get(entry.pkey(), &query_ts)
                .expect(&format!("Get failed for entry {} after update", i));
            let expected_new_value = entry.value().repeat(10);
            assert_eq!(
                fetched.value(),
                expected_new_value.as_slice(),
                "Updated value mismatch for entry {}",
                i
            );
            // Optionally, verify that the timestamps remain unchanged.
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for entry {}",
                i
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for entry {}",
                i
            );
        }

        // let stat_str = chain.stat();
        // println!("Chain statistics after update:\n{}", stat_str);
        // assert!(
        //     stat_str.contains("Total pages:"),
        //     "Chain stat() should include total pages info"
        // );
    }

    /// Test multiple random insert, delete, and update operations on the recent chain.
    ///
    /// In this test:
    /// 1. We generate many random entries and insert them into the chain.
    /// 2. We delete entries at even indices (using a query timestamp in the valid range).
    /// 3. We update entries at odd indices so that the new value is 10× larger.
    /// 4. Finally, we verify that deleted entries are no longer retrievable,
    ///    and that updated entries return the new (10× larger) value.
    #[test]
    fn test_chain_multiple_random_insert_delete_update_get() {
        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(11, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, Arc::clone(&mem_pool)));

        // Generate random entries.
        const NUM_ENTRIES: usize = 100;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert all entries.
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }

        // let stat_str = chain.stat();
        // println!("Chain statistics after insert:\n{}", stat_str);
        // assert!(
        //     stat_str.contains("Total pages:"),
        //     "Chain stat() should include total pages info"
        // );

        // For each entry, use a query timestamp in its valid range (midpoint of [start_ts, end_ts)).
        // Delete even-indexed entries; update odd-indexed entries.
        for (i, entry) in entries.iter().enumerate() {
            let query_ts = entry.start_ts();
            if i % 2 == 0 {
                // Delete the entry.
                let deleted = chain
                    .delete(entry.pkey(), &query_ts)
                    .expect(&format!("Delete failed for entry {}", i));
                // Verify that the deleted value matches the original.
                assert_eq!(
                    deleted.value(),
                    entry.value(),
                    "Deleted value mismatch for entry {}",
                    i
                );
            } else {
                // Update the entry: new value will be 10 times bigger.
                let new_value = entry.value().repeat(10);
                let updated_entry = MvccEntry::new(
                    entry.key().to_vec(),
                    entry.pkey().to_vec(),
                    new_value.clone(),
                    entry.start_ts(),
                    entry.end_ts(),
                );
                let old_entry = chain
                    .update(entry.pkey(), &updated_entry)
                    .expect(&format!("Update failed for entry {}", i));
                // Verify that the update returns the old value.
                assert_eq!(
                    old_entry.value(),
                    entry.value(),
                    "Old value mismatch for entry {}",
                    i
                );
            }
        }

        // Finally, verify the final state of each entry.
        for (i, entry) in entries.iter().enumerate() {
            let query_ts = entry.start_ts();
            if i % 2 == 0 {
                // Deleted entries should no longer be retrievable.
                let res = chain.get(entry.pkey(), &query_ts);
                assert!(
                    res.is_err(),
                    "Expected get() to fail for deleted entry {}",
                    i
                );
            } else {
                // Updated entries should return the new value.
                let fetched = chain
                    .get(entry.pkey(), &query_ts)
                    .expect(&format!("Get failed for entry {} after update", i));
                let expected_new_value = entry.value().repeat(10);
                assert_eq!(
                    fetched.value(),
                    expected_new_value.as_slice(),
                    "Updated value mismatch for entry {}",
                    i
                );
                // Optionally, verify that the timestamps remain unchanged.
                assert_eq!(
                    fetched.start_ts(),
                    entry.start_ts(),
                    "Start timestamp mismatch for entry {}",
                    i
                );
                assert_eq!(
                    fetched.end_ts(),
                    entry.end_ts(),
                    "End timestamp mismatch for entry {}",
                    i
                );
            }
        }

        // let stat_str = chain.stat();
        // println!("Chain statistics after delete and update:\n{}", stat_str);
        // assert!(
        //     stat_str.contains("Total pages:"),
        //     "Chain stat() should include total pages info"
        // );
    }

    #[test]
    fn test_chain_concurrent_inserts_and_reads_with_concurrent_inserts() {
        use std::thread;

        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));

        // Generate random entries.
        const NUM_ENTRIES: usize = 1000;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200;
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Wrap the entries in an Arc so they can be shared among threads.
        let entries_arc = Arc::new(entries);

        // Partition entries among multiple insert threads.
        let num_insert_threads = 5;
        let chunk_size = (NUM_ENTRIES + num_insert_threads - 1) / num_insert_threads; // ceiling division
        let mut insert_handles = Vec::new();
        for t in 0..num_insert_threads {
            let chain_for_insert = chain.clone();
            let entries_for_insert = Arc::clone(&entries_arc);
            let start_idx = t * chunk_size;
            let end_idx = ((t + 1) * chunk_size).min(NUM_ENTRIES);
            let handle = thread::spawn(move || {
                for i in start_idx..end_idx {
                    let entry = &entries_for_insert[i];
                    chain_for_insert
                        .insert(entry)
                        .expect(&format!("Insert failed for entry {}", i));
                }
            });
            insert_handles.push(handle);
        }

        // Spawn several reader threads to concurrently perform get() operations.
        let num_reader_threads = 5;
        let mut read_handles = Vec::new();
        for _ in 0..num_reader_threads {
            let chain_for_reads = chain.clone();
            let entries_for_reads = Arc::clone(&entries_arc);
            let handle = thread::spawn(move || {
                // Each reader thread loops several times over all entries.
                for _ in 0..10 {
                    for entry in entries_for_reads.iter() {
                        let query_ts = entry.start_ts();
                        // It's acceptable that some get() calls fail if the insert hasn't happened yet.
                        let _ = chain_for_reads.get(entry.pkey(), &query_ts);
                    }
                }
            });
            read_handles.push(handle);
        }

        // Wait for all insert threads to finish.
        for handle in insert_handles {
            handle.join().unwrap();
        }

        // Wait for all reader threads to finish.
        for handle in read_handles {
            handle.join().unwrap();
        }

        // Finally, verify that every inserted entry is retrievable.
        for (i, entry) in entries_arc.iter().enumerate() {
            let query_ts = entry.start_ts();
            let fetched = chain
                .get(entry.pkey(), &query_ts)
                .expect(&format!("Get failed for entry {}", i));
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for entry {}",
                i
            );
            // Optionally, you can also check that the timestamps match.
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for entry {}",
                i
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for entry {}",
                i
            );
        }
        // let stat_str = chain.stat();
        // println!("Chain statistics after concurrent inserts and reads:\n{}", stat_str);
    }

    #[test]
    fn test_chain_concurrent_updates() {
        use std::thread;

        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(12, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));

        // Generate a number of random entries.
        // Increase NUM_ENTRIES to force the chain to span multiple pages.
        const NUM_ENTRIES: usize = 2000;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert every generated entry.
        // (Since recent chain disallows duplicate primary keys, we assume that the random pkeys are unique.)
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }
        // Optionally, print chain statistics after insertion.
        // println!("Chain statistics after insertion:\n{}", chain.stat());

        // Wrap the entries vector in an Arc so it can be shared across threads.
        let entries_arc = Arc::new(entries);

        // Spawn multiple threads to concurrently update entries.
        // We divide the work among a fixed number of threads.
        let num_update_threads = 5;
        let mut update_handles = Vec::new();
        for t in 0..num_update_threads {
            let chain_clone = chain.clone();
            let entries_clone = Arc::clone(&entries_arc);
            let handle = thread::spawn(move || {
                // Each thread updates entries with indices congruent to t mod num_update_threads.
                for i in (t..NUM_ENTRIES).step_by(num_update_threads) {
                    let entry = &entries_clone[i];
                    // Create a new value that is 10 times larger than the original.
                    let new_value = entry.value().repeat(10);
                    // Construct an updated MVCC entry with the same key, same primary key, and same timestamps.
                    let updated_entry = MvccEntry::new(
                        entry.key().to_vec(),
                        entry.pkey().to_vec(),
                        new_value.clone(),
                        entry.start_ts(),
                        entry.end_ts(),
                    );
                    // Perform the update. The chain returns the old entry.
                    let old_entry = chain_clone
                        .update(entry.pkey(), &updated_entry)
                        .expect(&format!("Update failed for entry {}", i));
                    // Verify that the returned old value matches the original.
                    assert_eq!(
                        old_entry.value(),
                        entry.value(),
                        "Mismatch in old value for entry {}",
                        i
                    );
                }
            });
            update_handles.push(handle);
        }

        // Wait for all update threads to finish.
        for handle in update_handles {
            handle.join().unwrap();
        }

        // Finally, verify that every updated entry is retrievable.
        for (i, entry) in entries_arc.iter().enumerate() {
            // Choose a query timestamp in the middle of the valid range.
            let query_ts = entry.start_ts();
            let fetched = chain
                .get(entry.pkey(), &query_ts)
                .expect(&format!("Get failed for entry {} after update", i));
            let expected_new_value = entry.value().repeat(10);
            assert_eq!(
                fetched.value(),
                expected_new_value.as_slice(),
                "Updated value mismatch for entry {}",
                i
            );
            // Optionally, verify that the timestamps remain unchanged.
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for entry {}",
                i
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for entry {}",
                i
            );
        }

        // Optionally, print chain statistics after updates.
        // println!("Chain statistics after updates:\n{}", chain.stat());
    }

    #[test]
    fn test_chain_concurrent_deletes() {
        use std::thread;

        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(13, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));

        // Generate random entries.
        const NUM_ENTRIES: usize = 5000;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert every generated entry.
        // Since duplicate primary keys are not allowed, our helper ensures each entry has a unique pkey.
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }

        // Print chain statistics after insertion.
        // println!("Chain statistics after insertion:\n{}", chain.stat());

        // Wrap the entries in an Arc so they can be shared among deletion threads.
        let entries_arc = Arc::new(entries);

        // Spawn multiple deletion threads.
        let num_delete_threads = 5;
        let mut delete_handles = Vec::new();
        for t in 0..num_delete_threads {
            let chain_clone = chain.clone();
            let entries_clone = Arc::clone(&entries_arc);
            let handle = thread::spawn(move || {
                // Each thread processes every num_delete_threads-th entry starting at index t.
                for i in (t..NUM_ENTRIES).step_by(num_delete_threads) {
                    let entry = &entries_clone[i];
                    // Choose a query timestamp in the middle of the valid range.
                    let query_ts = entry.start_ts() / 2 + entry.end_ts() / 2;
                    // Attempt to delete the entry.
                    let deleted = chain_clone
                        .delete(entry.pkey(), &query_ts)
                        .expect(&format!("Delete failed for entry {}", i));
                    // Verify that the deleted entry's value matches the original.
                    assert_eq!(
                        deleted.value(),
                        entry.value(),
                        "Mismatch in deleted value for entry {}",
                        i
                    );
                }
            });
            delete_handles.push(handle);
        }

        // Wait for all deletion threads to finish.
        for handle in delete_handles {
            handle.join().unwrap();
        }

        // Finally, verify that every deleted entry is no longer retrievable.
        for (i, entry) in entries_arc.iter().enumerate() {
            let query_ts = entry.start_ts() / 2 + entry.end_ts() / 2;
            let res = chain.get(entry.pkey(), &query_ts);
            assert!(
                res.is_err(),
                "Expected get() to fail for deleted entry {}",
                i
            );
        }

        // Optionally, print chain statistics after deletions.
        // println!("Chain statistics after deletion:\n{}", chain.stat());
    }

    #[test]
    fn test_chain_concurrent_mixed_operations() {
        use rand::Rng;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        use std::thread; // Ensure the rand crate is added in Cargo.toml

        // Create a mem pool and initialize a recent chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(14, 0);
        // Here we use your chain type; for example, ChainedHashRecentChain.
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));

        // Generate a set of random entries.
        const NUM_ENTRIES: usize = 10000;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );
        let entries_arc = Arc::new(entries);

        // Pre-insert all entries.
        for (i, entry) in entries_arc.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Pre-insert failed for entry {}", i));
        }
        // println!("Chain statistics after pre-insertion:\n{}", chain.stat());

        // Spawn a number of worker threads to perform mixed operations concurrently.
        const NUM_WORKER_THREADS: usize = 10;
        const NUM_ITERATIONS: usize = 500; // number of operations per thread
        let mut worker_handles = Vec::new();
        for _ in 0..NUM_WORKER_THREADS {
            let chain_clone = chain.clone();
            let entries_clone = Arc::clone(&entries_arc);
            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..NUM_ITERATIONS {
                    // Pick a random entry index.
                    let idx = rng.gen_range(0..NUM_ENTRIES);
                    let entry = &entries_clone[idx];
                    // Use a query timestamp in the middle of the valid range.
                    let query_ts = entry.start_ts() / 2 + entry.end_ts() / 2;
                    // Randomly choose an operation: 0 => get, 1 => update, 2 => delete.
                    let op = rng.gen_range(0..3);
                    match op {
                        0 => {
                            // Get operation: ignore errors.
                            let _ = chain_clone.get(entry.pkey(), &query_ts);
                        }
                        1 => {
                            // Update operation: set new value = original value repeated 10 times.
                            let new_value = entry.value().repeat(10);
                            let updated_entry = MvccEntry::new(
                                entry.key().to_vec(),
                                entry.pkey().to_vec(),
                                new_value,
                                entry.start_ts(),
                                entry.end_ts(),
                            );
                            let _ = chain_clone.update(entry.pkey(), &updated_entry);
                        }
                        2 => {
                            // Delete operation.
                            let _ = chain_clone.delete(entry.pkey(), &query_ts);
                        }
                        _ => {} // Should never happen.
                    }
                    // Optionally yield to allow other threads to run.
                    // thread::yield_now();
                }
            });
            worker_handles.push(handle);
        }

        // Wait for all worker threads to finish.
        for handle in worker_handles {
            handle.join().unwrap();
        }

        // Final verification.
        // For each entry, try to get it using a query timestamp in its valid range.
        // If get() returns Ok, then the final value must equal either the original value or original.repeat(10).
        // If get() returns an error, then the entry was deleted.
        let mut count_deleted = 0;
        let mut count_updated = 0;
        let mut count_unchanged = 0;
        for (i, entry) in entries_arc.iter().enumerate() {
            let query_ts = entry.start_ts() / 2 + entry.end_ts() / 2;
            let res = chain.get(entry.pkey(), &query_ts);
            match res {
                Ok(fetched) => {
                    let original = entry.value();
                    let updated = entry.value().repeat(10);
                    if fetched.value() == &*original {
                        count_unchanged += 1;
                    } else if fetched.value() == updated.as_slice() {
                        count_updated += 1;
                    } else {
                        self::panic!("Entry {}: unexpected value in final state", i);
                    }
                }
                Err(_) => {
                    count_deleted += 1;
                }
            }
        }
        // println!("Final chain statistics:\n{}", chain.stat());
        // println!("Summary: {} unchanged, {} updated, {} deleted", count_unchanged, count_updated, count_deleted);
    }

    #[test]
    fn test_chain_scanner_scan_and_scan_all() {
        use std::collections::HashSet;
        use std::sync::Arc;

        // Create a mem pool and initialize the chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(15, 0);
        let chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));

        // Generate a set of random MVCC entries.
        const NUM_ENTRIES: usize = 100;
        let key_size = 30;
        let pkey_size = 30;
        let nominal_value_size = 200; // Actual value size will vary ±20%
        let entries = generate_random_recent_mvcc_entries(
            NUM_ENTRIES,
            key_size,
            pkey_size,
            nominal_value_size,
        );

        // Insert all generated entries into the chain.
        for (i, entry) in entries.iter().enumerate() {
            chain
                .insert(entry)
                .expect(&format!("Insert failed for entry {}", i));
        }
        // println!("Chain statistics after insertion:\n{}", chain.stat());

        // Choose a query timestamp.
        // Given that our generator produces start_ts in [1000, 10000),
        let query_ts: u64 = 8000;

        // Compute the expected set of primary keys for entries valid at query_ts.
        let expected_pkeys: HashSet<Vec<u8>> = entries
            .iter()
            .filter(|e| e.start_ts() <= query_ts && e.end_ts() > query_ts)
            .map(|e| e.pkey().to_vec())
            .collect();
        // println!(
        //     "Expected number of entries valid at ts = {}: {}",
        //     query_ts,
        //     expected_pkeys.len()
        // );

        // Use the scanner with a specific timestamp.
        let scanner = chain.scan(query_ts);
        let scanned_entries: Vec<MvccEntry> = scanner.collect();
        // println!(
        //     "Scanner (with ts = {}) returned {} entries.",
        //     query_ts,
        //     scanned_entries.len()
        // );
        let scanned_pkeys: HashSet<Vec<u8>> =
            scanned_entries.iter().map(|e| e.pkey().to_vec()).collect();
        assert_eq!(
            expected_pkeys, scanned_pkeys,
            "Mismatch between expected and scanned entries for query timestamp {}",
            query_ts
        );

        // Now, perform a full scan using scan_all() (which should return all inserted entries).
        let full_scanner = chain.scan_all();
        let full_scanned_entries: Vec<MvccEntry> = full_scanner.collect();
        // println!("Full scan returned {} entries.", full_scanned_entries.len());
        let full_scanned_pkeys: HashSet<Vec<u8>> = full_scanned_entries
            .iter()
            .map(|e| e.pkey().to_vec())
            .collect();
        let all_expected_pkeys: HashSet<Vec<u8>> =
            entries.iter().map(|e| e.pkey().to_vec()).collect();
        assert_eq!(
            full_scanned_pkeys, all_expected_pkeys,
            "Full scan did not return all inserted entries"
        );
    }
}
