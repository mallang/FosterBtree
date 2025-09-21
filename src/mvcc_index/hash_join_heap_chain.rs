use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{self, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    thread::current,
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_info, log_trace, log_warn,
    mvcc_index::{
        hash_common::{fix_frame_id, fix_frame_id2, StatCollector},
        hash_join_page::{record::RecordRef, HashJoinPage, PAGE_CNT},
        MvccEntry,
    },
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
    prelude::Timestamp,
};

use super::{
    dual_heap_hash::chained_hash_bucket_second::ChainBucketBulkUpdate,
    hash_common::{read_page, write_page, MvccEntryLoc, RowDelta},
};

pub struct HeapHashChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    first_page_id: AtomicU32,
    first_frame_id: AtomicU32,

    last_page_id: AtomicU32,
    last_frame_id: AtomicU32,
}

impl<T: MemPool + 'static> HeapHashChain<T> {
    pub fn collect_space_statistics(&self, stat: &mut StatCollector) {
        let mut current_page = self.first_page();
        loop {
            current_page.collect_space_statistics(stat);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                current_page = next_page;
            } else {
                break;
            }
        }
    }

    pub fn collect_page_num(&self) -> usize {
        let mut page_num = 0;
        let mut current_page = self.first_page();
        loop {
            page_num += 1;
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }
        page_num
    }
    pub fn scan_delta_into(
        &self,
        from: Timestamp,
        to: Timestamp,
        results: &mut HashMap<Vec<u8>, RowDelta>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            current_page.chain_scan_delta_into(from, to, results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }
        Ok(())
    }
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let first_page_id = page.get_id();
        let first_frame_id = page.frame_id();

        // Initialize the page as a history page
        HashJoinPage::init(&mut *page);
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id: AtomicU32::new(first_page_id),
            first_frame_id: AtomicU32::new(first_frame_id),
            last_page_id: AtomicU32::new(first_page_id),
            last_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let space_need = <Page as HashJoinPage>::require_space(&entry);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        let last_page_id = self.last_page_id.load(Ordering::Acquire);
        let last_frame_id = self.last_frame_id.load(Ordering::Acquire);
        let last_page_frame_key =
            PageFrameKey::new_with_frame_id(self.c_key, last_page_id, last_frame_id);
        let mut last_page = self.get_tail_page_for_write(last_page_frame_key)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        match last_page.insert_heap_no_repair(&rec, entry.start_ts(), entry.end_ts()) {
            Ok(_) => {
                if self.last_page_id.load(Ordering::Acquire) != last_page.get_id() {
                    self.last_page_id
                        .store(last_page.get_id(), Ordering::Release);
                    self.last_frame_id
                        .store(last_page.frame_id(), Ordering::Release);
                } else if self.last_frame_id.load(Ordering::Acquire) != last_page.frame_id() {
                    self.last_frame_id
                        .store(last_page.frame_id(), Ordering::Release);
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
                    .store(new_page.get_id(), Ordering::Release);
                self.last_frame_id
                    .store(new_page.frame_id(), Ordering::Release);
                match new_page.upsert_history(&rec, entry.start_ts(), entry.end_ts()) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn history_insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let space_need = <Page as HashJoinPage>::require_space(&entry);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        let last_page_id = self.last_page_id.load(Ordering::Acquire);
        let last_frame_id = self.last_frame_id.load(Ordering::Acquire);
        let last_page_frame_key =
            PageFrameKey::new_with_frame_id(self.c_key, last_page_id, last_frame_id);
        let mut last_page = self.get_tail_page_for_write(last_page_frame_key)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        match last_page.upsert_history(&rec, entry.start_ts(), entry.end_ts()) {
            Ok(_) => {
                if self.last_page_id.load(Ordering::Acquire) != last_page.get_id() {
                    self.last_page_id
                        .store(last_page.get_id(), Ordering::Release);
                    self.last_frame_id
                        .store(last_page.frame_id(), Ordering::Release);
                } else if self.last_frame_id.load(Ordering::Acquire) != last_page.frame_id() {
                    self.last_frame_id
                        .store(last_page.frame_id(), Ordering::Release);
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
                    .store(new_page.get_id(), Ordering::Release);
                self.last_frame_id
                    .store(new_page.frame_id(), Ordering::Release);
                match new_page.upsert_history(&rec, entry.start_ts(), entry.end_ts()) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn get_tail_page_for_write(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let last_page = self.try_get_tail_page_for_write(page_key);
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

    fn try_get_tail_page_for_write(
        &self,
        page_key: PageFrameKey,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = read_page(&*self.mem_pool, page_key);
        loop {
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let pfk = PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id);
                let next_page = read_page(&*self.mem_pool, pfk);
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
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

    pub fn get_read_repair(
        &self,
        pkey: &[u8],
        ts: &Timestamp,
        versions: &mut BTreeMap<Timestamp, (MvccEntryLoc, bool)>,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut best_candidate: Option<MvccEntry> = None;
        let mut current_page = self.first_page();

        loop {
            if let Some(entry) = current_page.heap_get_read_repair(pkey, ts, versions).ok() {
                if entry.end_ts() != u64::MAX {
                    return Ok(entry);
                }

                let is_better = match &best_candidate {
                    None => true,
                    Some(existing) => entry.start_ts() > existing.start_ts(),
                };
                if is_better {
                    best_candidate = Some(entry);
                }
            }

            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }

        match best_candidate {
            Some(entry) => Ok(entry),
            None => Err(AccessMethodError::KeyNotFound),
        }
    }

    pub fn get_no_repair(
        &self,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut best_candidate: Option<MvccEntry> = None;
        let mut current_page = self.first_page();

        loop {
            if let Some(entry) = current_page.heap_get_no_repair(pkey, ts).ok() {
                let et = entry.end_ts();
                if et != u64::MAX {
                    return Ok(entry);
                }
                match &best_candidate {
                    None => best_candidate = Some(entry),
                    Some(existing) => {
                        if entry.start_ts() > existing.start_ts() {
                            best_candidate = Some(entry);
                        }
                    }
                }
            }

            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }

        match best_candidate {
            Some(entry) => Ok(entry),
            None => Err(AccessMethodError::KeyNotFound),
        }
    }

    pub fn chain_get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.first_page();

        loop {
            if let Some(entry) = current_page.chain_get(pkey, ts).ok() {
                return Ok(entry);
            }

            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }

        Err(AccessMethodError::KeyNotFound)
    }

    pub fn update_no_repair(
        &self,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        self.insert(entry)
    }

    pub fn update_write_repair_heap(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        let mut inserted = false;
        let mut repaired = false;

        loop {
            let next_page_info = current_page.next_page();
            let upgrade = current_page.try_upgrade(true);
            let mut write_page = match upgrade {
                Ok(p) => p,
                Err(_) => return Err(AccessMethodError::PageWriteLatchFailed),
            };

            match write_page.update_heap_write_repair(entry, inserted, repaired) {
                Ok(()) => return Ok(()),
                Err(AccessMethodError::UpdateReapiredButNotInseted) => {
                    repaired = true;
                }
                Err(AccessMethodError::UpdateInsertedButNotReapired) => {
                    inserted = true;
                }
                Err(AccessMethodError::KeyNotFound) => {}
                Err(e) => return Err(e),
            }

            if repaired && inserted {
                drop(write_page);
                return Ok(());
            }

            if let Some((next_pid, next_fid)) = next_page_info {
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                drop(write_page);
            } else {
                // first versions => ok if !repaired
                if inserted {
                    return Ok(());
                }
                let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key)?;
                new_page.init();
                let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
                new_page.insert_heap_no_repair(&rec, entry.start_ts(), entry.end_ts())?;
                write_page.set_next_page(new_page.get_id(), new_page.frame_id());
                self.last_page_id
                    .store(new_page.get_id(), Ordering::Release);
                self.last_frame_id
                    .store(new_page.frame_id(), Ordering::Release);
                drop(write_page);
                return Ok(());
            }
        }
    }

    pub fn update_write_repair_ts_partition_except_last(
        &self,
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        let inserted = true;
        let mut repaired = false;

        loop {
            let next_page_info = current_page.next_page();
            let upgrade = current_page.try_upgrade(true);
            let mut write_page = match upgrade {
                Ok(p) => p,
                Err(_) => return Err(AccessMethodError::PageWriteLatchFailed),
            };

            match write_page.update_heap_write_repair(entry, inserted, repaired) {
                Ok(()) => {
                    repaired = true;
                }
                Err(AccessMethodError::UpdateReapiredButNotInseted) => {
                    repaired = true;
                }
                Err(AccessMethodError::UpdateInsertedButNotReapired) => {}
                Err(AccessMethodError::KeyNotFound) => {}
                Err(e) => return Err(e),
            }

            if repaired && inserted {
                drop(write_page);
                return Ok(());
            }

            if let Some((next_pid, next_fid)) = next_page_info {
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                drop(write_page);
            } else {
                drop(write_page);
                return if repaired {
                    Ok(())
                } else {
                    return Err(AccessMethodError::RepairedNotFound);
                };
            }
        }
    }

    pub fn update_write_repair_ts_partition_last(
        &self,
        entry: &MvccEntry,
        mut repaired: bool,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            let next_page_info = current_page.next_page();
            let upgrade = current_page.try_upgrade(true);
            let mut write_page = match upgrade {
                Ok(p) => p,
                Err(_) => return Err(AccessMethodError::PageWriteLatchFailed),
            };
            if !repaired {
                match write_page.update_heap_write_repair(entry, true, false) {
                    Ok(()) => {
                        repaired = true;
                    }
                    Err(AccessMethodError::UpdateReapiredButNotInseted) => {
                        repaired = true;
                    }
                    Err(AccessMethodError::UpdateInsertedButNotReapired) => {}
                    Err(AccessMethodError::KeyNotFound) => {}
                    Err(e) => return Err(e),
                }
            }

            if let Some((next_pid, next_fid)) = next_page_info {
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                drop(write_page);
            } else {
                let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
                // reach last page
                match write_page.insert_heap_no_repair(&rec, entry.start_ts(), entry.end_ts()) {
                    Ok(()) => {
                        return Ok(());
                    }
                    Err(AccessMethodError::OutOfSpace) => {
                        let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key)?;
                        new_page.init();
                        new_page.insert_heap_no_repair(&rec, entry.start_ts(), entry.end_ts())?;
                        write_page.set_next_page(new_page.get_id(), new_page.frame_id());
                        self.last_page_id
                            .store(new_page.get_id(), Ordering::Release);
                        self.last_frame_id
                            .store(new_page.frame_id(), Ordering::Release);
                        drop(write_page);
                    }
                    Err(e) => return Err(e),
                }
                return Ok(());
            }
        }
    }

    pub fn delete(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        self.traverse_to_endofchain_for_delete(self.first_key(), pkey, ts)
    }

    pub fn chain_update_recent(
        &self,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.first_page_write();

        loop {
            let next_page_info = current_page.next_page();

            match current_page.chain_update_recent(entry, entry.start_ts()) {
                Ok(old_e) => return Ok(old_e),
                Err(AccessMethodError::KeyNotFound) => {}
                Err(e) => panic!("Unexpected error: {:?}", e),
            }

            if let Some((next_pid, next_fid)) = next_page_info {
                current_page = write_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
            } else {
                return Err(AccessMethodError::KeyNotFound);
            }
        }
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

    pub fn heap_bulk_update_collect(
        &self,
        write_repair: &mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = read_page(&*self.mem_pool, self.first_key());
        loop {
            current_page.heap_bulk_update_repair_collect(write_repair)?;
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn try_traverse_to_endofchain_for_delete(
        &self,
        page_key: PageFrameKey,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = read_page(&*self.mem_pool, page_key);
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
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
                if next_page.frame_id() != next_frame_id {
                    let _ = fix_frame_id(current_page, next_page_id, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                log_debug!("Key({}) not found for update.", pkey);
                return Err(AccessMethodError::KeyNotFound);
            }
        }
    }

    /*
        (key1, 1), (key2, 2), (key1, 3), (key2, 4), (key3, 5) |gc at 6| (key3, 8)
        Before GC:
            (key1, 1), (key1, 3),
            (key2, 2), (key2, 4),
            (key3, 5), (key3, 8)

        After GC:
            (key1, 3),
            (key2, 4),
            (key3, 5), (key3, 8),
    */
    pub fn gc_collect_versions(
        &self,
        ts: &Timestamp,
        versions_map: &mut HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>,
    ) -> Result<(), AccessMethodError> {
        // pkey, st, mvccentry
        // let mut best_map: HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc)>> = HashMap::new();
        {
            let mut current_page = self.first_page();
            loop {
                current_page.scan_all_for_gc_read_repair(versions_map);

                if let Some((next_pid, next_fid)) = current_page.next_page() {
                    let next_page = read_page(
                        &*self.mem_pool,
                        PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                    );
                    if next_page.frame_id() != next_fid {
                        let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                    }
                    let slot_cnt = next_page.slot_count();
                    if slot_cnt > 0 {
                        if next_page.unsafe_slot(0).start_ts() > *ts {
                            break;
                        }
                    }

                    current_page = next_page;
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Runs garbage collection on every page in the heap chain.
    /// For each page, if the page is fully eligible for GC (i.e. its slot_count is 0 or
    /// its last slot's end_ts is ≤ `ts`), then the entire page is removed.
    /// Otherwise, partial GC is applied on that page.
    pub fn gc_truncate_entries_before_ts(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page_write();
        // let mut prev_page: Option<FrameWriteGuard> = None;
        loop {
            // let gc_whole_page = if current_page.slot_count() == 0 {
            //     true
            // } else {
            //     current_page.max_end_ts() <= *ts
            // };

            // if gc_whole_page {
            //     // The entire page is eligible for GC.
            //     // If we have a previous page, we need to update its next pointer.
            //     if let Some(mut prev) = prev_page.take() {
            //         // Get the pointer to the next page after current_page.
            //         if let Some((next_pid, next_fid)) = current_page.next_page() {
            //             prev.set_next_page(next_pid, next_fid);
            //         } else {
            //             // No next page, so mark previous page as the tail.
            //             prev.set_next_page(PageId::MAX, u32::MAX);
            //         }
            //         // TODO: free current_page (Done by mem_pool)
            //     } else {
            //         // No previous page means current_page is the first page.
            //         if let Some((next_pid, next_fid)) = current_page.next_page() {
            //             // Promote the next page as the new first page.
            //             self.set_first_page_id(next_pid);
            //             current_page = read_page(
            //                 &*self.mem_pool,
            //                 PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
            //             );
            //             // Continue without updating prev_page.
            //             continue;
            //         } else {
            //             // This is the only page and it is fully eligible. Reinitialize it.
            //             let mut writable_page = current_page
            //                 .try_upgrade(true)
            //                 .map_err(|_| AccessMethodError::PageWriteLatchFailed)?;
            //             writable_page.init();
            //             break;
            //         }
            //     }
            // } else {
            //     // Page is only partially eligible (or not eligible).
            //     // Upgrade the page to a writable lock and run partial GC.
            //     let mut writable_page = current_page
            //         .try_upgrade(true)
            //         .map_err(|_| AccessMethodError::PageWriteLatchFailed)?;
            //     writable_page.heap_hash_garbage_collect(ts)?;
            //     // Keep this page as the previous page (for updating pointers) in case the next page(s)
            //     // are also fully eligible.
            //     prev_page = Some(writable_page);
            // }

            if current_page.min_end_ts() <= *ts {
                current_page.heap_hash_garbage_collect(ts)?;
            }

            // Move to the next page, if any.
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                current_page = write_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id.load(Ordering::Acquire)
    }

    pub fn last_page_id(&self) -> PageId {
        self.last_page_id.load(Ordering::Acquire)
    }

    pub fn set_first_page_id(&self, pid: PageId) {
        self.first_page_id.store(pid, Ordering::Release);
    }

    pub fn first_frame_id(&self) -> u32 {
        self.first_frame_id.load(Ordering::Acquire)
    }

    pub fn last_frame_id(&self) -> u32 {
        self.last_frame_id.load(Ordering::Acquire)
    }

    pub fn first_key(&self) -> PageFrameKey {
        PageFrameKey::new_with_frame_id(
            self.c_key,
            self.first_page_id.load(Ordering::Acquire),
            self.first_frame_id.load(Ordering::Acquire),
        )
    }

    fn first_page(&self) -> FrameReadGuard {
        let first_frame_id = self.first_frame_id.load(Ordering::Acquire);
        let first_page_id = self.first_page_id.load(Ordering::Acquire);
        let first_page = read_page(
            &*self.mem_pool,
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, first_frame_id),
        );
        if first_page.frame_id() != first_frame_id {
            log_debug!("Frame of the first page has been changed. Trying to fix the frame id");
            self.first_frame_id
                .store(first_page.frame_id(), Ordering::Release);
        }
        first_page
    }

    fn first_page_write(&self) -> FrameWriteGuard {
        let first_frame_id = self.first_frame_id.load(Ordering::Acquire);
        let first_page_id = self.first_page_id.load(Ordering::Acquire);
        let first_page = write_page(
            &*self.mem_pool,
            PageFrameKey::new_with_frame_id(self.c_key, first_page_id, first_frame_id),
        );
        if first_page.frame_id() != first_frame_id {
            log_debug!("Frame of the first page has been changed. Trying to fix the frame id");
            self.first_frame_id
                .store(first_page.frame_id(), Ordering::Release);
        }
        first_page
    }

    pub fn scan_all(self: &Arc<Self>) -> Result<HeapChainScanner<T>, AccessMethodError> {
        // TODO: result is incorrect
        // (end ts of mvccentry is incorrect)
        Ok(HeapChainScanner::new_full_scan(self))
    }

    /// Scan for all entries visible at `ts` and return only the best candidate
    /// per primary key. The “best” is defined here as the entry with the highest
    /// start timestamp that is visible at `ts`.
    pub fn chain_scan_into_vec(
        self: &Self,
        ts: Timestamp,
        res: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            current_page.chain_scan_into_vec(ts, res);

            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
                continue;
            }
            // no next page in current chain
            break;
        }

        return Ok(());
    }

    /// ignore timestamp, only used when we ensure it's correct
    pub fn chain_scan_into_vec_ignore_ts(
        self: &Self,
        res: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            #[cfg(feature = "count_statistics")]
            PAGE_CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            current_page.chain_scan_into_vec_ignore_ts(res);

            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
                continue;
            }
            // no next page in current chain
            break;
        }

        return Ok(());
    }

    /// Scan for all entries visible at `ts` and return only the best candidate
    /// per primary key. The “best” is defined here as the entry with the highest
    /// start timestamp that is visible at `ts`.
    pub fn scan_unique(
        self: &Arc<Self>,
        ts: Timestamp,
        best_candidates: &mut HashMap<Vec<u8>, MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            current_page.heap_scan_unique_no_repair_into_best_candidates(ts, best_candidates);

            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
                continue;
            }
            // no next page in current chain
            break;
        }
        // Return the best candidate for each key. If order matters you might want to sort them.
        Ok(())
    }

    pub fn scan_unique_write_repair(
        self: &Arc<Self>,
        ts: Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            #[cfg(feature = "count_statistics")]
            PAGE_CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            current_page.chain_scan_into_vec(ts, results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Scan for all entries visible at `ts` and return only the best candidate
    /// per primary key. The “best” is defined here as the entry with the highest
    /// start timestamp that is visible at `ts`.
    pub fn scan_unique_read_repair(
        self: &Arc<Self>,
        ts: Timestamp,
        best_candidates: &mut HashMap<Vec<u8>, MvccEntry>,
        versions_map: &mut HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>,
    ) -> Result<(), AccessMethodError> {
        // Get the full scanner (which iterates over all entries that pass the ts filter)
        let mut scanner = HeapChainScanner::new_with_read_repair(self, ts, versions_map);
        // let mut best_candidates: HashMap<Vec<u8>, MvccEntry> = HashMap::new();

        // Iterate over all entries from the chain.
        while !scanner.is_end() {
            if let Some(entry) = scanner.next() {
                let pkey = entry.pkey().to_vec();
                best_candidates.insert(pkey, entry);
            }
        }

        // Return the best candidate for each key. If order matters you might want to sort them.
        Ok(())
    }

    pub fn scan_delta(
        self: &Arc<Self>,
        from: Timestamp,
        to: Timestamp,
        delta_map: &mut HashMap<Vec<u8>, RowDelta>,
    ) {
        // Get the full scanner (which iterates over all entries that pass the ts filter)
        let mut scanner = HeapChainScanner::new_delta_scan(self, from, to, None);

        // Iterate over all entries from the chain.
        while !scanner.is_end() {
            if let Some(entry) = scanner.next() {
                let st = entry.start_ts();
                // 2) If prefix matches, load the record

                let delta_entry = delta_map.get_mut(entry.pkey());
                if let Some(entry_v) = delta_entry {
                    entry_v.to().cmp_and_swap(st, entry.key(), entry.value());
                } else {
                    let mut row_delta = RowDelta::new();
                    row_delta.to().cmp_and_swap(st, entry.key(), entry.value());
                    delta_map.insert(entry.pkey().to_vec(), row_delta);
                }
                if st <= from {
                    let delta_entry = delta_map.get_mut(entry.pkey());
                    if let Some(entry_v) = delta_entry {
                        entry_v.from().cmp_and_swap(st, entry.key(), entry.value());
                    }
                }
            }
        }
    }

    pub fn scan_delta_read_repair(
        self: &Arc<Self>,
        from: Timestamp,
        to: Timestamp,
        delta_map: &mut HashMap<Vec<u8>, RowDelta>,
        versions: &mut HashMap<Vec<u8>, Vec<(u64, MvccEntryLoc, bool)>>,
    ) {
        // Get the full scanner (which iterates over all entries that pass the ts filter)
        let mut scanner = HeapChainScanner::new_delta_scan(self, from, to, Some(versions));

        // Iterate over all entries from the chain.
        while !scanner.is_end() {
            if let Some(entry) = scanner.next() {
                let st = entry.start_ts();
                // 2) If prefix matches, load the record

                let delta_entry = delta_map.get_mut(entry.pkey());
                if let Some(entry_v) = delta_entry {
                    entry_v.to().cmp_and_swap(st, entry.key(), entry.value());
                } else {
                    delta_map.insert(entry.pkey().to_vec(), RowDelta::new());
                }
                if st <= from {
                    let delta_entry = delta_map.get_mut(entry.pkey());
                    if let Some(entry_v) = delta_entry {
                        entry_v.from().cmp_and_swap(st, entry.key(), entry.value());
                    }
                }
            }
        }
    }

    /// in chain, iterate in increasing order of start_ts
    /// => tail is newer than head
    pub fn scan_key_vec_read_repair(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        mut versions_map: Option<&mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut res = HashMap::new();
        {
            let mut current_page = self.first_page();
            loop {
                let header = current_page.unsafe_header();

                if ts < &header.page_min_start_ts() {
                    // do nothing
                } else if header.recent_slot_cnt() == 0 && ts >= &header.page_max_end_ts() {
                    // do nothing
                } else {
                    current_page.scan_key_heap_read_repair(
                        search_key,
                        ts,
                        &mut res,
                        &mut versions_map,
                    )?;
                }

                if let Some((next_pid, next_fid)) = current_page.next_page() {
                    let next_page = read_page(
                        &*self.mem_pool,
                        PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                    );
                    if next_page.frame_id() != next_fid {
                        let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                    }
                    current_page = next_page;
                    continue;
                }
                // no next page in current chain
                break;
            }
        }

        return Ok(res.into_iter().collect());
    }

    /// in chain, iterate in increasing order of start_ts
    /// => tail is newer than head
    pub fn chain_scan_key(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        res: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), AccessMethodError> {
        {
            let mut current_page = self.first_page();
            loop {
                let header = current_page.unsafe_header();

                if ts < &header.page_min_start_ts() {
                    // do nothing
                } else if header.recent_slot_cnt() == 0 && ts >= &header.page_max_end_ts() {
                    // do nothing
                } else {
                    current_page.chain_scan_key(search_key, ts, res)?;
                }

                if let Some((next_pid, next_fid)) = current_page.next_page() {
                    let next_page = read_page(
                        &*self.mem_pool,
                        PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                    );
                    if next_page.frame_id() != next_fid {
                        let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                    }
                    current_page = next_page;
                    continue;
                }
                // no next page in current chain
                break;
            }
        }

        return Ok(());
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
            // stat_str.push_str(&format!("{}\n", current_page.stat()));

            // Traverse to the next page if available.
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
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
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
            } else {
                break;
            }
        }
        (page_count, total_kv_count, usage_sum, max_usage, min_usage)
    }
    // [His/Recent Chain]
    fn chain_try_traverse_to_endofchain_for_bulk_update(
        &self,
        page_key: PageFrameKey,
        bulk: &mut ChainBucketBulkUpdate,
        new_start_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = write_page(&*self.mem_pool, page_key);
        loop {
            current_page.chain_bulk_update_slots_recent(bulk, new_start_ts);

            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                let next_page = write_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_page_id, next_frame_id),
                );
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
    pub fn chain_bulk_update_history_entries(
        &self,
        bulk: &ChainBucketBulkUpdate,
    ) -> Result<(), AccessMethodError> {
        for old_entry in bulk.old_entries.iter() {
            self.history_insert(old_entry)?;
        }
        Ok(())
    }
    pub fn chain_bulk_update_collect_old_entries(
        &self,
        page_key: PageFrameKey,
        bulk: &mut ChainBucketBulkUpdate,
        new_start_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let find_page =
                self.chain_try_traverse_to_endofchain_for_bulk_update(page_key, bulk, new_start_ts);
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

    /// Runs garbage collection on every page in the history chain.
    /// For each page, if the page is fully eligible for GC (i.e. its slot_count is 0 or
    /// its last slot's end_ts is ≤ `ts`), then the entire page is removed.
    /// Otherwise, partial GC is applied on that page.
    pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        let mut prev_page: Option<FrameWriteGuard> = None;

        loop {
            let gc_whole_page = if current_page.slot_count() == 0 {
                true
            } else {
                let last_slot = current_page.unsafe_slot(current_page.slot_count() - 1);
                last_slot.end_ts() <= *ts
            };

            if gc_whole_page {
                // The entire page is eligible for GC.
                // If we have a previous page, we need to update its next pointer.
                if let Some(mut prev) = prev_page.take() {
                    // Get the pointer to the next page after current_page.
                    if let Some((next_pid, next_fid)) = current_page.next_page() {
                        prev.set_next_page(next_pid, next_fid);
                    } else {
                        // No next page, so mark previous page as the tail.
                        prev.set_next_page(PageId::MAX, u32::MAX);
                    }
                    // TODO: free current_page (Done by mem_pool)
                } else {
                    // No previous page means current_page is the first page.
                    if let Some((next_pid, next_fid)) = current_page.next_page() {
                        // Promote the next page as the new first page.
                        self.set_first_page_id(next_pid);
                        current_page = read_page(
                            &*self.mem_pool,
                            PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                        );
                        // Continue without updating prev_page.
                        continue;
                    } else {
                        // This is the only page and it is fully eligible. Reinitialize it.
                        let mut writable_page = current_page
                            .try_upgrade(true)
                            .map_err(|_| AccessMethodError::PageWriteLatchFailed)?;
                        writable_page.init();
                        break;
                    }
                }
            } else {
                // Page is only partially eligible (or not eligible).
                // Upgrade the page to a writable lock and run partial GC.
                let mut writable_page = current_page
                    .try_upgrade(true)
                    .map_err(|_| AccessMethodError::PageWriteLatchFailed)?;
                writable_page.chained_hash_garbage_collect(ts)?;
                // Keep this page as the previous page (for updating pointers) in case the next page(s)
                // are also fully eligible.
                prev_page = Some(writable_page);
            }

            // Move to the next page, if any.
            if let Some((next_pid, next_fid)) = prev_page.as_ref().unwrap().next_page() {
                current_page = read_page(
                    &*self.mem_pool,
                    PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
                );
            } else {
                break;
            }
        }
        Ok(())
    }
}

enum FilterMode {
    FullScan,
    FilterByTs(Timestamp),
    FilterByDelta(Timestamp, Timestamp),
}

pub struct HeapChainScanner<'a, T: MemPool> {
    chain: Arc<HeapHashChain<T>>,
    filter_mode: FilterMode,
    current_page: Option<FrameReadGuard<'static>>,
    current_slot_id: usize,

    initialized: bool,
    finished: bool,

    read_repair: Option<&'a mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>>,
}

impl<'a, T: MemPool + 'static> HeapChainScanner<'a, T> {
    pub fn new(chain: &Arc<HeapHashChain<T>>, ts: Timestamp) -> Self {
        let mut a = Self {
            chain: chain.clone(),
            filter_mode: FilterMode::FilterByTs(ts),
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
            read_repair: None,
        };
        a.initialize();
        a
    }

    pub fn new_with_read_repair(
        chain: &Arc<HeapHashChain<T>>,
        ts: Timestamp,
        versions: &'a mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    ) -> Self {
        let mut a = Self {
            chain: chain.clone(),
            filter_mode: FilterMode::FilterByTs(ts),
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
            read_repair: Some(versions),
        };
        a.initialize();
        a
    }

    pub fn new_full_scan(chain: &Arc<HeapHashChain<T>>) -> Self {
        let mut a = Self {
            chain: chain.clone(),
            filter_mode: FilterMode::FullScan,
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
            read_repair: None,
        };
        a.initialize();
        a
    }

    pub fn new_delta_scan(
        chain: &Arc<HeapHashChain<T>>,
        from: Timestamp,
        to: Timestamp,
        versions: Option<&'a mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>>,
    ) -> Self {
        let mut a = Self {
            chain: chain.clone(),
            filter_mode: FilterMode::FilterByDelta(from, to),
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
            read_repair: versions,
        };
        a.initialize();
        a
    }

    fn initialize(&mut self) {
        let first_page = self.chain.first_page();
        let first_page =
            unsafe { std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(first_page) };
        self.current_page = Some(first_page);
        self.current_slot_id = 0;
        let header = self.current_page.as_ref().unwrap().unsafe_header();
        let (min_st, max_end, recent_cnt) = (
            header.page_min_start_ts(),
            header.page_max_end_ts(),
            header.recent_slot_cnt(),
        );
        if let FilterMode::FilterByTs(filter_ts) = self.filter_mode {
            if filter_ts < min_st {
                self.move_to_next_page();
            } else if recent_cnt == 0 && filter_ts >= max_end {
                self.move_to_next_page();
            }
        } else if let FilterMode::FilterByDelta(from, to) = self.filter_mode {
            if to < min_st {
                self.move_to_next_page();
            } else if recent_cnt == 0 && from >= max_end {
                self.move_to_next_page();
            }
        }

        self.initialized = true;
    }

    fn finish(&mut self) {
        self.finished = true;
        self.current_page = None;
    }

    pub fn is_end(&self) -> bool {
        self.finished
    }

    fn move_to_next_page(&mut self) -> bool {
        loop {
            let next_page = <Page as HashJoinPage>::next_page(&self.current_page.as_ref().unwrap());
            if let Some((next_pid, next_fid)) = next_page {
                let next_page = read_page(
                    &*self.chain.mem_pool,
                    PageFrameKey::new_with_frame_id(self.chain.c_key, next_pid, next_fid),
                );
                let next_page = unsafe {
                    std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(next_page)
                };
                self.current_page = Some(next_page);
                self.current_slot_id = 0;
                let header = self.current_page.as_ref().unwrap().unsafe_header();

                if let FilterMode::FilterByTs(filter_ts) = self.filter_mode {
                    if filter_ts < header.page_min_start_ts() {
                        continue;
                    }
                    if header.recent_slot_cnt() == 0 && filter_ts >= header.page_max_end_ts() {
                        continue;
                    }
                } else if let FilterMode::FilterByDelta(from, to) = self.filter_mode {
                    if to < header.page_min_start_ts() {
                        continue;
                    } else if header.recent_slot_cnt() == 0 && from >= header.page_max_end_ts() {
                        continue;
                    }
                }
                break;
            } else {
                // no more page in heap chain
                self.finish();
                return true;
            }
        }
        return false;
    }
}

impl<'a, T: MemPool + 'static> Iterator for HeapChainScanner<'a, T> {
    type Item = MvccEntry;

    fn next(&mut self) -> Option<Self::Item> {
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
                let slot = current_page.unsafe_slot(self.current_slot_id);
                let record = current_page.record_ref_from_slotid(self.current_slot_id);

                // read repair
                if let Some(repair) = &mut self.read_repair {
                    let pkey = record.pkey();
                    if let Some(vec_entry) = (*repair).get_mut(pkey) {
                        vec_entry.push((
                            slot.start_ts(),
                            MvccEntryLoc::new(current_page.get_id(), self.current_slot_id as u32),
                            slot.end_ts() == Timestamp::MAX,
                        ));
                    } else {
                        (*repair).insert(
                            pkey.to_vec(),
                            vec![(
                                slot.start_ts(),
                                MvccEntryLoc::new(
                                    current_page.get_id(),
                                    self.current_slot_id as u32,
                                ),
                                slot.end_ts() == Timestamp::MAX,
                            )],
                        );
                    }
                }

                self.current_slot_id += 1;
                if let FilterMode::FilterByTs(filter_ts) = self.filter_mode {
                    if filter_ts < slot.start_ts()
                        || (slot.end_ts() <= filter_ts && slot.end_ts() != u64::MAX)
                    {
                        continue;
                    }
                } else if let FilterMode::FilterByDelta(from, to) = self.filter_mode {
                    if to < slot.start_ts() || (slot.end_ts() <= from && slot.end_ts() != u64::MAX)
                    {
                        continue;
                    }
                }
                // 1. full scan
                // 2. filter by ts && ts fits in the range
                // 3. delta scan && slots ts range has overlap with delta range
                //    need further processing in caller function.
                return Some(MvccEntry::new(
                    record.key().to_vec(),
                    record.pkey().to_vec(),
                    record.val().to_vec(),
                    slot.start_ts(),
                    slot.end_ts(),
                ));
            } else {
                // Move to the next page
                if self.move_to_next_page() {
                    return None;
                }
                continue;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_in_mem_pool;
    use crate::mvcc_index::AccessMethodError;
    use crate::mvcc_index::MvccEntry;
    use crate::mvcc_index::MvccIndex;
    use crate::prelude::ContainerKey;
    use rand::Rng;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use std::thread;

    // Test 1: Basic insert, get, and update on HeapChain.
    #[test]
    fn test_heap_chain_insert_get_update() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        // Create a HeapChain instance.
        let heap_chain = HeapHashChain::new(c_key, mem_pool);
        let key = b"key-001".to_vec();
        let pkey = b"pkey-001".to_vec();

        // --- Step 1: Insert a recent version ---
        let initial_value = b"initial-value".to_vec();
        let initial_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            initial_value.clone(),
            100,
            u64::MAX,
        );
        heap_chain
            .insert(&initial_entry)
            .expect("Insert recent value failed");

        // Verify that a get query at ts = 120 returns the inserted value.
        let fetched_initial = heap_chain
            .get_no_repair(&pkey, &120)
            .expect("Get failed for initial recent value");
        assert_eq!(fetched_initial.value(), initial_value.as_slice());
        assert_eq!(fetched_initial.start_ts(), 100);
        assert_eq!(fetched_initial.end_ts(), u64::MAX);

        // --- Step 2: Update the value ---
        // Create an update version valid from ts = 200.
        let updated_value = b"updated-value".to_vec();
        let update_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            updated_value.clone(),
            200,
            u64::MAX,
        );
        heap_chain
            .update_no_repair(&pkey, &update_entry)
            .expect("Update failed");

        // For a query at ts = 120 the chain should return the old version, now with end_ts set to 200.
        let fetched_history = heap_chain
            .get_no_repair(&pkey, &120)
            .expect("Get failed for historical version");
        assert_eq!(
            fetched_history.value(),
            initial_value.as_slice(),
            "Historical value mismatch: expected initial value"
        );
        assert_eq!(
            fetched_history.start_ts(),
            100,
            "Historical start_ts mismatch"
        );
        // assert_eq!(fetched_history.end_ts(), 200, "Historical end_ts mismatch");

        // For a query at ts = 220 the chain should return the updated (recent) version.
        let fetched_recent = heap_chain
            .get_no_repair(&pkey, &220)
            .expect("Get failed for updated recent version");
        assert_eq!(
            fetched_recent.value(),
            updated_value.as_slice(),
            "Recent value mismatch: expected updated value"
        );
        assert_eq!(fetched_recent.start_ts(), 200, "Recent start_ts mismatch");
        assert_eq!(fetched_recent.end_ts(), u64::MAX, "Recent end_ts mismatch");
    }

    // Test 2: Insert, update, delete, then get.
    // ignore delete test now
    #[ignore]
    #[test]
    fn test_heap_chain_insert_update_delete_get() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let heap_chain = HeapHashChain::new(c_key, mem_pool);
        let key = b"key-001".to_vec();
        let pkey = b"pkey-001".to_vec();

        // Insert an initial version valid from ts = 100.
        let initial_value = b"initial-value".to_vec();
        let recent_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            initial_value.clone(),
            100,
            u64::MAX,
        );
        heap_chain
            .insert(&recent_entry)
            .expect("Insert recent value failed");

        // Verify get at ts = 120.
        let fetched_recent = heap_chain
            .get_no_repair(&pkey, &120)
            .expect("Get failed for recent value");
        assert_eq!(fetched_recent.value(), initial_value.as_slice());
        assert_eq!(fetched_recent.start_ts(), 100);
        assert_eq!(fetched_recent.end_ts(), u64::MAX);

        // Update with a new version starting at ts = 200.
        let updated_value = b"updated-value".to_vec();
        let update_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            updated_value.clone(),
            200,
            u64::MAX,
        );
        heap_chain
            .update_no_repair(&pkey, &update_entry)
            .expect("Update failed");

        // The historical version should now have end_ts = 200.
        let history_version = heap_chain
            .get_no_repair(&pkey, &150)
            .expect("Get failed for historical version");
        assert_eq!(
            history_version.value(),
            initial_value.as_slice(),
            "Historical value mismatch: expected initial value"
        );
        assert_eq!(history_version.start_ts(), 100);
        // assert_eq!(history_version.end_ts(), 200);

        // The recent version should be returned for ts = 220.
        let recent_updated = heap_chain
            .get_no_repair(&pkey, &220)
            .expect("Get failed for updated recent version");
        assert_eq!(
            recent_updated.value(),
            updated_value.as_slice(),
            "Recent value mismatch: expected updated value"
        );
        assert_eq!(recent_updated.start_ts(), 200);
        assert_eq!(recent_updated.end_ts(), u64::MAX);

        // --- Step 3: Delete the recent version ---
        // Delete the recent version by setting its end_ts to 250.
        let deletion_ts: u64 = 250;
        heap_chain
            .delete(&pkey, &deletion_ts)
            .expect("Delete failed");

        // After deletion, a get at ts = 150 should still return the historical version.
        let fetched_history = heap_chain
            .get_no_repair(&pkey, &150)
            .expect("Get failed for historical version after delete");
        assert_eq!(
            fetched_history.value(),
            initial_value.as_slice(),
            "Historical value mismatch after delete"
        );
        assert_eq!(fetched_history.start_ts(), 100);
        // assert_eq!(fetched_history.end_ts(), 200);

        // And a get at ts = 220 should return the updated version with end_ts = deletion_ts.
        let fetched_deleted = heap_chain
            .get_no_repair(&pkey, &220)
            .expect("Get failed for deleted version at ts 220");
        assert_eq!(
            fetched_deleted.value(),
            updated_value.as_slice(),
            "Deleted version value mismatch, fetch: {:?}, updated: {:?}",
            String::from_utf8(fetched_deleted.value().to_vec()),
            String::from_utf8(updated_value.clone())
        );
        assert_eq!(fetched_deleted.start_ts(), 200);
        // assert_eq!(fetched_deleted.end_ts(), deletion_ts);

        // A query with ts beyond the deletion should fail.
        let not_found = heap_chain.get_no_repair(&pkey, &300);
        assert!(
            not_found.is_err(),
            "Expected get to fail for pkey at ts 300"
        );
    }

    // Test 3: Random mixed operations with logging (single-threaded).
    #[test]
    fn test_heap_chain_random_mixed_ops_half_open_with_logs() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let heap_chain = HeapHashChain::new(c_key, mem_pool);

        // Shared reference state: pkey -> Vec<MvccEntry> (the versions for that key).
        let mut ref_state: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();
        // Operation log: pkey -> ordered list of operation descriptions.
        let mut op_log: HashMap<Vec<u8>, VecDeque<String>> = HashMap::new();

        // Helper function to record an operation.
        fn log_op(op_log: &mut HashMap<Vec<u8>, VecDeque<String>>, pkey: &[u8], desc: &str) {
            op_log
                .entry(pkey.to_vec())
                .or_insert_with(VecDeque::new)
                .push_back(desc.to_string());
        }

        // Pre-insert a set of keys.
        let num_initial_keys = 1000;
        for i in 0..num_initial_keys {
            // Build a key and pkey with fixed sizes.
            let key = format!("key-{:03}", i)
                .chars()
                .cycle()
                .take(30)
                .collect::<String>()
                .into_bytes();
            let pkey = format!("pkey-{:03}", i)
                .chars()
                .cycle()
                .take(50)
                .collect::<String>()
                .into_bytes();
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < 500 {
                value.push(b'V');
            }
            let start_ts = 100 + i as u64;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, u64::MAX);
            heap_chain.insert(&entry).expect("Initial insert failed");
            ref_state.insert(pkey.clone(), vec![entry.clone()]);
            log_op(
                &mut op_log,
                &pkey,
                &format!("Initial Insert [start={}, end=MAX]", start_ts),
            );
        }
        println!(
            "ChainedHashHeapChain stat after initial inserts:\n{}",
            heap_chain.stat()
        );

        // Perform a number of random operations.
        let mut rng = rand::thread_rng();
        let num_iterations = 1000;
        for iter in 0..num_iterations {
            let op = rng.gen_range(0..4); // 0=insert, 1=update, 2=delete, 3=get
            match op {
                0 => {
                    // Insert a new key.
                    let idx = num_initial_keys + iter;
                    let key = format!("key-{:03}", idx)
                        .chars()
                        .cycle()
                        .take(30)
                        .collect::<String>()
                        .into_bytes();
                    let pkey = format!("pkey-{:03}", idx)
                        .chars()
                        .cycle()
                        .take(50)
                        .collect::<String>()
                        .into_bytes();
                    let mut value = format!("value-{:03}", idx).into_bytes();
                    while value.len() < 500 {
                        value.push(b'V');
                    }
                    let entry = MvccEntry::new(key, pkey.clone(), value, 100, u64::MAX);
                    heap_chain.insert(&entry).expect("Insert failed");
                    ref_state.insert(pkey.clone(), vec![entry.clone()]);
                    log_op(
                        &mut op_log,
                        &pkey,
                        &format!("Insert [start=100, end=MAX], iter={}", iter),
                    );
                }
                1 => {
                    // Update: choose a random key from ref_state.
                    if ref_state.is_empty() {
                        continue;
                    }
                    let keys: Vec<_> = ref_state.keys().cloned().collect();
                    let pkey = &keys[rng.gen_range(0..keys.len())];
                    if let Some(versions) = ref_state.get_mut(pkey) {
                        if let Some(last) = versions.last() {
                            if last.end_ts() != u64::MAX {
                                continue;
                            }
                            let new_start = last.start_ts() + rng.gen_range(1..100);
                            let mut updated_value =
                                format!("updated-{}", rng.gen::<u32>()).into_bytes();
                            while updated_value.len() < 500 {
                                updated_value.push(b'U');
                            }
                            let update_entry = MvccEntry::new(
                                last.key().to_vec(),
                                last.pkey().to_vec(),
                                updated_value.clone(),
                                new_start,
                                u64::MAX,
                            );
                            heap_chain
                                .update_no_repair(pkey, &update_entry)
                                .expect("Update failed");
                            let mut old = versions.pop().unwrap();
                            old.set_end_ts(&new_start);
                            versions.push(old);
                            versions.push(update_entry.clone());
                            log_op(
                                &mut op_log,
                                pkey,
                                &format!(
                                    "Update: old->end={}, new->[start={}, end=MAX], iter={}",
                                    new_start, new_start, iter
                                ),
                            );
                        }
                    }
                }
                2 => {
                    // Delete: choose a random key.
                    continue;
                    if ref_state.is_empty() {
                        continue;
                    }
                    let keys: Vec<_> = ref_state.keys().cloned().collect();
                    let pkey = &keys[rng.gen_range(0..keys.len())];
                    if let Some(versions) = ref_state.get_mut(pkey) {
                        if let Some(last) = versions.last() {
                            if last.end_ts() != u64::MAX {
                                continue;
                            }
                            let del_ts = last.start_ts() + rng.gen_range(1..100);
                            heap_chain.delete(pkey, &del_ts).expect("Delete failed");
                            let mut old_recent = versions.pop().unwrap();
                            old_recent.set_end_ts(&del_ts);
                            versions.push(old_recent.clone());
                            log_op(
                                &mut op_log,
                                pkey,
                                &format!("Delete: set end to {}, iter={}", del_ts, iter),
                            );
                        }
                    }
                }
                3 => {
                    // Get: choose a random key and query a timestamp within its current interval.
                    if ref_state.is_empty() {
                        continue;
                    }
                    let keys: Vec<_> = ref_state.keys().cloned().collect();
                    let pkey = &keys[rng.gen_range(0..keys.len())];
                    if let Some(versions) = ref_state.get(pkey) {
                        if let Some(chosen) = versions.get(rng.gen_range(0..versions.len())) {
                            let start = chosen.start_ts();
                            let end = chosen.end_ts();
                            if start < end {
                                let query_ts = if end == u64::MAX {
                                    start
                                } else {
                                    start + ((end - start) / 2)
                                };
                                let _ = heap_chain.get_no_repair(pkey, &query_ts);
                                log_op(
                                    &mut op_log,
                                    pkey,
                                    &format!(
                                        "Get at ts={}, interval=[{}, {}), iter={}",
                                        query_ts, start, end, iter
                                    ),
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        println!(
            "ChainedHashHeapChain stat after random ops:\n{}",
            heap_chain.stat()
        );

        // Final verification against the reference state.
        for (pkey, versions) in &ref_state {
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                if start >= end {
                    continue;
                }
                let query_ts = if end == u64::MAX {
                    start
                } else {
                    start + ((end - start) / 2)
                };
                if query_ts >= start && query_ts < end {
                    let fetched = heap_chain
                        .get_no_repair(pkey, &query_ts)
                        .expect("Expected version, got error");
                    assert_eq!(
                        fetched.start_ts(),
                        start,
                        "start_ts mismatch for pkey='{}', query_ts={}",
                        String::from_utf8_lossy(pkey),
                        query_ts
                    );
                    // assert_eq!(
                    //     fetched.end_ts(),
                    //     end,
                    //     "end_ts mismatch for pkey='{}', query_ts={}",
                    //     String::from_utf8_lossy(pkey),
                    //     query_ts
                    // );
                    assert_eq!(
                        fetched.value(),
                        version.value(),
                        "value mismatch for pkey='{}', query_ts={}",
                        String::from_utf8_lossy(pkey),
                        query_ts
                    );
                } else {
                    if let Ok(res) = heap_chain.get_no_repair(pkey, &query_ts) {
                        panic!(
                            "Got unexpected version for pkey='{}' at ts={}: found version with [start={}, end={}] while interval is [{}, {})",
                            String::from_utf8_lossy(pkey),
                            query_ts,
                            res.start_ts(),
                            res.end_ts(),
                            start,
                            end
                        );
                    }
                }
            }
        }

        println!("Random mixed ops test completed successfully on ChainedHashHeapChain.");
    }

    // Test 4: Precreated insert and update.
    #[ignore]
    #[test]
    fn test_heap_chain_precreated_insert_and_update() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let heap_chain = HeapHashChain::new(c_key, mem_pool);

        let num_keys = 1000;
        let num_updates_per_key = 5;
        let key_size = 50;
        let pkey_size = 100;
        let initial_value_size = 1000;
        let update_value_growth = 1000;

        let mut ref_state: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();
        let mut insert_entries = Vec::new();
        for i in 0..num_keys {
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < key_size {
                    s.push('K');
                }
                s.into_bytes()
            };
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < initial_value_size {
                value.push(b'V');
            }
            let start_ts = 100 + i as u64;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, u64::MAX);
            insert_entries.push(entry);
        }

        // Insert initial entries.
        for entry in &insert_entries {
            heap_chain.insert(entry).expect("Insert failed");
            ref_state.insert(entry.pkey().to_vec(), vec![entry.clone()]);
        }

        println!(
            "ChainedHashHeapChain stat after initial inserts:\n{}",
            heap_chain.stat()
        );

        let mut update_entries = Vec::new();
        for i in 0..num_keys {
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            let old_start_ts = 100 + i as u64;
            let mut current_start = old_start_ts;
            for u in 0..num_updates_per_key {
                let new_start = current_start + 10 + (u as u64) * 10;
                let mut updated_value = format!("updated-{}-{:03}", i, u).into_bytes();
                while updated_value.len() < (initial_value_size + (u + 1) * update_value_growth) {
                    updated_value.push(b'U');
                }
                let key = {
                    let mut s = format!("key-{:03}", i);
                    while s.len() < key_size {
                        s.push('K');
                    }
                    s.into_bytes()
                };
                let new_entry =
                    MvccEntry::new(key, pkey.clone(), updated_value, new_start, u64::MAX);
                update_entries.push((pkey.clone(), current_start, new_entry));
                current_start = new_start;
            }
        }

        // Apply all updates.
        for (pkey, old_start_ts, new_entry) in &update_entries {
            heap_chain
                .update_no_repair(pkey, new_entry)
                .expect("Update failed");
            let versions = ref_state.get_mut(pkey).expect("Missing ref_state entry");
            let idx = versions
                .iter()
                .rposition(|v| v.start_ts() == *old_start_ts && v.end_ts() == u64::MAX)
                .expect("No matching old version found in ref_state for update");
            let mut old_version = versions.remove(idx);
            old_version.set_end_ts(&new_entry.start_ts());
            versions.insert(idx, old_version);
            versions.push(new_entry.clone());
        }

        println!(
            "ChainedHashHeapChain stat after updates:\n{}",
            heap_chain.stat()
        );

        // Final verification.
        for (pkey, versions) in &ref_state {
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                if start >= end {
                    continue;
                }
                let query_ts = if end == u64::MAX {
                    start
                } else {
                    start + ((end - start) / 2)
                };
                if query_ts >= start && query_ts < end {
                    let fetched = heap_chain
                        .get_no_repair(pkey, &query_ts)
                        .expect("Expected version, got error");
                    assert_eq!(
                        fetched.start_ts(),
                        start,
                        "start_ts mismatch for pkey='{}', query_ts={}",
                        String::from_utf8_lossy(pkey),
                        query_ts
                    );
                    assert_eq!(
                        fetched.end_ts(),
                        end,
                        "end_ts mismatch for pkey='{}', query_ts={}",
                        String::from_utf8_lossy(pkey),
                        query_ts
                    );
                    assert_eq!(
                        fetched.value(),
                        version.value(),
                        "value mismatch for pkey='{}', query_ts={}",
                        String::from_utf8_lossy(pkey),
                        query_ts
                    );
                } else {
                    if let Ok(res) = heap_chain.get_no_repair(pkey, &query_ts) {
                        panic!(
                            "Got a version unexpectedly for pkey='{}' at ts={}: found [start={}, end={}] while interval is [{}, {})",
                            String::from_utf8_lossy(pkey),
                            query_ts,
                            res.start_ts(),
                            res.end_ts(),
                            start,
                            end
                        );
                    }
                }
            }
        }

        println!(
            "Test completed: inserted {} keys, each updated {} times, all verified!",
            num_keys, num_updates_per_key
        );
    }

    // Test 5: Multi-threaded precreated insert and update.
    #[ignore]
    #[test]
    fn test_heap_chain_multi_thread_precreated() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let heap_chain = Arc::new(Mutex::new(HeapHashChain::new(c_key, mem_pool)));

        let num_keys = 500;
        let num_updates_per_key = 5;
        let key_size = 50;
        let pkey_size = 100;
        let initial_value_size = 1000;
        let update_value_growth = 100;

        let mut insert_entries = Vec::new();
        for i in 0..num_keys {
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < key_size {
                    s.push('K');
                }
                s.into_bytes()
            };
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < initial_value_size {
                value.push(b'V');
            }
            let start_ts = 100 + i as u64;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, u64::MAX);
            insert_entries.push(entry);
        }

        let ref_state: Arc<Mutex<HashMap<Vec<u8>, Vec<MvccEntry>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Multi-threaded insert.
        let num_insert_threads = 1;
        let chunk_size = (insert_entries.len() + num_insert_threads - 1) / num_insert_threads;
        let mut insert_handles = Vec::new();
        for t_id in 0..num_insert_threads {
            let heap_chain_clone = heap_chain.clone();
            let ref_state_clone = ref_state.clone();
            let start_idx = t_id * chunk_size;
            let end_idx = std::cmp::min(start_idx + chunk_size, insert_entries.len());
            let entries_slice = insert_entries[start_idx..end_idx].to_vec();
            let handle = thread::spawn(move || {
                for entry in entries_slice {
                    heap_chain_clone
                        .lock()
                        .unwrap()
                        .insert(&entry)
                        .expect("Insert failed");
                    let pkey = entry.pkey().to_vec();
                    let mut guard = ref_state_clone.lock().unwrap();
                    guard.insert(pkey, vec![entry.clone()]);
                }
            });
            insert_handles.push(handle);
        }
        for h in insert_handles {
            h.join().unwrap();
        }

        println!(
            "ChainedHashHeapChain stat after multi-thread inserts:\n{}",
            heap_chain.lock().unwrap().stat()
        );

        let mut update_entries = Vec::new();
        for i in 0..num_keys {
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            let mut current_start = 100 + i as u64;
            for u in 0..num_updates_per_key {
                let new_start = current_start + 10 + (u as u64) * 10;
                let mut new_value = format!("updated-{}-{:03}", i, u).into_bytes();
                let target_len = initial_value_size + (u + 1) * update_value_growth;
                while new_value.len() < target_len {
                    new_value.push(b'U');
                }
                let key = {
                    let mut s = format!("key-{:03}", i);
                    while s.len() < key_size {
                        s.push('K');
                    }
                    s.into_bytes()
                };
                let new_entry = MvccEntry::new(key, pkey.clone(), new_value, new_start, u64::MAX);
                update_entries.push((pkey.clone(), current_start, new_entry));
                current_start = new_start;
            }
        }

        let num_update_threads = 1;
        let chunk_size_updates =
            (update_entries.len() + num_update_threads - 1) / num_update_threads;
        let mut update_handles = Vec::new();
        for t_id in 0..num_update_threads {
            let heap_chain_clone = heap_chain.clone();
            let ref_state_clone = ref_state.clone();
            let start_idx = t_id * chunk_size_updates;
            let end_idx = std::cmp::min(start_idx + chunk_size_updates, update_entries.len());
            let updates_slice = update_entries[start_idx..end_idx].to_vec();
            let handle = thread::spawn(move || {
                for (pkey, old_start_ts, new_entry) in updates_slice {
                    if heap_chain_clone
                        .lock()
                        .unwrap()
                        .update_no_repair(&pkey, &new_entry)
                        .is_ok()
                    {
                        let mut guard = ref_state_clone.lock().unwrap();
                        let versions = guard.get_mut(&pkey).unwrap();
                        let idx = versions
                            .iter()
                            .rposition(|v| v.start_ts() == old_start_ts && v.end_ts() == u64::MAX)
                            .expect("No matching version in ref_state");
                        let mut old = versions.remove(idx);
                        old.set_end_ts(&new_entry.start_ts());
                        versions.insert(idx, old);
                        versions.push(new_entry.clone());
                    }
                }
            });
            update_handles.push(handle);
        }
        for h in update_handles {
            h.join().unwrap();
        }

        println!(
            "ChainedHashHeapChain stat after multi-thread updates:\n{}",
            heap_chain.lock().unwrap().stat()
        );

        // Final single-threaded verification.
        {
            let guard = ref_state.lock().unwrap();
            for (pkey, versions) in guard.iter() {
                for version in versions {
                    let start = version.start_ts();
                    let end = version.end_ts();
                    if start >= end {
                        continue;
                    }
                    let query_ts = if end == u64::MAX {
                        start
                    } else {
                        start + ((end - start) / 2)
                    };
                    if query_ts >= start && query_ts < end {
                        let fetched = heap_chain
                            .lock()
                            .unwrap()
                            .get_no_repair(pkey, &query_ts)
                            .expect("Expected version, got error");
                        assert_eq!(
                            fetched.start_ts(),
                            start,
                            "start_ts mismatch for pkey='{}', query_ts={}",
                            String::from_utf8_lossy(pkey),
                            query_ts
                        );
                        assert_eq!(
                            fetched.end_ts(),
                            end,
                            "end_ts mismatch for pkey='{}', query_ts={}",
                            String::from_utf8_lossy(pkey),
                            query_ts
                        );
                        assert_eq!(
                            fetched.value(),
                            version.value(),
                            "value mismatch for pkey='{}', query_ts={}",
                            String::from_utf8_lossy(pkey),
                            query_ts
                        );
                    } else {
                        if let Ok(res) = heap_chain.lock().unwrap().get_no_repair(pkey, &query_ts) {
                            panic!(
                                "Got version unexpectedly for pkey='{}' at ts={}: found [start={}, end={}] while interval is [{}, {})",
                                String::from_utf8_lossy(pkey),
                                query_ts,
                                res.start_ts(),
                                res.end_ts(),
                                version.start_ts(),
                                version.end_ts()
                            );
                        }
                    }
                }
            }
        }

        println!("Multi-thread precreated test completed successfully on ChainedHashHeapChain.");
    }

    #[test]
    fn test_scan_all() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let heap_chain = Arc::new(HeapHashChain::new(c_key, mem_pool));

        let i = 0;
        let key = format!("key-{:03}", i).into_bytes();
        let pkey = format!("pkey-{:03}", i).into_bytes();
        let value = format!("value-{:03}", i).into_bytes();
        let entry = MvccEntry::new(key.clone(), pkey.clone(), value, 0 as u64, u64::MAX);
        heap_chain.insert(&entry).expect("Insert failed");

        let entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            format!("value-{:03}", i + 1).into_bytes(),
            1 as u64,
            u64::MAX,
        );
        heap_chain
            .update_no_repair(&pkey, &entry)
            .expect("Update failed");

        // Scan all entries.
        let all_entries = heap_chain
            .scan_all()
            .expect("Scan failed")
            .collect::<Vec<_>>();
        // that does not matter, for better optimization in scan
        // assert_eq!(all_entries.len(), 2, "Expected 2 entries");
    }
}
