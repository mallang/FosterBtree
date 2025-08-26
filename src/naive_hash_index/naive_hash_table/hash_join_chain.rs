use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{self, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_info, log_trace, log_warn,
    mvcc_index::{
        hash_common::{
            fix_frame_id, fix_frame_id2, read_page, write_page, RowDelta, StatCollector,
        },
        hash_join_page::record::{Record, RecordRef},
        MvccEntry,
    },
    naive_hash_index::naive_hash_table::hash_join_page::NaiveHashPage,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
    prelude::Timestamp,
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

    pub fn collect_space_stat(&self, stat: &mut StatCollector) {
        let mut current_page = self.first_page();
        loop {
            current_page.collect_space_stat(stat);
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

    pub fn scan_deltas(
        from: &Arc<Self>,
        to: &Arc<Self>,
        results: &mut HashMap<Vec<u8>, RowDelta>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = from.first_page();
        loop {
            current_page.scan_delta_as_from(results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*from.mem_pool,
                    PageFrameKey::new_with_frame_id(from.c_key, next_pid, next_fid),
                );
                if next_page.frame_id() != next_fid {
                    let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
                }
                current_page = next_page;
            } else {
                break;
            }
        }

        let mut current_page = to.first_page();
        loop {
            current_page.scan_delta_as_to(results);
            if let Some((next_pid, next_fid)) = current_page.next_page() {
                let next_page = read_page(
                    &*to.mem_pool,
                    PageFrameKey::new_with_frame_id(to.c_key, next_pid, next_fid),
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
        NaiveHashPage::init(&mut *page);
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

    pub fn insert(&self, rec: &RecordRef) -> Result<(), AccessMethodError> {
        let space_need = <Page as NaiveHashPage>::require_space_rec(&rec);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        let last_page_id = self.last_page_id.load(Ordering::Acquire);
        let last_frame_id = self.last_frame_id.load(Ordering::Acquire);
        let last_page_frame_key =
            PageFrameKey::new_with_frame_id(self.c_key, last_page_id, last_frame_id);
        let mut last_page = self.get_tail_page_for_write(last_page_frame_key)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        match last_page.insert_heap_no_repair(&rec, 0, 0) {
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
                match new_page.insert_heap_no_repair(&rec, 0, 0) {
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

    pub fn get_no_repair(&self, pkey: &[u8]) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.first_page();

        loop {
            if let Some(entry) = current_page.heap_get(pkey).ok() {
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

    pub fn scan_into_vec(
        self: &Arc<Self>,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            // println!("scan a page");
            current_page.chain_scan_into_vec(results);
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

    /// in chain, iterate in increasing order of start_ts
    /// => tail is newer than head
    pub fn chain_scan_key(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        res: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), AccessMethodError> {
        // {
        //     let mut current_page = self.first_page();
        //     loop {
        //         let header = current_page.unsafe_header();

        //         if ts < &header.page_min_start_ts() {
        //             // do nothing
        //         } else if header.recent_slot_cnt() == 0 && ts >= &header.page_max_end_ts() {
        //             // do nothing
        //         } else {
        //             current_page.chain_scan_key(
        //                 search_key,
        //                 ts,
        //                 res,
        //             )?;
        //         }

        //         if let Some((next_pid, next_fid)) = current_page.next_page() {
        //             let next_page = read_page(
        //                 &*self.mem_pool,
        //                 PageFrameKey::new_with_frame_id(self.c_key, next_pid, next_fid),
        //             );
        //             if next_page.frame_id() != next_fid {
        //                 let _ = fix_frame_id(current_page, next_pid, next_page.frame_id());
        //             }
        //             current_page = next_page;
        //             continue;
        //         }
        //         // no next page in current chain
        //         break;
        //     }
        // }
        todo!()
        // return Ok(());
    }
}
