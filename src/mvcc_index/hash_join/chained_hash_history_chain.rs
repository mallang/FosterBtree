use std::{
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    time::Duration,
    vec::IntoIter,
};

use dashmap::mapref::entry;

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_trace, log_warn,
    mvcc_index::MvccEntry,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::{chained_hash_page::HashJoinPage, Timestamp};

pub struct ChainedHashHistoryChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    first_page_id: PageId,
    first_frame_id: AtomicU32,
}

impl<T: MemPool> ChainedHashHistoryChain<T> {
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
            first_page_id,
            first_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn load(c_key: ContainerKey, mem_pool: Arc<T>, pid: PageId) -> Self {
        Self {
            mem_pool,
            c_key,
            first_page_id: pid,
            first_frame_id: AtomicU32::new(u32::MAX),
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let entry = &mut entry.clone();
        let space_need = <Page as HashJoinPage>::require_space(&entry);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }
        let mut last_page = self.traverse_until_endofchain_for_insert(self.first_key(), entry)?;
        log_trace!("Acquired write lock for page {}", last_page.get_id());
        match last_page.upsert_history(entry) {
            Ok(_) => Ok(()),
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
                match new_page.upsert_history(entry) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn traverse_until_endofchain_for_insert(
        &self,
        page_key: PageFrameKey,
        entry: &MvccEntry,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            let last_page = self.try_traverse_until_endofchain_for_insert(page_key, entry);
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
                Err(AccessMethodError::KeyDuplicate) => {
                    return Err(AccessMethodError::KeyDuplicate);
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn try_traverse_until_endofchain_for_insert(
        &self,
        page_key: PageFrameKey,
        entry: &MvccEntry,
    ) -> Result<FrameWriteGuard, AccessMethodError> {
        let mut current_page = self.read_page(page_key);
        loop {
            if let Some((next_page_id, next_frame_id)) = current_page.next_page() {
                if current_page.binary_search(entry.search_key()).0 {
                    return Err(AccessMethodError::KeyDuplicate);
                }
                // TODO: check free space may can insert here later.
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
                // TODO: check key to avoid write lock in case of duplicate key
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

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let mut current_page = self.first_page();
        loop {
            match current_page.get_history(pkey, ts) {
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

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id
    }

    pub fn first_frame_id(&self) -> u32 {
        self.first_frame_id.load(atomic::Ordering::Acquire)
    }

    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!(
                        "All frames are latched and cannot evict page to read the page: {:?}. Will retry",
                        page_key
                    );
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
    // ) -> Result<MvccHashJoinHistoryChainScanner<T>, AccessMethodError> {
    //     Ok(MvccHashJoinHistoryChainScanner::new(
    //         Arc::new(self.clone()),
    //         ts,
    //     ))
    // }

    pub fn scan(
        self: &Arc<Self>,
        ts: Timestamp,
    ) -> Result<ChainedHashHistoryChainScanner<T>, AccessMethodError> {
        Ok(ChainedHashHistoryChainScanner::new(self, ts))
    }

    // pub fn scan_all(&self) -> Result<MvccHashJoinHistoryChainScanner<T>, AccessMethodError> {
    //     // Create a scanner with ts = u64::MAX and no timestamp filtering
    //     Ok(MvccHashJoinHistoryChainScanner::new_full_scan(Arc::new(
    //         self.clone(),
    //     )))
    // }

    pub fn scan_all(
        self: &Arc<Self>,
    ) -> Result<ChainedHashHistoryChainScanner<T>, AccessMethodError> {
        Ok(ChainedHashHistoryChainScanner::new_full_scan(self))
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

// Implement Clone for MvccHashJoinHistoryChain to allow cloning
impl<T: MemPool> Clone for ChainedHashHistoryChain<T> {
    fn clone(&self) -> Self {
        Self {
            mem_pool: Arc::clone(&self.mem_pool),
            c_key: self.c_key,
            first_page_id: self.first_page_id,
            first_frame_id: AtomicU32::new(self.first_frame_id.load(atomic::Ordering::SeqCst)),
        }
    }
}

pub struct ChainedHashHistoryChainScanner<T: MemPool> {
    chain: Arc<ChainedHashHistoryChain<T>>,
    ts: Timestamp,
    filter_by_ts: bool,

    current_page: Option<FrameReadGuard<'static>>,
    current_slot_id: usize,

    initialized: bool,
    finished: bool,
}
impl<T: MemPool> ChainedHashHistoryChainScanner<T> {
    pub fn new(chain: &Arc<ChainedHashHistoryChain<T>>, ts: Timestamp) -> Self {
        Self {
            chain: chain.clone(),
            ts,
            filter_by_ts: true,
            current_page: None,
            current_slot_id: 0,
            initialized: false,
            finished: false,
        }
    }

    pub fn new_full_scan(chain: &Arc<ChainedHashHistoryChain<T>>) -> Self {
        Self {
            chain: chain.clone(),
            ts: u64::MAX, // ts is irrelevant in full scan
            filter_by_ts: false,
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

impl<T: MemPool> Iterator for ChainedHashHistoryChainScanner<T> {
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
                let entry = current_page
                    .get_entry_at_slot_id(self.current_slot_id)
                    .unwrap();
                self.current_slot_id += 1;
                if self.filter_by_ts {
                    if self.ts < entry.start_ts() || entry.end_ts <= self.ts {
                        continue;
                    } else {
                        return Some(entry);
                    }
                } else {
                    return Some(entry);
                }
            } else {
                // Move to the next page
                if let Some((next_pid, next_fid)) = current_page.next_page() {
                    let next_page = self.chain.read_page(PageFrameKey::new_with_frame_id(
                        self.chain.c_key,
                        next_pid,
                        next_fid,
                    ));
                    let next_page = unsafe {
                        std::mem::transmute::<FrameReadGuard, FrameReadGuard<'static>>(next_page)
                    };
                    self.current_page = Some(next_page);
                    self.current_slot_id = 0;
                } else {
                    self.finish();
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Returns a string of exactly `target_length` characters. It starts with `base` and then
    /// appends the `pad` repeatedly until reaching or exceeding the target length, and finally
    /// truncates the result to exactly `target_length`.
    fn fixed_length_string(base: &str, target_length: usize, pad: &str) -> String {
        let mut s = base.to_string();
        while s.len() < target_length {
            s.push_str(pad);
        }
        s.truncate(target_length);
        s
    }

    /// Generates history MVCC entries for multiple primary keys, where each primary key has multiple versions.
    /// - `num_pkeys`: Number of distinct primary keys to generate.
    /// - `versions_per_pkey`: Number of history versions for each primary key.
    /// - `base_start`: The start timestamp for the first version of each primary key.
    /// - `interval`: The duration of each version's valid interval.
    /// - `key_size`: The fixed length (in bytes) for the key string.
    /// - `pkey_size`: The fixed length (in bytes) for the primary key string.
    /// - `value_size`: The fixed length (in bytes) for the value string.
    ///
    /// For each primary key, the key is generated using the primary key index (as a string)
    /// padded with "-key" until it reaches `key_size`. Similarly, the primary key is generated
    /// using the same index padded with "-pkey". For each version, the value is generated using
    /// the version number padded with "-value" until reaching `value_size`.
    pub fn generate_history_mvcc_entries_multi(
        num_pkeys: usize,
        versions_per_pkey: usize,
        base_start: u64,
        interval: u64,
        key_size: usize,
        pkey_size: usize,
        value_size: usize,
    ) -> Vec<MvccEntry> {
        let mut entries = Vec::with_capacity(num_pkeys * versions_per_pkey);
        for p in 0..num_pkeys {
            // Build fixed-length key and primary key strings.
            let key = fixed_length_string(&format!("{}", p), key_size, "-key");
            let pkey = fixed_length_string(&format!("{}", p), pkey_size, "-pkey");
            for v in 0..versions_per_pkey {
                let start_ts = base_start + (v as u64) * interval;
                let end_ts = start_ts + interval;
                // Build a fixed-length value string.
                let value = fixed_length_string(&format!("{}", v), value_size, "-value");
                let entry = MvccEntry::new(
                    key.clone().into_bytes(),
                    pkey.clone().into_bytes(),
                    value.into_bytes(),
                    start_ts,
                    end_ts,
                );
                entries.push(entry);
            }
        }
        entries
    }

    #[test]
    fn test_generate_history_mvcc_entries_multi() {
        // Parameters for generation.
        let num_pkeys = 5;
        let versions_per_pkey = 3;
        let base_start = 100;
        let interval = 50;
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 30;

        // Generate history entries.
        let entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );

        // Check that we have the expected number of entries.
        assert_eq!(entries.len(), num_pkeys * versions_per_pkey);

        // For each entry, verify that:
        // - The key is of the correct length.
        // - The primary key is of the correct length.
        // - The value is of the correct length.
        // - The timestamps are as expected.
        for p in 0..num_pkeys {
            // Expected key and pkey.
            let expected_key = fixed_length_string(&format!("{}", p), key_size, "-key");
            let expected_pkey = fixed_length_string(&format!("{}", p), pkey_size, "-pkey");
            for v in 0..versions_per_pkey {
                let index = p * versions_per_pkey + v;
                let entry = &entries[index];
                let expected_value = fixed_length_string(&format!("{}", v), value_size, "-value");
                let expected_start_ts = base_start + (v as u64) * interval;
                let expected_end_ts = expected_start_ts + interval;

                assert_eq!(String::from_utf8_lossy(&entry.key()), expected_key);
                assert_eq!(String::from_utf8_lossy(&entry.pkey()), expected_pkey);
                assert_eq!(String::from_utf8_lossy(&entry.value()), expected_value);
                assert_eq!(entry.start_ts(), expected_start_ts);
                assert_eq!(entry.end_ts(), expected_end_ts);
            }
        }
    }

    #[test]
    fn test_history_chain_basic_insert_get() {
        use std::sync::Arc;

        // Create a mem pool and initialize the history chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(16, 0);
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        // Parameters for generating history entries:
        // - num_pkeys: number of distinct primary keys.
        // - versions_per_pkey: number of versions (history entries) for each primary key.
        // - base_start: the starting timestamp for the first version.
        // - interval: the width (duration) of each version's valid time interval.
        // - key_size, pkey_size, value_size: fixed lengths for the key, primary key, and value.
        let num_pkeys = 100;
        let versions_per_pkey = 10;
        let base_start = 100;
        let interval = 50;
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 300;

        // Generate history entries for multiple primary keys.
        let entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );

        // Insert each generated entry into the history chain.
        for (i, entry) in entries.iter().enumerate() {
            history_chain
                .insert(entry)
                .expect(&format!("Insert failed for history entry {}", i));
        }
        // println!("History chain statistics after insertion:\n{}", history_chain.stat());

        // For each inserted entry, choose a query timestamp in the middle of its interval
        // and verify that get() returns the correct entry.
        for entry in entries.iter() {
            let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
            let fetched = history_chain
                .get(&entry.pkey(), &query_ts)
                .expect("Get failed for an inserted history entry");
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for entry with pkey {:?}",
                String::from_utf8_lossy(&entry.pkey())
            );
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for entry with pkey {:?}",
                String::from_utf8_lossy(&entry.pkey())
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for entry with pkey {:?}",
                String::from_utf8_lossy(&entry.pkey())
            );
        }
    }

    #[test]
    fn test_history_chain_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        // Create a mem pool and initialize a history chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(20, 0);
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        // Generate history entries for multiple primary keys.
        // For example, generate entries for 100 primary keys with 5 versions each.
        let num_pkeys = 1000;
        let versions_per_pkey = 5;
        let base_start = 100; // the first version for each pkey starts at 1000
        let interval = 10; // each version is valid for 10 time units: [start, start+10)
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 300;
        let entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );
        let total_entries = entries.len();
        // println!("Generated {} history entries", total_entries);

        // Wrap the generated entries in an Arc so they can be shared among threads.
        let entries_arc = Arc::new(entries);

        // --- Concurrent Inserts ---
        // Partition the entries among a fixed number of insert threads.
        let num_insert_threads = 5;
        let chunk_size = (total_entries + num_insert_threads - 1) / num_insert_threads; // ceiling division
        let mut insert_handles = Vec::new();
        for t in 0..num_insert_threads {
            let chain_for_insert = history_chain.clone();
            let entries_for_insert = Arc::clone(&entries_arc);
            let start_idx = t * chunk_size;
            let end_idx = ((t + 1) * chunk_size).min(total_entries);
            let handle = thread::spawn(move || {
                for i in start_idx..end_idx {
                    // Clone the entry since upsert_history() expects a mutable reference.
                    let mut entry = entries_for_insert[i].clone();
                    chain_for_insert
                        .insert(&mut entry)
                        .expect(&format!("Insert failed for history entry {}", i));
                }
            });
            insert_handles.push(handle);
        }
        // Wait for all insert threads to finish.
        for handle in insert_handles {
            handle.join().unwrap();
        }
        // println!("History chain statistics after insertion:\n{}", history_chain.stat());

        // --- Final Verification ---
        // For each generated history entry, choose a query timestamp in the middle of its interval
        // and verify that get() returns an entry with the same value and timestamps.
        for (i, entry) in entries_arc.iter().enumerate() {
            let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
            let fetched = history_chain
                .get(&entry.pkey(), &query_ts)
                .expect(&format!("Get failed for history entry {}", i));
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for history entry {}",
                i
            );
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "Start timestamp mismatch for history entry {}",
                i
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "End timestamp mismatch for history entry {}",
                i
            );
        }
    }

    #[test]
    fn test_history_chain_concurrent_inserts_and_concurrent_gets() {
        use std::sync::Arc;
        use std::thread;

        // Create a mem pool and initialize a history chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(20, 0);
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        // Generate history entries for multiple primary keys.
        // For example, generate entries for 1000 primary keys with 5 versions per primary key.
        let num_pkeys = 1000;
        let versions_per_pkey = 5;
        let base_start = 100; // The first version for each pkey starts at 100.
        let interval = 10; // Each version is valid for 10 time units: [start, start+10)
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 300;
        let entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );
        let total_entries = entries.len();
        // println!("Generated {} history entries", total_entries);

        // Wrap the generated entries in an Arc so they can be shared among threads.
        let entries_arc = Arc::new(entries);

        // --- Concurrent Inserts ---
        // Partition the entries among several insert threads.
        let num_insert_threads = 5;
        let chunk_size = (total_entries + num_insert_threads - 1) / num_insert_threads; // ceiling division
        let mut insert_handles = Vec::new();
        for t in 0..num_insert_threads {
            let chain_for_insert = history_chain.clone();
            let entries_for_insert = Arc::clone(&entries_arc);
            let start_idx = t * chunk_size;
            let end_idx = ((t + 1) * chunk_size).min(total_entries);
            let handle = thread::spawn(move || {
                for i in start_idx..end_idx {
                    // Clone the entry so that we can pass a mutable reference to upsert_history().
                    let mut entry = entries_for_insert[i].clone();
                    chain_for_insert
                        .insert(&mut entry)
                        .expect(&format!("Insert failed for history entry {}", i));
                }
            });
            insert_handles.push(handle);
        }
        // Wait for all insert threads to finish.
        for handle in insert_handles {
            handle.join().unwrap();
        }
        // println!(
        //     "History chain statistics after concurrent insertion:\n{}",
        //     history_chain.stat()
        // );

        // --- Concurrent Gets Verification ---
        // Spawn several reader threads that concurrently verify the inserted entries.
        // Each reader thread iterates over all generated entries and performs a get()
        // using a query timestamp set to the midpoint of each entry’s interval.
        let num_reader_threads = 5;
        let mut reader_handles = Vec::new();
        for _ in 0..num_reader_threads {
            let chain_for_get = history_chain.clone();
            let entries_for_get = Arc::clone(&entries_arc);
            let handle = thread::spawn(move || {
                for entry in entries_for_get.iter() {
                    let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
                    let fetched = chain_for_get
                        .get(&entry.pkey(), &query_ts)
                        .expect("Concurrent get failed for a history entry");
                    assert_eq!(
                        fetched.value(),
                        entry.value(),
                        "Concurrent get: value mismatch for an entry"
                    );
                    assert_eq!(
                        fetched.start_ts(),
                        entry.start_ts(),
                        "Concurrent get: start_ts mismatch for an entry"
                    );
                    assert_eq!(
                        fetched.end_ts(),
                        entry.end_ts(),
                        "Concurrent get: end_ts mismatch for an entry"
                    );
                }
            });
            reader_handles.push(handle);
        }
        // Wait for all reader threads to finish.
        for handle in reader_handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_history_chain_concurrent_inserts_and_gets_mixed() {
        use rand::Rng;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        };
        use std::thread;

        // --- Helper to generate a single new history entry for a new pkey ---
        // This function creates a history entry with a unique primary key.
        // We use an atomic counter to generate a new index for the pkey.
        fn generate_history_entry_for_new_pkey(new_index: usize) -> MvccEntry {
            // Use fixed sizes for key, pkey, and value.
            let key_size = 20;
            let pkey_size = 20;
            let value_size = 300;
            // For simplicity, create key and pkey by formatting the new index.
            // (In your real code you might use your fixed_length_string helper.)
            let key = format!("key-new-{:05}", new_index).into_bytes();
            let pkey = format!("pkey-new-{:05}", new_index).into_bytes();
            let value = format!("new-history-value-{:05}", new_index).into_bytes();
            // For the timestamp range, we can use a deterministic scheme.
            // For example, start_ts = 1000 + new_index * 20, end_ts = start_ts + 10.
            let start_ts = 1000 + (new_index as u64) * 20;
            let end_ts = start_ts + 10;
            MvccEntry::new(key, pkey, value, start_ts, end_ts)
        }

        // --- Setup: Pre-insert a set of history entries ---
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(20, 0);
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        // Generate pre-inserted history entries.
        // For example, generate entries for 1000 primary keys with 5 versions each.
        let num_pkeys = 1000;
        let versions_per_pkey = 5;
        let base_start = 100;
        let interval = 10;
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 300;
        let pre_entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );
        let total_pre_entries = pre_entries.len();
        // println!("Pre-insert: Generated {} history entries", total_pre_entries);

        // Pre-insert all generated history entries.
        for (i, entry) in pre_entries.iter().enumerate() {
            // For history chain, insert() requires a mutable reference.
            let mut entry_clone = entry.clone();
            history_chain
                .insert(&mut entry_clone)
                .expect(&format!("Pre-insert failed for history entry {}", i));
        }
        // println!("History chain statistics after pre-insertion:\n{}", history_chain.stat());

        // --- Setup for Concurrent Operations ---
        // We'll spawn worker threads that randomly perform get() or insert() operations.
        // For new inserts, we need a global atomic counter to generate unique new primary keys.
        let new_pkey_counter = Arc::new(AtomicUsize::new(0));
        // Also, we use a shared Mutex to record new inserted entries for later verification.
        let new_inserts: Arc<Mutex<Vec<MvccEntry>>> = Arc::new(Mutex::new(Vec::new()));

        // Total number of pre-inserted entries.
        let num_pre_entries = total_pre_entries;

        // Number of worker threads and iterations per thread.
        const NUM_WORKER_THREADS: usize = 10;
        const NUM_ITERATIONS: usize = 500;

        let mut worker_handles = Vec::new();
        for _ in 0..NUM_WORKER_THREADS {
            let chain_clone = history_chain.clone();
            // We'll use the pre-inserted entries for get operations.
            let pre_entries_clone = Arc::new(pre_entries.clone());
            let new_pkey_counter_clone = new_pkey_counter.clone();
            let new_inserts_clone = new_inserts.clone();

            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _ in 0..NUM_ITERATIONS {
                    // Randomly choose an operation: 0 => get, 1 => insert.
                    let op = rng.gen_range(0..2);
                    if op == 0 {
                        // Get operation.
                        // Pick a random pre-inserted entry.
                        let idx = rng.gen_range(0..num_pre_entries);
                        let entry = &pre_entries_clone[idx];
                        // Use a query timestamp at the midpoint.
                        let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
                        // Call get; ignore errors.
                        let _ = chain_clone.get(&entry.pkey(), &query_ts);
                    } else {
                        // Insert operation.
                        // Generate a new history entry with a unique primary key.
                        let new_idx = new_pkey_counter_clone.fetch_add(1, Ordering::SeqCst);
                        let new_entry = generate_history_entry_for_new_pkey(new_idx);
                        // Insert the new entry.
                        let mut new_entry_clone = new_entry.clone();
                        if chain_clone.insert(&mut new_entry_clone).is_ok() {
                            // Record the successfully inserted new entry.
                            let mut guard = new_inserts_clone.lock().unwrap();
                            guard.push(new_entry);
                        }
                        // If the insert fails, we ignore it.
                    }
                }
            });
            worker_handles.push(handle);
        }

        // Wait for all worker threads to finish.
        for handle in worker_handles {
            handle.join().unwrap();
        }

        // --- Final Verification ---
        // 1. Verify that every pre-inserted entry is still retrievable.
        for (i, entry) in pre_entries.iter().enumerate() {
            let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
            let res = history_chain.get(&entry.pkey(), &query_ts);
            assert!(
                res.is_ok(),
                "Final get failed for pre-inserted entry {} (pkey {:?}) at ts {}",
                i,
                String::from_utf8_lossy(&entry.pkey()),
                query_ts
            );
            let fetched = res.unwrap();
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for pre-inserted entry {} (pkey {:?})",
                i,
                String::from_utf8_lossy(&entry.pkey())
            );
        }
        // 2. Verify that every new inserted entry is retrievable.
        let new_inserts_guard = new_inserts.lock().unwrap();
        for (i, entry) in new_inserts_guard.iter().enumerate() {
            let query_ts = (entry.start_ts() + entry.end_ts()) / 2;
            let res = history_chain.get(&entry.pkey(), &query_ts);
            assert!(
                res.is_ok(),
                "Final get failed for new inserted entry {} (pkey {:?}) at ts {}",
                i,
                String::from_utf8_lossy(&entry.pkey()),
                query_ts
            );
            let fetched = res.unwrap();
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for new inserted entry {} (pkey {:?})",
                i,
                String::from_utf8_lossy(&entry.pkey())
            );
        }

        // println!("Final history chain statistics:\n{}", history_chain.stat());
    }

    #[test]
    fn test_history_chain_scan_and_scan_all() {
        use std::collections::HashSet;
        use std::sync::Arc;

        // Create a mem pool and initialize the history chain.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(15, 0);
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        // Generate history entries for multiple primary keys.
        // For example, generate entries for 100 primary keys with 2 versions each.
        let num_pkeys = 1000;
        let versions_per_pkey = 5;
        let base_start = 100;
        let interval = 50; // each version is valid for 50 time units: [start, start+50)
        let key_size = 20;
        let pkey_size = 20;
        let value_size = 300;
        let entries = generate_history_mvcc_entries_multi(
            num_pkeys,
            versions_per_pkey,
            base_start,
            interval,
            key_size,
            pkey_size,
            value_size,
        );
        let total_entries = entries.len();
        // println!("Generated {} history entries", total_entries);

        // Insert all generated history entries into the history chain.
        for (i, entry) in entries.iter().enumerate() {
            // upsert_history requires a mutable reference, so we clone each entry.
            let mut entry_clone = entry.clone();
            history_chain
                .insert(&mut entry_clone)
                .expect(&format!("Insert failed for history entry {}", i));
        }
        // println!("History chain statistics after insertion:\n{}", history_chain.stat());

        // --- Filtered Scan Test ---
        // For our generated entries, assume that for each primary key:
        // Version 0 is valid in [base_start, base_start+50) and
        // Version 1 is valid in [base_start+50, base_start+100).
        // Choose a query timestamp that lies in the first version's interval.
        let query_ts = base_start + 25;
        let scanner = history_chain
            .scan(query_ts)
            .expect("Failed to create filtered scanner");
        let scanned_entries: Vec<MvccEntry> = scanner.collect();
        // println!(
        //     "Filtered scan (ts = {}) returned {} entries",
        //     query_ts,
        //     scanned_entries.len()
        // );

        // We expect one entry per primary key from the filtered scan.
        let scanned_pkeys: HashSet<Vec<u8>> = scanned_entries
            .iter()
            .map(|entry| entry.pkey().to_vec())
            .collect();
        assert_eq!(
            scanned_pkeys.len(),
            num_pkeys,
            "Expected {} unique primary keys in filtered scan, got {}",
            num_pkeys,
            scanned_pkeys.len()
        );

        // --- Full Scan Test ---
        let full_scanner = history_chain
            .scan_all()
            .expect("Failed to create full scanner");
        let full_scanned_entries: Vec<MvccEntry> = full_scanner.collect();
        // println!("Full scan returned {} entries", full_scanned_entries.len());
        assert_eq!(
            full_scanned_entries.len(),
            total_entries,
            "Full scan did not return all inserted entries"
        );
    }
}

//     #[test]
//     fn test_history_chain_insert_and_get() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Entries to insert
//         let entries: Vec<(&[u8], &[u8], Timestamp, Timestamp, &[u8])> = vec![
//             (b"key1", b"pkey1", 10u64, 20u64, b"value1"),
//             (b"key2", b"pkey2", 15u64, 25u64, b"value2"),
//             (b"key1", b"pkey1", 20u64, 30u64, b"value3"),
//         ];

//         // Insert entries
//         for (key, pkey, start_ts, end_ts, val) in &entries {
//             chain.insert(key, pkey, *start_ts, *end_ts, val).unwrap();
//         }

//         // Retrieve entries at different timestamps
//         let retrieved_val = chain.get(b"key1", b"pkey1", 12u64).unwrap();
//         assert_eq!(retrieved_val, b"value1");

//         let retrieved_val = chain.get(b"key1", b"pkey1", 22u64).unwrap();
//         assert_eq!(retrieved_val, b"value3");

//         // Attempt to retrieve a non-existent key
//         let result = chain.get(b"key3", b"pkey3", 18u64);
//         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
//     }

//     // #[test]
//     // fn test_history_chain_update() {
//     //     let mem_pool = get_in_mem_pool();
//     //     let c_key = ContainerKey::new(0, 0);

//     //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//     //     // Insert an entry
//     //     chain.insert(b"key1", b"pkey1", 10u64, 20u64, b"value1").unwrap();

//     //     // Update the entry's end_ts
//     //     chain.update(b"key1", b"pkey1", 10u64, 25u64, b"value1").unwrap();

//     //     // Retrieve the entry at a timestamp within the new range
//     //     let retrieved_val = chain.get(b"key1", b"pkey1", 22u64).unwrap();
//     //     assert_eq!(retrieved_val, b"value1");

//     //     // Attempt to retrieve at a timestamp outside the new range
//     //     let result = chain.get(b"key1", b"pkey1", 26u64);
//     //     assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
//     // }

//     #[test]
//     fn test_history_chain_basic_insert_and_get() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Entries to insert
//         let entries: Vec<(&[u8], &[u8], Timestamp, Timestamp, &[u8])> = vec![
//             (b"key1", b"pkey1", 10u64, 20u64, b"value1"),
//             (b"key2", b"pkey2", 15u64, 25u64, b"value2"),
//             (b"key3", b"pkey3", 20u64, 30u64, b"value3"),
//         ];

//         // Insert entries
//         for (key, pkey, start_ts, end_ts, val) in &entries {
//             chain.insert(key, pkey, *start_ts, *end_ts, val).unwrap();
//         }

//         // Retrieve entries at different timestamps
//         for (key, pkey, start_ts, end_ts, val) in &entries {
//             let ts_within_range = (*start_ts + *end_ts) / 2;
//             let retrieved_val = chain.get(key, pkey, ts_within_range).unwrap();
//             assert_eq!(retrieved_val, *val);

//             // Attempt to retrieve at a timestamp before the range
//             let ts_before_range = start_ts - 1;
//             let result = chain.get(key, pkey, ts_before_range);
//             assert!(matches!(
//                 result,
//                 Err(AccessMethodError::KeyNotFound)
//                     | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
//             ));

//             // Attempt to retrieve at a timestamp after the range
//             let ts_after_range = end_ts;
//             let result = chain.get(key, pkey, *ts_after_range);
//             assert!(matches!(
//                 result,
//                 Err(AccessMethodError::KeyNotFound)
//                     | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
//             ));
//         }
//     }

//     #[test]
//     fn test_history_chain_edge_cases_with_timestamps() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Insert an entry with minimal timestamp range
//         chain
//             .insert(b"key_edge", b"pkey_edge", 0u64, 1u64, b"value_edge")
//             .unwrap();

//         // Retrieve at start_ts
//         let retrieved_val = chain.get(b"key_edge", b"pkey_edge", 0u64).unwrap();
//         assert_eq!(retrieved_val, b"value_edge");

//         // Attempt to retrieve at end_ts (should fail)
//         let result = chain.get(b"key_edge", b"pkey_edge", 1u64);
//         assert!(matches!(
//             result,
//             Err(AccessMethodError::KeyNotFound)
//                 | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
//         ));
//     }

//     #[test]
//     fn test_history_chain_multiple_versions_same_key() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Insert multiple versions of the same key-pkey
//         let versions = vec![
//             (10u64, 20u64, b"value_v1"),
//             (20u64, 30u64, b"value_v2"),
//             (30u64, 40u64, b"value_v3"),
//         ];

//         for (start_ts, end_ts, val) in &versions {
//             chain
//                 .insert(b"key_multi", b"pkey_multi", *start_ts, *end_ts, *val)
//                 .unwrap();
//         }

//         // Retrieve each version at different timestamps
//         for (i, (start_ts, end_ts, val)) in versions.iter().enumerate() {
//             let ts_within_range = (*start_ts + *end_ts) / 2;
//             let retrieved_val = chain
//                 .get(b"key_multi", b"pkey_multi", ts_within_range)
//                 .unwrap();
//             assert_eq!(retrieved_val, *val);

//             // Attempt to retrieve at a timestamp outside the range
//             let ts_out_of_range = if i == 0 { start_ts - 1 } else { 41 };
//             let result = chain.get(b"key_multi", b"pkey_multi", ts_out_of_range);
//             assert!(matches!(
//                 result,
//                 Err(AccessMethodError::KeyNotFound)
//                     | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
//             ));
//         }
//     }

//     #[test]
//     fn test_history_chain_insert_causing_page_splits() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Create a large value to fill the page quickly
//         let large_val = vec![b'x'; (AVAILABLE_PAGE_SIZE / 4) as usize];

//         // Insert entries until a new page is allocated
//         let mut inserted_entries = vec![];
//         for i in 0..10 {
//             let key = format!("key_page_split_{}", i).into_bytes();
//             let pkey = format!("pkey_page_split_{}", i).into_bytes();
//             let start_ts = i * 10;
//             let end_ts = start_ts + 10;
//             chain
//                 .insert(&key, &pkey, start_ts, end_ts, &large_val)
//                 .unwrap();
//             inserted_entries.push((key, pkey, start_ts, end_ts, large_val.clone()));
//         }

//         // Verify that all entries can be retrieved
//         for (key, pkey, start_ts, end_ts, val) in &inserted_entries {
//             let ts_within_range = (start_ts + end_ts) / 2;
//             let retrieved_val = chain.get(key, pkey, ts_within_range).unwrap();
//             assert_eq!(retrieved_val, *val);
//         }
//     }

//     #[test]
//     fn test_history_chain_retrieve_non_existent_keys() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Insert some entries
//         chain
//             .insert(b"key_exist", b"pkey_exist", 10u64, 20u64, b"value_exist")
//             .unwrap();

//         // Attempt to retrieve a key that was never inserted
//         let result = chain.get(b"key_nonexistent", b"pkey_nonexistent", 15u64);
//         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

//         // Attempt to retrieve with an incorrect pkey
//         let result = chain.get(b"key_exist", b"pkey_wrong", 15u64);
//         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

//         // Attempt to retrieve with a timestamp outside the range
//         let result = chain.get(b"key_exist", b"pkey_exist", 25u64);
//         assert!(matches!(
//             result,
//             Err(AccessMethodError::KeyNotFound)
//                 | Err(AccessMethodError::KeyFoundButInvalidTimestamp)
//         ));
//     }

//     #[test]
//     fn test_history_chain_insert_overlapping_timestamps() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // Insert entries with overlapping timestamp ranges for different keys
//         chain
//             .insert(b"key_overlap1", b"pkey1", 10u64, 30u64, b"value1")
//             .unwrap();
//         chain
//             .insert(b"key_overlap2", b"pkey2", 20u64, 40u64, b"value2")
//             .unwrap();

//         // Retrieve entries at timestamps where ranges overlap
//         let retrieved_val1 = chain.get(b"key_overlap1", b"pkey1", 25u64).unwrap();
//         assert_eq!(retrieved_val1, b"value1");

//         let retrieved_val2 = chain.get(b"key_overlap2", b"pkey2", 25u64).unwrap();
//         assert_eq!(retrieved_val2, b"value2");
//     }

//     // #[test]
//     // fn test_history_chain_insert_same_key_overlapping_ranges() {
//     //     let mem_pool = get_in_mem_pool();
//     //     let c_key = ContainerKey::new(0, 0);

//     //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//     //     // Insert entries with overlapping timestamp ranges for the same key-pkey
//     //     chain.insert(b"key_same", b"pkey_same", 10u64, 30u64, b"value1").unwrap();
//     //     chain.insert(b"key_same", b"pkey_same", 20u64, 40u64, b"value2").unwrap();

//     //     // Retrieve at timestamps covered by both ranges
//     //     let retrieved_val = chain.get(b"key_same", b"pkey_same", 25u64).unwrap();
//     //     // Depending on the implementation, the chain might return the first or the last inserted value
//     //     // Let's assume it returns the value with the latest start_ts less than or equal to ts
//     //     assert_eq!(retrieved_val, b"value2");

//     //     // Retrieve at timestamps covered by only one range
//     //     let retrieved_val = chain.get(b"key_same", b"pkey_same", 15u64).unwrap();
//     //     assert_eq!(retrieved_val, b"value1");
//     // }

//     // #[test]
//     // fn test_history_chain_insert_with_max_timestamps() {
//     //     let mem_pool = get_in_mem_pool();
//     //     let c_key = ContainerKey::new(0, 0);

//     //     let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//     //     let max_timestamp = u64::MAX;

//     //     // Insert an entry with end_ts as u64::MAX
//     //     chain.insert(b"key_max_ts", b"pkey_max_ts", 50u64, max_timestamp, b"value_max_ts").unwrap();

//     //     // Retrieve at a timestamp less than max_timestamp
//     //     let retrieved_val = chain.get(b"key_max_ts", b"pkey_max_ts", 100u64).unwrap();
//     //     assert_eq!(retrieved_val, b"value_max_ts");

//     //     // Attempt to retrieve at max_timestamp (should fail as end_ts is exclusive)
//     //     let result = chain.get(b"key_max_ts", b"pkey_max_ts", max_timestamp);
//     //     assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
//     // }

//     #[test]
//     fn test_history_chain_insert_many_entries_spanning_multiple_pages() {
//         let mem_pool = get_in_mem_pool();
//         let c_key = ContainerKey::new(0, 0);

//         let chain = MvccHashJoinHistoryChain::new(c_key, mem_pool.clone());

//         // We'll calculate the number of entries needed to fill more than one page.
//         // We'll use small keys and values to make the calculation straightforward.
//         let key_base = b"key_multi_page";
//         let pkey_base = b"pkey_multi_page";
//         let val_base = b"value_multi_page";

//         // Determine the space needed per entry.
//         let key = key_base;
//         let pkey = pkey_base;
//         let val = val_base;
//         let start_ts = 10u64;
//         let end_ts = 20u64;

//         let space_per_entry =
//             <Page as HashJoinPage>::space_need(key, pkey, val) as usize;

//         // Calculate the number of entries to exceed one page
//         let available_space = AVAILABLE_PAGE_SIZE;
//         let entries_per_page = available_space / space_per_entry;
//         let num_entries = entries_per_page * 3; // Enough to fill 3 pages

//         // Insert entries
//         for i in 0..num_entries {
//             let key = format!("{}{}", std::str::from_utf8(key_base).unwrap(), i).into_bytes();
//             let pkey = format!("{}{}", std::str::from_utf8(pkey_base).unwrap(), i).into_bytes();
//             let val = format!("{}{}", std::str::from_utf8(val_base).unwrap(), i).into_bytes();
//             let start_ts = 10u64 + i as u64;
//             let end_ts = start_ts + 10;
//             chain.insert(&key, &pkey, start_ts, end_ts, &val).unwrap();
//         }

//         // Retrieve entries
//         for i in 0..num_entries {
//             let key = format!("{}{}", std::str::from_utf8(key_base).unwrap(), i).into_bytes();
//             let pkey = format!("{}{}", std::str::from_utf8(pkey_base).unwrap(), i).into_bytes();
//             let val = format!("{}{}", std::str::from_utf8(val_base).unwrap(), i).into_bytes();
//             let ts = 15u64 + i as u64; // Within the timestamp range of each entry

//             let retrieved_val = chain.get(&key, &pkey, ts).unwrap();
//             assert_eq!(retrieved_val, val);
//         }

//         // Optionally, verify that attempting to retrieve an entry at an invalid timestamp fails
//         let invalid_ts = 5u64; // Before any entry's start_ts
//         let result = chain.get(&key_base.to_vec(), &pkey_base.to_vec(), invalid_ts);
//         assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
//     }
// }
