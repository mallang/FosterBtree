use core::panic;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    time::Duration,
};

use crate::{
    access_method::AccessMethodError,
    bp::prelude::*,
    log_debug, log_trace, log_warn,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::{mvcc_hash_join_recent_page::MvccHashJoinRecentPage, Timestamp};

pub struct MvccHashJoinRecentChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    first_page_id: PageId,
    first_frame_id: AtomicU32,

    last_page_id: AtomicU32, // cache the last page id for faster insertion
    last_frame_id: AtomicU32,
}

impl<T: MemPool> MvccHashJoinRecentChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let first_page_id = page.get_id();
        let first_frame_id = page.frame_id();

        MvccHashJoinRecentPage::init(&mut *page);
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

    pub fn new_from_page(c_key: ContainerKey, mem_pool: Arc<T>, pid: PageId) -> Self {
        let page_key = PageFrameKey::new(c_key, pid);
        let page = mem_pool.get_page_for_read(page_key).unwrap();
        let first_frame_id = page.frame_id();
        drop(page);

        Self {
            mem_pool,
            c_key,
            first_page_id: pid,
            first_frame_id: AtomicU32::new(first_frame_id),
            last_page_id: AtomicU32::new(pid),
            last_frame_id: AtomicU32::new(first_frame_id),
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let space_need = <Page as MvccHashJoinRecentPage>::space_need(key, pkey, val);
        if space_need > AVAILABLE_PAGE_SIZE.try_into().unwrap() {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let mut current_pid = self.last_page_id.load(atomic::Ordering::Acquire);
        let mut current_fid = self.last_frame_id.load(atomic::Ordering::Acquire);

        let base = 2;
        let mut attempts = 0;

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);
            match MvccHashJoinRecentPage::next_page(&*page) {
                Some((next_pid, next_fid)) => {
                    current_pid = next_pid;
                    current_fid = next_fid;
                    continue;
                }
                None => {
                    log_debug!("Reached end of chain, inserting into end of chain");
                }
            }
            if space_need < MvccHashJoinRecentPage::free_space_with_compaction(&*page) {
                if space_need > MvccHashJoinRecentPage::free_space_without_compaction(&*page) {
                    log_debug!("Compaction needed");
                    // Compaction needed, now just add new page.
                } else {
                    match page.try_upgrade(true) {
                        Ok(mut upgraded_page) => {
                            match MvccHashJoinRecentPage::insert(
                                &mut *upgraded_page,
                                key,
                                pkey,
                                ts,
                                val,
                            ) {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(_) => {
                                    panic!("Unexpected error");
                                }
                            }
                        }
                        Err(_) => {
                            log_debug!("Page upgrade failed, retrying");
                            attempts += 1;
                            std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                            continue;
                        }
                    }
                }
            }
            match page.try_upgrade(true) {
                Ok(mut upgraded_page) => {
                    let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                    let new_pid = new_page.get_id();
                    let new_fid = new_page.frame_id();
                    MvccHashJoinRecentPage::init(&mut *new_page);
                    match MvccHashJoinRecentPage::insert(&mut *new_page, key, pkey, ts, val) {
                        Ok(_) => {}
                        Err(_) => {
                            panic!("Unexpected error");
                        }
                    }
                    MvccHashJoinRecentPage::set_next_page(&mut *upgraded_page, new_pid, new_fid);
                    self.last_page_id.store(new_pid, atomic::Ordering::Release);
                    self.last_frame_id.store(new_fid, atomic::Ordering::Release);
                    drop(new_page);
                    return Ok(());
                }
                Err(_) => {
                    log_debug!("Page upgrade failed, retrying");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
            }
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let page = self.read_page(page_key);

            // Attempt to retrieve the value from the current page
            match MvccHashJoinRecentPage::get(&*page, key, pkey, ts) {
                Ok(val) => {
                    // Value found
                    return Ok(val);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    // Key not found in this page, check for next page
                    match MvccHashJoinRecentPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            // Move to the next page
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            // End of the chain reached, key not found
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    // Propagate other errors
                    return Err(e);
                }
            }
        }
    }

    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let mut page = self.write_page(page_key);

            // Attempt to update the value in the current page
            match MvccHashJoinRecentPage::update(&mut *page, key, pkey, ts, val) {
                Ok((old_ts, old_val)) => {
                    // Update successful
                    return Ok((old_ts, old_val));
                }
                Err(AccessMethodError::KeyNotFound) => {
                    // Key not found in this page, check for next page
                    match MvccHashJoinRecentPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            // Move to the next page
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            // End of the chain reached, key not found
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    // Propagate other errors
                    return Err(e);
                }
            }
        }
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let mut current_pid = self.first_page_id;
        let mut current_fid = self.first_frame_id.load(atomic::Ordering::Acquire);

        loop {
            let page_key = PageFrameKey::new_with_frame_id(self.c_key, current_pid, current_fid);
            let mut page = self.write_page(page_key);

            // Attempt to delete the value in the current page
            match MvccHashJoinRecentPage::delete(&mut *page, key, pkey, ts) {
                Ok((old_ts, old_val)) => {
                    // Delete successful
                    return Ok((old_ts, old_val));
                }
                Err(AccessMethodError::KeyNotFound) => {
                    // Key not found in this page, check for next page
                    match MvccHashJoinRecentPage::next_page(&*page) {
                        Some((next_pid, next_fid)) => {
                            // Move to the next page
                            current_pid = next_pid;
                            current_fid = next_fid;
                            continue;
                        }
                        None => {
                            // End of the chain reached, key not found
                            return Err(AccessMethodError::KeyNotFound);
                        }
                    }
                }
                Err(e) => {
                    // Propagate other errors
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
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_chain_insert_and_get_entries() {
        // Initialize mem_pool and container key
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);

        // Create a new chain
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Entries to insert
        let entries: Vec<(&[u8], &[u8], Timestamp, &[u8])> = vec![
            (b"key1", b"pkey1", 10u64, b"value1"),
            (b"key2", b"pkey2", 20u64, b"value2"),
            (b"key_longer_than_prefix", b"pkey3", 30u64, b"value3"),
            (b"key4", b"pkey_longer_than_prefix", 40u64, b"value4"),
            (b"key_long", b"pkey_long", 50u64, b"value5"),
        ];

        // Insert entries
        for (key, pkey, ts, val) in &entries {
            chain.insert(key, pkey, *ts, val).unwrap();
        }

        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = chain.get(key, pkey, *ts).unwrap();
            assert_eq!(retrieved_val, *val);
        }

        // Attempt to retrieve a non-existent key
        let result = chain.get(b"nonexistent_key", b"nonexistent_pkey", 60u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_insert_empty_keys_and_values() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Entries with empty keys, pkeys, and values
        let entries: Vec<(&[u8], &[u8], Timestamp, &[u8])> = vec![
            (b"", b"pkey1", 10u64, b"value1"),
            (b"key2", b"", 20u64, b"value2"),
            (b"key3", b"pkey3", 30u64, b""),
            (b"", b"", 40u64, b""),
        ];

        // Insert entries
        for (key, pkey, ts, val) in &entries {
            chain.insert(key, pkey, *ts, val).unwrap();
        }

        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = chain.get(key, pkey, *ts).unwrap();
            assert_eq!(retrieved_val, *val);
        }
    }

    #[test]
    fn test_chain_insert_duplicate_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        chain.insert(b"key1", b"pkey1", 10u64, b"value1").unwrap();

        // Attempt to insert the same key-pkey with a different value and timestamp
        chain.insert(b"key1", b"pkey2", 20u64, b"value2").unwrap();

        // Retrieve the entry with the latest timestamp
        let retrieved_val = chain.get(b"key1", b"pkey2", 20u64).unwrap();
        assert_eq!(retrieved_val, b"value2");

        // Retrieve the entry with the earlier timestamp
        let retrieved_val = chain.get(b"key1", b"pkey1", 10u64).unwrap();
        assert_eq!(retrieved_val, b"value1");
    }

    #[test]
    fn test_chain_insert_large_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Generate a large value that is close to the page size limit
        let large_value = vec![b'a'; (AVAILABLE_PAGE_SIZE / 2) as usize];

        // Insert entries until a new page is allocated
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            chain
                .insert(&key, &pkey, i as u64 * 10, &large_value)
                .unwrap();
        }

        // Verify that all entries can be retrieved
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let retrieved_val = chain.get(&key, &pkey, i as u64 * 10).unwrap();
            assert_eq!(retrieved_val, large_value);
        }
    }

    #[test]
    fn test_chain_insert_oversized_entry() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Generate a value larger than the page size
        let oversized_value = vec![b'a'; (AVAILABLE_PAGE_SIZE + 1) as usize];

        // Attempt to insert the oversized entry
        let result = chain.insert(b"key1", b"pkey1", 10u64, &oversized_value);
        assert!(matches!(result, Err(AccessMethodError::RecordTooLarge)));
    }

    #[test]
    fn test_chain_update_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        chain.insert(b"key1", b"pkey1", 10u64, b"value1").unwrap();

        // Update the entry
        let (old_ts, old_val) = chain.update(b"key1", b"pkey1", 20u64, b"value2").unwrap();
        assert_eq!(old_ts, 10u64);
        assert_eq!(old_val, b"value1");

        // Retrieve the updated entry
        let retrieved_val = chain.get(b"key1", b"pkey1", 20u64).unwrap();
        assert_eq!(retrieved_val, b"value2");

        // Attempt to update a non-existent entry
        let result = chain.update(b"key_nonexistent", b"pkey_nonexistent", 30u64, b"value3");
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_concurrent_inserts_and_reads() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(MvccHashJoinRecentChain::new(c_key, mem_pool.clone()));

        let chain_clone = chain.clone();
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in 0..100 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                chain_clone.insert(&key, &pkey, i as u64, &value).unwrap();
            }
        });

        // Read entries while inserts are happening
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = chain.get(&key, &pkey, i as u64);
        }

        handle.join().unwrap();

        // Verify all entries after insertions are complete
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = chain.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val, expected_value);
        }
    }

    // New tests for the delete method

    #[test]
    fn test_chain_delete_existing_entry() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        let key = b"key_delete";
        let pkey = b"pkey_delete";
        let ts_insert: Timestamp = 100;
        let ts_delete: Timestamp = 200;
        let val = b"value_delete";

        chain.insert(key, pkey, ts_insert, val).unwrap();

        // Verify the entry exists
        let retrieved_val = chain.get(key, pkey, ts_insert).unwrap();
        assert_eq!(retrieved_val, val);

        // Delete the entry
        let (old_ts, old_val) = chain.delete(key, pkey, ts_delete).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val);

        // Attempt to get the deleted entry
        let result = chain.get(key, pkey, ts_delete);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_delete_non_existent_entry() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Attempt to delete a non-existent entry
        let result = chain.delete(b"nonexistent_key", b"nonexistent_pkey", 100);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_delete_with_invalid_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        let key = b"key_invalid_ts";
        let pkey = b"pkey_invalid_ts";
        let ts_insert: Timestamp = 200;
        let ts_delete: Timestamp = 100; // Earlier than ts_insert
        let val = b"value_invalid_ts";

        chain.insert(key, pkey, ts_insert, val).unwrap();

        // Attempt to delete with an earlier timestamp
        let result = chain.delete(key, pkey, ts_delete);
        assert!(matches!(
            result,
            Err(AccessMethodError::KeyFoundButInvalidTimestamp)
        ));

        // Verify the entry still exists
        let retrieved_val = chain.get(key, pkey, ts_insert).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_chain_delete_entries_across_multiple_pages() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert entries to span multiple pages
        for i in 0..50 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let val = format!("value{}", i).into_bytes();
            chain.insert(&key, &pkey, i as u64, &val).unwrap();
        }

        // Delete an entry that should be in a later page
        let key_to_delete = b"key25";
        let pkey_to_delete = b"pkey25";
        let ts_delete: Timestamp = 100;

        let (old_ts, old_val) = chain.delete(key_to_delete, pkey_to_delete, ts_delete).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, 25u64);
        assert_eq!(old_val, b"value25");

        // Attempt to get the deleted entry
        let result = chain.get(key_to_delete, pkey_to_delete, ts_delete);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

        // Verify other entries are still retrievable
        for i in 0..50 {
            if i == 25 {
                continue;
            }
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_val = format!("value{}", i).into_bytes();
            let retrieved_val = chain.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val, expected_val);
        }
    }

    #[test]
    fn test_chain_delete_and_insert_new_entry() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        let key = b"key_cycle";
        let pkey = b"pkey_cycle";
        let ts_insert1: Timestamp = 100;
        let ts_delete: Timestamp = 200;
        let ts_insert2: Timestamp = 300;
        let val1 = b"value1";
        let val2 = b"value2";

        // Insert the first entry
        chain.insert(key, pkey, ts_insert1, val1).unwrap();

        // Delete the entry
        chain.delete(key, pkey, ts_delete).unwrap();

        // Insert a new entry with the same key and pkey
        chain.insert(key, pkey, ts_insert2, val2).unwrap();

        // Retrieve the new entry
        let retrieved_val = chain.get(key, pkey, ts_insert2).unwrap();
        assert_eq!(retrieved_val, val2);

        // Attempt to get the old entry with an earlier timestamp
        let result = chain.get(key, pkey, ts_insert1);
        assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
    }

    #[test]
    fn test_chain_delete_all_entries() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert multiple entries
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let val = format!("value{}", i).into_bytes();
            chain.insert(&key, &pkey, i as u64, &val).unwrap();
        }

        // Delete all entries
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let ts_delete = i as u64 + 100;
            chain.delete(&key, &pkey, ts_delete).unwrap();
        }

        // Verify that all entries are deleted
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let result = chain.get(&key, &pkey, i as u64 + 100);
            assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
        }
    }

    #[test]
    fn test_chain_delete_non_existent_after_deletion() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Insert an entry
        chain.insert(b"key1", b"pkey1", 10u64, b"value1").unwrap();

        // Delete the entry
        chain.delete(b"key1", b"pkey1", 20u64).unwrap();

        // Attempt to delete again
        let result = chain.delete(b"key1", b"pkey1", 30u64);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_delete_with_multiple_versions() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        let key = b"key_multi";
        let pkey = b"pkey_multi";
        let ts1: Timestamp = 100;
        let ts2: Timestamp = 200;
        let ts_delete: Timestamp = 300;
        let val1 = b"value1";
        let val2 = b"value2";

        // Insert first version
        chain.insert(key, pkey, ts1, val1).unwrap();

        // Update to create a second version
        chain.update(key, pkey, ts2, val2).unwrap();

        // Delete the entry
        chain.delete(key, pkey, ts_delete).unwrap();

        // Attempt to get with various timestamps
        let result = chain.get(key, pkey, ts1);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

        let result = chain.get(key, pkey, ts2);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));

        let result = chain.get(key, pkey, ts_delete);
        assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
    }

    #[test]
    fn test_chain_delete_concurrent_operations() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = Arc::new(MvccHashJoinRecentChain::new(c_key, mem_pool.clone()));

        let chain_insert = chain.clone();
        let handle_insert = thread::spawn(move || {
            // Insert entries
            for i in 0..100 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                chain_insert.insert(&key, &pkey, i as u64, &value).unwrap();
            }
        });

        let chain_delete = chain.clone();
        let handle_delete = thread::spawn(move || {
            // Wait a bit to ensure some entries are inserted
            std::thread::sleep(std::time::Duration::from_millis(50));

            // Delete entries
            for i in 0..50 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let ts_delete = i as u64 + 100;
                let _ = chain_delete.delete(&key, &pkey, ts_delete);
            }
        });

        handle_insert.join().unwrap();
        handle_delete.join().unwrap();

        // Verify entries
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let ts = i as u64;
            let result = chain.get(&key, &pkey, ts + 100);
            if i < 50 {
                // Entries that were attempted to be deleted
                assert!(matches!(result, Err(AccessMethodError::KeyNotFound)));
            } else {
                // Entries that should still exist
                let expected_value = format!("value{}", i).into_bytes();
                let retrieved_val = result.unwrap();
                assert_eq!(retrieved_val, expected_value);
            }
        }
    }

    #[test]
    fn test_chain_random_operations_no_duplicate_pkeys() {
        use rand::prelude::*;
        use std::collections::HashMap;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let chain = MvccHashJoinRecentChain::new(c_key, mem_pool.clone());

        // Define the number of operations
        let num_operations = 10_000;

        // Define possible operations
        enum Operation {
            Insert,
            Get,
            Update,
            Delete,
        }

        // Create a random number generator
        let mut rng = rand::thread_rng();

        // HashMap to keep track of the expected state
        // Key: (key, pkey), Value: (ts, value)
        let mut expected_state: HashMap<(Vec<u8>, Vec<u8>), (Timestamp, Vec<u8>)> = HashMap::new();

        // Set to keep track of inserted pkeys to avoid duplicates
        let mut inserted_pkeys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

        // Possible keys, pkeys, and values
        let keys: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("key{}", i).into_bytes())
            .collect();
        let pkeys: Vec<Vec<u8>> = (0..1000) // Increase the range to have enough unique pkeys
            .map(|i| format!("pkey{}", i).into_bytes())
            .collect();
        let values: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("value{}", i).into_bytes())
            .collect();

        // Perform random operations
        for _ in 0..num_operations {
            let op = match rng.gen_range(0..4) {
                0 => Operation::Insert,
                1 => Operation::Get,
                2 => Operation::Update,
                3 => Operation::Delete,
                _ => unreachable!(),
            };

            // Randomly select key, pkey, value, and timestamp
            let key = keys.choose(&mut rng).unwrap().clone();
            let pkey = pkeys.choose(&mut rng).unwrap().clone();
            let value = values.choose(&mut rng).unwrap().clone();
            let ts: Timestamp = rng.gen_range(1..1_000_000);

            match op {
                Operation::Insert => {
                    // Insert operation
                    if inserted_pkeys.contains(&pkey) {
                        // Skip insertion if pkey already exists
                        continue;
                    }
                    let res = chain.insert(&key, &pkey, ts, &value);
                    if res.is_ok() {
                        expected_state.insert((key.clone(), pkey.clone()), (ts, value.clone()));
                        inserted_pkeys.insert(pkey.clone());
                    } else {
                        assert!(matches!(res, Err(AccessMethodError::OutOfSpace) | Err(AccessMethodError::RecordTooLarge)));
                    }
                }
                Operation::Get => {
                    // Get operation
                    let res = chain.get(&key, &pkey, ts);
                    match expected_state.get(&(key.clone(), pkey.clone())) {
                        Some(&(stored_ts, ref stored_value)) if stored_ts <= ts => {
                            // The entry should be retrievable
                            let retrieved_value = res.unwrap();
                            assert_eq!(&retrieved_value, stored_value);
                        }
                        _ => {
                            // The entry should not be found
                            assert!(matches!(res, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
                        }
                    }
                }
                Operation::Update => {
                    // Update operation
                    if !inserted_pkeys.contains(&pkey) {
                        // Skip update if pkey does not exist
                        continue;
                    }
                    let res = chain.update(&key, &pkey, ts, &value);
                    match expected_state.get_mut(&(key.clone(), pkey.clone())) {
                        Some((stored_ts, stored_value)) if *stored_ts <= ts => {
                            // The update should succeed
                            let (old_ts, old_value) = res.unwrap();
                            assert_eq!(old_ts, *stored_ts);
                            assert_eq!(old_value, stored_value.clone());
                            *stored_ts = ts;
                            *stored_value = value.clone();
                        }
                        _ => {
                            // The update should fail
                             
                             assert!(matches!(res, Err(AccessMethodError::KeyFoundButInvalidTimestamp) | Err(AccessMethodError::KeyNotFound)));
                        }
                    }
                }
                Operation::Delete => {
                    // Delete operation
                    if !inserted_pkeys.contains(&pkey) {
                        // Skip deletion if pkey does not exist
                        continue;
                    }
                    let res = chain.delete(&key, &pkey, ts);
                    match expected_state.remove(&(key.clone(), pkey.clone())) {
                        Some((stored_ts, stored_value)) if stored_ts <= ts => {
                            // The deletion should succeed
                            let (old_ts, old_value) = res.unwrap();
                            assert_eq!(old_ts, stored_ts);
                            assert_eq!(old_value, stored_value);
                            inserted_pkeys.remove(&pkey);
                        }
                        Some((stored_ts, stored_value)) => {
                            // The deletion should fail due to invalid timestamp
                            expected_state.insert((key.clone(), pkey.clone()), (stored_ts, stored_value));
                            assert!(matches!(res, Err(AccessMethodError::KeyFoundButInvalidTimestamp) | Err(AccessMethodError::KeyNotFound)));
                        }
                        None => {
                            // pkey is inseterd but (key, pkey) is inserted. actually its weird case
                        }
                    }
                }
            }
        }

        // After all operations, verify the final state
        for ((key, pkey), (stored_ts, stored_value)) in &expected_state {
            let res = chain.get(key, pkey, *stored_ts);
            let retrieved_value = res.unwrap();
            assert_eq!(&retrieved_value, stored_value);
        }

        // Optionally, perform additional verification for timestamps beyond the stored timestamp
        for ((key, pkey), (stored_ts, stored_value)) in &expected_state {
            let ts_future = stored_ts + 1000;
            let res = chain.get(key, pkey, ts_future);
            let retrieved_value = res.unwrap();
            assert_eq!(&retrieved_value, stored_value);
        }

        // Verify that entries are not retrievable with timestamps before they were inserted
        for ((key, pkey), (stored_ts, _)) in &expected_state {
            let ts_past = if *stored_ts > 1 { stored_ts - 1 } else { 0 };
            let res = chain.get(key, pkey, ts_past);
            assert!(matches!(res, Err(AccessMethodError::KeyNotFound) | Err(AccessMethodError::KeyFoundButInvalidTimestamp)));
        }
    }
}
