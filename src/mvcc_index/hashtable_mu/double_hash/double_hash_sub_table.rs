use core::str;
use std::{
    collections::HashSet,
    iter::Enumerate,
    marker::PhantomData,
    sync::{atomic::AtomicU32, Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{
    bp::{
        get_in_mem_pool, ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus,
        PageFrameKey,
    },
    // lockmanager::{LockManager, Permissions, TransactionId, ValueId},
    log_warn,
    mvcc_index::{
        hashtable_mu::hash_join_table_common::HashTableAccessMethodError, DeltaEntry, MvccEntry,
        MvccIndex, Timestamp,
    },
    page::{Page, PageId},
    prelude::AccessMethodError,
};

use super::{
    double_hash_common::{BucketEntry, SUBTABLE_HASHER_SEED},
    double_hash_data_page::TableDataPage,
};

/// responsible for update meta page of HashJoinTable<T>
pub struct DHashSubTable<T: MemPool> {
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    // meta: Arc<(PageId, AtomicU32)>,
    /// shared: read & update & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    buckets_rwlock: RwLock<Vec<BucketEntry>>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once
}

// ----------------- SCANNER START ------------------------------
mod iterators {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{
        bp::{MemPool, PageFrameKey},
        mvcc_index::{
            hashtable_mu::double_hash::{
                double_hash_common::SUBTABLE_HASHER_SEED, double_hash_data_page::TableDataPage,
            },
            Delta, DeltaEntry, MvccEntry, MvccIndex, Timestamp,
        },
        page::Page,
    };

    use super::DHashSubTable;

    pub enum SubTableScannerOption {
        AllVersionsAllKeys,
        OneVersionAllKeys(Timestamp),
        OneVersionOneKey(Timestamp, Vec<u8>),
    }

    pub struct SubTableDeltaScannerOption {
        pub small_ts: Timestamp,
        pub large_ts: Timestamp,
    }

    pub struct SubTableScanner<T: MemPool> {
        table: Arc<DHashSubTable<T>>,
        option: SubTableScannerOption,

        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<MvccEntry>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool> SubTableScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>, option: SubTableScannerOption) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = {
                if let SubTableScannerOption::OneVersionOneKey(_, key) = &option {
                    vec![(farmhash::hash32_with_seed(key, SUBTABLE_HASHER_SEED) % (bucket_num))]
                } else {
                    (0..bucket_num).into_iter().collect::<Vec<_>>()
                }
            };
            Self {
                table: table.clone(),
                option,
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool> Iterator for SubTableScanner<T> {
        type Item = MvccEntry;

        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = self.table.read_page(pfkey);

                        let entries = match &self.option {
                            SubTableScannerOption::AllVersionsAllKeys => {
                                <Page as TableDataPage>::scan_all_versions_all_keys(&page)
                            }
                            SubTableScannerOption::OneVersionAllKeys(ts) => {
                                <Page as TableDataPage>::scan_one_version(&page, *ts, None)
                            }
                            SubTableScannerOption::OneVersionOneKey(ts, key) => {
                                <Page as TableDataPage>::scan_one_version(&page, *ts, Some(key))
                            }
                            _ => {
                                unreachable!()
                            }
                        };
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }

    pub struct SubTableDeltaScanner<T: MemPool> {
        table: Arc<DHashSubTable<T>>,
        option: SubTableDeltaScannerOption,

        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<DeltaEntry<Vec<u8>>>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool> SubTableDeltaScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>, option: SubTableDeltaScannerOption) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = (0..bucket_num).into_iter().collect::<Vec<_>>();

            Self {
                table: table.clone(),
                option,
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool> Iterator for SubTableDeltaScanner<T> {
        type Item = DeltaEntry<Vec<u8>>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = self.table.read_page(pfkey);

                        let entries = {
                            page.scan_delta_of_btw_ts(self.option.small_ts, self.option.large_ts)
                        };
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }
}

pub use iterators::{
    SubTableDeltaScanner, SubTableDeltaScannerOption, SubTableScanner, SubTableScannerOption,
};

// ----------------- SCANNER END ------------------------------

type Result<T> = core::result::Result<T, HashTableAccessMethodError>;
impl<T: MemPool> DHashSubTable<T> {
    pub fn test_singlethread_rehash(&self) {
        self.rehash(self.bucket_num() * 2);
    }

    /// assume buckets is not locked!
    ///
    pub fn bucket_num(&self) -> u32 {
        self.buckets_rwlock.read().unwrap().len() as u32
    }

    pub fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
        // meta: &Arc<(PageId, AtomicU32)>,
    ) -> Self {
        let mut bucket_entry_vec = vec![];
        for _ in 0..num_buckets {
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let pid = page.get_id();
            let fid = page.frame_id();

            TableDataPage::init(&mut *page);
            drop(page);

            bucket_entry_vec.push(BucketEntry::new_with_frame_id(pid, fid));
        }
        Self {
            c_key,
            mem_pool,
            buckets_rwlock: RwLock::new(bucket_entry_vec),
            rehash_mutex: Mutex::new(()),
            // meta: meta.clone(),
        }
    }

    pub fn upsert(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) -> Result<()> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.upsert_inner(key, pkey, ts, val) {
                Ok(()) => {
                    return Ok(());
                }
                Err(HashTableAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();
                    self.rehash(new_hash_size);
                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();
                    continue;
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> core::result::Result<(), AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.insert_inner(key, pkey, ts, val) {
                Ok(()) => {
                    return Ok(());
                }
                Err(HashTableAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();
                    self.rehash(new_hash_size);
                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();
                    continue;
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
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
    ) -> core::result::Result<(), AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.update_inner(key, pkey, ts, val) {
                Ok(()) => {
                    return Ok(());
                }
                Err(HashTableAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();
                    self.rehash(new_hash_size);
                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();
                    continue;
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<Option<Vec<u8>>, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.get_inner(key, pkey, ts) {
                Ok(val) => {
                    return Ok(Some(val));
                }
                Err(HashTableAccessMethodError::KeyNotFound) => {
                    return Ok(None);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn get_keys(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.get_keys_inner(key, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<(), AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.delete_inner(key, pkey, ts) {
                Ok(()) | Err(HashTableAccessMethodError::KeyNotFound) => {
                    return Ok(());
                }
                Err(HashTableAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();
                    self.rehash(new_hash_size);
                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();
                    continue;
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn garbage_collect(
        &self,
        safe_ts: Timestamp,
    ) -> core::result::Result<(), AccessMethodError> {
        let buckets = self.buckets_rwlock.read().unwrap();

        for bucket_idx in 0..buckets.len() {
            let pid = buckets[bucket_idx].page_id();
            let fid: u32 = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = self.write_page(page_f_key);
            // write_page.dbg_print_slots();
            write_page.garbage_collect(safe_ts);
        }

        Ok(())
    }
}

/// Recent & History basic functions
impl<T: MemPool> DHashSubTable<T> {
    // helper function
    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.try_write_page(page_key);
            // protected by LockManager, only acceptable error is "CannotEvictPage"
            match page {
                Ok(page) => {
                    return page;
                }
                Err(HashTableAccessMethodError::MemPoolStatus(MemPoolStatus::CannotEvictPage)) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    std::hint::spin_loop();
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }
    // helper function
    fn try_write_page(&self, page_key: PageFrameKey) -> Result<FrameWriteGuard> {
        let page = self.mem_pool.get_page_for_write(page_key);
        match page {
            Ok(page) => {
                return Ok(page);
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                log_warn!("All frames are latched and cannot evict page to write the page: {:?}. Will retry", page_key);
                std::thread::sleep(Duration::from_millis(1));
                return Err(HashTableAccessMethodError::MemPoolStatus(
                    MemPoolStatus::CannotEvictPage,
                ));
            }
            Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
    // helper function
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.try_read_page(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(HashTableAccessMethodError::MemPoolStatus(MemPoolStatus::CannotEvictPage)) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    std::hint::spin_loop();
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }
    // helper function
    fn try_read_page(&self, page_key: PageFrameKey) -> Result<FrameReadGuard> {
        let page = self.mem_pool.get_page_for_read(page_key);
        match page {
            Ok(page) => {
                return Ok(page);
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                std::thread::sleep(Duration::from_millis(1));
                return Err(HashTableAccessMethodError::MemPoolStatus(
                    MemPoolStatus::CannotEvictPage,
                ));
            }
            Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }

    fn hash_to_index(key: &[u8], total_size: u32) -> usize {
        (farmhash::hash32_with_seed(key, SUBTABLE_HASHER_SEED) % total_size) as usize
    }

    fn upsert_inner(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) -> Result<()> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.len() as u32;

        let bucket_idx = Self::hash_to_index(key, bucket_num);

        let pid = buckets[bucket_idx].page_id();
        let fid = buckets[bucket_idx].frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = self.try_write_page(page_f_key).ok();
        let mut inserted_page = match acq_result {
            None => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Some(p) => p,
        };
        let inserted_result =
            <Page as TableDataPage>::upsert(&mut inserted_page, key, pkey, val, ts);

        match inserted_result {
            Err(HashTableAccessMethodError::OutOfSpace) => {
                // rehash
                return Err(HashTableAccessMethodError::HashPageOutOfSpace(
                    bucket_num * 2,
                ));
            }
            Err(e) => {
                panic!(
                    "should not happen! have checked before insert. err: {:?}, insert_key: {:?}",
                    e,
                    str::from_utf8(key),
                );
            }
            Ok(()) => {
                return Ok(());
            }
        }
    }

    fn insert_inner(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) -> Result<()> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.len() as u32;

        let bucket_idx = Self::hash_to_index(key, bucket_num);

        let pid = buckets[bucket_idx].page_id();
        let fid = buckets[bucket_idx].frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = self.try_write_page(page_f_key).ok();
        let mut inserted_page = match acq_result {
            None => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Some(p) => p,
        };
        let inserted_result =
            <Page as TableDataPage>::insert(&mut inserted_page, key, pkey, val, ts);

        match inserted_result {
            Err(HashTableAccessMethodError::OutOfSpace) => {
                // rehash
                return Err(HashTableAccessMethodError::HashPageOutOfSpace(
                    bucket_num * 2,
                ));
            }
            Err(e) => {
                panic!(
                    "should not happen! have checked before insert. err: {:?}, insert_key: {:?}",
                    e,
                    str::from_utf8(key),
                );
            }
            Ok(()) => {
                return Ok(());
            }
        }
    }

    fn update_inner(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) -> Result<()> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.len() as u32;

        let bucket_idx = Self::hash_to_index(key, bucket_num);

        let pid = buckets[bucket_idx].page_id();
        let fid = buckets[bucket_idx].frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = self.try_write_page(page_f_key).ok();
        let mut inserted_page = match acq_result {
            None => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Some(p) => p,
        };
        let inserted_result =
            <Page as TableDataPage>::update(&mut inserted_page, key, pkey, val, ts);

        match inserted_result {
            Err(HashTableAccessMethodError::OutOfSpace) => {
                // rehash
                return Err(HashTableAccessMethodError::HashPageOutOfSpace(
                    bucket_num * 2,
                ));
            }
            Err(e) => {
                panic!(
                    "should not happen! have checked before insert. err: {:?}, insert_key: {:?}",
                    e,
                    str::from_utf8(key),
                );
            }
            Ok(()) => {
                return Ok(());
            }
        }
    }

    fn rehash(&self, hash_size: u32) -> bool {
        println!("[REHASH] rehash call to new size: {}", hash_size);
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.buckets_rwlock.read().unwrap();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.len() as u32 >= hash_size {
                    // log_warn!("[rehash abort]");
                    return false;
                }
                assert_eq!(buckets.len() as u32 * 2, hash_size);
            }
        }

        // we need to rehash
        let mut buckets = self.buckets_rwlock.write().unwrap();
        let old_entry_num = buckets.len() as u32;
        for _ in 0..old_entry_num {
            buckets.push(BucketEntry::new(
                0, // dummy
            ));
        }

        // log_warn!("[re-hash] old_entry_num: {:?}", old_entry_num);

        for hashed_bucket_idx in 0..old_entry_num {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let new_pid = new_page.get_id();
            let new_fid = new_page.frame_id();
            <Page as TableDataPage>::init(&mut new_page);

            buckets[(hashed_bucket_idx + old_entry_num) as usize] =
                BucketEntry::new_with_frame_id(new_pid, new_fid);

            let hashed_bucket_entry = &buckets[hashed_bucket_idx as usize];
            let hashed_pfk = PageFrameKey::new_with_frame_id(
                self.c_key,
                hashed_bucket_entry.page_id(),
                hashed_bucket_entry.frame_id(),
            );
            let mut hashed_page = self.write_page(hashed_pfk);
            <Page as TableDataPage>::rehash(&mut hashed_page, &mut new_page, |key: &[u8]| {
                Self::hash_to_index(key, hash_size) as u32 != hashed_bucket_idx
            });
            // log_warn!(
            //     "rehashed_page_id: {:?}, rec_start_offset: {:?} slot_count {:?} slot_end {:?}",
            //     hashed_page.page_key().unwrap().page_id,
            //     hashed_page.header().rec_start_offset(),
            //     hashed_page.slot_count(),
            //     hashed_page.header().slot_end_offset()
            // );
            // log_warn!(
            //     "new_page_id: {:?}, rec_start_offset: {:?} slot_count {:?} slot_end {:?} free_with_cpt {:?}",
            //     new_page.page_key().unwrap().page_id,
            //     new_page.header().rec_start_offset(),
            //     new_page.slot_count(),
            //     new_page.header().slot_end_offset(),
            //     new_page.free_space_with_compaction(),
            // );
        }

        // let meta_page_key = PageFrameKey::new_with_frame_id(
        //     self.c_key,
        //     self.meta.0,
        //     self.meta.1.load(std::sync::atomic::Ordering::Acquire),
        // );
        // let mut meta_page = self.write_page(meta_page_key);
        // let new_page_ids = buckets
        //     .buckets
        //     .iter()
        //     .map(|x| x.page_id())
        //     .collect::<Vec<_>>();
        // <Page as MvccHashJoinCuckooMetaPage>::rehash_update_recent(&mut *meta_page, &new_page_ids);
        return true;
    }

    /// only used when we set key as sub_table hashing argument
    fn get_keys_inner(&self, key: &[u8], ts: Timestamp) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = Self::hash_to_index(key, buckets.len() as u32);
        let read_page = {
            let pid = buckets[bucket_idx].page_id();
            let fid = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_read_page(page_f_key).ok();
            match acq_result {
                None => {
                    return Err(HashTableAccessMethodError::AcquireLockFailed);
                }
                Some(page) => page,
            }
        };

        let get_result = <Page as TableDataPage>::get_keys(&read_page, key, ts);
        get_result
    }

    /*
        acquire 1 page lock
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        return Err(keynotfound)
    */
    fn get_inner(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<Vec<u8>> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = Self::hash_to_index(key, buckets.len() as u32);
        let read_page = {
            let pid = buckets[bucket_idx].page_id();
            let fid = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_read_page(page_f_key).ok();
            match acq_result {
                None => {
                    return Err(HashTableAccessMethodError::AcquireLockFailed);
                }
                Some(page) => page,
            }
        };

        let get_result = <Page as TableDataPage>::get(&read_page, key, pkey, ts);
        match get_result {
            Ok(val) => {
                return Ok(val);
            }
            Err(HashTableAccessMethodError::KeyNotFound) => {
                return Err(HashTableAccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!("Should not happen! error: {:?}", e);
            }
        }
    }

    fn delete_inner(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<()> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_num = buckets.len() as u32;
        let bucket_idx = Self::hash_to_index(key, buckets.len() as u32);

        let mut write_page = {
            let pid = buckets[bucket_idx].page_id();
            let fid = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_write_page(page_f_key).ok();
            match acq_result {
                None => {
                    return Err(HashTableAccessMethodError::AcquireLockFailed);
                }
                Some(page) => page,
            }
        };

        let deleted_result = <Page as TableDataPage>::delete(&mut write_page, key, pkey, ts);

        match deleted_result {
            Err(HashTableAccessMethodError::OutOfSpace) => {
                // rehash
                return Err(HashTableAccessMethodError::HashPageOutOfSpace(
                    bucket_num * 2,
                ));
            }
            Err(HashTableAccessMethodError::KeyNotFound) => {
                return Err(HashTableAccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!(
                    "should not happen! have checked before insert. err: {:?}, insert_key: {:?}",
                    e,
                    str::from_utf8(key),
                );
            }
            Ok(()) => {
                return Ok(());
            }
        }
    }

    pub fn dbg_dump_all_entry(&self) -> usize {
        let buckets = self.buckets_rwlock.read().unwrap();
        let mut num = 0_usize;
        log_warn!("<<DUMP HASH TABLE>> ------------ START AN SUBTABLE! -------");
        for entry in &*buckets {
            let pfkey = PageFrameKey::new(self.c_key, entry.page_id());
            let page = self.read_page(pfkey);
            num += <Page as TableDataPage>::dbg_print_slots(&page);
        }
        return num;
    }

    pub fn scan_mvcc_entries(
        self: &Arc<Self>,
        option: SubTableScannerOption,
    ) -> SubTableScanner<T> {
        SubTableScanner::new(self, option)
    }
    pub fn scan_delta(
        self: &Arc<Self>,
        option: SubTableDeltaScannerOption,
    ) -> SubTableDeltaScanner<T> {
        SubTableDeltaScanner::new(self, option)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        bp::{get_in_mem_pool, ContainerKey},
        mvcc_index::{
            hashtable_mu::{
                double_hash::double_hash_sub_table::DHashSubTable,
                hash_join_table_common::DEFAULT_NUM_BUCKETS,
            },
            Timestamp,
        },
        page::AVAILABLE_PAGE_SIZE,
    };

    #[test]
    fn simple_insert() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table =
            DHashSubTable::new_with_bucket_num(c_key, mem_pool, DEFAULT_NUM_BUCKETS);
        hash_join_table.upsert(&[1], &[1], 1, &[1]).unwrap();
        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn many_inserts_until_rehash() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = DHashSubTable::new_with_bucket_num(c_key, mem_pool, 1);

        let pair_space_need = 54;
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            hash_join_table
                .upsert(
                    format!("{:06}", i).as_bytes(),
                    format!("{:06}", i).as_bytes(),
                    i as Timestamp,
                    format!("{:06}", i).as_bytes(),
                )
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result = hash_join_table.get(
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                i as Timestamp,
            );
            assert_eq!(
                get_result.unwrap().unwrap(),
                format!("{:06}", i).as_bytes().to_vec()
            );
        }
    }
}
