use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{
    bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    lockmanager::{LockManager, Permissions, TransactionId, ValueId},
    log_debug, log_warn,
    mvcc_index::Timestamp,
    page::{Page, PageId},
};

use super::{
    mvcc_hash_join_cuckoo_common::{
        arcrwlock::*, BucketEntry, Buckets, CuckooAccessMethodError, LockManagerGuard,
    },
    mvcc_hash_join_cuckoo_history_page::MvccHashJoinCuckooHistoryPage,
};

/* --------------------------- Scanner START ---------------------------------- */

use super::mvcc_hash_join_cuckoo_common::arcrwlock::*;
pub struct HistoryScanTsWithBucketsReadGuard<T: MemPool> {
    lock_manager: Arc<Mutex<LockManager>>,
    tid: TransactionId,
    current_entry: u32,

    mem_pool: Arc<T>,

    current_slot_id: u32,

    ts: Timestamp,
    c_key: ContainerKey,

    buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    scan_key: Option<Vec<u8>>,
}

impl<T: MemPool> HistoryScanTsWithBucketsReadGuard<T> {
    /// assume has get lock for that tid+vid
    pub fn new(
        lm: &Arc<Mutex<LockManager>>,
        tid: TransactionId,
        mem_pool: &Arc<T>,
        ts: Timestamp,
        c_key: ContainerKey,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
        scan_key: Option<Vec<u8>>,
    ) -> Self {
        Self {
            lock_manager: lm.clone(),
            tid,
            current_entry: 0,
            mem_pool: mem_pool.clone(),
            current_slot_id: 0,
            ts,
            c_key,
            buckets_read_guard,
            scan_key,
        }
    }
}

impl<T: MemPool> HistoryScanTsWithBucketsReadGuard<T> {
    fn read_page(&self) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(self.page_key());
            // protected by LockManager, only acceptable error is "CannotEvictPage"
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", self.page_key());
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn valueid(&self) -> ValueId {
        let page_id = self
            .buckets_read_guard
            .get_bucket_entry(self.current_entry as usize)
            .page_id();
        ValueId {
            container_id: 0,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    fn page_key(&self) -> PageFrameKey {
        let page_id = self
            .buckets_read_guard
            .get_bucket_entry(self.current_entry as usize)
            .page_id();
        PageFrameKey::new(self.c_key, page_id)
    }
}

/// Ensure no re-hash by acquire read lock of buckets \
///
impl<T: MemPool> Iterator for HistoryScanTsWithBucketsReadGuard<T> {
    type Item = (Vec<u8>, Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // log_warn!("current entry: {:?}", self.current_entry);
            if self.current_entry >= self.buckets_read_guard.get_bucket_num() {
                // log_warn!("bucket num: {:?}", self.buckets_read_guard.get_bucket_num());
                // scan ends
                return None;
            }

            let base = 2;
            let mut attempts = 0;

            if self.current_slot_id == 0 {
                // new page to scan, get lock from lock_manager
                '_acquire_page_lock: loop {
                    let ret = self.lock_manager.lock().unwrap().acquire_lock(
                        self.tid,
                        self.valueid(),
                        Permissions::ReadOnly,
                    );
                    if !ret {
                        // log_debug!("acquire write lock of page failed, re-do");
                        attempts += 1;
                        std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                        continue;
                    } else {
                        break;
                    }
                }
            }

            let read_page = self.read_page();
            let mut current_slot_id = self.current_slot_id;
            let mut ret = Option::<Self::Item>::None;
            '_scan_a_page: loop {
                match <Page as MvccHashJoinCuckooHistoryPage>::slot(&*read_page, current_slot_id) {
                    Some(slot) => {
                        let (slot_key, slot_pkey, slot_val, slot_start_ts, slot_end_ts) =
                            <Page as MvccHashJoinCuckooHistoryPage>::get_key_pkey_val_ts_with_slot(
                                &*read_page,
                                &slot,
                            );
                        // log_warn!("get slot_id: {:?}, slot_key: {:?}", current_slot_id, slot_key);
                        if slot_start_ts <= self.ts && self.ts < slot_end_ts {
                            if self.scan_key.is_some() {
                                // scan_key, if key matches -> return
                                // else continue;
                                if self.scan_key.as_ref().unwrap() == &slot_key {
                                    // match -> return
                                    ret = Some((vec![], slot_pkey, slot_val));
                                    break;
                                } else {
                                    current_slot_id += 1;
                                }
                            } else {
                                // simple scan
                                ret = Some((slot_key, slot_pkey, slot_val));
                                break;
                            }
                            break;
                        } else {
                            current_slot_id += 1;
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
            drop(read_page);

            if ret.is_none() {
                // release current page locks
                self.lock_manager
                    .lock()
                    .unwrap()
                    .release_lock(self.tid, self.valueid())
                    .unwrap();
                // current page scan ends
                // log_warn!("read a page end!!");
                self.current_slot_id = 0;
                self.current_entry += 1;
                continue;
            } else {
                self.current_slot_id = current_slot_id + 1;
                // find a next value for iterator
                return ret;
            }
        }
    }
}

/* --------------------------- Scanner END!! ---------------------------------- */

/// responsible for update meta page of HashJoinTable<T>
pub struct CuckooHashHistoryTable<T: MemPool> {
    // hasher_idx: usize,
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    /// shared: read & update (rehash) & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    rwlock: ArcRwlock<Buckets>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once

    lock_manager: Arc<Mutex<LockManager>>, // function level serializability : get/insert/update/...
}

impl<T: MemPool> CuckooHashHistoryTable<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let pid = page.get_id();
        let fid = page.frame_id();

        MvccHashJoinCuckooHistoryPage::init(&mut *page);
        drop(page);

        let buckets = Buckets {
            num_buckets: 1,
            buckets: vec![BucketEntry::new_with_frame_id(pid, fid)],
        };

        Self {
            c_key,
            mem_pool,
            rwlock: new_arc_rw_lock(buckets),
            rehash_mutex: Mutex::new(()),
            lock_manager: Arc::new(Mutex::new(LockManager::new())),
        }
    }

    /// init or re-hash
    pub fn get_all_bucket_pages_for_init(&self) -> Vec<PageId> {
        let buckets = self.rwlock.read();
        let ret = buckets
            .buckets
            .iter()
            .map(|x| x.page_id())
            .collect::<Vec<_>>();
        ret
    }

    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut bucket_entry_vec = vec![];
        for _ in 0..num_buckets {
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let pid = page.get_id();
            let fid = page.frame_id();

            MvccHashJoinCuckooHistoryPage::init(&mut *page);
            drop(page);

            bucket_entry_vec.push(BucketEntry::new_with_frame_id(pid, fid));
        }
        let buckets = Buckets {
            num_buckets: num_buckets as u32,
            buckets: bucket_entry_vec,
        };
        Self {
            c_key,
            mem_pool,
            rwlock: new_arc_rw_lock(buckets),
            rehash_mutex: Mutex::new(()),
            lock_manager: Arc::new(Mutex::new(LockManager::new())),
        }
    }

    fn gen_valueid(page_id: u32) -> ValueId {
        ValueId {
            container_id: 0,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    /// assert have gotten buckets lock
    fn try_acq_write_lock_manager(
        &self,
        tid: TransactionId,
        page_f_key: PageFrameKey,
    ) -> Option<(LockManagerGuard, FrameWriteGuard)> {
        let mut write_guard = self.lock_manager.lock().unwrap();
        let vid = Self::gen_valueid(page_f_key.p_key().page_id);
        let acq_result = write_guard.acquire_lock(tid, vid, Permissions::ReadWrite);
        if !acq_result {
            return None; // REDO
        }
        Some((
            LockManagerGuard::new(&self.lock_manager, tid, vid),
            self.write_page(page_f_key),
        ))
    }

    /// assert have gotten buckets lock
    fn try_acq_read_lock_manager(
        &self,
        tid: TransactionId,
        page_f_key: PageFrameKey,
    ) -> Option<(LockManagerGuard, FrameReadGuard)> {
        let mut write_guard = self.lock_manager.lock().unwrap();
        let vid = Self::gen_valueid(page_f_key.p_key().page_id);
        let acq_result = write_guard.acquire_lock(tid, vid, Permissions::ReadOnly);
        if !acq_result {
            return None; // REDO
        }
        Some((
            LockManagerGuard::new(&self.lock_manager, tid, vid),
            self.read_page(page_f_key),
        ))
    }

    fn gen_scan_iterator(
        &self,
        tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    ) -> HistoryScanTsWithBucketsReadGuard<T> {
        let scan_guard = HistoryScanTsWithBucketsReadGuard::new(
            &self.lock_manager,
            tid,
            &self.mem_pool,
            ts,
            self.c_key,
            buckets_read_guard,
            None,
        );
        scan_guard.into_iter()
    }

    fn gen_scan_key_iterator(
        &self,
        tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
        scan_key: Option<Vec<u8>>,
    ) -> HistoryScanTsWithBucketsReadGuard<T> {
        let scan_guard = HistoryScanTsWithBucketsReadGuard::new(
            &self.lock_manager,
            tid,
            &self.mem_pool,
            ts,
            self.c_key,
            buckets_read_guard,
            scan_key,
        );
        scan_guard.into_iter()
    }

    pub fn scan(&self, ts: Timestamp) -> HistoryScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_iterator(TransactionId::new(), ts, buckets)
    }

    pub fn scan_key(&self, ts: Timestamp, key: &[u8]) -> HistoryScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
    }

    /*
        if have free space -> insert
        if free space after compaction -> compaction

        else -> rehash: Err(CuckooOutOfSpace)


        1. get buckets' read lock
        2. access buckets, randomly get one bucket from 2 hash functions
        3. get write lock of page
            if failed -> Err(AcquireLockFailed)
        4. check space
            if failed -> Err(CuckooOutOfSpace(new_rehash_size: u32))
        5. do insert or compaction-and-insert or re-hash
    */
    fn insert_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let buckets = self.rwlock.read();
        let bucket_num = buckets.get_bucket_num();
        // log_warn!("[history::insert_inner] insert key: {:?}", key);

        let bucket_idx: usize = buckets.get_bucket_index_random(key);
        let inserted_pid = buckets.get_bucket_entry(bucket_idx).page_id();
        let inserted_fid = buckets.get_bucket_entry(bucket_idx).frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, inserted_pid, inserted_fid);
        // try to acquire lock
        let (_lock_manager_guard, mut inserted_page) =
            match self.try_acq_write_lock_manager(TransactionId::new(), page_f_key) {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed); // REDO
                }
                Some(x) => x,
            };

        let check_insert_result = {
            let insert_space_need =
                <Page as MvccHashJoinCuckooHistoryPage>::space_need(key, pkey, val);
            let page_free_space =
                <Page as MvccHashJoinCuckooHistoryPage>::free_space_with_compaction(
                    &*inserted_page,
                );
            // log_warn!(
            //     "[history::insert_inner] page free space: {:?}, insert_size: {:?}",
            //     page_free_space,
            //     insert_space_need
            // );

            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as MvccHashJoinCuckooHistoryPage>::insert(
                &mut *inserted_page,
                key,
                pkey,
                start_ts,
                end_ts,
                val,
            );
            match insert_result {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    panic!(
                        "should not happen! have checked before insert. err: {:?}",
                        e
                    );
                }
            }
        } else {
            return Err(CuckooAccessMethodError::CuckooOutOfSpace(bucket_num * 2));
            // rehash
        }
    }

    pub fn rehash(&self, hash_size: u32) -> bool {
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.rwlock.read();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.get_bucket_num() >= hash_size {
                    return false;
                }
                assert_eq!(buckets.get_bucket_num() * 2, hash_size);
            }
        }

        // we need to rehash
        let mut buckets = self.rwlock.write();
        let old_entry_num = buckets.get_bucket_num();
        for _ in 0..old_entry_num {
            buckets.buckets.push(BucketEntry::new(
                0, // dummy
            ));
        }

        for hashed_bucket_idx in 0..old_entry_num {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let new_pid = new_page.get_id();
            let new_fid = new_page.frame_id();
            MvccHashJoinCuckooHistoryPage::init(&mut *new_page);
            buckets.buckets[(hashed_bucket_idx + old_entry_num) as usize] =
                BucketEntry::new_with_frame_id(new_pid, new_fid);

            let bucket_entry = buckets.get_bucket_entry(hashed_bucket_idx as usize);
            let page_frame_k = PageFrameKey::new_with_frame_id(
                self.c_key,
                bucket_entry.page_id(),
                bucket_entry.frame_id(),
            );

            let mut hashed_page = self.write_page(page_frame_k);
            let slot_count = hashed_page.slot_count();

            for slot_idx in (0..slot_count).rev() {
                let (key, pkey, val, start_ts, end_ts) =
                    hashed_page.get_key_pkey_val_ts_with_slot_id(slot_idx);
                if let Some(idx) =
                    buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize, false)
                {
                    assert_eq!(idx as u32, (hashed_bucket_idx + old_entry_num));
                    match new_page.insert(&key, &pkey, start_ts, end_ts, &val) {
                        Ok(_) => {}
                        Err(_) => {
                            panic!("should not happen in re-hash!");
                        }
                    }
                    hashed_page.delete_slot_at_id(slot_idx).unwrap();
                }
            }
        }
        buckets.num_buckets = old_entry_num * 2;
        return true;
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<bool, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        let mut rehash_flag = false;
        loop {
            match self.insert_inner(key, pkey, start_ts, end_ts, val) {
                Ok(()) => {
                    // log_warn!("history table insert ok!");
                    return Ok(rehash_flag);
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    rehash_flag = self.rehash(new_hash_size);
                    // log_warn!("Page insert out of space, re-hash");
                    log_debug!("Page insert out of space, re-hash");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
                    log_warn!("acquire write lock of page failed, re-do");
                    log_debug!("acquire write lock of page failed, re-do");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        return Err(keynotfound)
    */
    fn get_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let buckets = self.rwlock.read();

        let indexes = buckets.get_all_bucket_index(key);
        let tid = TransactionId::new();
        let mut pages = vec![];
        let mut lock_manager_guards = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_read_lock_manager(tid, page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some((guard, page)) => {
                    lock_manager_guards.push(guard);
                    pages.push(page);
                }
            }
        }

        for read_page in pages {
            let get_result =
                <Page as MvccHashJoinCuckooHistoryPage>::get(&*read_page, key, pkey, ts);
            match get_result {
                Ok(val) => {
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(e) => {
                    panic!("Should not happen! error: {:?}", e);
                }
            }
        }
        Err(CuckooAccessMethodError::KeyNotFound)
    }

    /*
       search both hasher_idx
       if find -> return value
       return Err(keynotfound)


       remember to keep atomicity!
    */
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.get_inner(key, pkey, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    return Err(CuckooAccessMethodError::KeyNotFound);
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
                    log_debug!("acquire read lock of pages failed, re-do");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        return Vector
    */
    fn get_all_inner(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        let buckets = self.rwlock.read();

        let indexes = buckets.get_all_bucket_index(key);
        let tid = TransactionId::new();
        let mut pages = vec![];
        let mut lock_manager_guards = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_read_lock_manager(tid, page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some((guard, page)) => {
                    lock_manager_guards.push(guard);
                    pages.push(page);
                }
            }
        }
        let mut ret = vec![];
        for read_page in pages {
            let get_result = <Page as MvccHashJoinCuckooHistoryPage>::get_all(&*read_page, key, ts);
            match get_result {
                Ok(val) => {
                    ret.extend_from_slice(&val);
                }
                Err(e) => {
                    panic!("Should not happen! error: {:?}", e);
                }
            }
        }
        return Ok(ret);
    }

    pub fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.get_all_inner(key, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
                    log_debug!("acquire read lock of pages failed, re-do");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }

    pub fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError> {
        todo!()
    }

    // helper function
    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.mem_pool.get_page_for_write(page_key);
            // protected by LockManager, only acceptable error is "CannotEvictPage"
            match page {
                Ok(page) => {
                    return page;
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

    // helper function
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            // protected by LockManager, , only acceptable error is "CannotEvictPage"
            match page {
                Ok(page) => {
                    return page;
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
}
