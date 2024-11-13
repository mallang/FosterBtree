use core::str;
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

/* --------------------------- Scanner START ---------------------------------- */

use super::{
    mvcc_hash_join_cuckoo_common::{
        arcrwlock::*, BucketEntry, Buckets, CuckooAccessMethodError, LockManagerGuard,
    },
    mvcc_hash_join_page::MvccHashJoinCuckooPage,
};
pub struct ScanTsWithBucketsReadGuard<T: MemPool> {
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

impl<T: MemPool> ScanTsWithBucketsReadGuard<T> {
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

impl<T: MemPool> ScanTsWithBucketsReadGuard<T> {
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
impl<T: MemPool> Iterator for ScanTsWithBucketsReadGuard<T> {
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
                match <Page as MvccHashJoinCuckooPage>::slot(&*read_page, current_slot_id) {
                    Some(slot) => {
                        let (slot_key, slot_pkey, slot_val, slot_start_ts, slot_end_ts) =
                            <Page as MvccHashJoinCuckooPage>::get_key_pkey_val_ts_with_slot(
                                &*read_page,
                                &slot,
                            );
                        // log_warn!("get slot_id: {:?}, slot_key: {:?}", current_slot_id, slot_key);
                        if slot_start_ts <= self.ts
                            && self.ts < slot_end_ts
                            && !slot.is_mark_deleted()
                        {
                            if self.scan_key.is_some() {
                                // scan_key, if key matches -> return
                                // else continue;
                                if self.scan_key.as_ref().unwrap() == &slot_key {
                                    // match -> return
                                    ret = Some((vec![], slot_pkey, slot_val));
                                    break;
                                }
                            } else {
                                // simple scan
                                ret = Some((slot_key, slot_pkey, slot_val));
                                break;
                            }
                        }
                        current_slot_id += 1;
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

/// responsible for update meta page of HashJoinTable<T>
pub struct CuckooHashTable<T: MemPool> {
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

pub trait CuckooRecentHashTable<T: MemPool> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self;
    fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_nums: usize) -> Self;
    fn get_all_bucket_page_ids(&self) -> Vec<PageId>;
    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T>;
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(bool, Option<Timestamp>), CuckooAccessMethodError>;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError>;
    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError>;
    fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>, bool), CuckooAccessMethodError>;
    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError>;
}

pub trait CuckooHistoryHashTable<T: MemPool> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self;
    fn get_all_bucket_page_ids(&self) -> Vec<PageId>;
    fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_nums: usize) -> Self;
    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T>;
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<bool, CuckooAccessMethodError>;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError>;
    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError>;
    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError>;
    fn insert_deleted(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<bool, CuckooAccessMethodError>;
}

/// Recent & History basic functions
impl<T: MemPool> CuckooHashTable<T> {
    fn new_with_bucket_num_inner(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
    ) -> Self {
        let mut bucket_entry_vec = vec![];
        for _ in 0..num_buckets {
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let pid = page.get_id();
            let fid = page.frame_id();

            MvccHashJoinCuckooPage::init(&mut *page);
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
    /// used to provide an interface for lock_manager::valueid \
    /// only use page_id,
    fn gen_valueid(page_id: u32) -> ValueId {
        ValueId {
            container_id: 0,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
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
    // helper function
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
    // helper function
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
    /* NEW
       if have free space -> insert
       if free space after compaction -> compact
       else -> rehash(Err(CuckooOutOfSpace))

       1. get buckets' read lock
       2. access buckets, get 2 buckets from 2 hash functions;
       3. get write lock of pages
           if failed -> Err(AckLockFailed)
       4. try find delete mark of key, if find -> add to history and delete
       5. randomly choose one page to insert and check space
           if failed -> Err(CuckooOutOfSpace(new_hash_size))
       5. do insert or compact-and-insert
    */
    fn recent_insert_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<Option<Timestamp>, CuckooAccessMethodError> {
        let buckets = self.rwlock.read();
        let bucket_num = buckets.get_bucket_num();

        let indexes: Vec<usize> = buckets.get_all_bucket_index(key);
        let tid = TransactionId::new();

        let mut guard_and_page_vec = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_write_lock_manager(tid, page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(g_and_p) => {
                    // lock_manager_guards.push(guard);
                    // pages.push(page);
                    guard_and_page_vec.push(g_and_p);
                }
            }
        }
        let mut old_delete_marker = None;
        for write_page in &mut guard_and_page_vec {
            // Attempt to retrieve the value from the current page
            match MvccHashJoinCuckooPage::get_delete_mark_slot_id(&*write_page.1, key, pkey, ts) {
                Ok(slot_id) => {
                    // Value found
                    match MvccHashJoinCuckooPage::delete_slot_at_id(&mut *write_page.1, slot_id) {
                        Ok((old_ts, _)) => {
                            old_delete_marker = Some(old_ts);
                            break;
                        }
                        Err(x) => {
                            panic!("should not happen for that error! {:?}", x);
                        }
                    }
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(_) => {
                    panic!("should not happen!");
                }
            };
        }

        let (mut _inserted_guard, mut inserted_page) = {
            if guard_and_page_vec.len() == 1 {
                guard_and_page_vec.pop().unwrap()
            } else {
                assert!(guard_and_page_vec.len() == 2);
                let random_idx = rand::random::<u8>() & 1;
                guard_and_page_vec.remove(random_idx as usize)
            }
        };
        drop(guard_and_page_vec);

        let check_insert_result = {
            let insert_space_need = <Page as MvccHashJoinCuckooPage>::space_need(key, pkey, val);
            let page_free_space =
                <Page as MvccHashJoinCuckooPage>::free_space_with_compaction(&*inserted_page);
            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as MvccHashJoinCuckooPage>::insert(
                &mut *inserted_page,
                key,
                pkey,
                ts,
                Timestamp::MAX,
                val,
            );
            match insert_result {
                Ok(_) => {
                    // log_warn!("[OK!] insert key: {:?}", key);
                    return Ok(old_delete_marker);
                }
                Err(e) => {
                    panic!(
                        "should not happen! have checked before insert. err: {:?}, insert_key: {:?}",
                        e,
                        str::from_utf8(key),
                    );
                }
            }
        } else {
            return Err(CuckooAccessMethodError::CuckooOutOfSpace(bucket_num * 2));
            // rehash
        }
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
    fn history_insert_inner(
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
            let insert_space_need = <Page as MvccHashJoinCuckooPage>::space_need(key, pkey, val);
            let page_free_space =
                <Page as MvccHashJoinCuckooPage>::free_space_with_compaction(&*inserted_page);
            // log_warn!(
            //     "[history::insert_inner] page free space: {:?}, insert_size: {:?}",
            //     page_free_space,
            //     insert_space_need
            // );

            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as MvccHashJoinCuckooPage>::insert(
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

    fn history_insert_deleted_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
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
                <Page as MvccHashJoinCuckooPage>::space_need(key, pkey, &vec![]);
            let page_free_space =
                <Page as MvccHashJoinCuckooPage>::free_space_with_compaction(&*inserted_page);
            // log_warn!(
            //     "[history::insert_inner] page free space: {:?}, insert_size: {:?}",
            //     page_free_space,
            //     insert_space_need
            // );

            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as MvccHashJoinCuckooPage>::insert_deleted(
                &mut *inserted_page,
                key,
                pkey,
                start_ts,
                end_ts,
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

    fn rehash(&self, hash_size: u32) -> bool {
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.rwlock.read();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.get_bucket_num() >= hash_size {
                    // log_warn!("[rehash abort]");
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

        // log_warn!("[re-hash] old_entry_num: {:?}", old_entry_num);

        for hashed_bucket_idx in 0..old_entry_num {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let new_pid = new_page.get_id();
            let new_fid = new_page.frame_id();
            MvccHashJoinCuckooPage::init(&mut *new_page);
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
                    buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize, true)
                {
                    // log_warn!("we can get a second idx!!!");
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
            // log_warn!(
            //     "rehashed_page_id: {:?}, rec_start_offset: {:?} slot_end {:?}",
            //     hashed_page.page_key().unwrap().page_id,
            //     hashed_page.header().rec_start_offset(),
            //     hashed_page.header().slot_end_offset()
            // );
            // log_warn!(
            //     "new_page_id: {:?}, rec_start_offset: {:?}, slot_end {:?}",
            //     new_page.page_key().unwrap().page_id,
            //     new_page.header().rec_start_offset(),
            //     new_page.header().slot_end_offset()
            // );
        }
        buckets.num_buckets = old_entry_num * 2;
        return true;
    }

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
        return Err(keynotfound)
    */
    fn recent_get_inner(
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
                <Page as MvccHashJoinCuckooPage>::get(&*read_page, key, pkey, ts, true);
            match get_result {
                Ok(val) => {
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(e) => {
                    panic!("Should not happen! error: {:?}", e);
                }
            }
        }
        Err(CuckooAccessMethodError::KeyNotFound)
    }

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        return Err(keynotfound)
    */
    fn history_get_inner(
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
                <Page as MvccHashJoinCuckooPage>::get(&*read_page, key, pkey, ts, false);
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
        BOTH HISTORY and RECENT are the same in get_all_inner
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
            let get_result = <Page as MvccHashJoinCuckooPage>::get_all(&*read_page, key, ts);
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

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
        if not find -> return Err(KeyNotFound)
        if find -> try to update value
            if space not enough -> Err(CuckooOutOfSpace(new_bucket_num:u32)): RE-HASH
            else return OK

        return Err(keynotfound)
    */
    fn recent_update_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let buckets = self.rwlock.read();
        let bucket_num = buckets.get_bucket_num();

        let indexes: Vec<usize> = buckets.get_all_bucket_index(key);
        let tid = TransactionId::new();
        let mut pages = vec![];
        let mut lock_manager_guards = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_write_lock_manager(tid, page_f_key);
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

        for mut write_page in pages {
            // Attempt to retrieve the value from the current page
            match MvccHashJoinCuckooPage::get_slot_id(&*write_page, key, pkey, ts) {
                Ok(slot_id) => {
                    // Value found
                    log_warn!(
                        "[update key:{:?}, page: {:?}]updated value found!",
                        String::from_utf8(key.to_vec()),
                        write_page.page_key().unwrap().page_id
                    );
                    match MvccHashJoinCuckooPage::check_and_update_at_slot_id(
                        &mut *write_page,
                        slot_id,
                        key,
                        pkey,
                        val,
                        ts,
                    ) {
                        Ok(old_res) => {
                            log_warn!("return ok!");
                            return Ok(old_res);
                        }
                        Err(CuckooAccessMethodError::OutOfSpace) => {
                            log_warn!("out of space");
                            return Err(CuckooAccessMethodError::CuckooOutOfSpace(bucket_num * 2));
                        }
                        Err(x) => {
                            panic!("should not happen for that error! {:?}", x);
                        }
                    }
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(_) => {
                    panic!("should not happen!");
                }
            };
        }

        return Err(CuckooAccessMethodError::KeyNotFound);
    }

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
        if not find -> return Err(KeyNotFound)
        if find -> mark delete && return OK
        return Err(keynotfound)
    */
    fn recent_delete_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let buckets = self.rwlock.read();

        let indexes: Vec<usize> = buckets.get_all_bucket_index(key);
        let tid = TransactionId::new();
        let mut guard_and_page_vec = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_write_lock_manager(tid, page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(g_and_p) => {
                    guard_and_page_vec.push(g_and_p);
                }
            }
        }

        for (_lock_manager_guard, mut write_page) in guard_and_page_vec {
            // Attempt to retrieve the value from the current page
            match MvccHashJoinCuckooPage::get_slot_id(&*write_page, key, pkey, ts) {
                Ok(slot_id) => {
                    // Value found
                    match MvccHashJoinCuckooPage::mark_delete_slot_at_id(
                        &mut *write_page,
                        slot_id,
                        ts,
                    ) {
                        Ok(old_res) => return Ok(old_res),
                        Err(x) => {
                            panic!("should not happen for that error! {:?}", x);
                        }
                    }
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(_) => {
                    panic!("should not happen!");
                }
            };
        }

        return Err(CuckooAccessMethodError::KeyNotFound);
    }

    fn gen_scan_iterator(
        &self,
        tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    ) -> ScanTsWithBucketsReadGuard<T> {
        let scan_guard = ScanTsWithBucketsReadGuard::new(
            &self.lock_manager,
            tid,
            &self.mem_pool,
            ts,
            self.c_key,
            buckets_read_guard,
            None,
        );
        scan_guard
    }

    fn gen_scan_key_iterator(
        &self,
        tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
        scan_key: Option<Vec<u8>>,
    ) -> ScanTsWithBucketsReadGuard<T> {
        let scan_guard = ScanTsWithBucketsReadGuard::new(
            &self.lock_manager,
            tid,
            &self.mem_pool,
            ts,
            self.c_key,
            buckets_read_guard,
            scan_key,
        );
        scan_guard
    }
}

impl<T: MemPool> CuckooRecentHashTable<T> for CuckooHashTable<T> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, 1)
    }
    fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_nums: usize) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, bucket_nums)
    }
    fn get_all_bucket_page_ids(&self) -> Vec<PageId> {
        let buckets = self.rwlock.read();
        let ret = buckets
            .buckets
            .iter()
            .map(|x| x.page_id())
            .collect::<Vec<_>>();
        ret
    }
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(bool, Option<Timestamp>), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        let mut rehash_flag = false;
        loop {
            match self.recent_insert_inner(key, pkey, ts, val) {
                Ok(delete_marker) => {
                    return Ok((rehash_flag, delete_marker));
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    rehash_flag = self.rehash(new_hash_size);
                    log_debug!("Page insert out of space, re-hash");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
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
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        log_warn!("start get key: {:?}", key);
        loop {
            match self.recent_get_inner(key, pkey, ts) {
                Ok(val) => {
                    log_warn!("finish get key: {:?}", key);
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    return Err(CuckooAccessMethodError::KeyNotFound);
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
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

    fn get_all(
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

    /*
        if not find => Err(KeyNotFound)
        if invalid ts => Err(KeyFoundButInvalidTimestamp)
        else {
            if update succ => return (old_ts, old_val)
            if out of space => return Err(OutOfSpace) => Rehash
        }
    */
    fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>, bool), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        let mut rehash_flag = false;
        loop {
            log_warn!("start update loop");
            match self.recent_update_inner(key, pkey, ts, val) {
                Ok((old_ts, old_val)) => {
                    return Ok((old_ts, old_val, rehash_flag));
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    rehash_flag = self.rehash(new_hash_size);
                    log_warn!("Page insert out of space, re-hash");
                    // attempts += 1;
                    // std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    return Err(CuckooAccessMethodError::KeyNotFound);
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
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

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.recent_delete_inner(key, pkey, ts) {
                Ok(old_res) => {
                    return Ok(old_res);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    return Err(CuckooAccessMethodError::KeyNotFound);
                }
                Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(CuckooAccessMethodError::AcquireLockFailed) => {
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

    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_iterator(TransactionId::new(), ts, buckets)
    }

    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
    }
}

impl<T: MemPool> CuckooHistoryHashTable<T> for CuckooHashTable<T> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, 1)
    }
    fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_nums: usize) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, bucket_nums)
    }
    fn insert(
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
            match self.history_insert_inner(key, pkey, start_ts, end_ts, val) {
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
       search both hasher_idx
       if find -> return value
       return Err(keynotfound)


       remember to keep atomicity!
    */
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.history_get_inner(key, pkey, ts) {
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

    fn get_all(
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

    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_iterator(TransactionId::new(), ts, buckets)
    }

    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
    }

    fn get_all_bucket_page_ids(&self) -> Vec<PageId> {
        let buckets = self.rwlock.read();
        let ret = buckets
            .buckets
            .iter()
            .map(|x| x.page_id())
            .collect::<Vec<_>>();
        ret
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError> {
        todo!()
    }

    fn insert_deleted(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<bool, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        let mut rehash_flag = false;
        loop {
            match self.history_insert_deleted_inner(key, pkey, start_ts, end_ts) {
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
}
