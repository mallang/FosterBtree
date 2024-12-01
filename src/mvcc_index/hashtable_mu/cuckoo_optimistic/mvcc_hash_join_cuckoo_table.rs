use core::str;
use std::{
    collections::HashSet,
    iter::Enumerate,
    sync::{atomic::AtomicU32, Arc, Mutex},
    time::Duration,
};

use crate::{
    bp::{
        get_in_mem_pool, ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus,
        PageFrameKey,
    },
    // lockmanager::{LockManager, Permissions, TransactionId, ValueId},
    log_debug,
    log_warn,
    mvcc_index::{
        hashtable_mu::mvcc_hash_join_cuckoo::{MvccHashJoinCuckooMetaPage, MvccHashJoinTable},
        MvccEntry, Timestamp,
    },
    page::{Page, PageId},
};

/* --------------------------- Scanner START ---------------------------------- */

use super::{
    mvcc_hash_join_cuckoo_common::{arcrwlock::*, BucketEntry, Buckets, CuckooAccessMethodError},
    mvcc_hash_join_page::MvccHashJoinCuckooPage,
};

pub struct CuckooHashJoinTableScanner<T: MemPool> {
    table: Arc<CuckooHashTable<T>>,
    ts: Timestamp,
    current_bucket_index: usize,
    initial_bucket_num: u32,

    scan_all_flag: bool,

    all_entries: Vec<MvccEntry>,
    current_entry_index: usize,
}

impl<T: MemPool> CuckooHashJoinTableScanner<T> {
    pub fn new(table: &Arc<CuckooHashTable<T>>, ts: Timestamp, scan_all_flag: bool) -> Self {
        Self {
            table: table.clone(),
            ts,
            current_bucket_index: 0,
            initial_bucket_num: table.bucket_num(),
            scan_all_flag,
            all_entries: vec![],
            current_entry_index: 0,
        }
    }

    fn integer_is_a_power_of_2(val: u32) -> bool {
        return (val & (val - 1)) == 0;
    }
}

impl<T: MemPool> Iterator for CuckooHashJoinTableScanner<T> {
    type Item = MvccEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_entry_index < self.all_entries.len() {
                let entry = self.all_entries[self.current_entry_index].clone();
                self.current_entry_index += 1;
                return Some(entry);
            } else {
                if self.current_bucket_index as u32 >= self.initial_bucket_num {
                    return None;
                }

                self.all_entries.clear();
                self.current_entry_index = 0;

                let buckets = self.table.rwlock.read();
                let current_bucket_num = buckets.get_bucket_num();
                assert!(
                    current_bucket_num >= self.initial_bucket_num
                        && current_bucket_num % self.initial_bucket_num == 0
                        && Self::integer_is_a_power_of_2(
                            current_bucket_num / self.initial_bucket_num
                        )
                );
                let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                for i in 0..check_bucket_num {
                    let bucket_entry_idx =
                        self.current_bucket_index + (i * self.initial_bucket_num) as usize;
                    let bucket_entry = &buckets.buckets[bucket_entry_idx];
                    let pfkey = PageFrameKey::new_with_frame_id(
                        self.table.c_key,
                        bucket_entry.page_id(),
                        bucket_entry.frame_id(),
                    );
                    let cuckoo_page = self.table.read_page(pfkey);

                    let slot_cnt = <Page as MvccHashJoinCuckooPage>::slot_count(&*cuckoo_page);
                    for slot_id in 0..slot_cnt {
                        let slot =
                            <Page as MvccHashJoinCuckooPage>::get_slot(&*cuckoo_page, slot_id)
                                .unwrap();
                        if slot.is_mark_deleted() {
                            continue;
                        }
                        let (k, pk, v, sts, ets) =
                            <Page as MvccHashJoinCuckooPage>::get_key_pkey_val_ts_with_slot_id(
                                &*cuckoo_page,
                                slot_id,
                            );
                        if self.scan_all_flag
                            || sts <= self.ts
                                && (self.ts < ets
                                    || (ets == Timestamp::MAX && self.ts == ets/* recent scan when input ts is MAX  => scan all but deleted*/))
                        {
                            let entry = MvccEntry {
                                key: k,
                                pkey: pk,
                                value: v,
                                start_ts: sts,
                                end_ts: ets,
                            };
                            self.all_entries.push(entry);
                        }
                    }
                }

                self.current_bucket_index += 1;
            }
        }
    }
}

pub struct ScanTsWithBucketsReadGuard<T: MemPool> {
    // lock_manager: Arc<Mutex<LockManager>>,
    // tid: TransactionId,
    current_entry: u32,

    mem_pool: Arc<T>,

    current_slot_id: u32,

    ts: Option<Timestamp>,
    c_key: ContainerKey,

    buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    scan_key: Option<Vec<u8>>,
}

impl<T: MemPool> ScanTsWithBucketsReadGuard<T> {
    /// assume has get lock for that tid+vid
    pub fn new(
        // lm: &Arc<Mutex<LockManager>>,
        // tid: TransactionId,
        mem_pool: &Arc<T>,
        ts: Option<Timestamp>,
        c_key: ContainerKey,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
        scan_key: Option<Vec<u8>>,
    ) -> Self {
        Self {
            // lock_manager: lm.clone(),
            // tid,
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

    // fn valueid(&self) -> ValueId {
    //     let page_id = self
    //         .buckets_read_guard
    //         .get_bucket_entry(self.current_entry as usize)
    //         .page_id();
    //     ValueId {
    //         container_id: 0,
    //         segment_id: None,
    //         page_id: Some(page_id),
    //         slot_id: None,
    //     }
    // }

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

            // if self.current_slot_id == 0 {
            //     // new page to scan, get lock from lock_manager
            //     '_acquire_page_lock: loop {
            //         let ret = self.lock_manager.lock().unwrap().acquire_lock(
            //             self.tid,
            //             self.valueid(),
            //             Permissions::ReadOnly,
            //         );
            //         if !ret {
            //             // log_debug!("acquire write lock of page failed, re-do");
            //             attempts += 1;
            //             std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
            //             continue;
            //         } else {
            //             break;
            //         }
            //     }
            // }

            let read_page = self.read_page();
            let mut current_slot_id = self.current_slot_id;
            let mut ret = Option::<Self::Item>::None;
            '_scan_a_page: loop {
                match <Page as MvccHashJoinCuckooPage>::get_slot(&*read_page, current_slot_id) {
                    Some(slot) => {
                        let (slot_key, slot_pkey, slot_val, slot_start_ts, slot_end_ts) =
                            <Page as MvccHashJoinCuckooPage>::get_key_pkey_val_ts_with_slot(
                                &*read_page,
                                &slot,
                            );
                        // log_warn!("get slot_id: {:?}, slot_key: {:?}", current_slot_id, slot_key);
                        let ts_match_result = {
                            if let Some(ts) = self.ts {
                                slot_start_ts <= ts && ts < slot_end_ts && !slot.is_mark_deleted()
                            } else {
                                // scan all entry
                                !slot.is_mark_deleted()
                            }
                        };
                        if ts_match_result {
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
                // // release current page locks
                // self.lock_manager
                //     .lock()
                //     .unwrap()
                //     .release_lock(self.tid, self.valueid())
                //     .unwrap();
                // // current page scan ends
                // // log_warn!("read a page end!!");
                // self.current_slot_id = 0;
                // self.current_entry += 1;
                // continue;
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

    meta: Arc<(PageId, AtomicU32)>,

    /// shared: read & update & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    rwlock: ArcRwlock<Buckets>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once

                             // lock_manager: Arc<Mutex<LockManager>>, // function level serializability : get/insert/update/...
}

pub trait CuckooRecentHashTable<T: MemPool> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>, meta: &Arc<(PageId, AtomicU32)>) -> Self;
    fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        meta: &Arc<(PageId, AtomicU32)>,
        bucket_nums: usize,
    ) -> Self;
    fn get_all_bucket_page_ids(&self) -> Vec<PageId>;
    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_all(&self) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T>;
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<Option<Timestamp>, CuckooAccessMethodError>;
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
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError>;
    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError>;
}

pub trait CuckooHistoryHashTable<T: MemPool> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>, meta: &Arc<(PageId, AtomicU32)>) -> Self;
    fn get_all_bucket_page_ids(&self) -> Vec<PageId>;
    fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        meta: &Arc<(PageId, AtomicU32)>,
        bucket_nums: usize,
    ) -> Self;
    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_all(&self) -> ScanTsWithBucketsReadGuard<T>;
    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T>;
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError>;
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
    ) -> Result<(), CuckooAccessMethodError>;
}

/// Recent & History basic functions
impl<T: MemPool> CuckooHashTable<T> {
    /// assume buckets is not locked!
    ///
    pub fn bucket_num(&self) -> u32 {
        self.rwlock.read().num_buckets
    }

    fn new_with_bucket_num_inner(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
        meta: &Arc<(PageId, AtomicU32)>,
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
            // lock_manager: Arc::new(Mutex::new(LockManager::new())),
            meta: meta.clone(),
        }
    }
    // /// used to provide an interface for lock_manager::valueid \
    // /// only use page_id,
    // fn gen_valueid(page_id: u32) -> ValueId {
    //     ValueId {
    //         container_id: 0,
    //         segment_id: None,
    //         page_id: Some(page_id),
    //         slot_id: None,
    //     }
    // }

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
    fn try_write_page(&self, page_key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        let page = self.mem_pool.get_page_for_write(page_key);
        match page {
            Ok(page) => {
                return Ok(page);
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                log_warn!("All frames are latched and cannot evict page to write the page: {:?}. Will retry", page_key);
                std::thread::sleep(Duration::from_millis(1));
                return Err(MemPoolStatus::CannotEvictPage);
            }
            Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                return Err(MemPoolStatus::FrameWriteLatchGrantFailed);
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
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
    fn try_read_page(&self, page_key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        let page = self.mem_pool.get_page_for_read(page_key);
        match page {
            Ok(page) => {
                return Ok(page);
            }
            Err(MemPoolStatus::CannotEvictPage) => {
                log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                std::thread::sleep(Duration::from_millis(1));
                return Err(MemPoolStatus::CannotEvictPage);
            }
            Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                return Err(MemPoolStatus::FrameReadLatchGrantFailed);
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
    // helper function
    /// assert have gotten buckets lock
    fn try_acq_write_page(&self, page_f_key: PageFrameKey) -> Option<FrameWriteGuard> {
        // let mut write_guard = self.lock_manager.lock().unwrap();
        // let vid = Self::gen_valueid(page_f_key.p_key().page_id);
        // let acq_result = write_guard.acquire_lock(tid, vid, Permissions::ReadWrite);
        let write_res = self.try_write_page(page_f_key);
        match write_res {
            Ok(page) => return Some(page),
            Err(MemPoolStatus::CannotEvictPage | MemPoolStatus::FrameWriteLatchGrantFailed) => {
                return None;
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }
    // helper function
    /// assert have gotten buckets lock
    fn try_acq_read_page(&self, page_f_key: PageFrameKey) -> Option<FrameReadGuard> {
        let read_res = self.try_read_page(page_f_key);
        match read_res {
            Ok(page) => return Some(page),
            Err(MemPoolStatus::FrameReadLatchGrantFailed | MemPoolStatus::CannotEvictPage) => {
                return None;
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
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

        let bucket_idx = buckets.get_bucket_index_random(key);

        let pid = buckets.get_bucket_entry(bucket_idx).page_id();
        let fid = buckets.get_bucket_entry(bucket_idx).frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = self.try_acq_write_page(page_f_key);
        let mut inserted_page = match acq_result {
            None => {
                return Err(CuckooAccessMethodError::AcquireLockFailed);
            }
            Some(p) => {
                // lock_manager_guards.push(guard);
                // pages.push(page);
                p
            }
        };

        // match MvccHashJoinCuckooPage::get_delete_mark_slot_id(& *inserted_page, key, pkey, ts) {
        //     Ok(slot_id) => {
        //         // Value found
        //         match MvccHashJoinCuckooPage::delete_slot_at_id(&mut *inserted_page, slot_id) {
        //             Ok((old_ts, _)) => {
        //                 old_delete_marker = Some(old_ts);
        //                 break;
        //             }
        //             Err(x) => {
        //                 panic!("should not happen for that error! {:?}", x);
        //             }
        //         }
        //     }
        //     Err(CuckooAccessMethodError::KeyNotFound) => {
        //         continue;
        //     }
        //     Err(_) => {
        //         panic!("should not happen!");
        //     }
        // };

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
                    return Ok(None);
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
        let mut inserted_page = match self.try_acq_write_page(page_f_key) {
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
        let mut inserted_page = match self.try_acq_write_page(page_f_key) {
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

    fn rehash_recent(&self, hash_size: u32) -> bool {
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
            let hashed_page_old_slot_count = hashed_page.slot_count();
            // log_warn!("original slot count: {:?}", hashed_page_old_slot_count);
            new_page.set_slot_count(hashed_page_old_slot_count);

            if hashed_page_old_slot_count == 0 {
                continue;
            }

            let mut new_inserted_slot_id_decrease: i32 = hashed_page_old_slot_count as i32 - 1;
            let mut invalid_idxes = HashSet::new();
            for slot_idx in (0..hashed_page_old_slot_count).rev() {
                let (key, pkey, val, start_ts, end_ts) =
                    hashed_page.get_key_pkey_val_ts_with_slot_id(slot_idx);
                if let Some(idx) =
                    buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize)
                {
                    assert_eq!(idx as u32, (hashed_bucket_idx + old_entry_num));
                    match new_page.insert_at_slot_id(
                        &key,
                        &pkey,
                        start_ts,
                        end_ts,
                        &val,
                        new_inserted_slot_id_decrease as u32,
                    ) {
                        Ok(_) => {
                            new_inserted_slot_id_decrease -= 1;
                        }
                        Err(e) => {
                            panic!("should not happen in re-hash! e: {:?}", e);
                        }
                    }
                    // hashed_page.delete_slot_at_id(slot_idx).unwrap();
                    hashed_page.decrease_bytes_for_rehash(slot_idx);
                    invalid_idxes.insert(slot_idx);
                }
            }
            new_page.rehash_truncate_for_new_page(
                (new_inserted_slot_id_decrease + 1) as u32,
                hashed_page_old_slot_count,
            );
            let hashed_page_new_slot_count =
                hashed_page_old_slot_count - invalid_idxes.len() as u32;
            let mut i: u32 = 0;
            for j in 0..hashed_page_old_slot_count {
                if invalid_idxes.contains(&j) {
                    continue;
                }
                hashed_page.swap_slot(i, j);
                i += 1;
            }
            assert_eq!(i, hashed_page_new_slot_count);
            hashed_page.set_slot_count(hashed_page_new_slot_count);
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

        buckets.num_buckets = old_entry_num * 2;
        let meta_page_key = PageFrameKey::new_with_frame_id(
            self.c_key,
            self.meta.0,
            self.meta.1.load(std::sync::atomic::Ordering::Acquire),
        );
        let mut meta_page = self.write_page(meta_page_key);
        let new_page_ids = buckets
            .buckets
            .iter()
            .map(|x| x.page_id())
            .collect::<Vec<_>>();
        <Page as MvccHashJoinCuckooMetaPage>::rehash_update_recent(&mut *meta_page, &new_page_ids);
        return true;
    }

    fn rehash_history(&self, hash_size: u32) -> bool {
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
            let hashed_page_old_slot_count = hashed_page.slot_count();
            // log_warn!("original slot count: {:?}", hashed_page_old_slot_count);
            new_page.set_slot_count(hashed_page_old_slot_count);

            if hashed_page_old_slot_count == 0 {
                continue;
            }

            let mut new_inserted_slot_id_decrease: i32 = hashed_page_old_slot_count as i32 - 1;
            let mut invalid_idxes = HashSet::new();
            for slot_idx in (0..hashed_page_old_slot_count).rev() {
                let (key, pkey, val, start_ts, end_ts) =
                    hashed_page.get_key_pkey_val_ts_with_slot_id(slot_idx);
                if let Some(idx) =
                    buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize)
                {
                    assert_eq!(idx as u32, (hashed_bucket_idx + old_entry_num));
                    match new_page.insert_at_slot_id(
                        &key,
                        &pkey,
                        start_ts,
                        end_ts,
                        &val,
                        new_inserted_slot_id_decrease as u32,
                    ) {
                        Ok(_) => {
                            new_inserted_slot_id_decrease -= 1;
                        }
                        Err(e) => {
                            panic!("should not happen in re-hash! e: {:?}", e);
                        }
                    }
                    // hashed_page.delete_slot_at_id(slot_idx).unwrap();
                    hashed_page.decrease_bytes_for_rehash(slot_idx);
                    invalid_idxes.insert(slot_idx);
                }
            }
            new_page.rehash_truncate_for_new_page(
                (new_inserted_slot_id_decrease + 1) as u32,
                hashed_page_old_slot_count,
            );
            let hashed_page_new_slot_count =
                hashed_page_old_slot_count - invalid_idxes.len() as u32;
            let mut i: u32 = 0;
            for j in 0..hashed_page_old_slot_count {
                if invalid_idxes.contains(&j) {
                    continue;
                }
                hashed_page.swap_slot(i, j);
                i += 1;
            }
            assert_eq!(i, hashed_page_new_slot_count);
            hashed_page.set_slot_count(hashed_page_new_slot_count);
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
        let meta_page_key = PageFrameKey::new_with_frame_id(
            self.c_key,
            self.meta.0,
            self.meta.1.load(std::sync::atomic::Ordering::Acquire),
        );
        let mut meta_page = self.write_page(meta_page_key);
        let new_page_ids = buckets
            .buckets
            .iter()
            .map(|x| x.page_id())
            .collect::<Vec<_>>();
        <Page as MvccHashJoinCuckooMetaPage>::rehash_update_history(&mut *meta_page, &new_page_ids);
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
        let mut pages = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_read_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
                    pages.push(page);
                }
            }
        }

        for read_page in pages {
            let get_result =
                <Page as MvccHashJoinCuckooPage>::recent_get(&*read_page, key, pkey, ts);
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
        let mut pages = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_read_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
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
        let mut pages = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_read_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
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
        let mut pages = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_write_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
                    pages.push(page);
                }
            }
        }

        for mut write_page in pages {
            // Attempt to retrieve the value from the current page
            match MvccHashJoinCuckooPage::get_slot_id(&*write_page, key, pkey, ts) {
                Ok(slot_id) => {
                    // Value found
                    match MvccHashJoinCuckooPage::check_and_update_at_slot_id(
                        &mut *write_page,
                        slot_id,
                        key,
                        pkey,
                        val,
                        ts,
                    ) {
                        Ok(old_res) => {
                            return Ok(old_res);
                        }
                        Err(CuckooAccessMethodError::OutOfSpace) => {
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
        let mut page_vec = vec![];
        for bucket_idx in indexes {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.try_acq_write_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(p) => {
                    page_vec.push(p);
                }
            }
        }
        let mut found_flag = None;
        for (idx, page) in (page_vec).iter().enumerate() {
            let write_page = page;
            // Attempt to retrieve the value from the current page
            match <Page as MvccHashJoinCuckooPage>::get_slot_id(&**write_page, key, pkey, ts) {
                Ok(slot_id) => {
                    // Value found
                    found_flag = Some((idx, slot_id));
                    // log_warn!("idx: {:?}, found!!!", idx);
                    continue;
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    let delete_marker_space_need =
                        <Page as MvccHashJoinCuckooPage>::space_need(key, pkey, &vec![]);
                    let free_without_compaction =
                        <Page as MvccHashJoinCuckooPage>::free_space_without_compaction(
                            &**write_page,
                        );
                    let free_with_compaction: u32 =
                        <Page as MvccHashJoinCuckooPage>::free_space_with_compaction(&**write_page);
                    if free_without_compaction < delete_marker_space_need {
                        if free_with_compaction < delete_marker_space_need {
                            return Err(CuckooAccessMethodError::CuckooOutOfSpace(
                                buckets.num_buckets * 2,
                            ));
                        }
                    }
                    // log_warn!("idx: {:?}, NOT found!!!", idx);
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
        // log_warn!("found flag is {:?}", found_flag);

        if let Some((vec_idx, slot_id)) = found_flag {
            let mut get_old_res = (0, vec![]);
            for (idx, guard_with_page) in page_vec.into_iter().enumerate() {
                let mut write_page = guard_with_page;
                if idx == vec_idx {
                    match <Page as MvccHashJoinCuckooPage>::mark_delete_slot_at_id(
                        &mut *write_page,
                        slot_id,
                        ts,
                    ) {
                        Ok(old_res) => {
                            get_old_res = old_res;
                        }
                        Err(x) => {
                            panic!("should not happen for that error! {:?}", x);
                        }
                    }
                }
                // else {
                //     match <Page as MvccHashJoinCuckooPage>::insert_deleted(
                //         &mut *write_page,
                //         key,
                //         pkey,
                //         ts,
                //         Timestamp::MAX,
                //     ) {
                //         Ok(_) => {}
                //         Err(e) => {
                //             panic!("should not have error! error: {:?}", e);
                //         }
                //     }
                // }
            }
            return Ok(get_old_res);
        } else {
            return Err(CuckooAccessMethodError::KeyNotFound);
        }
    }

    fn gen_scan_iterator(
        &self,
        // tid: TransactionId,
        ts: Option<Timestamp>,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    ) -> ScanTsWithBucketsReadGuard<T> {
        let scan_guard = ScanTsWithBucketsReadGuard::new(
            // &self.lock_manager,
            // tid,
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
        // tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
        scan_key: Option<Vec<u8>>,
    ) -> ScanTsWithBucketsReadGuard<T> {
        let scan_guard = ScanTsWithBucketsReadGuard::new(
            // &self.lock_manager,
            // tid,
            &self.mem_pool,
            Some(ts),
            self.c_key,
            buckets_read_guard,
            scan_key,
        );
        scan_guard
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<CuckooHashJoinTableScanner<T>, CuckooAccessMethodError> {
        todo!()
    }

    fn dump_all_entry(&self) {
        let buckets = self.rwlock.read();
        let mut entr_id = 0;
        for entry in &buckets.buckets {
            let pfkey = PageFrameKey::new(self.c_key, entry.page_id());
            let page = self.read_page(pfkey);
            log_warn!("<<DUMP HASH TABLE>> ENTRY: {entr_id}, rec_start_offset: {:?}, slot_end: {:?}, free_without_compaction: {:?}, free_with_compaction: {:?}", page.rec_start_offset(), page.slot_offset(page.slot_count()),  page.free_space_without_compaction(), page.free_space_with_compaction());
            entr_id += 1;
            for i in 0..page.slot_count() {
                let slot = page.get_slot(i).unwrap();
                let mvcc_entry = page.get_key_pkey_val_ts_with_slot(&slot);
                log_warn!(
                    "<<DUMP HASH TABLE>> [slot in ID: {:?}]: key: {:?}",
                    i,
                    String::from_utf8(mvcc_entry.0)
                );
            }
        }
    }
}

impl<T: MemPool> CuckooRecentHashTable<T> for CuckooHashTable<T> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>, meta: &Arc<(PageId, AtomicU32)>) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, 1, meta)
    }
    fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        meta: &Arc<(PageId, AtomicU32)>,
        bucket_nums: usize,
    ) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, bucket_nums, meta)
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
    ) -> Result<Option<Timestamp>, CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.recent_insert_inner(key, pkey, ts, val) {
                Ok(delete_marker) => {
                    return Ok(delete_marker);
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();

                    self.rehash_recent(new_hash_size);

                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();

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
        loop {
            match self.recent_get_inner(key, pkey, ts) {
                Ok(val) => {
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
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.recent_update_inner(key, pkey, ts, val) {
                Ok((old_ts, old_val)) => {
                    return Ok((old_ts, old_val));
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    self.rehash_recent(new_hash_size);
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
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    self.rehash_recent(new_hash_size);
                    // log_warn!("Page insert out of space, re-hash");
                    log_debug!("Page delete out of space, re-hash");
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

    fn scan(&self, ts: Timestamp) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        let ts = if ts != Timestamp::MAX { Some(ts) } else { None };
        // self.gen_scan_iterator(TransactionId::new(), ts, buckets)
        self.gen_scan_iterator(ts, buckets)
    }

    fn scan_all(&self) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        // self.gen_scan_iterator(TransactionId::new(), None, buckets)
        self.gen_scan_iterator(None, buckets)
    }

    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        // self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
        self.gen_scan_key_iterator(ts, buckets, Some(key.to_vec()))
    }
}

impl<T: MemPool> CuckooHistoryHashTable<T> for CuckooHashTable<T> {
    fn new(c_key: ContainerKey, mem_pool: Arc<T>, meta: &Arc<(PageId, AtomicU32)>) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, 1, meta)
    }
    fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        meta: &Arc<(PageId, AtomicU32)>,
        bucket_nums: usize,
    ) -> Self {
        Self::new_with_bucket_num_inner(c_key, mem_pool, bucket_nums, meta)
    }
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.history_insert_inner(key, pkey, start_ts, end_ts, val) {
                Ok(()) => {
                    // log_warn!("history table insert ok!");
                    return Ok(());
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    self.rehash_history(new_hash_size);
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
        // self.gen_scan_iterator(TransactionId::new(), Some(ts), buckets)
        self.gen_scan_iterator(Some(ts), buckets)
    }

    fn scan_all(&self) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        // self.gen_scan_iterator(TransactionId::new(), None, buckets)
        self.gen_scan_iterator(None, buckets)
    }

    fn scan_key(&self, ts: Timestamp, key: &[u8]) -> ScanTsWithBucketsReadGuard<T> {
        let buckets = self.rwlock.read_arc();
        // self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
        self.gen_scan_key_iterator(ts, buckets, Some(key.to_vec()))
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
    ) -> Result<(), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.history_insert_deleted_inner(key, pkey, start_ts, end_ts) {
                Ok(()) => {
                    // log_warn!("history table insert ok!");
                    return Ok(());
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    self.rehash_recent(new_hash_size);
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

#[test]
fn test_delete_two_markers() {
    let mem_pool = get_in_mem_pool();
    let mem_pool1 = mem_pool.clone();
    let c_key = ContainerKey::new(0, 0);
    let meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
    let meta = Arc::new((meta_page.get_id(), AtomicU32::new(0)));
    let table = <CuckooHashTable<_> as CuckooRecentHashTable<_>>::new_with_bucket_num(
        c_key, mem_pool1, &meta, 16,
    );

    let key = b"key111".to_vec();
    let pkey = b"pkey1".to_vec();
    let value = b"233".to_vec();

    <CuckooHashTable<_> as CuckooRecentHashTable<_>>::insert(&table, &key, &pkey, 0, &value)
        .unwrap();
    let old_res =
        <CuckooHashTable<_> as CuckooRecentHashTable<_>>::delete(&table, &key, &pkey, 1).unwrap();
    assert_eq!(old_res.0, 0);
    assert_eq!(old_res.1, b"233".to_vec());

    // table.dump_all_entry();
}
