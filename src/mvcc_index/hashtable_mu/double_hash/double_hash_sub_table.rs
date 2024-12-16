use core::str;
use std::{
    collections::HashSet,
    iter::Enumerate,
    sync::{atomic::AtomicU32, Arc, Mutex, RwLock},
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
        hashtable_mu::hash_join_table_common::CuckooAccessMethodError,
        MvccEntry, Timestamp,
    },
    page::{Page, PageId},
};

use super::{
    double_hash_common::{ BucketEntry, Buckets},
    double_hash_data_page::DoubleHashPage,
};

/// responsible for update meta page of HashJoinTable<T>
pub struct DoubleHashSubTable<T: MemPool> {
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    meta: Arc<(PageId, AtomicU32)>,

    /// shared: read & update & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    buckets_rwlock: RwLock<Buckets>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once
}

// ----------------- SCANNER START ------------------------------

pub struct DoubleHashSubTableScanner<T: MemPool> {
    table: Arc<DoubleHashSubTable<T>>,
    ts: Timestamp,
    current_bucket_index: usize,
    initial_bucket_num: u32,

    scan_all_flag: bool,

    all_entries: Vec<MvccEntry>,
    current_entry_index: usize,
}

impl<T: MemPool> DoubleHashSubTableScanner<T> {
    pub fn new(table: &Arc<DoubleHashSubTable<T>>, ts: Timestamp, scan_all_flag: bool) -> Self {
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

impl<T: MemPool> Iterator for DoubleHashSubTableScanner<T> {
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

                let buckets = self.table.buckets_rwlock.read().unwrap();
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

                    let slot_cnt = <Page as DoubleHashPage>::slot_count(&*cuckoo_page);
                    for slot_id in 0..slot_cnt {
                        let slot =
                            <Page as DoubleHashPage>::get_slot(&*cuckoo_page, slot_id)
                                .unwrap();
                        if slot.is_mark_deleted() {
                            continue;
                        }
                        let (k, pk, v, sts, ets) =
                            <Page as DoubleHashPage>::get_key_pkey_val_ts_with_slot_id(
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


// ----------------- SCANNER END ------------------------------

pub trait RecentSubHashTable<T: MemPool> {
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

pub trait HistorySubHashTable<T: MemPool> {
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
   
    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError>;
    /// NOT used, only if deleted tuple is inserted again will this function be used.
    #[warn(unused)]
    fn insert_deleted(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), CuckooAccessMethodError>;
}

/// Recent & History basic functions
impl<T: MemPool> DoubleHashSubTable<T> {
    /// assume buckets is not locked!
    ///
    pub fn bucket_num(&self) -> u32 {
        self.buckets_rwlock.read().unwrap().num_buckets
    }

    pub fn new_with_bucket_num(
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

            DoubleHashPage::init(&mut *page);
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
            buckets_rwlock: RwLock::new(buckets),
            rehash_mutex: Mutex::new(()),
            meta: meta.clone(),
        }
    }

    // helper function
    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.try_write_page(page_key);
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
            let page = self.try_read_page(page_key);
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
    fn fused_try_write_page(&self, page_f_key: PageFrameKey) -> Option<FrameWriteGuard> {
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
    fn fused_try_read_page(&self, page_f_key: PageFrameKey) -> Option<FrameReadGuard> {
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
       else -> rehash(Err(HashPageOutOfSpace))

       1. get buckets' read lock
       2. access buckets, get 2 buckets from 2 hash functions;
       3. get write lock of pages
           if failed -> Err(AckLockFailed)
       4. try find delete mark of key, if find -> add to history and delete
       5. randomly choose one page to insert and check space
           if failed -> Err(HashPageOutOfSpace(new_hash_size))
       5. do insert or compact-and-insert
    */
    fn recent_insert_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<Option<Timestamp>, CuckooAccessMethodError> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.get_bucket_num();

        let bucket_idx = buckets.get_bucket_index(pkey);

        let pid = buckets.get_bucket_entry(bucket_idx).page_id();
        let fid = buckets.get_bucket_entry(bucket_idx).frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = self.fused_try_write_page(page_f_key);
        let mut inserted_page = match acq_result {
            None => {
                return Err(CuckooAccessMethodError::AcquireLockFailed);
            }
            Some(p) => {
                p
            }
        };

        let check_insert_result = {
            let insert_space_need = <Page as DoubleHashPage>::space_need(key, pkey, val);
            let page_free_space =
                <Page as DoubleHashPage>::free_space_with_compaction(&*inserted_page);
            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as DoubleHashPage>::insert(
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
            return Err(CuckooAccessMethodError::HashPageOutOfSpace(bucket_num * 2));
        }
    }

    /*
        if have free space -> insert
        if free space after compaction -> compaction

        else -> rehash: Err(HashPageOutOfSpace)


        1. get buckets' read lock
        2. access buckets, randomly get one bucket from 2 hash functions
        3. get write lock of page
            if failed -> Err(AcquireLockFailed)
        4. check space
            if failed -> Err(HashPageOutOfSpace(new_rehash_size: u32))
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
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.get_bucket_num();
        // log_warn!("[history::insert_inner] insert key: {:?}", key);

        let bucket_idx: usize = buckets.get_bucket_index(pkey);
        let inserted_pid = buckets.get_bucket_entry(bucket_idx).page_id();
        let inserted_fid = buckets.get_bucket_entry(bucket_idx).frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, inserted_pid, inserted_fid);
        // try to acquire lock
        let mut inserted_page = match self.fused_try_write_page(page_f_key) {
            None => {
                return Err(CuckooAccessMethodError::AcquireLockFailed); // REDO
            }
            Some(x) => x,
        };

        let check_insert_result = {
            let insert_space_need = <Page as DoubleHashPage>::space_need(key, pkey, val);
            let page_free_space =
                <Page as DoubleHashPage>::free_space_with_compaction(&*inserted_page);
            // log_warn!(
            //     "[history::insert_inner] page free space: {:?}, insert_size: {:?}",
            //     page_free_space,
            //     insert_space_need
            // );

            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as DoubleHashPage>::insert(
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
            return Err(CuckooAccessMethodError::HashPageOutOfSpace(bucket_num * 2));
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
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.get_bucket_num();
        // log_warn!("[history::insert_inner] insert key: {:?}", key);

        let bucket_idx: usize = buckets.get_bucket_index(pkey);
        let inserted_pid = buckets.get_bucket_entry(bucket_idx).page_id();
        let inserted_fid = buckets.get_bucket_entry(bucket_idx).frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, inserted_pid, inserted_fid);
        // try to acquire lock
        let mut inserted_page = match self.fused_try_write_page(page_f_key) {
            None => {
                return Err(CuckooAccessMethodError::AcquireLockFailed); // REDO
            }
            Some(x) => x,
        };

        let check_insert_result = {
            let insert_space_need =
                <Page as DoubleHashPage>::space_need(key, pkey, &vec![]);
            let page_free_space =
                <Page as DoubleHashPage>::free_space_with_compaction(&*inserted_page);
            // log_warn!(
            //     "[history::insert_inner] page free space: {:?}, insert_size: {:?}",
            //     page_free_space,
            //     insert_space_need
            // );

            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result = <Page as DoubleHashPage>::insert_deleted(
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
            return Err(CuckooAccessMethodError::HashPageOutOfSpace(bucket_num * 2));
            // rehash
        }
    }

    fn rehash_recent(&self, hash_size: u32) -> bool {
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.buckets_rwlock.read().unwrap();
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
        let mut buckets = self.buckets_rwlock.write().unwrap();
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
            DoubleHashPage::init(&mut *new_page);

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
                    buckets.get_a_second_bucket_index(&pkey, hashed_bucket_idx as usize)
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

    fn rehash_history(&self, hash_size: u32) -> bool {
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.buckets_rwlock.read().unwrap();
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
        let mut buckets = self.buckets_rwlock.write().unwrap();
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
            DoubleHashPage::init(&mut *new_page);

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
                    buckets.get_a_second_bucket_index(&pkey, hashed_bucket_idx as usize)
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
        // <Page as MvccHashJoinCuckooMetaPage>::rehash_update_history(&mut *meta_page, &new_page_ids);
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
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = buckets.get_bucket_index(pkey);
        let read_page = {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.fused_try_read_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
                    page
                }
            }
        };

   
        let get_result =
            <Page as DoubleHashPage>::recent_get(&*read_page, key, pkey, ts);
        match get_result {
            Ok(val) => {
                return Ok(val);
            }
            Err(CuckooAccessMethodError::KeyNotFound) => {
                return Err(CuckooAccessMethodError::KeyNotFound);
            }
            Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
            }
            Err(e) => {
                panic!("Should not happen! error: {:?}", e);
            }
        }
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
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx: usize = buckets.get_bucket_index(pkey);
        let read_page = {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);

            let acq_result = self.fused_try_read_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed); // REDO
                }
                Some(x) => x,
            }
        };

       
        let get_result =
            <Page as DoubleHashPage>::get(&*read_page, key, pkey, ts, false);
        match get_result {
            Ok(val) => {
                return Ok(val);
            }
            Err(CuckooAccessMethodError::KeyNotFound) => {
                return Err(CuckooAccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!("Should not happen! error: {:?}", e);
            }
        }
    }

    pub fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_num = buckets.buckets.len();
        let mut ret = vec![];

        for bucket_idx in 0..bucket_num {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let read_page = self.read_page(page_f_key);
            let get_result = <Page as DoubleHashPage>::get_all(&*read_page, key, ts);
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
            if space not enough -> Err(HashPageOutOfSpace(new_bucket_num:u32)): RE-HASH
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
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.get_bucket_num();

        let bucket_idx = buckets.get_bucket_index(pkey);

        let mut write_page = {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.fused_try_write_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
                    page
                }
            }
        };

        // Attempt to retrieve the value from the current page
        match DoubleHashPage::get_slot_id(&*write_page, key, pkey, ts) {
            Ok(slot_id) => {
                // Value found
                match DoubleHashPage::check_and_update_at_slot_id(
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
                        return Err(CuckooAccessMethodError::HashPageOutOfSpace(bucket_num * 2));
                    }
                    Err(x) => {
                        panic!("should not happen for that error! {:?}", x);
                    }
                }
            }
            Err(CuckooAccessMethodError::KeyNotFound) => {
                return Err(CuckooAccessMethodError::KeyNotFound);
            }
            Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
            }
            Err(_) => {
                panic!("should not happen!");
            }
        };
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
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = buckets.get_bucket_index(pkey);

        let mut write_page = {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid: u32 = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = self.fused_try_write_page(page_f_key);
            match acq_result {
                None => {
                    return Err(CuckooAccessMethodError::AcquireLockFailed);
                }
                Some(page) => {
                    page
                }
            }
        };
       
        // Attempt to retrieve the value from the current page
        match <Page as DoubleHashPage>::get_slot_id(& *write_page, key, pkey, ts) {
            Ok(slot_id) => {
                match <Page as DoubleHashPage>::mark_delete_slot_at_id(
                    &mut *write_page,
                    slot_id,
                    ts,
                ) {
                    Ok(old_res) => {
                        return Ok(old_res);
                    }
                    Err(x) => {
                        panic!("should not happen for that error! {:?}", x);
                    }
                }
            }
            Err(CuckooAccessMethodError::KeyNotFound) => {
                return Err(CuckooAccessMethodError::KeyNotFound);
            }
            Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
            }
            Err(_) => {
                panic!("should not happen!");
            }
        };
    }

    pub fn dump_all_entry(&self) -> usize {
        let buckets = self.buckets_rwlock.read().unwrap();
        let mut entr_id = 0;
        let mut num = 0_usize;
        log_warn!("<<DUMP HASH TABLE>> ------------ START AN SUBTABLE! -------");
        for entry in &buckets.buckets {
            let pfkey = PageFrameKey::new(self.c_key, entry.page_id());
            let page = self.read_page(pfkey);
            log_warn!("<<DUMP HASH TABLE>> ENTRY: {entr_id}, SLOT COUNT: {}, rec_start_offset: {:?}, slot_end: {:?}, free_without_compaction: {:?}, free_with_compaction: {:?}",page.slot_count(), page.rec_start_offset(), page.slot_offset(page.slot_count()),  page.free_space_without_compaction(), page.free_space_with_compaction());
            entr_id += 1;
            num += page.slot_count() as usize;
            // for i in 0..page.slot_count() {
            //     let slot = page.get_slot(i).unwrap();
            //     let mvcc_entry = page.get_key_pkey_val_ts_with_slot(&slot);
            //     log_warn!(
            //         "<<DUMP HASH TABLE>> [slot in ID: {:?}]: key: {:?}",
            //         i,
            //         String::from_utf8(mvcc_entry.0)
            //     );
            // }
        }
        return num;
    }
    fn scan_all(self: &Arc<Self>) -> impl Iterator<Item = MvccEntry>{
        let scanner = DoubleHashSubTableScanner::new(self, Timestamp::MAX, true);
        scanner
    }
}

impl<T: MemPool> RecentSubHashTable<T> for DoubleHashSubTable<T> {
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
                Err(CuckooAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
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
                Err(CuckooAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
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
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }




    // fn scan_key(&self, ts: Timestamp, key: &[u8]) -> impl Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> {
    //     let buckets = self.rwlock.read_arc();
    //     // self.gen_scan_key_iterator(TransactionId::new(), ts, buckets, Some(key.to_vec()))
    //     self.gen_scan_key_iterator(ts, buckets, Some(key.to_vec()))
    // }
}

impl<T: MemPool> HistorySubHashTable<T> for DoubleHashSubTable<T> {
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
                Err(CuckooAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
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


    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError> {
        let buckets = self.buckets_rwlock.read().unwrap();

        for bucket_idx in 0..buckets.buckets.len() {
            let pid = buckets.get_bucket_entry(bucket_idx).page_id();
            let fid: u32 = buckets.get_bucket_entry(bucket_idx).frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = self.write_page(page_f_key);

            write_page.garbage_collect(safe_ts);
        }

        Ok(())
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
                Err(CuckooAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
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
    let table = DoubleHashSubTable::new_with_bucket_num(
        c_key, mem_pool1, 16, &meta
    );

    let key = b"key111".to_vec();
    let pkey = b"pkey1".to_vec();
    let value = b"233".to_vec();

    <DoubleHashSubTable<_> as RecentSubHashTable<_>>::insert(&table, &key, &pkey, 0, &value)
        .unwrap();
    let old_res =
        <DoubleHashSubTable<_> as RecentSubHashTable<_>>::delete(&table, &key, &pkey, 1).unwrap();
    assert_eq!(old_res.0, 0);
    assert_eq!(old_res.1, b"233".to_vec());

    // table.dump_all_entry();
}
