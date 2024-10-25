use std::{
    sync::{Arc, Mutex, RwLock}, time::Duration, u32
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
        BucketEntry, Buckets, CuckooAccessMethodError, 
    },
    mvcc_hash_join_cuckoo_recent_page::MvccHashJoinCuckooPage,
};

mod arcrwlock {
    use lock_api::GuardSend;
    use std::{ops::Deref, sync::{self, atomic::AtomicI16}, thread, time::Duration};

    use crate::rwlatch::RwLatch;

    pub struct RawRwLock (RwLatch);
    impl Deref for RawRwLock {
        type Target = RwLatch;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    unsafe impl lock_api::RawRwLock for RawRwLock {
        type GuardMarker = GuardSend;
        const INIT: Self = RawRwLock(RwLatch{cnt: AtomicI16::new(0)});
        fn try_lock_exclusive(&self) -> bool {
            self.try_exclusive()
        }
        fn try_lock_shared(&self) -> bool {
            self.try_shared()
        }
        fn lock_exclusive(&self) {
            self.exclusive();
        }
        fn lock_shared(&self) {
            self.shared();
        }
        unsafe fn unlock_exclusive(&self) {
            self.release_exclusive();
        }
        unsafe fn unlock_shared(&self) {
            self.release_shared();
        }
    }

    pub type ArcRwlock<T> = sync::Arc<lock_api::RwLock<RawRwLock, T>>;
    pub type ArcRwlockReadGuard<T> = lock_api::ArcRwLockReadGuard<RawRwLock, T>;
    pub type ArcRwlockWriteGuard<T> = lock_api::ArcRwLockWriteGuard<RawRwLock, T>;


    pub fn new_arc_rw_lock<T>(instance: T) -> ArcRwlock<T> {
        return sync::Arc::new(lock_api::RwLock::<RawRwLock, T>::new(instance))
    }



    #[test]
    fn arc_rwlock_simple_test() {
        struct FinalStruct{
            buckets: ArcRwlock<i32>,
            buckets_read: ArcRwlockReadGuard<i32>,
        }
    
        impl FinalStruct {
            pub fn new(buckets: ArcRwlock<i32>) -> Self {
                let read = buckets.read_arc();
                return Self {
                    buckets,
                    buckets_read: read,
                }
            } 
        }

        fn is_send<T: Send>() {}
        is_send::<ArcRwlockReadGuard<i32>>();
        let a = new_arc_rw_lock(1_i32);
        let c = a.clone();
        let b = FinalStruct::new(a);
        
        let handle = thread::spawn(move || {
            let try_result = c.try_write(); 
            assert!(try_result.is_none());
        });
        handle.join().unwrap();
        drop(b);
    }
}

use arcrwlock::*;

struct ScanTsWithBucketsReadGuard<T: MemPool> {
    lock_manager: Arc<Mutex<LockManager>>,
    tid: TransactionId,
    current_entry: u32,

    mem_pool: Arc<T>,

    current_slot_id: u32,

    ts: Timestamp,
    c_key: ContainerKey,

    buckets_read_guard: ArcRwlockReadGuard<Buckets>, 
}

impl<T: MemPool> ScanTsWithBucketsReadGuard<T> {
    /// assume has get lock for that tid+vid
    pub fn new(lm: &Arc<Mutex<LockManager>>, tid: TransactionId, mem_pool: &Arc<T>, ts: Timestamp, c_key: ContainerKey, buckets_read_guard: ArcRwlockReadGuard<Buckets>) -> Self {
        Self {
            lock_manager: lm.clone(),
            tid,
            current_entry : 0,
            mem_pool: mem_pool.clone(),
            current_slot_id: 0,
            ts,
            c_key,
            buckets_read_guard,
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
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    fn valueid(&self) -> ValueId {
        let page_id = self.buckets_read_guard.get_bucket_entry(self.current_entry as usize).page_id();
        ValueId {
            container_id: 0,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    fn page_key(&self) -> PageFrameKey {
        let page_id = self.buckets_read_guard.get_bucket_entry(self.current_entry as usize).page_id();
        PageFrameKey::new(self.c_key, page_id)
    }
}


/// Ensure no re-hash by acquire read lock of buckets \
/// 
impl<T: MemPool> Iterator for ScanTsWithBucketsReadGuard<T> {
    type Item = (Vec<u8>, Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_entry >= self.buckets_read_guard.get_bucket_num() {
                // scan ends
                return None;
            }

            let base = 2;
            let mut attempts = 0;
            
            if self.current_slot_id == 0 {
                // new page to scan, get lock from lock_manager
                '_acquire_page_lock: loop {
                    let ret = self.lock_manager.lock().unwrap().acquire_lock(self.tid, self.valueid(), Permissions::ReadOnly);
                    if !ret {
                        log_debug!("acquire write lock of page failed, re-do");
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
                match <Page as MvccHashJoinCuckooPage>::slot(& *read_page,current_slot_id) {
                    Some(slot) => {
                        let (slot_key, slot_pkey, slot_val, slot_ts) = <Page as MvccHashJoinCuckooPage>::get_key_pkey_val_ts_with_slot(& *read_page, &slot);
                        current_slot_id += 1;
                        if slot_ts <= self.ts {
                            ret = Some((slot_key, slot_pkey, slot_val));
                            break;
                        }
                    },
                    None => {
                        break;
                    },
                }
            }
            drop(read_page);

            if ret.is_none() {
                // release current page locks
                self.lock_manager.lock().unwrap().release_lock(self.tid, self.valueid()).unwrap();
                // current page scan ends
                self.current_slot_id = 0;
                self.current_entry += 1;
                continue;
            } else {
                self.current_slot_id  = current_slot_id + 1;
                // find a next value for iterator
                return ret;
            }
        }
    }
}

// guard is a non-Send version of RAII of LockManager
struct LockManagerGuard{
    lock_manager: Arc<Mutex<LockManager>>,
    tid: TransactionId,
    pid: ValueId,
}

impl LockManagerGuard {
    pub fn new(lock_manager: &Arc<Mutex<LockManager>>, tid: TransactionId, pid: ValueId) -> Self {
        Self {
            lock_manager: lock_manager.clone(),
            tid,
            pid,
        }
    }
}

impl Drop for LockManagerGuard {
    fn drop(&mut self) {
        self.lock_manager.lock().unwrap().release_lock(self.tid, self.pid).unwrap();
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
    rwlock: ArcRwlock<Buckets>,  // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>,  // re-hash only once

    lock_manager: Arc<Mutex<LockManager>>,  // function level serializability : get/insert/update/...
}


impl<T: MemPool> CuckooHashTable<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let pid = page.get_id();
        let fid = page.frame_id();

        MvccHashJoinCuckooPage::init(&mut *page);
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

    fn gen_valueid(page_id: u32) -> ValueId {
        ValueId {
            container_id: 0,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    fn gen_buckets_write_id(page_id: u32) -> ValueId {
        ValueId {
            container_id: 0,
            segment_id: Some(0),
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

    fn gen_read_iterator(
        &self,
        tid: TransactionId,
        ts: Timestamp,
        buckets_read_guard: ArcRwlockReadGuard<Buckets>,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send> {
        let scan_guard = ScanTsWithBucketsReadGuard::new(&self.lock_manager, tid, &self.mem_pool, ts, self.c_key, buckets_read_guard);
        Box::new(scan_guard.into_iter())
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send> {
        let buckets = self.rwlock.read_arc();
        self.gen_read_iterator(TransactionId::new(), ts, buckets)
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
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let buckets = self.rwlock.read();
        let bucket_num = buckets.get_bucket_num();

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
            page_free_space >= insert_space_need
        };
        if check_insert_result {
            // can insert
            let insert_result =
                <Page as MvccHashJoinCuckooPage>::insert(&mut *inserted_page, key, pkey, ts, val);
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

    pub fn rehash(&self, hash_size: u32) {
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.rwlock.read();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.get_bucket_num() >= hash_size {
                    return;
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
                let (key, pkey, val, ts) = hashed_page.get_key_pkey_val_ts_with_slot_id(slot_idx);
                if let Some(idx) =
                    buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize)
                {
                    assert_eq!(idx as u32, (hashed_bucket_idx + old_entry_num));
                    match new_page.insert(&key, &pkey, ts, &val) {
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
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.insert_inner(key, pkey, ts, val) {
                Ok(()) => {
                    return Ok(());
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    self.rehash(new_hash_size);
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

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
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
            let get_result = <Page as MvccHashJoinCuckooPage>::get(&*read_page, key, pkey, ts);
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
       search both hasher_idx
       if find -> return value
       if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
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
    fn update_inner(
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
                    match MvccHashJoinCuckooPage::check_and_update_at_slot_id(
                        &mut *write_page,
                        slot_id,
                        key,
                        pkey,
                        val,
                        ts,
                    ) {
                        Ok(old_res) => return Ok(old_res),
                        Err(CuckooAccessMethodError::OutOfSpace) => {
                            return Err(CuckooAccessMethodError::CuckooOutOfSpace(bucket_num));
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
        if not find => Err(KeyNotFound)
        if invalid ts => Err(KeyFoundButInvalidTimestamp)
        else {
            if update succ => return (old_ts, old_val)
            if out of space => return Err(OutOfSpace) => Rehash
        }
    */
    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.update_inner(key, pkey, ts, val) {
                Ok(old_res) => {
                    return Ok(old_res);
                }
                Err(CuckooAccessMethodError::CuckooOutOfSpace(new_hash_size)) => {
                    // rehash
                    self.rehash(new_hash_size);
                    log_debug!("Page insert out of space, re-hash");
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

    /*
        acquire 2 pages lock at the same time
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find but invalid timestamp -> return Err(KeyFoundButInvalidTimestamp)
        if not find -> return Err(KeyNotFound)
        if find -> delete value & return OK
        return Err(keynotfound)
    */
    fn delete_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let buckets = self.rwlock.read();

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
                    match MvccHashJoinCuckooPage::delete_slot_at_id(&mut *write_page, slot_id) {
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

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let base = 2;
        let mut attempts = 0;
        loop {
            match self.delete_inner(key, pkey, ts) {
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
