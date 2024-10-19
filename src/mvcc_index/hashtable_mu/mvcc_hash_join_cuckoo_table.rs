use std::{collections::HashMap, hash::{Hash, Hasher, SipHasher}, sync::{atomic::AtomicU32, Arc, Mutex, RwLock}, time::Duration, u32};

use crate::{bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey}, log_debug, log_warn, mvcc_index::Timestamp, page::{Page, PageId}, rwlatch::RwLatch};

use super::{mvcc_hash_join_cuckoo::HASHER_KEYS, mvcc_hash_join_cuckoo_common::CuckooAccessMethodError, mvcc_hash_join_cuckoo_page::MvccHashJoinCuckooPage};

pub struct BucketEntry {
    page_id: PageId,  
    frame_id: AtomicU32, // changed when normal case, except REHASH
}

impl BucketEntry {
    pub fn new(pid: PageId) -> Self {
        Self {
            page_id: pid,
            frame_id: AtomicU32::new(u32::MAX),
        }
    }
    pub fn page_id(&self) -> PageId {
        self.page_id
    }
}

const MAX_CUCKOO_ITERATE_COUNT: usize = 3;

#[derive(Debug, Clone)]
enum SwapChainStatus {
    Swap((u32, Vec<u8>, u32, )), // swap at slot: $1, swapped key: $2, swapped space_need: $3 
    Insert(u32), // can be inserted with space_need: $2
}

#[derive(Debug, Clone)]
pub struct ChainStatusEntry(usize, SwapChainStatus);

pub struct Buckets {
    num_buckets: u32,
    // with length = `num_buckets`
    buckets: Vec<BucketEntry>,
}

impl Buckets {
    pub fn get_bucket_num(&self) -> u32 {
        self.num_buckets
    }

    pub fn get_bucket_entry(&self, entry_idx: usize) -> &BucketEntry {
        &self.buckets[entry_idx]
    }

    // assert have had a latch
    pub fn get_bucket_index(&self, key: &[u8], hasher_idx: usize) ->  usize {
        let num_buckets = self.num_buckets;
        let mut hasher = SipHasher::new_with_keys(
            HASHER_KEYS[hasher_idx].0, 
            HASHER_KEYS[hasher_idx].1,
        );
        key.hash(&mut hasher);
        (hasher.finish() as usize) % num_buckets as usize
    }

    // maybe NOT exist -> None
    pub fn get_a_second_bucket_index(&self, key: &[u8], first_idx: usize) -> Option<usize> {
        let num_buckets = self.num_buckets;
        let bucket_idxs = HASHER_KEYS.iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(
                    *key0, 
                    *key1,
                );
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_buckets as usize
            })
            .collect::<Vec<_>>();

        for idx in bucket_idxs {
            if idx != first_idx {
                return Some(idx);
            }
        }
        
        None
    }

    pub fn get_all_bucket_index(&self, key: &[u8]) -> Vec<usize> {
        let num_buckets = self.num_buckets;
        let mut bucket_idxs = HASHER_KEYS.iter()
            .map(|(key0, key1)| {
                let mut hasher = SipHasher::new_with_keys(
                    *key0, 
                    *key1,
                );
                key.hash(&mut hasher);
                (hasher.finish() as usize) % num_buckets as usize
            })
            .collect::<Vec<_>>();

        bucket_idxs.sort();
        bucket_idxs.dedup();
        bucket_idxs
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
    rwlock: RwLock<Buckets>,

    rehash_mutex: Mutex<()>,
    
}

impl<T: MemPool> CuckooHashTable<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let pid = page.get_id();
        let fid = page.frame_id();

        MvccHashJoinCuckooPage::init(&mut *page);
        drop(page);

        let buckets= Buckets{
            num_buckets: 1,
            buckets: vec![BucketEntry{
                page_id: pid,
                frame_id: AtomicU32::new(fid),
            }],
        };

        Self {
            c_key,
            mem_pool,
            rwlock: RwLock::new(buckets),
            rehash_mutex: Mutex::new(()),
        }

    }

    pub fn get_all_bucket_pages_for_init(&self) -> Vec<PageId> {
        let buckets = self.rwlock.read().unwrap();
        let ret = buckets.buckets.iter()
            .map(|x| {
                x.page_id
            })
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

            bucket_entry_vec.push(BucketEntry{
                page_id: pid, 
                frame_id: AtomicU32::new(fid),
            });
        }
        let buckets = Buckets {
            num_buckets: num_buckets as u32,
            buckets: bucket_entry_vec,
        };
        Self {
            c_key,
            mem_pool,
            rwlock: RwLock::new(buckets),
            rehash_mutex: Mutex::new(()),
        }
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
                },
                Err(CuckooAccessMethodError::CuckooIterateFailed(new_hash_size)) => {
                    // rehash
                    self.rehash(new_hash_size);
                    log_debug!("Page insert failed, re-hash");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue;
                },
                Err(_) => {
                    panic!("should not happen");
                }
            }
        }
    }



    /* 
        if have free space -> insert
        if free space after compaction -> compaction
        if have record size larger than inserted record -> update
        else -> rehash: Err(IterateFailed)
    */
    fn insert_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let mut attempts = 0;
        let base = 2;

        'insert: loop{
            // lock to ban re-hash
            let buckets = self.rwlock.read().unwrap();

            let entry_index_with_op_sequential_chain = {
                match self.get_swapped_entry_index_chain(key, pkey, val, & *buckets) {
                    Ok(ret) => {
                        ret
                    },
                    Err(e) => {
                        // iterate failed -> rehash
                        return Err(e);
                    }
                }
            };
            
            let mut write_pages_map = {
                let mut entries_idxes = entry_index_with_op_sequential_chain.iter()
                    .map(|elem| {
                        elem.0
                    })
                    .collect::<Vec<_>>();
                let entries_idxes_sorted = {entries_idxes.sort(); entries_idxes.dedup(); entries_idxes};
                let all_write_pages_map = self.write_sequentially_all_pages(entries_idxes_sorted, & *buckets);
                
                // we need to check whether these are valid
                if !Self::check_ops_valid(&entry_index_with_op_sequential_chain, &all_write_pages_map) {
                    // insert again from start
                    log_debug!("insert chain collect failed, retrying");
                    attempts += 1;
                    std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                    continue 'insert;
                }

                all_write_pages_map
            };

            // we are valid
            Self::execute_ops_after_check(
                &entry_index_with_op_sequential_chain, 
                &mut write_pages_map, 
                (key, pkey, val, ts),
            );

            break 'insert;

        }
        Ok(())        
    }

    /*
        if search both hasher_idx
        if find -> return value
        if find but invalid timestamp -> return Err(InvalidTimestamp)
        return Err(keynotfound)


        remember to keep atomicity!
     */
    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let buckets = self.rwlock.read().unwrap();
        
        let check_bucket_pages = {
            let bucket_hash_idxs_sorted = buckets.get_all_bucket_index(key);
            self.read_sequentially_all_pages(bucket_hash_idxs_sorted, & *buckets)
        };

        for bucket_page in check_bucket_pages {
            // Attempt to retrieve the value from the current page
            match MvccHashJoinCuckooPage::get(& *bucket_page, key, pkey, ts) {
                Ok(val) => {
                    // Value found
                    return Ok(val);
                }
                Err(CuckooAccessMethodError::KeyNotFound) => {
                    continue;
                }
                Err(CuckooAccessMethodError::InvalidTimestamp) => {
                    return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(_) => {
                    panic!("should not happen!");
                }
            }
        }
        Err(CuckooAccessMethodError::KeyNotFound)
    }

    /* 
        if not find => Err(KeyNotFound)
        if invalid ts => Err(InvalidTimestamp)
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
        let mut attempts = 0;
        let base = 2;

        '_update_loop: loop {
            let buckets = self.rwlock.read().unwrap();

            let check_bucket_pages_read = {
                let ret = buckets.get_all_bucket_index(key);

                self.read_sequentially_all_pages(ret, & *buckets)
            };

            for bucket_page in check_bucket_pages_read.into_iter() {
                // Attempt to retrieve the value from the current page
                match MvccHashJoinCuckooPage::get_slot_id(& *bucket_page, key, pkey, ts) {
                    Ok(slot_id) => {
                        // Value found
                        match bucket_page.try_upgrade(true) {
                            Ok(mut upgraded_page) => {
                                match MvccHashJoinCuckooPage::check_and_update_at_slot_id(&mut *upgraded_page, slot_id, key, pkey, val, ts) {
                                    Ok(old_res) => {
                                        return Ok(old_res)
                                    },
                                    Err(CuckooAccessMethodError::OutOfSpace) => {
                                        return Err(CuckooAccessMethodError::OutOfSpace)
                                    },
                                    Err(x) => {
                                        panic!("should not happen for that error! {:?}", x);
                                    }
                                }
                            },
                            Err(_) => {
                                log_debug!("Page upgrade failed, retrying");
                                attempts += 1;
                                std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                                continue '_update_loop;
                            }
                        }
                    },
                    Err(CuckooAccessMethodError::KeyNotFound) => {
                        continue;
                    },
                    Err(CuckooAccessMethodError::InvalidTimestamp) => {
                        return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                    },
                    Err(_) => {
                        panic!("should not happen!");
                    },
                };
            }
            return Err(CuckooAccessMethodError::KeyNotFound);
        }
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let mut attempts = 0;
        let base = 2;

        '_delete_loop: loop {
            let buckets = self.rwlock.read().unwrap();

            let check_bucket_pages_read = {
                let ret = buckets.get_all_bucket_index(key);

                self.read_sequentially_all_pages(ret, & *buckets)
            };

            for bucket_page in check_bucket_pages_read.into_iter() {
                // Attempt to retrieve the value from the current page
                match MvccHashJoinCuckooPage::get_slot_id(& *bucket_page, key, pkey, ts) {
                    Ok(slot_id) => {
                        // Value found
                        match bucket_page.try_upgrade(true) {
                            Ok(mut upgraded_page) => {
                                match MvccHashJoinCuckooPage::delete_slot_at_id(&mut *upgraded_page, slot_id) {
                                    Ok(old_res) => {
                                        return Ok(old_res)
                                    },
                                    Err(x) => {
                                        panic!("should not happen for that error! {:?}", x);
                                    }
                                }
                            },
                            Err(_) => {
                                log_debug!("Page upgrade failed, retrying");
                                attempts += 1;
                                std::thread::sleep(Duration::from_millis(u64::pow(base, attempts)));
                                continue '_delete_loop;
                            }
                        }
                    },
                    Err(CuckooAccessMethodError::KeyNotFound) => {
                        continue;
                    },
                    Err(CuckooAccessMethodError::InvalidTimestamp) => {
                        return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                    },
                    Err(_) => {
                        panic!("should not happen!");
                    },
                };
            }
            return Err(CuckooAccessMethodError::KeyNotFound);
        }
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
                    log_warn!("Exclusive page latch grant failed: {:?}. Will retry", page_key);
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

    /// ok or err(iterateFailed)
    pub fn get_swapped_entry_index_chain (
        &self,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        buckets: &Buckets,
    ) -> Result<Vec<ChainStatusEntry>, CuckooAccessMethodError>{
        let hasher_idx = rand::random::<u8>() as usize & 1;

        let mut check_space_need = <Page as MvccHashJoinCuckooPage>::space_need(key, pkey, val);
        let mut check_entry_idx = buckets.get_bucket_index(key, hasher_idx);

        let mut all_chain_entry_idx_with_status = vec![];


        let mut iterate_cnt = 0;


        // insert chain indexes
        'get_sorted_chain: loop {

            let check_page_frame_key = PageFrameKey::new_with_frame_id(
                self.c_key, 
                buckets.get_bucket_entry(check_entry_idx).page_id,
                buckets.get_bucket_entry(check_entry_idx).frame_id.load(std::sync::atomic::Ordering::Acquire),
            );

            // NEED release guard
            let page = self.read_page(check_page_frame_key);

            if check_space_need <= MvccHashJoinCuckooPage::free_space_with_compaction(& *page) {
                // chain ends
                all_chain_entry_idx_with_status.push(ChainStatusEntry(check_entry_idx, SwapChainStatus::Insert(check_space_need)));
                break 'get_sorted_chain;
            }

            // if reach max ite count -> Err(iterateFailed)
            iterate_cnt += 1;
            if iterate_cnt == MAX_CUCKOO_ITERATE_COUNT {
                return Err(CuckooAccessMethodError::CuckooIterateFailed(buckets.get_bucket_num()));
            }

            // need to find a slot to swap
            let mut check_slot_start_idx = 0;
            'find_larger_and_swapptable_record: loop{
                match MvccHashJoinCuckooPage::check_larger_record(& *page, check_space_need, check_slot_start_idx) {
                    None | Some((u32::MAX, _, _, _)) => {
                        // reach end not found swapped index => rehash
                        return Err(CuckooAccessMethodError::CuckooIterateFailed(buckets.get_bucket_num()));
                    },
                    Some((swapped_slot_idx, swapped_key, swapped_pkey, swapped_val)) => {
                        // have found a larger record

                        // check whether can be swapped(whether has a second hash idx)
                        if let Some(a_second_entry_idx_of_slot) = buckets.get_a_second_bucket_index(&swapped_key, check_slot_start_idx as usize) {
                            /* new */check_space_need = <Page as MvccHashJoinCuckooPage>::space_need(&swapped_key, &swapped_pkey, &swapped_val);
                            all_chain_entry_idx_with_status.push(ChainStatusEntry(/* old entry idx */check_entry_idx, SwapChainStatus::Swap((swapped_slot_idx, swapped_key.clone(), check_space_need))));                            
                            
                            check_entry_idx = a_second_entry_idx_of_slot;
                            break 'find_larger_and_swapptable_record;
                        } else {
                            // this swapped slot can NOT be used in swap, continue find another larger record
                            check_slot_start_idx = swapped_slot_idx + 1;
                            continue 'find_larger_and_swapptable_record;
                        }
                    },
                }
            }


            // have found a larger and swappable record.
            continue 'get_sorted_chain;
        }

        Ok(all_chain_entry_idx_with_status)
    }

    fn write_sequentially_all_pages(
        &self,
        sorted_entry_idxes: Vec<usize>,
        buckets: &Buckets,
    ) -> HashMap<usize, FrameWriteGuard> {
        let mut ret = HashMap::<usize, FrameWriteGuard>::new();
        for entry_idx in sorted_entry_idxes {
            let write_page = self.write_page(PageFrameKey::new_with_frame_id(
                self.c_key, 
                buckets.get_bucket_entry(entry_idx).page_id, 
                buckets.get_bucket_entry(entry_idx).frame_id.load(std::sync::atomic::Ordering::Acquire),
            ));
            ret.insert(entry_idx, write_page);
        }

        ret
    }

    fn read_sequentially_all_pages(
        &self,
        sorted_entry_idxes: Vec<usize>,
        buckets: &Buckets,
    ) -> Vec<FrameReadGuard> {
        let mut ret = vec![];
        for entry_idx in sorted_entry_idxes {
            let read_page = self.read_page(PageFrameKey::new_with_frame_id(
                self.c_key, 
                buckets.get_bucket_entry(entry_idx).page_id, 
                buckets.get_bucket_entry(entry_idx).frame_id.load(std::sync::atomic::Ordering::Acquire),
            ));
            ret.push(read_page)
        }

        ret
    }

    fn check_ops_valid(
        op_chain: &Vec<ChainStatusEntry>,
        pages_map: &HashMap<usize, FrameWriteGuard>,
    ) -> bool {
        for op in op_chain {
            let write_page_guard = pages_map.get(&op.0).unwrap();

            match op.clone().1 {
                SwapChainStatus::Swap((swapped_slot_id, swapped_key, swapped_space_need)) => {
                    if <Page as MvccHashJoinCuckooPage>::check_slot_key_and_space_of_id(& *write_page_guard, swapped_slot_id, &swapped_key, swapped_space_need) {
                        // we can do that swap
                        continue;
                    } else {
                        return false;
                    }
                },
                SwapChainStatus::Insert(insert_space_need) => {
                    let page_free_space = <Page as MvccHashJoinCuckooPage>::free_space_with_compaction(& *&write_page_guard);
                    if page_free_space >= insert_space_need {
                        continue;
                    } else {
                        return false;
                    }
                }
            }
            
        }

        return true;
    }

    fn execute_ops_after_check(
        op_chain: &Vec<ChainStatusEntry>,
        pages_map: &mut HashMap<usize, FrameWriteGuard>,
        first_record: (&[u8], &[u8], &[u8], Timestamp),
    ) {
        let mut new_record = (
            first_record.0.to_vec(), 
            first_record.1.to_vec(),
            first_record.2.to_vec(),
            first_record.3,
        );
        let mut end_flag = false;
        for op in op_chain {

            if end_flag == true {
                panic!("insert should only exist once");
            }

            let write_page_guard = pages_map.get_mut(&op.0).unwrap();

            match &op.1 {
                SwapChainStatus::Swap((want_swapping_slot_id, want_swapped_key, want_swapped_space_need)) => {
                    // SWAP
                    match <Page as MvccHashJoinCuckooPage>::swap_record_at_slot_id(
                        &mut *write_page_guard, 
                        *want_swapping_slot_id,
                        &new_record.0,
                        &new_record.1,
                        &new_record.2,
                        new_record.3,
                    ) {
                        Ok((swapped_key, swapped_pkey, swapped_val, swapped_ts)) => {
                            // we can do that swap
                            let calc_space_need = <Page as MvccHashJoinCuckooPage>::space_need(&swapped_key, &swapped_pkey, &swapped_val);
                            assert_eq!(calc_space_need, *want_swapped_space_need);
                            assert_eq!(swapped_key, *want_swapped_key);
                            
                            new_record = (swapped_key, swapped_pkey, swapped_val, swapped_ts);
                            
                            continue;
                        },
                        Err(_) => {
                            panic!("Should not happen");
                        },
                    }
                },
                SwapChainStatus::Insert(insert_space_need) => {
                    // INSERT, maybe compact
                    match <Page as MvccHashJoinCuckooPage>::insert(
                        &mut *write_page_guard, 
                        &new_record.0,
                        &new_record.1,
                        new_record.3,
                        &new_record.2,
                    ) {
                        Ok(_) => {
                            end_flag = true;
                            continue;
                        }
                        Err(_) => {
                            panic!("should not happen!");
                        }
                    }
                }
            }
        }
    }

    pub fn rehash(
        &self,
        hash_size: u32,
    ) {
        // ensure only one thread can rehash in any time
        let _guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.rwlock.read().unwrap();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.get_bucket_num() >= hash_size {
                    return;
                }
                assert_eq!(buckets.get_bucket_num() * 2,  hash_size);
            }
        }
        
        // we need to rehash
        let mut buckets = self.rwlock.write().unwrap();
        let old_entry_num = buckets.get_bucket_num();
        for _ in 0..old_entry_num {
            buckets.buckets.push(BucketEntry{page_id: 0, frame_id: AtomicU32::new(0),});
        }

        for hashed_bucket_idx in 0..old_entry_num {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let new_pid = new_page.get_id();
            let new_fid = new_page.frame_id();
            MvccHashJoinCuckooPage::init(&mut *new_page);
            buckets.buckets[(hashed_bucket_idx + old_entry_num) as usize] = BucketEntry{page_id: new_pid, frame_id: AtomicU32::new(new_fid)};

            let bucket_entry = buckets.get_bucket_entry(hashed_bucket_idx as usize);
            let page_frame_k = PageFrameKey::new_with_frame_id(
                self.c_key, 
                bucket_entry.page_id, 
                bucket_entry.frame_id.load(std::sync::atomic::Ordering::Acquire),
            );

            let mut hashed_page = self.write_page(page_frame_k);
            let slot_count = hashed_page.slot_count();
            
            for slot_idx in (0..slot_count).rev() {
                let (key, pkey, val, ts) = hashed_page.get_key_pkey_val_ts_with_slot_id(slot_idx);
                if let Some(idx) = buckets.get_a_second_bucket_index(&key, hashed_bucket_idx as usize) {
                    assert_eq!(idx as u32, (hashed_bucket_idx + old_entry_num));
                    match new_page.insert(&key, &pkey, ts, &val) {
                        Ok(_) => {},
                        Err(_) => {
                            panic!("should not happen in re-hash!");
                        }
                    }
                    hashed_page.delete_slot_at_id(slot_idx).unwrap();
                }
            }
        }
    }

}


