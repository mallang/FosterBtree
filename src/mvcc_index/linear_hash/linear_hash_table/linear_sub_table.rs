use core::panic;
use std::sync::{atomic::AtomicU32, Arc};

use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey}, log_warn, mvcc_index::{
        hash_common::{
            get_hashed_bucket_index, read_page, write_page, BucketEntry, DEFAULT_BUCKET_NUM,
        },
        hash_join_page::{self, HashJoinPage},
        MvccEntry, MvccIndex,
    }, page::{self, Page, PageId}, prelude::{AccessMethodError, Timestamp}
};

pub struct LinearSubTable<T: MemPool> {
    pub(super) mem_pool: Arc<T>,
    pub(super) c_key: ContainerKey,

    pub(super) buckets: RwLock<Vec<BucketEntry>>,
}

impl<T: MemPool + 'static> LinearSubTable<T> {
    pub fn new(mem_pool: Arc<T>, c_key: ContainerKey) -> Self {
        Self::new_with_bucket_num(mem_pool, c_key, DEFAULT_BUCKET_NUM)
    }
    pub fn new_with_bucket_num(mem_pool: Arc<T>, c_key: ContainerKey, bucket_size: usize) -> Self {
        let mut buckets = Vec::with_capacity(bucket_size);
        for _ in 0..bucket_size {
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let page_id = page.get_id();
            HashJoinPage::init(&mut *page);
            drop(page);
            buckets.push(BucketEntry::new(page_id));
        }
        Self {
            mem_pool,
            c_key,
            buckets: RwLock::new(buckets),
        }
    }

    fn get_bucket_index(readguard: &Vec<BucketEntry>, key: &[u8]) -> usize {
        get_hashed_bucket_index(key, readguard.len() as u32)
    }

    fn get_ite_threshold(readguard: &Vec<BucketEntry>) -> usize {
        readguard.len() / 16 + 1
    }

    fn _rehash(&self, new_size: u32, guard: &mut Vec<BucketEntry>, is_recent: bool) {
        if guard.len() >= new_size as usize {
            return;
        }

        let new_table =
            Self::new_with_bucket_num(self.mem_pool.clone(), self.c_key, new_size as usize);
        for bucket in guard.iter() {
            let pid = bucket.page_id();
            let fid = bucket.frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let read_page = read_page(&*self.mem_pool, page_f_key);
            for slot_id in 0..read_page.slot_count() {
                match read_page.get_entry_at_slot_id(slot_id) {
                    Ok(mut e) => {
                        if is_recent {
                            new_table.insert(&e).unwrap();
                        } else {
                            new_table.insert_history(&mut e).unwrap();
                        }
                    }
                    Err(e) => {
                        panic!("unexpected error: {:?}", e);
                    }
                }
            }
        }

        guard.clear();

        for bucket in new_table.buckets.into_inner() {
            guard.push(bucket);
        }

        assert_eq!(guard.len(), new_size as usize);
    }

    fn _insert_with_guard(
        &self,
        entry: &MvccEntry,
        guard: &Vec<BucketEntry>,
    ) -> Result<(), AccessMethodError> {
        let readguard = guard;
        let bucket_idx = Self::get_bucket_index(&readguard, &entry.key);
        let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        let buckets_num = buckets.len();
        for i in 0..threadold {
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let insert_result = <Page as HashJoinPage>::insert(&mut *write_page, entry);
            match insert_result {
                Ok(_) => {
                    return Ok(());
                }
                Err(AccessMethodError::OutOfSpace) => {
                    // set page as full
                    let mut header = <Page as HashJoinPage>::header(&write_page);
                    header.set_full();
                    <Page as HashJoinPage>::set_header(&mut *write_page, &header);

                    // next page
                    continue;
                }
                Err(e) => {
                    // full
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        // too much page is full
        return Err(AccessMethodError::Rehash(buckets_num as u32 * 2));
    }

    fn _upsert_history_with_guard(
        &self,
        entry: &mut MvccEntry,
        guard: &Vec<BucketEntry>,
    ) -> Result<(), AccessMethodError> {
        let readguard = guard;
        let start_bucket_idx = Self::get_bucket_index(&readguard, &entry.key);
        let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        let buckets_num = buckets.len();
        for i in 0..threadold {
            let cur_bucket_idx = (start_bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let insert_result = <Page as HashJoinPage>::upsert_history(&mut *write_page, entry);
            match insert_result {
                Ok(_) => {
                    return Ok(());
                }
                Err(AccessMethodError::OutOfSpace) => {
                    // set page as full
                    let mut header = <Page as HashJoinPage>::header(&write_page);
                    header.set_full();
                    <Page as HashJoinPage>::set_header(&mut *write_page, &header);

                    // next page
                    continue;
                }
                Err(e) => {
                    // full
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        // too much page is full
        return Err(AccessMethodError::Rehash(buckets_num as u32 * 2));
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        match self._insert_with_guard(entry, &*readguard) {
            Ok(_) => Ok(()),
            Err(AccessMethodError::Rehash(new_size)) => {
                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                self._rehash(new_size, &mut *write_guard, true);
                self._insert_with_guard(entry, &*write_guard).unwrap();
                log_warn!("rehashing");
                Ok(())
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    pub fn insert_history(&self, entry: &mut MvccEntry) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        match self._upsert_history_with_guard(entry, &*readguard) {
            Ok(_) => Ok(()),
            Err(AccessMethodError::Rehash(new_size)) => {
                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                self._rehash(new_size, &mut *write_guard, false);
                self._upsert_history_with_guard(entry, &*write_guard)
                    .unwrap();
                Ok(())
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let readguard = self.buckets.read();
        let bucket_idx = Self::get_bucket_index(&readguard, key);
        // let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        // let mut count_in_a_row = 0;
        let buckets_num = buckets.len();
        for i in 0..buckets.len() {
            // count_in_a_row += 1;
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let read_page = read_page(&*self.mem_pool, page_f_key);
            let result = <Page as HashJoinPage>::get(&*read_page, pkey, &ts);
            match result {
                Ok(entry) => {
                    return Ok(entry);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    if <Page as HashJoinPage>::header(&read_page).is_full() {
                        // full
                        continue;
                    } else {
                        return Err(AccessMethodError::KeyNotFound);
                    }
                }
                Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                    return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        Err(AccessMethodError::KeyNotFound)
    }

    pub fn get_history(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let readguard = self.buckets.read();
        let start_bucket_idx = Self::get_bucket_index(&readguard, key);
        // let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        // let mut count_in_a_row = 0;
        let buckets_num = buckets.len();
        for i in 0..buckets.len() {
            // count_in_a_row += 1;
            let cur_bucket_idx = (start_bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let read_page = read_page(&*self.mem_pool, page_f_key);
            let result = <Page as HashJoinPage>::get_history(&*read_page, pkey, &ts);
            match result {
                Ok(entry) => {
                    return Ok(entry);
                }
                Err(AccessMethodError::KeyNotFound) => {
                    if <Page as HashJoinPage>::header(&read_page).is_full() {
                        // full
                        continue;
                    } else {
                        return Err(AccessMethodError::KeyNotFound);
                    }
                }
                // Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                //     return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                // }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
        Err(AccessMethodError::KeyNotFound)
    }

    fn _update(&self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        let bucket_idx = Self::get_bucket_index(&readguard, &entry.key);
        let buckets = &readguard;
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let search_result = <Page as HashJoinPage>::search_slot(&mut *write_page, pkey);
            if search_result.0 {
                // find the updated entry
                match <Page as HashJoinPage>::update_at_slot_id(
                    &mut write_page,
                    entry,
                    search_result.1,
                ) {
                    Ok(old_entry) => return Ok(old_entry),
                    Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                        // has deleted old,
                        // insert new and return old
                        drop(write_page);

                        match self._insert_with_guard(&entry, &*readguard) {
                            Ok(_) => return Ok(old_entry),
                            Err(AccessMethodError::Rehash(new_size)) => {
                                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                                self._rehash(new_size, &mut *write_guard, true);
                                self._insert_with_guard(&entry, &*write_guard).unwrap();
                                return Ok(old_entry);
                            }
                            Err(e) => {
                                panic!("unexpected error: {:?}", e);
                            }
                        }
                    }
                    Err(AccessMethodError::KeyNotFound) => {
                        unreachable!("KeyNotFound should not exist");
                    }
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            } else {
                if <Page as HashJoinPage>::header(&write_page).is_full() {
                    // full
                    continue;
                } else {
                    return Err(AccessMethodError::KeyNotFound);
                }
            }
        }
        return Err(AccessMethodError::KeyNotFound);
    }

    pub fn update(&self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError> {
        match self._update(pkey, entry) {
            Ok(old_entry) => Ok(old_entry),
            Err(AccessMethodError::KeyNotFound) => {
                return Err(AccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    fn _delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let readguard = self.buckets.read();
        let bucket_idx = Self::get_bucket_index(&readguard, key);
        let buckets = &readguard;
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let search_result = <Page as HashJoinPage>::search_slot(&mut *write_page, pkey);
            if search_result.0 {
                // find the updated entry
                match <Page as HashJoinPage>::delete_at_slot_id(
                    &mut write_page,
                    &ts,
                    search_result.1,
                ) {
                    Ok(old_entry) => return Ok(old_entry),
                    Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            } else {
                if <Page as HashJoinPage>::header(&write_page).is_full() {
                    // full
                    continue;
                } else {
                    return Err(AccessMethodError::KeyNotFound);
                }
            }
        }
        return Err(AccessMethodError::KeyNotFound);
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        match self._delete(key, pkey, ts) {
            Ok(old_entry) => Ok(old_entry),
            Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
            }
            Err(AccessMethodError::KeyNotFound) => {
                return Err(AccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }
}
