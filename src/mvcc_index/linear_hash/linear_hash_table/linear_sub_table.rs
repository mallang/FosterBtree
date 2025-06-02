use core::panic;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};

use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    log_warn,
    mvcc_index::{
        hash_common::{
            get_hashed_bucket_index, read_page, write_page, BucketEntry, RowDelta,
            DEFAULT_BUCKET_NUM,
        },
        hash_join_page::{self, record::RecordRef, HashJoinPage},
        linear_hash::linear_hash_table::linear_hash_table::LinearBulkUpdate,
        MvccEntry,
    },
    page::{self, Page, PageId},
    prelude::{AccessMethodError, Timestamp},
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
        readguard.len() / 8 + 1
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
                let slot = read_page.unsafe_slot(slot_id);
                let rec = read_page.record_ref_from_slotid(slot_id);
                if is_recent {
                    new_table
                        .insert(&rec, slot.start_ts(), slot.end_ts())
                        .unwrap();
                } else {
                    new_table
                        .insert_history(&rec, slot.start_ts(), slot.end_ts())
                        .unwrap();
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
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
        guard: &Vec<BucketEntry>,
    ) -> Result<(), AccessMethodError> {
        let readguard = guard;
        let bucket_idx = Self::get_bucket_index(&readguard, rec.key());
        let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        let buckets_num = buckets.len();
        for i in 0..threadold {
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let insert_result = <Page as HashJoinPage>::insert_recent_history(
                &mut *write_page,
                rec,
                start_ts,
                end_ts,
            );
            match insert_result {
                Ok(_) => {
                    return Ok(());
                }
                Err(AccessMethodError::OutOfSpace) => {
                    // set page as full
                    let header = <Page as HashJoinPage>::unsafe_header_mut(&write_page);
                    header.set_full();
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
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
        guard: &Vec<BucketEntry>,
    ) -> Result<(), AccessMethodError> {
        let readguard = guard;
        let start_bucket_idx = Self::get_bucket_index(&readguard, rec.key());
        let threadold = Self::get_ite_threshold(&readguard);
        let buckets = readguard;
        let buckets_num = buckets.len();
        for i in 0..threadold {
            let cur_bucket_idx = (start_bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let insert_result =
                <Page as HashJoinPage>::upsert_history(&mut *write_page, rec, start_ts, end_ts);
            match insert_result {
                Ok(_) => {
                    return Ok(());
                }
                Err(AccessMethodError::OutOfSpace) => {
                    // set page as full
                    let header = <Page as HashJoinPage>::unsafe_header_mut(&write_page);
                    header.set_full();

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

    pub fn insert(
        &self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        match self._insert_with_guard(&rec, start_ts, end_ts, &*readguard) {
            Ok(_) => Ok(()),
            Err(AccessMethodError::Rehash(new_size)) => {
                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                self._rehash(new_size, &mut *write_guard, true);
                self._insert_with_guard(&rec, start_ts, end_ts, &*write_guard)
                    .unwrap();
                log_warn!("[rehashing] rehash to {:?}", new_size);
                Ok(())
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    pub fn insert_history(
        &self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        match self._upsert_history_with_guard(rec, start_ts, end_ts, &*readguard) {
            Ok(_) => Ok(()),
            Err(AccessMethodError::Rehash(new_size)) => {
                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                self._rehash(new_size, &mut *write_guard, false);
                self._upsert_history_with_guard(rec, start_ts, end_ts, &*write_guard)
                    .unwrap();
                Ok(())
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    pub fn history_garbage_collect(&self, ts: Timestamp) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.read();
        let buckets = &readguard;
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let pid = buckets[i].page_id();
            let fid = buckets[i].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            <Page as HashJoinPage>::chained_hash_garbage_collect(&mut *write_page, &ts)?;
        }
        Ok(())
    }

    pub fn scan_delta_into(
        &self,
        from: Timestamp,
        to: Timestamp,
        deltas: &mut HashMap<Vec<u8>, RowDelta>,
    ) -> Result<(), AccessMethodError> {
        let readguard = self.buckets.read();
        let buckets = &readguard;
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let pid = buckets[i].page_id();
            let fid = buckets[i].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let read_page = read_page(&*self.mem_pool, page_f_key);
            <Page as HashJoinPage>::chain_scan_delta_into(&read_page, from, to, deltas);
        }
        Ok(())
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
                    if <Page as HashJoinPage>::unsafe_header(&read_page).is_full() {
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
                    if <Page as HashJoinPage>::unsafe_header(&read_page).is_full() {
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

    pub fn bulk_update_recent(
        &self,
        new_start_ts: Timestamp,
        bulk: &mut LinearBulkUpdate,
    ) -> Result<(), AccessMethodError> {
        let buckets = &self.buckets.upgradable_read();
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let pid = buckets[i].page_id();
            let fid = buckets[i].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let mut page = &mut *write_page;
            <Page as HashJoinPage>::linear_bulk_update_slots_recent(&mut page, bulk, new_start_ts);
        }

        Ok(())
    }

    fn _update(
        &self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let readguard = self.buckets.upgradable_read();
        let bucket_idx = Self::get_bucket_index(&readguard, rec.key());
        let buckets = &readguard;
        let buckets_num = buckets.len();
        for i in 0..buckets_num {
            let cur_bucket_idx = (bucket_idx + i) % buckets_num;
            let pid = buckets[cur_bucket_idx].page_id();
            let fid = buckets[cur_bucket_idx].frame_id();
            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let mut write_page = write_page(&*self.mem_pool, page_f_key);
            let search_result = <Page as HashJoinPage>::search_slot(&mut *write_page, rec.pkey());
            if search_result.0 {
                // find the updated entry
                match <Page as HashJoinPage>::update_at_slot_id(
                    &mut write_page,
                    rec,
                    start_ts,
                    end_ts,
                    search_result.1,
                ) {
                    Ok(old_entry) => return Ok(old_entry),
                    Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry)) => {
                        // has deleted old,
                        // insert new and return old
                        drop(write_page);

                        match self._insert_with_guard(&rec, start_ts, end_ts, &*readguard) {
                            Ok(_) => return Ok(old_entry),
                            Err(AccessMethodError::Rehash(new_size)) => {
                                let mut write_guard = RwLockUpgradableReadGuard::upgrade(readguard);
                                self._rehash(new_size, &mut *write_guard, true);
                                self._insert_with_guard(&rec, start_ts, end_ts, &*write_guard)
                                    .unwrap();
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
                if <Page as HashJoinPage>::unsafe_header(&write_page).is_full() {
                    // full
                    continue;
                } else {
                    return Err(AccessMethodError::KeyNotFound);
                }
            }
        }
        return Err(AccessMethodError::KeyNotFound);
    }

    pub fn update(
        &self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        match self._update(rec, start_ts, end_ts) {
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
                if <Page as HashJoinPage>::unsafe_header(&write_page).is_full() {
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
