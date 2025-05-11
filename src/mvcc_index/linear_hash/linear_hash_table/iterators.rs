use std::{hash::Hash, mem::transmute, sync::Arc};

use parking_lot::RwLockReadGuard;

use crate::{
    bp::{FrameReadGuard, MemPool, PageFrameKey},
    mvcc_index::{
        hash_common::{get_hashed_bucket_index, read_page, BucketEntry},
        hash_join_page::HashJoinPage,
        MvccEntry,
    },
    page::Page,
    prelude::Timestamp,
};

use super::linear_sub_table::LinearSubTable;

pub struct LinearSubTableScanner<T: MemPool + 'static> {
    subtable: Arc<LinearSubTable<T>>,
    guard_buckets: Option<RwLockReadGuard<'static, Vec<BucketEntry>>>,
    current_page: Option<FrameReadGuard<'static>>,
    cur_bucket_idx: usize,
    cur_slot_idx: usize,
    ts: Option<Timestamp>,
    is_end: bool,
}

impl<T: MemPool + 'static> LinearSubTableScanner<T> {
    pub fn new(subtable: Arc<LinearSubTable<T>>, ts: Option<Timestamp>) -> Self {
        Self {
            subtable,
            guard_buckets: None,
            current_page: None,
            cur_bucket_idx: 0,
            cur_slot_idx: 0,
            ts,
            is_end: false,
        }
    }
}

impl<T: MemPool + 'static> Iterator for LinearSubTableScanner<T> {
    type Item = MvccEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end {
            return None;
        }

        if self.guard_buckets.is_none() {
            let guard = self.subtable.buckets.read();
            let new_guard = unsafe {
                transmute::<
                    RwLockReadGuard<'_, Vec<BucketEntry>>,
                    RwLockReadGuard<'static, Vec<BucketEntry>>,
                >(guard)
            };
            self.guard_buckets = Some(new_guard);
        }

        loop {
            if self.current_page.is_none() {
                if self.cur_bucket_idx >= self.guard_buckets.as_ref().unwrap().len() {
                    self.is_end = true;
                    self.guard_buckets = None;
                    return None;
                }

                let bucket = &self.guard_buckets.as_ref().unwrap()[self.cur_bucket_idx];
                self.cur_bucket_idx += 1;

                let page_id = bucket.page_id();
                let frame_id = bucket.frame_id();
                let page_f_key =
                    PageFrameKey::new_with_frame_id(self.subtable.c_key.clone(), page_id, frame_id);
                let read_page = read_page(&*self.subtable.mem_pool, page_f_key);
                let read_page =
                    unsafe { transmute::<FrameReadGuard, FrameReadGuard<'static>>(read_page) };
                self.current_page = Some(read_page);

                self.cur_slot_idx = 0;
            }

            let current_page = self.current_page.as_ref().unwrap();
            if self.cur_slot_idx < current_page.slot_count() {
                let slot = current_page.unsafe_slot(self.cur_slot_idx);
                if self.ts.is_some()
                    && (self.ts.as_ref().unwrap() < &slot.start_ts()
                        || (self.ts.as_ref().unwrap() >= &slot.end_ts()
                            && slot.end_ts() != u64::MAX))
                {
                    self.cur_slot_idx += 1;
                    continue;
                }
                let entry = <Page as HashJoinPage>::get_entry_at_slot_id(
                    &current_page,
                    self.cur_slot_idx,
                ).unwrap();
                self.cur_slot_idx += 1;
                return Some(entry);
            } else {
                self.current_page = None;
            }
        }
    }
}

pub struct LinearSubTableKeyScanner<T: MemPool + 'static> {
    subtable: Arc<LinearSubTable<T>>,
    guard_buckets: Option<RwLockReadGuard<'static, Vec<BucketEntry>>>,
    current_page: Option<FrameReadGuard<'static>>,
    cur_bucket_idx: usize,
    cur_slot_idx: usize,
    ts: Option<Timestamp>,
    key: Vec<u8>,
    is_end: bool,
}

impl<T: MemPool + 'static> LinearSubTableKeyScanner<T> {
    pub fn new(subtable: Arc<LinearSubTable<T>>, ts: Option<Timestamp>, key: Vec<u8>) -> Self {
        Self {
            subtable,
            guard_buckets: None,
            current_page: None,
            cur_bucket_idx: 0,
            cur_slot_idx: 0,
            ts,
            key,
            is_end: false,
        }
    }
}

impl<T: MemPool + 'static> Iterator for LinearSubTableKeyScanner<T> {
    type Item = MvccEntry;
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end {
            return None;
        }

        if self.guard_buckets.is_none() {
            let guard = self.subtable.buckets.read();
            let new_guard = unsafe {
                transmute::<
                    RwLockReadGuard<'_, Vec<BucketEntry>>,
                    RwLockReadGuard<'static, Vec<BucketEntry>>,
                >(guard)
            };
            self.guard_buckets = Some(new_guard);
            self.cur_bucket_idx = get_hashed_bucket_index(
                &self.key,
                self.guard_buckets.as_ref().unwrap().len() as u32,
            );
        }

        loop {
            if self.current_page.is_none() {
                if self.cur_bucket_idx == self.guard_buckets.as_ref().unwrap().len() {
                    self.is_end = true;
                    self.guard_buckets = None;
                    return None;
                }
                let bucket = &self.guard_buckets.as_ref().unwrap()[self.cur_bucket_idx];
                self.cur_bucket_idx += 1;

                let page_id = bucket.page_id();
                let frame_id = bucket.frame_id();
                let page_f_key =
                    PageFrameKey::new_with_frame_id(self.subtable.c_key.clone(), page_id, frame_id);
                let read_page = read_page(&*self.subtable.mem_pool, page_f_key);

                let read_page =
                    unsafe { transmute::<FrameReadGuard, FrameReadGuard<'static>>(read_page) };
                self.current_page = Some(read_page);

                self.cur_slot_idx = 0;
            }

            let current_page = self.current_page.as_ref().unwrap();
            if self.cur_slot_idx < self.current_page.as_ref().unwrap().slot_count() {
                let slot = current_page.unsafe_slot(self.cur_slot_idx);
                let rec = current_page.record_ref_from_slot(slot);
                if self.ts.is_some()
                    && (self.ts.as_ref().unwrap() < &slot.start_ts()
                        || (self.ts.as_ref().unwrap() >= &slot.end_ts()
                            && slot.end_ts()!= u64::MAX))
                {
                    self.cur_slot_idx += 1;
                    continue;
                }

                if rec.key() != self.key {
                    self.cur_slot_idx += 1;
                    continue;
                }

                let entry = <Page as HashJoinPage>::get_entry_at_slot_id(
                    &current_page,
                    self.cur_slot_idx,
                ).unwrap();
                self.cur_slot_idx += 1;
                return Some(entry);
            } else {
                if !current_page
                    .unsafe_header()
                    .is_full()
                {
                    self.is_end = true;
                    return None;
                }
                self.current_page = None;
            }
        }
    }
}
