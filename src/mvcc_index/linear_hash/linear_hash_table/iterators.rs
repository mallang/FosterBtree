use std::{mem::transmute, sync::Arc};

use parking_lot::RwLockReadGuard;

use crate::{bp::{FrameReadGuard, MemPool, PageFrameKey}, mvcc_index::{hash_common::{read_page, BucketEntry}, hash_join_page::HashJoinPage, MvccEntry}, page::Page, prelude::Timestamp};

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
            let new_guard = unsafe{
                transmute::<RwLockReadGuard<'_, Vec<BucketEntry>>, RwLockReadGuard<'static, Vec<BucketEntry>>>(guard)
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
                let page_f_key = PageFrameKey::new_with_frame_id(
                    self.subtable.c_key.clone(), page_id, frame_id);
                let read_page = read_page(&*self.subtable.mem_pool, page_f_key);
                let read_page = unsafe{ transmute::<FrameReadGuard, FrameReadGuard<'static>>(read_page) };
                self.current_page = Some(read_page);

                self.cur_slot_idx = 0;
            }

            if self.cur_slot_idx < self.current_page.as_ref().unwrap().slot_count() {
                let entry = match <Page as HashJoinPage>::get_entry_at_slot_id(&*self.current_page.as_ref().unwrap(), self.cur_slot_idx) {
                    Ok(entry) => {
                        self.cur_slot_idx += 1;
                        if self.ts.is_some() && (self.ts.as_ref().unwrap() < &entry.start_ts() || (self.ts.as_ref().unwrap() >= &entry.end_ts() && entry.end_ts() != u64::MAX)) {
                            continue;
                        }
                        entry
                    },
                    Err(e) => {
                        panic!("unexpected error: {:?}", e);
                    }
                };

                return Some(entry);
            } else {
                self.current_page = None;
            }
        }
    }
}