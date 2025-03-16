use core::str;
use std::{
    collections::HashSet,
    iter::Enumerate,
    marker::PhantomData,
    sync::{atomic::AtomicU32, Arc, Mutex, RwLock},
    time::Duration,
};

use crate::{
    bp::{
        get_in_mem_pool, ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus,
        PageFrameKey,
    },
    // lockmanager::{LockManager, Permissions, TransactionId, ValueId},
    log_warn,
    mvcc_index::{
        hybrid_hash::{
            attached_container_page::attached_container_page::{
                slot::{
                    get_remain_key, get_remain_pkey, get_slot_hash, InterPageLoc, Slot, SlotMeta,
                    SLOT_SIZE,
                },
                AttachedPage,
            },
            hash_join_table_common::HashTableAccessMethodError,
            hybrid_hash_table::hybrid_hash_common::read_page,
        },
        DeltaEntry, MvccEntry, MvccIndex, Timestamp,
    },
    page::{Page, PageId},
    prelude::AccessMethodError,
};

use super::{
    hybrid_hash_common::{
        get_hashed_bucket_index, try_read_page, try_write_page, write_page, BucketEntry,
        SUBTABLE_HASHER_SEED,
    },
    hybrid_hash_slot_page::{TableDataPageBase, TableSlotsPage},
};

/// responsible for update meta page of HashJoinTable<T>
pub struct DHashSubTable<T: MemPool + 'static> {
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    // meta: Arc<(PageId, AtomicU32)>,
    /// shared: read & update & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    buckets_rwlock: RwLock<Vec<BucketEntry>>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once
}

// ----------------- SCANNER START ------------------------------
mod iterators {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{
        bp::{MemPool, PageFrameKey},
        mvcc_index::{
            hybrid_hash::{attached_container_page::attached_container_page::slot::get_slot_hash, hybrid_hash_table::{
                hybrid_hash_common::{read_page, SUBTABLE_HASHER_SEED},
                hybrid_hash_slot_page::TableSlotsPage,
            }},
            Delta, DeltaEntry, MvccEntry, MvccIndex, Timestamp,
        },
        page::Page,
    };

    use super::DHashSubTable;

    pub enum SubTableSimpleScannerOption {
        OneVersionAllKeys(Timestamp),
        OneVersionOneKey(Timestamp, Vec<u8>),
    }

    pub struct SubTableDeltaScannerOption {
        pub small_ts: Timestamp,
        pub large_ts: Timestamp,
    }
    pub struct AllSubTableOneVersionOneKeyScanner<T: MemPool + 'static> {
        table: Arc<DHashSubTable<T>>,
        creteria: (Timestamp, Vec<u8>),
        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<(Vec<u8>, Vec<u8>)>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool + 'static> AllSubTableOneVersionOneKeyScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>, option: (Timestamp, Vec<u8>)) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = {
                vec![(get_slot_hash(&option.1[..])) % (bucket_num)]
            };
            Self {
                table: table.clone(),
                creteria: option,
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool + 'static> Iterator for AllSubTableOneVersionOneKeyScanner<T> {
        type Item = (Vec<u8>, Vec<u8>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = read_page(&*self.table.mem_pool, pfkey);

                        let entries = {
                            <Page as TableSlotsPage>::scan_one_version_one_key(
                                &*page,
                                &*self.table.mem_pool,
                                self.table.c_key,
                                self.creteria.0,
                                &self.creteria.1,
                            )
                        };
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }

    pub struct AllSubTableOneVersionAllKeyScanner<T: MemPool + 'static> {
        table: Arc<DHashSubTable<T>>,
        creteria: Timestamp,

        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool + 'static> AllSubTableOneVersionAllKeyScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>, option: Timestamp) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = { (0..bucket_num).into_iter().collect::<Vec<_>>() };
            Self {
                table: table.clone(),
                creteria: option,
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool + 'static> Iterator for AllSubTableOneVersionAllKeyScanner<T> {
        type Item = (Vec<u8>, Vec<u8>, Vec<u8>);
        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = read_page(&*self.table.mem_pool, pfkey);

                        let entries = {
                            <Page as TableSlotsPage>::scan_one_version_all_keys(
                                &*page,
                                &*self.table.mem_pool,
                                self.table.c_key,
                                self.creteria,
                            )
                        };
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }

    pub struct AllSubTableMvccEntryScanner<T: MemPool + 'static> {
        table: Arc<DHashSubTable<T>>,

        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<MvccEntry>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool> AllSubTableMvccEntryScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = { (0..bucket_num).into_iter().collect::<Vec<_>>() };
            Self {
                table: table.clone(),
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool> Iterator for AllSubTableMvccEntryScanner<T> {
        type Item = MvccEntry;

        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = read_page(&*self.table.mem_pool, pfkey);

                        let entries = <Page as TableSlotsPage>::scan_all_versions_all_keys(
                            &*page,
                            &*self.table.mem_pool,
                            self.table.c_key,
                        );
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }

    pub struct AllSubTableDeltaScanner<T: MemPool + 'static> {
        table: Arc<DHashSubTable<T>>,
        option: SubTableDeltaScannerOption,

        next_bucket_index: usize,
        initial_bucket_idxes: Vec<u32>,
        initial_bucket_num: u32,

        all_entries: Vec<DeltaEntry<Vec<u8>>>,
        current_entry_index: usize,
        is_end: bool,
    }

    impl<T: MemPool> AllSubTableDeltaScanner<T> {
        pub fn new(table: &Arc<DHashSubTable<T>>, option: SubTableDeltaScannerOption) -> Self {
            let bucket = table.buckets_rwlock.read().unwrap();
            let bucket_num = bucket.len() as u32;
            let initial_bucket_idxes = (0..bucket_num).into_iter().collect::<Vec<_>>();

            Self {
                table: table.clone(),
                option,
                next_bucket_index: 0,
                initial_bucket_idxes,
                initial_bucket_num: bucket_num,

                all_entries: vec![],
                current_entry_index: 0,
                is_end: false,
            }
        }

        fn integer_is_a_power_of_2(val: u32) -> bool {
            return (val & (val - 1)) == 0;
        }
    }

    impl<T: MemPool + 'static> Iterator for AllSubTableDeltaScanner<T> {
        type Item = DeltaEntry<Vec<u8>>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }

            loop {
                if self.current_entry_index < self.all_entries.len() {
                    let entry = self.all_entries[self.current_entry_index].clone();
                    self.current_entry_index += 1;
                    return Some(entry);
                } else {
                    self.all_entries.clear();
                    self.current_entry_index = 0;

                    if self.next_bucket_index >= self.initial_bucket_idxes.len() {
                        self.is_end = true;
                        return None;
                    }

                    let buckets = self.table.buckets_rwlock.read().unwrap();
                    let current_bucket_num = buckets.len() as u32;
                    assert!(
                        current_bucket_num >= self.initial_bucket_num
                            && Self::integer_is_a_power_of_2(
                                (current_bucket_num / self.initial_bucket_num) as u32
                            )
                    );

                    let check_bucket_num = current_bucket_num / self.initial_bucket_num;

                    for i in 0..check_bucket_num {
                        let bucket_entry_idx = self.initial_bucket_idxes[self.next_bucket_index]
                            as usize
                            + i as usize * self.initial_bucket_num as usize;
                        let bucket_entry = &buckets[bucket_entry_idx];
                        let pfkey = PageFrameKey::new_with_frame_id(
                            self.table.c_key,
                            bucket_entry.page_id(),
                            bucket_entry.frame_id(),
                        );
                        let page = read_page(&*self.table.mem_pool, pfkey);

                        let entries = {
                            page.scan_delta_btw_ts(
                                self.option.small_ts,
                                self.option.large_ts,
                                &*self.table.mem_pool,
                                self.table.c_key,
                            )
                        };
                        self.all_entries.extend(entries);
                    }

                    self.next_bucket_index += 1;
                }
            }
        }
    }
}

pub use iterators::{
    AllSubTableDeltaScanner, AllSubTableMvccEntryScanner, AllSubTableOneVersionAllKeyScanner,
    AllSubTableOneVersionOneKeyScanner, SubTableDeltaScannerOption, SubTableSimpleScannerOption,
};

use crate::mvcc_index::hybrid_hash::attached_container_page::attached_container_common::*;

// ----------------- SCANNER END ------------------------------

type Result<T> = core::result::Result<T, HashTableAccessMethodError>;
impl<T: MemPool> DHashSubTable<T> {
    pub fn test_singlethread_rehash(&self) {
        self.rehash(self.bucket_num() * 2);
    }

    /// assume buckets is not locked!
    ///
    pub fn bucket_num(&self) -> u32 {
        self.buckets_rwlock.read().unwrap().len() as u32
    }

    pub fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        num_buckets: usize,
        // meta: &Arc<(PageId, AtomicU32)>,
    ) -> Self {
        let mut bucket_entry_vec = vec![];
        for _ in 0..num_buckets {
            let mut page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let pid = page.get_id();
            let fid = page.frame_id();

            let mut new_page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let new_pid = new_page.get_id();

            <Page as AttachedPage>::init(&mut *new_page);
            <Page as TableSlotsPage>::init(&mut *page, new_pid);
            drop(page);

            bucket_entry_vec.push(BucketEntry::new_with_frame_id(pid, fid));
        }
        Self {
            c_key,
            mem_pool,
            buckets_rwlock: RwLock::new(bucket_entry_vec),
            rehash_mutex: Mutex::new(()),
            // meta: meta.clone(),
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> core::result::Result<(), AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.try_insert(key, pkey, ts, val) {
                Ok(()) => {
                    return Ok(());
                }
                Err(HashTableAccessMethodError::HashPageOutOfSpace(new_hash_size)) => {
                    // rehash

                    // log_warn!("BEFORE REHASH");
                    // self.dump_all_entry();
                    self.rehash(new_hash_size);
                    // log_warn!("AFTER REHASH");
                    // self.dump_all_entry();
                    continue;
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
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

    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> core::result::Result<bool, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.try_update_or_delete(key, pkey, ts, val, false) {
                Ok(is_updated) => {
                    return Ok(is_updated);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
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

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<Option<Vec<u8>>, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.try_get(key, pkey, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(HashTableAccessMethodError::KeyNotFound) => {
                    return Ok(None);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
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

    pub fn get_keys(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.try_get_keys(key, ts) {
                Ok(val) => {
                    return Ok(val);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
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

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> core::result::Result<bool, AccessMethodError> {
        // let base = 2;
        // let mut attempts = 0;
        loop {
            match self.try_update_or_delete(key, pkey, ts, &[], true) {
                Ok(is_updated) => {
                    return Ok(is_updated);
                }
                Err(HashTableAccessMethodError::AcquireLockFailed) => {
                    // log_debug!("acquire write lock of page failed, re-do");
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

    pub fn garbage_collect(
        &self,
        safe_ts: Timestamp,
    ) -> core::result::Result<(), AccessMethodError> {
        todo!("garbage_collect");
    }
}

/// Recent & History basic functions
impl<T: MemPool> DHashSubTable<T> {
    fn try_get_keys_inner(
        &self,
        slot: &Slot,
        key: &[u8],
        ts: Timestamp,
    ) -> (Option<(Vec<u8>, Vec<u8>)>, bool) {
        if let Some((_, _, val)) =
            find_pivot_rec_to_write(&*self.mem_pool, slot, ts, self.c_key, key, &[], true)
        {
            (val, true)
        } else {
            (None, false)
        }
    }

    fn try_get_inner(
        &self,
        slot: &Slot,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> (Option<Vec<u8>>, bool) {
        if let Some((_, _, val)) =
            find_pivot_rec_to_write(&*self.mem_pool, slot, ts, self.c_key, key, pkey, false)
        {
            (val.map(|v| v.1), true)
        } else {
            (None, false)
        }
    }

    fn insert_inner(
        &self,
        slots_page: &mut impl TableSlotsPage,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> InterPageLoc {
        let attached_page_id = slots_page.get_attached_page_id();
        let mut attached_page = {
            let pfk = PageFrameKey::new(self.c_key, attached_page_id);
            let page = write_page(&*self.mem_pool, pfk);
            page
        };
        let attached_page: &mut dyn AttachedPage = &mut *attached_page as &mut dyn AttachedPage;
        let slot_meta = SlotMeta {
            latest_version_loc: InterPageLoc::new_end(),
            prev_meta_loc: InterPageLoc::new_end(),
            remain_key: get_remain_key(key),
            remain_pkey: get_remain_pkey(pkey),
        };
        let set_meta_res = attached_page.set_slot_meta(&slot_meta);
        match set_meta_res {
            Ok(meta_off) => {
                let insert_res = attached_page.insert_first_version(val, ts, false);
                match insert_res {
                    Ok(first_version_off) => {
                        let next_version_loc_bytes = {
                            let mut b = [0_u8; 8];
                            b[..4].copy_from_slice(&u32::to_be_bytes(attached_page_id));
                            b[4..].copy_from_slice(&u32::to_be_bytes(first_version_off));
                            b
                        };
                        attached_page.write_bytes(meta_off, &next_version_loc_bytes);
                        return InterPageLoc {
                            page_id: attached_page_id,
                            b_offset: meta_off,
                        };
                    }
                    Err(HashTableAccessMethodError::OutOfSpace) => {
                        let mut new_page =
                            self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                        let new_page_id = new_page.get_id();
                        slots_page.set_attached_page_id(new_page_id);

                        let new_attached_write_page: &mut dyn AttachedPage = &mut *new_page;
                        new_attached_write_page.init();
                        let new_version_off = new_attached_write_page
                            .insert_first_version(val, ts, false)
                            .unwrap();
                        let next_version_loc_bytes = {
                            let mut b = [0_u8; 8];
                            b[..4].copy_from_slice(&u32::to_be_bytes(new_page_id));
                            b[4..].copy_from_slice(&u32::to_be_bytes(new_version_off));
                            b
                        };
                        attached_page.write_bytes(meta_off, &next_version_loc_bytes);
                        return InterPageLoc {
                            page_id: attached_page_id,
                            b_offset: meta_off,
                        };
                    }
                    Err(_) => {
                        panic!("unknown err");
                    }
                }
            }
            Err(HashTableAccessMethodError::OutOfSpace) => {
                let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
                let new_page_id = new_page.get_id();
                slots_page.set_attached_page_id(new_page_id);
                let new_attached_write_page: &mut dyn AttachedPage = &mut *new_page;
                new_attached_write_page.init();

                let slot_meta_off = new_attached_write_page.set_slot_meta(&slot_meta).unwrap();

                let new_version_off = new_attached_write_page
                    .insert_first_version(val, ts, false)
                    .unwrap();
                let next_version_loc_bytes = {
                    let mut b = [0_u8; 8];
                    b[..4].copy_from_slice(&u32::to_be_bytes(new_page_id));
                    b[4..].copy_from_slice(&u32::to_be_bytes(new_version_off));
                    b
                };
                new_attached_write_page.write_bytes(slot_meta_off, &next_version_loc_bytes);
                return InterPageLoc {
                    page_id: new_page_id,
                    b_offset: slot_meta_off,
                };
            }
            Err(_) => {
                panic!("unknown err");
            }
        }
    }

    fn try_insert(&self, key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8]) -> Result<()> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.len() as u32;

        let bucket_idx = get_hashed_bucket_index(key, bucket_num);

        let pid: u32 = buckets[bucket_idx].page_id();
        let fid = buckets[bucket_idx].frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result = try_write_page(&*self.mem_pool, page_f_key);
        let mut inserted_slots_page = match acq_result {
            None => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Some(p) => p,
        };

        if inserted_slots_page.free_space() < SLOT_SIZE as u32 {
            return Err(HashTableAccessMethodError::HashPageOutOfSpace(
                bucket_num * 2,
            ));
        }

        let meta_loc = self.insert_inner(&mut *inserted_slots_page, key, pkey, ts, val);

        let slot = Slot::new(key, pkey, meta_loc);
        <Page as TableSlotsPage>::insert_slot(&mut *inserted_slots_page, slot).unwrap();

        return Ok(());
    }

    fn try_update_or_delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
        is_delete: bool,
    ) -> Result<bool> {
        let buckets = self.buckets_rwlock.read().unwrap();
        let bucket_num = buckets.len() as u32;

        let bucket_idx = get_hashed_bucket_index(key, bucket_num);

        let pid = buckets[bucket_idx].page_id();
        let fid = buckets[bucket_idx].frame_id();
        let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
        let acq_result: Option<FrameWriteGuard<'_>> = try_write_page(&*self.mem_pool, page_f_key);
        let mut updated_page = match acq_result {
            None => {
                return Err(HashTableAccessMethodError::AcquireLockFailed);
            }
            Some(p) => p,
        };

        let want_hashes = (get_slot_hash(key), get_slot_hash(pkey));

        let slot_sli = updated_page.get_slot_slice(0).to_vec();
        Ok(slot_sli
            .iter()
            .filter(|slot| slot.key_hash() == want_hashes.0 && slot.pkey_hash() == want_hashes.1)
            .filter(|slot| slot.match_k_pk_prefix(key, pkey))
            .any(|slot| {
                add_version(
                    &mut *updated_page,
                    self.c_key,
                    slot,
                    &*self.mem_pool,
                    val,
                    ts,
                    is_delete,
                    key,
                    pkey,
                )
            }))
    }

    fn rehash(&self, hash_size: u32) -> bool {
        println!("[REHASH] rehash call to new size: {}", hash_size);
        // ensure that re-hash only does once
        let _rehash_guard = self.rehash_mutex.lock().unwrap();

        {
            let buckets = self.buckets_rwlock.read().unwrap();
            // may have duplicate rehash call
            // check if hash re-hashed before
            {
                if buckets.len() as u32 >= hash_size {
                    // log_warn!("[rehash abort]");
                    return false;
                }
                assert_eq!(buckets.len() as u32 * 2, hash_size);
            }
        }

        // we need to rehash
        let mut buckets = self.buckets_rwlock.write().unwrap();
        let old_entry_num = buckets.len() as u32;
        for _ in 0..old_entry_num {
            buckets.push(BucketEntry::new(
                0, // dummy
            ));
        }

        // log_warn!("[re-hash] old_entry_num: {:?}", old_entry_num);

        for hashed_bucket_idx in 0..old_entry_num {
            let mut new_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let new_pid = new_page.get_id();
            let new_fid = new_page.frame_id();
            let mut attached_page = self.mem_pool.create_new_page_for_write(self.c_key).unwrap();
            let attached_page_id = attached_page.get_id();
            <Page as AttachedPage>::init(&mut attached_page);
            drop(attached_page);
            <Page as TableSlotsPage>::init(&mut new_page, attached_page_id);

            buckets[(hashed_bucket_idx + old_entry_num) as usize] =
                BucketEntry::new_with_frame_id(new_pid, new_fid);

            let hashed_bucket_entry = &buckets[hashed_bucket_idx as usize];
            let hashed_pfk = PageFrameKey::new_with_frame_id(
                self.c_key,
                hashed_bucket_entry.page_id(),
                hashed_bucket_entry.frame_id(),
            );
            let mut hashed_page = write_page(&*self.mem_pool, hashed_pfk);
            <Page as TableSlotsPage>::rehash(&mut hashed_page, &mut new_page, |key_hash: u32| {
                (key_hash % hash_size) as u32 != hashed_bucket_idx
            });
            // todo!("rehash");
        }

        return true;
    }

    /// only used when we set key as sub_table hashing argument
    fn try_get_keys(&self, key: &[u8], ts: Timestamp) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = get_hashed_bucket_index(key, buckets.len() as u32);
        let read_page = {
            let pid = buckets[bucket_idx].page_id();
            let fid = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = try_read_page(&*self.mem_pool, page_f_key);
            match acq_result {
                None => {
                    return Err(HashTableAccessMethodError::AcquireLockFailed);
                }
                Some(page) => page,
            }
        };

        let want_hash = get_slot_hash(key);

        let slot_sli = read_page.get_slot_slice(0).to_vec();
        let mut ret = vec![];
        for slot in slot_sli
            .iter()
            .filter(|slot| slot.key_hash() == want_hash)
            .filter(|slot| slot.match_k_prefix(key))
        {
            let get_keys_res = self.try_get_keys_inner(slot, key, ts);
            match get_keys_res {
                (get_val, true) => {
                    if let Some(val) = get_val {
                        ret.push(val);
                    }
                    continue;
                }
                (_, false) => {
                    continue;
                }
            }
        }
        Ok(ret)
    }

    /*
        acquire 1 page lock
        if acquire lock failed -> Err(AcquireLockFailed): REDO
        if find -> return value
        return Err(keynotfound)
    */
    fn try_get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<Option<Vec<u8>>> {
        let buckets = self.buckets_rwlock.read().unwrap();

        let bucket_idx = get_hashed_bucket_index(key, buckets.len() as u32);
        let read_page = {
            let pid = buckets[bucket_idx].page_id();
            let fid = buckets[bucket_idx].frame_id();

            let page_f_key = PageFrameKey::new_with_frame_id(self.c_key, pid, fid);
            let acq_result = try_read_page(&*self.mem_pool, page_f_key);
            match acq_result {
                None => {
                    return Err(HashTableAccessMethodError::AcquireLockFailed);
                }
                Some(page) => page,
            }
        };

        let want_hashes = (get_slot_hash(key), get_slot_hash(pkey));

        let slot_sli = read_page.get_slot_slice(0).to_vec();
        for slot in slot_sli
            .iter()
            .filter(|slot| slot.key_hash() == want_hashes.0 && slot.pkey_hash() == want_hashes.1)
            .filter(|slot| slot.match_k_pk_prefix(key, pkey))
        {
            let get_res = self.try_get_inner(slot, key, pkey, ts);
            match get_res {
                (get_val, true) => {
                    return Ok(get_val);
                }
                (_, false) => {
                    continue;
                }
            }
        }

        return Err(HashTableAccessMethodError::KeyNotFound);
    }

    pub fn dbg_dump_all_entry(&self) -> usize {
        let buckets = self.buckets_rwlock.read().unwrap();
        let mut num = 0_usize;
        log_warn!("<<DUMP HASH TABLE>> ------------ START AN SUBTABLE! -------");
        for entry in &*buckets {
            let pfkey = PageFrameKey::new(self.c_key, entry.page_id());
            let page = read_page(&*self.mem_pool, pfkey);
            num += <Page as TableSlotsPage>::dbg_print_slots(&page);
        }
        return num;
    }

    pub fn scan_simple(self: &Arc<Self>, ts: Timestamp) -> AllSubTableOneVersionAllKeyScanner<T> {
        AllSubTableOneVersionAllKeyScanner::new(self, ts)
    }

    pub fn scan_mvcc_entries(self: &Arc<Self>) -> AllSubTableMvccEntryScanner<T> {
        AllSubTableMvccEntryScanner::new(self)
    }

    pub fn scan_delta(
        self: &Arc<Self>,
        option: SubTableDeltaScannerOption,
    ) -> AllSubTableDeltaScanner<T> {
        AllSubTableDeltaScanner::new(self, option)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        bp::{get_in_mem_pool, ContainerKey},
        mvcc_index::{
            hybrid_hash::{
                hash_join_table_common::DEFAULT_NUM_BUCKETS,
                hybrid_hash_table::hybrid_hash_sub_table::DHashSubTable,
            },
            Timestamp,
        },
        page::AVAILABLE_PAGE_SIZE,
    };

    #[test]
    fn simple_insert() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table =
            DHashSubTable::new_with_bucket_num(c_key, mem_pool, DEFAULT_NUM_BUCKETS);
        hash_join_table.insert(&[1], &[1], 1, &[1]).unwrap();
        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn many_inserts_until_rehash() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = DHashSubTable::new_with_bucket_num(c_key, mem_pool, 1);

        let pair_space_need = 54;
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            hash_join_table
                .insert(
                    format!("{:06}", i).as_bytes(),
                    format!("{:06}", i).as_bytes(),
                    i as Timestamp,
                    format!("{:06}", i).as_bytes(),
                )
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result = hash_join_table.get(
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                i as Timestamp,
            );
            assert_eq!(
                get_result.unwrap().unwrap(),
                format!("{:06}", i).as_bytes().to_vec()
            );
        }
    }
}
