use std::{marker::PhantomData, sync::{atomic::AtomicU32, Arc, Mutex, RwLock}};

use crate::{bp::{ContainerKey, MemPool}, log_debug, log_warn, mvcc_index::{hashtable_mu::{hash_join_table_common::{CuckooAccessMethodError, CuckooHistoryHashTable, CuckooRecentHashTable, RecentHistoryTable, DEFAULT_NUM_BUCKETS}, mvcc_hash_join_table::CuckooHashJoinTableMergeScanner}, Delta, MvccEntry, Timestamp}, page::PageId};

use super::{double_hash_common::get_first_hash_idx, double_hash_sub_table::{DoubleHashSubTable, DoubleHashSubTableScanner, HistorySubHashTable, RecentSubHashTable}};

pub struct DoubleHashTableScanner<T: MemPool> {
    table_idx: u32,
    scanner: DoubleHashSubTableScanner<T>,
    table: Arc<DoubleHashTable<T>>,
    ts: Timestamp,
    scan_all_flag: bool,
}

impl<T: MemPool> DoubleHashTableScanner<T> {
    pub fn new(table: &Arc<DoubleHashTable<T>>, ts: Timestamp, scan_all_flag: bool) -> Self {
        Self {
            table_idx: 0,
            scanner: DoubleHashSubTableScanner::new(&table.buckets_rwlock[0], ts, scan_all_flag),
            table: table.clone(),
            ts,
            scan_all_flag,
        }
    }
}

impl<T: MemPool> Iterator for DoubleHashTableScanner<T> {
    type Item = MvccEntry;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.table_idx >= self.table.buckets_rwlock.len() as u32 {
                return None;
            }
            match self.scanner.next() {
                Some(entry) => {
                    return Some(entry);
                },
                None => {
                    self.table_idx += 1;
                    if self.table_idx >= self.table.buckets_rwlock.len() as u32 {
                        return None;
                    }
                    self.scanner = DoubleHashSubTableScanner::new(&self.table.buckets_rwlock[self.table_idx as usize], self.ts, self.scan_all_flag);
                }
            } 
        }
    }
}

pub struct DoubleHashTableTupleScanner<T: MemPool> {
    scanner: DoubleHashTableScanner<T>,
    is_end: bool
}

impl<T: MemPool> DoubleHashTableTupleScanner<T> {
    pub fn new(scanner: DoubleHashTableScanner<T>) -> Self {
        Self {
            scanner,
            is_end: false,
        }
    }
}

impl<T: MemPool> Iterator for DoubleHashTableTupleScanner<T> {
    type Item = (Vec<u8>, Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end {
            return None;
        }

        loop {
            let item = self.scanner.next();
            if item.is_none() {
                self.is_end = true;
                return None;
            }

            let item = item.unwrap();
            return Some((item.key, item.pkey, item.value))
        }
    }
}

pub struct DoubleHashTableSmallTupleScanner<T: MemPool> {
    key: Vec<u8>,
    scanner: DoubleHashTableScanner<T>,
    is_end: bool
}

impl<T: MemPool> DoubleHashTableSmallTupleScanner<T> {
    pub fn new(key: Vec<u8>, scanner: DoubleHashTableScanner<T>) -> Self {
        Self {
            key,
            scanner,
            is_end: false,
        }
    }
}

impl<T: MemPool> Iterator for DoubleHashTableSmallTupleScanner<T> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end {
            return None;
        }

        loop {
            let item = self.scanner.next();
            if item.is_none() {
                self.is_end = true;
                return None;
            }

            let item = item.unwrap();
            // scan_key
            if &item.key == &self.key {
                return Some((item.pkey, item.value))
            } else {
                continue;
            }
        }
    }
}



pub struct DoubleHashTableDeltaScanner<T: MemPool> {
    is_end: bool,
    ph: PhantomData<T>,
}

impl<T: MemPool> DoubleHashTableDeltaScanner<T> {
    pub fn new() -> Self {
        Self {
            is_end: false,
            ph: PhantomData,
        }
    }
}

impl<T: MemPool> Iterator for DoubleHashTableDeltaScanner<T> {
    type Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

pub struct DoubleHashTable<T: MemPool> {
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    meta: Arc<(PageId, AtomicU32)>,

    /// shared: read & update & insert & delete & get \
    /// exclusive: rehash \
    /// ensure atomic of (num_buckets, BucketEntry.page_id, BucketEntry.frame_id)
    buckets_rwlock: Vec<Arc<DoubleHashSubTable<T>>>, // isolation btw re-hash and get/insert/update/...

    rehash_mutex: Mutex<()>, // re-hash only once
}

impl<T: MemPool> RecentHistoryTable<T> for DoubleHashTable<T> {
    type ScanAllIter = DoubleHashTableScanner<T>;
    type ScanIter = DoubleHashTableTupleScanner<T>;
    type ScanKeyIter = DoubleHashTableSmallTupleScanner<T>;
    type ScanDeltaIter = DoubleHashTableDeltaScanner<T>;

    fn scan(self: &Arc<Self>, ts: Timestamp) -> Self::ScanIter {
        let scanner = DoubleHashTableScanner::new(self, ts, false);
        DoubleHashTableTupleScanner::new(scanner)
    }

    fn scan_all(self: &Arc<Self>) -> Self::ScanAllIter {
        let scanner = DoubleHashTableScanner::new(self, Timestamp::MAX, true);
        scanner
    }

    fn scan_key(self: &Arc<Self>, ts: Timestamp, key: &[u8]) -> Self::ScanKeyIter {
        let scanner = DoubleHashTableScanner::new(self, ts, false);
        DoubleHashTableSmallTupleScanner::new(key.to_vec(), scanner)
    }
}


impl<T: MemPool> DoubleHashTable<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, meta: &Arc<(PageId, AtomicU32)>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, meta, DEFAULT_NUM_BUCKETS)
    }
    pub fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        meta: &Arc<(PageId, AtomicU32)>,
        bucket_nums: usize,
    ) -> Self {
        let buckets_rwlock = 
            (0..bucket_nums).into_iter()
                .map(|_x| 
                    Arc::new(DoubleHashSubTable::new_with_bucket_num(c_key, mem_pool.clone(), 1, meta))
                )
                .collect()
        ;
        Self {
            c_key,
            mem_pool,
            meta: meta.clone(),
            buckets_rwlock,
            rehash_mutex: Mutex::new(()),
        }
    }

    fn get_all_inner(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        let mut res = vec![];
        for bucket_idx in 0..self.buckets_rwlock.len() {
            let get_all_res = self.buckets_rwlock[bucket_idx].get_all(
                key,
                ts,
            ).unwrap();
            res.extend(get_all_res);
        }
        Ok(res)
    }

    pub fn dump_all_entry(
        &self
    ) {
        let mut num = 0;
        for idx in 0..self.buckets_rwlock.len() {
            num += self.buckets_rwlock[idx].dump_all_entry();
        }
        log_warn!("TABLE entry num: {num}");
    }
}

impl<T: MemPool> CuckooRecentHashTable<T> for DoubleHashTable<T> {
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<Option<Timestamp>, CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_insert_result = <DoubleHashSubTable<T> as RecentSubHashTable<T>>::insert(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            ts,
            val,
        );
        match subtable_insert_result {
            Ok(res) => {Ok(res)},
            Err(_) => {
                panic!("should not happen!");
            }
        }
    }
    
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_get_result = <DoubleHashSubTable<T> as RecentSubHashTable<T>>::get(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            ts,
        );
        match subtable_get_result {
            Ok(res) => {Ok(res)},
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_update_result = <DoubleHashSubTable<T> as RecentSubHashTable<T>>::update(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            ts,
            val,
        );
        match subtable_update_result {
            Ok(res) => {Ok(res)},
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_update_result = <DoubleHashSubTable<T> as RecentSubHashTable<T>>::delete(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            ts,
        );
        match subtable_update_result {
            Ok(res) => {Ok(res)},
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        self.get_all_inner(key, ts)
    }

    fn get_all_bucket_page_ids(&self) -> Vec<PageId> {
        return vec![0]
    }

 
}

impl<T: MemPool> CuckooHistoryHashTable<T> for DoubleHashTable<T> {
    fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_insert_result = <DoubleHashSubTable<T> as HistorySubHashTable<T>>::insert(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            start_ts,
            end_ts,
            val,
        );
        match subtable_insert_result {
            Ok(_) => {Ok(())},
            Err(_) => {
                panic!("should not happen!");
            }
        }
    }
    
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_get_result = <DoubleHashSubTable<T> as HistorySubHashTable<T>>::get(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            ts,
        );
        match subtable_get_result {
            Ok(res) => {Ok(res)},
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CuckooAccessMethodError> {
        self.get_all_inner(key, ts)
    }



    fn insert_deleted(
        &self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), CuckooAccessMethodError> {
        let bucket_idx = {
            let bucket_num = self.buckets_rwlock.len() as u32;
            get_first_hash_idx(key, bucket_num)        
        };
        let subtable_insert_deleted_result = <DoubleHashSubTable<T> as HistorySubHashTable<T>>::insert_deleted(
            &self.buckets_rwlock[bucket_idx],
            key,
            pkey,
            start_ts,
            end_ts,
        );
        match subtable_insert_deleted_result {
            Ok(res) => {Ok(res)},
            Err(e) => {
                return Err(e)
            }
        }
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), CuckooAccessMethodError> {
        todo!()
    }


    fn get_all_bucket_page_ids(&self) -> Vec<PageId> {
        return vec![0]
    }
}