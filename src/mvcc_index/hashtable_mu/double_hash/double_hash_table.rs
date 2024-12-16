use std::{collections::HashMap, marker::PhantomData, sync::{atomic::AtomicU32, Arc, Mutex, RwLock}};

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



pub struct DoubleHashTableMergeDeltaScanner<T: MemPool> {
    from_ts: Timestamp,
    to_ts: Timestamp,
    // is_end: bool,
    recent_table: Arc<DoubleHashTable<T>>,
    history_table: Arc<DoubleHashTable<T>>,
    cur_sub_idx: usize,
    num_sub_tables: usize,

    cur_entry_idx: usize,
    sub_table_deltas: Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>,
}

impl<T: MemPool> DoubleHashTableMergeDeltaScanner<T> {
    pub fn new(
        recent_table: &Arc<DoubleHashTable<T>>,
        history_table: &Arc<DoubleHashTable<T>>,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Self {
        let recent = DoubleHashSubTableScanner::new(
            &recent_table.buckets_rwlock[0], Timestamp::MAX, true);
        let history = DoubleHashSubTableScanner::new(
            &history_table.buckets_rwlock[0], Timestamp::MAX, true);
        let deltas = Self::gen_deltas(recent, history, from_ts, to_ts);

        Self {
            from_ts,
            to_ts,
            // is_end: false,
            recent_table: recent_table.clone(),
            history_table: history_table.clone(),
            cur_sub_idx: 0,
            num_sub_tables: recent_table.buckets_rwlock.len(),

            cur_entry_idx: 0,
            sub_table_deltas: deltas,
        }
    }

    fn gen_deltas(
        mut recent: DoubleHashSubTableScanner<T>, 
        mut history: DoubleHashSubTableScanner<T>,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)> {
        let mut from_ts_tuples = HashMap::new();
        let mut to_ts_tuples = HashMap::new();
        let mut ret = vec![];
        let ts_within = |entry: &MvccEntry, ts: Timestamp| -> bool {
            ts >= entry.start_ts && ts < entry.end_ts
        };
        while let Some(item) = recent.next() {
            if ts_within(&item, from_ts) && ts_within(&item, to_ts) {
                continue;
            }
            if ts_within(&item, from_ts) {
                from_ts_tuples.insert((item.key, item.pkey), item.value);
            } else if ts_within(&item, to_ts) {
                to_ts_tuples.insert((item.key, item.pkey), item.value);
            }
        }
        while let Some(item) = history.next() {
            if ts_within(&item, from_ts) && ts_within(&item, to_ts) {
                continue;
            }
            if ts_within(&item, from_ts) {
                from_ts_tuples.insert((item.key, item.pkey), item.value);
            } else if ts_within(&item, to_ts) {
                to_ts_tuples.insert((item.key, item.pkey), item.value);
            }
        }

        for (k_pk, from_v) in from_ts_tuples {
            if to_ts_tuples.contains_key(&k_pk) {
                let to_v = to_ts_tuples.remove(&k_pk).unwrap();
                if &to_v != &from_v {
                    ret.push((k_pk.0, k_pk.1, Delta::Updated(to_v)));
                }
            } else {
                ret.push((k_pk.0, k_pk.1, Delta::Deleted));
            }
        }

        for (k_pk, to_v) in to_ts_tuples {
            ret.push((k_pk.0, k_pk.1, Delta::Inserted(to_v)));
        }

        return ret;
    }
}

impl<T: MemPool> Iterator for DoubleHashTableMergeDeltaScanner<T> {
    type Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_sub_idx >= self.num_sub_tables {
            return None;
        }
        if self.cur_entry_idx < self.sub_table_deltas.len() {
            self.cur_entry_idx += 1;
            return Some(self.sub_table_deltas[self.cur_entry_idx - 1].clone());
        } else {
            self.cur_sub_idx += 1;
            self.cur_entry_idx = 0;
            self.sub_table_deltas.clear();

            if self.cur_sub_idx >= self.num_sub_tables {
                return None;
            }

            let recent = DoubleHashSubTableScanner::new(
                &self.recent_table.buckets_rwlock[self.cur_sub_idx], Timestamp::MAX, true);
            let history = DoubleHashSubTableScanner::new(
                &self.history_table.buckets_rwlock[self.cur_sub_idx], Timestamp::MAX, true);
            self.sub_table_deltas = Self::gen_deltas(recent, history, self.from_ts, self.to_ts);
        }

        todo!()
    }
}

pub struct DoubleHashTable<T: MemPool> {
    c_key: ContainerKey,

    mem_pool: Arc<T>,

    meta: Arc<(PageId, AtomicU32)>,

    buckets_rwlock: Vec<Arc<DoubleHashSubTable<T>>>,
}

impl<T: MemPool> RecentHistoryTable<T> for DoubleHashTable<T> {
    type ScanAllIter = DoubleHashTableScanner<T>;
    type ScanIter = DoubleHashTableTupleScanner<T>;
    type ScanKeyIter = DoubleHashTableSmallTupleScanner<T>;

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
            // rehash_mutex: Mutex::new(()),
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
        vec![]
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
        for bucket_idx in 0..self.buckets_rwlock.len() {
            <DoubleHashSubTable<T> as HistorySubHashTable<T>>::garbage_collect(&self.buckets_rwlock[bucket_idx], safe_ts)?;
        }
        Ok(())
    }


    fn get_all_bucket_page_ids(&self) -> Vec<PageId> {
        vec![]
    }
}