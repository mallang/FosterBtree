use core::str;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Seek, Write},
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use tempfile::tempfile;

use crate::{
    bp::{ContainerKey, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{MvccIndex, Timestamp, TxId},
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

use super::cuckoo_optimistic::{
    mvcc_hash_join_cuckoo_common::CuckooAccessMethodError,
    mvcc_hash_join_cuckoo_table::{
        CuckooHashTable, CuckooHistoryHashTable, CuckooRecentHashTable, ScanTsWithBucketsReadGuard
    },
};

pub const HASHER_KEYS: [(u64, u64); 2] = [(0, 0), (1, 1)];
pub const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>();
pub const BUCKET_ENTRY_SIZE: usize = PAGE_ID_SIZE;
pub const DEFAULT_NUM_BUCKETS: usize = 16;

pub struct HashJoinTable<T: MemPool> {
    mem_pool: Arc<T>, // TODO: check may be deleted
    c_key: ContainerKey,

    meta_page_id: PageId, // fixed
    meta_frame_id: AtomicU32,

    recent_hash_table: CuckooHashTable<T>,
    history_hash_table: CuckooHashTable<T>,
}

impl<T: MemPool> MvccIndex for HashJoinTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = CuckooAccessMethodError;
    type MemPoolType = T;
    type DeltaIter = MyDeltaScanIter<T>;
    type Iter = MyScanIter<T>;
    type ScanKeyIter = MyScanKeyIter<T>;
    fn create(c_key: ContainerKey, mem_pool: Arc<Self::MemPoolType>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
    }

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::mvcc_index::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.insert_inner(key, pkey, ts, tx_id, value)
    }

    fn get(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: crate::mvcc_index::Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.get_inner(key, pkey, ts)
    }

    fn get_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        let mut ret = vec![];
        let recent_ret = self.recent().get_all(key, ts)?;
        ret.extend(recent_ret);
        let history_ret = self.history().get_all(key, ts)?;
        ret.extend(history_ret);
        Ok(ret)
    }

    fn scan(&self, ts: Timestamp) -> Result<Self::Iter, Self::Error> {
        let ret = self.scan_inner(ts);
        Ok(ret)
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::mvcc_index::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.update_inner(key, pkey, ts, tx_id, value)
    }

    fn delete(
        &self,
        key: &Self::Key,
        pkey: &Self::PKey,
        ts: crate::mvcc_index::Timestamp,
        tx_id: crate::mvcc_index::TxId,
    ) -> Result<(), Self::Error> {
        self.delete_inner(key, pkey, ts, tx_id)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<Self::DeltaIter, Self::Error> {
        Ok(self.delta_scan_inner(from_ts, to_ts))
    }

    fn scan_key(&self, key: &Self::Key, ts: Timestamp) -> Result<Self::ScanKeyIter, Self::Error> {
        Ok(self.scan_key_inner(ts, key))
    }

    fn garbage_collect(&self, safe_ts: crate::mvcc_index::Timestamp) -> Result<(), Self::Error> {
        todo!()
    }
}

pub struct MyScanIter<T: MemPool> {
    history: ScanTsWithBucketsReadGuard<T>,
    recent: ScanTsWithBucketsReadGuard<T>,
}

impl<T: MemPool> MyScanIter<T> {
    pub fn new(
        history: ScanTsWithBucketsReadGuard<T>,
        recent: ScanTsWithBucketsReadGuard<T>,
    ) -> Self {
        Self { history, recent }
    }
}

impl<T: MemPool> Iterator for MyScanIter<T> {
    type Item = (Vec<u8>, Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.recent.next();
        if item.is_none() {
            return self.history.next();
        }
        return item;
    }
}

pub struct MyScanKeyIter<T: MemPool> {
    history: ScanTsWithBucketsReadGuard<T>,
    recent: ScanTsWithBucketsReadGuard<T>,
}

impl<T: MemPool> MyScanKeyIter<T> {
    pub fn new(
        history: ScanTsWithBucketsReadGuard<T>,
        recent: ScanTsWithBucketsReadGuard<T>,
    ) -> Self {
        Self { history, recent }
    }
}

impl<T: MemPool> Iterator for MyScanKeyIter<T> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        let mut item = self.recent.next();
        if item.is_none() {
            item = self.history.next();
        }

        if item.is_none() {
            return None;
        } else {
            let (_k, pk, v) = item.unwrap();
            return Some((pk, v));
        }
    }
}

pub struct MyDeltaScanIter<T: MemPool> {
    from_ts_file: BufReader<File>,
    to_ts_file: BufReader<File>,
    to_ts: Timestamp,
    from_ts: Timestamp,
    table: Arc<HashJoinTable<T>>,
}

impl<T: MemPool> MyDeltaScanIter<T> {
    pub fn new(
        from_ts_iter: MyScanIter<T>,
        to_ts_iter: MyScanIter<T>,
        table: Arc<HashJoinTable<T>>,
        to_ts: Timestamp,
        from_ts: Timestamp,
    ) -> Self {
        Self {
            from_ts_file: Self::create_temp_file(from_ts_iter),
            to_ts_file: Self::create_temp_file(to_ts_iter),
            table,
            to_ts,
            from_ts,
        }
    }
    // key, pkey, val
    fn create_temp_file(iter: MyScanIter<T>) -> BufReader<File> {
        let tmp_file = tempfile::tempfile().unwrap();
        let mut tmp_writer = BufWriter::new(&tmp_file);
        for (key, pkey, value) in iter {
            let mut encode_bytes = Vec::<u8>::new();
            let space_need_pair = Self::space_need_pair(&key, &pkey, &value);
            encode_bytes.resize(space_need_pair, 0);
            encode_bytes[0..size_of::<u32>()].copy_from_slice(&((key.len() as u32).to_be_bytes()));
            encode_bytes[0..size_of::<u32>()].copy_from_slice(&((pkey.len() as u32).to_be_bytes()));
            encode_bytes[0..size_of::<u32>()]
                .copy_from_slice(&((value.len() as u32).to_be_bytes()));
            encode_bytes.extend(key);
            encode_bytes.extend(pkey);
            encode_bytes.extend(value);
            assert_eq!(encode_bytes.len(), space_need_pair);
            tmp_writer.write_all(&encode_bytes).unwrap();
        }
        tmp_writer.flush().unwrap();
        drop(tmp_writer);
        BufReader::new(tmp_file)
    }
    fn space_need_pair(key: &[u8], pkey: &[u8], val: &[u8]) -> usize {
        size_of::<u32>() * 3 + key.len() + pkey.len() + val.len()
    }
}

impl<T: MemPool> Iterator for MyDeltaScanIter<T> {
    type Item = (Vec<u8>, Vec<u8>, crate::mvcc_index::Delta<Vec<u8>>);
    fn next(&mut self) -> Option<Self::Item> {
        /*
            first read whole to_ts_file
            * to_ts exist and from_ts no exist: insert
            * to_ts exist and from_ts exist: check update
            * if not update: search a new to_ts_pair
        */
        '_find_delta_in_to_ts_pairs: loop {
            let to_ts_pair = {
                let is_eof = self.to_ts_file.fill_buf().unwrap().is_empty();
                if is_eof {
                    None
                } else {
                    let mut len_meta_buffer = [0_u8; size_of::<u32>() * 3];
                    match self.to_ts_file.read_exact(&mut len_meta_buffer) {
                        Err(_e) => {
                            panic!("should not occur!");
                        }
                        Ok(_) => {
                            let key_len = u32::from_be_bytes(
                                len_meta_buffer[0..size_of::<u32>()].try_into().unwrap(),
                            );
                            let pkey_len = u32::from_be_bytes(
                                len_meta_buffer[size_of::<u32>()..size_of::<u32>() * 2]
                                    .try_into()
                                    .unwrap(),
                            );
                            let val_len = u32::from_be_bytes(
                                len_meta_buffer[size_of::<u32>() * 2..size_of::<u32>() * 3]
                                    .try_into()
                                    .unwrap(),
                            );
                            let mut k_buffer = Vec::<u8>::new();
                            let mut pk_buffer = Vec::<u8>::new();
                            let mut v_buffer = Vec::<u8>::new();
                            k_buffer.resize(key_len as usize, 0);
                            pk_buffer.resize(pkey_len as usize, 0);
                            v_buffer.resize(val_len as usize, 0);

                            self.to_ts_file.read_exact(&mut k_buffer).unwrap();
                            self.to_ts_file.read_exact(&mut pk_buffer).unwrap();
                            self.to_ts_file.read_exact(&mut v_buffer).unwrap();

                            Some((k_buffer, pk_buffer, v_buffer))
                        }
                    }
                }
            };

            if let Some((key, pkey, to_ts_val)) = to_ts_pair {
                let from_ts_get_result = self.table.get_inner(&key, &pkey, self.from_ts);
                match from_ts_get_result {
                    Ok(None) => {
                        // inserted
                        return Some((key, pkey, crate::mvcc_index::Delta::Inserted(to_ts_val)));
                    }
                    Ok(Some(from_ts_val)) => {
                        // check if updated

                        if &to_ts_val != &from_ts_val {
                            // updated
                            return Some((key, pkey, crate::mvcc_index::Delta::Updated(to_ts_val)));
                        }
                        // not updated
                        continue;
                    }
                    Err(_) => {
                        panic!("should not occur!");
                    }
                }
            } else {
                // to_ts read all pairs out
                break;
            }
        }

        /*
            second read whole from_ts_file
            * from_ts exist and to_ts no exist: delete
        */
        '_find_delta_in_from_ts_pairs: loop {
            let from_ts_pair = {
                let is_eof = self.from_ts_file.fill_buf().unwrap().is_empty();
                if is_eof {
                    None
                } else {
                    let mut len_meta_buffer = [0_u8; size_of::<u32>() * 3];
                    match self.from_ts_file.read_exact(&mut len_meta_buffer) {
                        Err(_e) => {
                            panic!("should not occur!");
                        }
                        Ok(_) => {
                            let key_len = u32::from_be_bytes(
                                len_meta_buffer[0..size_of::<u32>()].try_into().unwrap(),
                            );
                            let pkey_len = u32::from_be_bytes(
                                len_meta_buffer[size_of::<u32>()..size_of::<u32>() * 2]
                                    .try_into()
                                    .unwrap(),
                            );
                            let val_len = u32::from_be_bytes(
                                len_meta_buffer[size_of::<u32>() * 2..size_of::<u32>() * 3]
                                    .try_into()
                                    .unwrap(),
                            );
                            let mut k_buffer = Vec::<u8>::new();
                            let mut pk_buffer = Vec::<u8>::new();
                            let mut v_buffer = Vec::<u8>::new();
                            k_buffer.resize(key_len as usize, 0);
                            pk_buffer.resize(pkey_len as usize, 0);
                            v_buffer.resize(val_len as usize, 0);

                            self.from_ts_file.read_exact(&mut k_buffer).unwrap();
                            self.from_ts_file.read_exact(&mut pk_buffer).unwrap();
                            self.from_ts_file.read_exact(&mut v_buffer).unwrap();

                            Some((k_buffer, pk_buffer, v_buffer))
                        }
                    }
                }
            };

            if let Some((key, pkey, _)) = from_ts_pair {
                let to_ts_get_result = self.table.get_inner(&key, &pkey, self.to_ts);
                match to_ts_get_result {
                    Ok(None) => {
                        // deleted
                        return Some((key, pkey, crate::mvcc_index::Delta::Deleted));
                    }
                    Ok(_) => {
                        continue;
                    }
                    Err(_) => {
                        panic!("should not occur!");
                    }
                }
            } else {
                // from_ts read all pairs out
                break;
            }
        }

        return None;
    }
}

impl<T: MemPool> HashJoinTable<T> {
    fn recent(&self) -> &impl CuckooRecentHashTable<T> {
        &self.recent_hash_table
    }
    fn recent_mut(&mut self) -> &mut impl CuckooRecentHashTable<T> {
        &mut self.recent_hash_table
    }
    fn history(&self) -> &impl CuckooHistoryHashTable<T> {
        &self.history_hash_table
    }
    fn history_mut(&mut self) -> &mut impl CuckooHistoryHashTable<T> {
        &mut self.history_hash_table
    }

    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEFAULT_NUM_BUCKETS)
    }

    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        MvccHashJoinCuckooMetaPage::init(&mut *meta_page, num_buckets);

        let recent_table =
            <CuckooHashTable<T> as CuckooRecentHashTable<T>>::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);
        let history_table =
            <CuckooHashTable<T> as CuckooHistoryHashTable<T>>::new_with_bucket_num(c_key, mem_pool.clone(), num_buckets);

        let recent_page_ids = <CuckooHashTable<T> as CuckooRecentHashTable<T>>::get_all_bucket_page_ids(&recent_table);
        let history_page_ids = <CuckooHashTable<T> as CuckooHistoryHashTable<T>>::get_all_bucket_page_ids(&history_table);

        <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_recent(
            &mut *meta_page,
            &recent_page_ids,
        );
        <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_history(
            &mut *meta_page,
            &history_page_ids,
        );

        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            recent_hash_table: recent_table,
            history_hash_table: history_table,
        }
    }

    pub fn insert_inner(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: crate::mvcc_index::Timestamp,
        tx_id: TxId,
        value: Vec<u8>,
    ) -> Result<(), CuckooAccessMethodError> {
        let insert_res = self.recent().insert(&key, &pkey, ts, &value);
        match insert_res {
            Ok((rehash_flag, old_delete_marker)) => {
                if rehash_flag {
                    let (meta_page_id, meta_frame_id) = {
                        (
                            self.meta_page_id,
                            self.meta_frame_id
                                .load(std::sync::atomic::Ordering::Acquire),
                        )
                    };
                    let page_frame_key =
                        PageFrameKey::new_with_frame_id(self.c_key, meta_page_id, meta_frame_id);
                    let mut meta_page = self.write_page(page_frame_key);

                    let entries = self.recent().get_all_bucket_page_ids();
                    let old_recent_entries_num =
                        <Page as MvccHashJoinCuckooMetaPage>::get_recent_bucket_num(&meta_page);
                    if old_recent_entries_num < entries.len() {
                        assert_eq!(old_recent_entries_num * 2, entries.len());
                        <Page as MvccHashJoinCuckooMetaPage>::set_recent_bucket_num(
                            &mut *meta_page,
                            old_recent_entries_num * 2,
                        );
                        <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_recent(
                            &mut *meta_page,
                            &entries,
                        );
                    }
                    drop(meta_page);
                }
                if let Some(old_delete_start_ts) = old_delete_marker {
                    let _todo = self.history().insert_deleted(&key, &pkey, old_delete_start_ts, ts);
                    todo!("check rehash and update meta page");
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn get_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Vec<u8>>, CuckooAccessMethodError> {
        let recent_val = self.recent().get(key, pkey, ts);
        match recent_val {
            Ok(val) => Ok(Some(val)),
            // delete marker works -> not find in recent means not find in both recent and history
            Err(CuckooAccessMethodError::KeyNotFound) => {
                log_warn!("[HashJoinTable::get_inner] return KeyNotFound in recent table!");    
                Ok(None)
            },
            Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp) => {
                log_warn!("[HashJoinTable::get_inner] return KeyFoundButInvalidTS in recent table!");    
                let history_val = self.history().get(key, pkey, ts);
                // log_warn!("try to find in history");
                match history_val {
                    Ok(val) => Ok(Some(val)),
                    Err(CuckooAccessMethodError::KeyNotFound) => Ok(None),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn update_inner(
        &self,
        key: Vec<u8>,
        pkey: Vec<u8>,
        ts: Timestamp,
        _tx_id: TxId,
        val: Vec<u8>,
    ) -> Result<(), CuckooAccessMethodError> {
        let old_result = self.recent().update(&key, &pkey, ts, &val);
        match old_result {
            Ok((old_ts, old_val, recent_rehash_flag)) => {
                if old_ts < ts {
                    let history_insert_res = self
                        .history()
                        .insert(&key, &pkey, old_ts, ts, &old_val);
                    match history_insert_res {
                        Ok(rehash_flag) => {
                            if rehash_flag {
                                // rehash into meta page
                                let (meta_page_id, meta_frame_id) = {
                                    (
                                        self.meta_page_id,
                                        self.meta_frame_id
                                            .load(std::sync::atomic::Ordering::Acquire),
                                    )
                                };
                                let page_frame_key = PageFrameKey::new_with_frame_id(
                                    self.c_key,
                                    meta_page_id,
                                    meta_frame_id,
                                );
                                let mut meta_page = self.write_page(page_frame_key);

                                let entries =
                                    self.history().get_all_bucket_page_ids();
                                let old_history_entries_num =
                                    <Page as MvccHashJoinCuckooMetaPage>::get_history_bucket_num(
                                        &*&meta_page,
                                    );
                                if old_history_entries_num < entries.len() {
                                    assert_eq!(old_history_entries_num * 2, entries.len());
                                    <Page as MvccHashJoinCuckooMetaPage>::set_history_bucket_num(
                                        &mut *meta_page,
                                        old_history_entries_num * 2,
                                    );
                                    <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_history(
                                        &mut *meta_page,
                                        &entries,
                                    );
                                }
                                drop(meta_page);
                            }
                        }
                        Err(e) => {
                            panic!("should not happen! err: {:?}", e);
                        }
                    }
                } else {
                    // update in the same ts => need not insert in history
                    // DO NOTHING HERE
                }
                if recent_rehash_flag {
                    let (meta_page_id, meta_frame_id) = {
                        (
                            self.meta_page_id,
                            self.meta_frame_id
                                .load(std::sync::atomic::Ordering::Acquire),
                        )
                    };
                    let page_frame_key =
                        PageFrameKey::new_with_frame_id(self.c_key, meta_page_id, meta_frame_id);
                    let mut meta_page = self.write_page(page_frame_key);

                    let entries = self.recent().get_all_bucket_page_ids();
                    let old_recent_entries_num =
                        <Page as MvccHashJoinCuckooMetaPage>::get_recent_bucket_num(&*&meta_page);
                    assert_eq!(old_recent_entries_num * 2, entries.len());

                    <Page as MvccHashJoinCuckooMetaPage>::set_recent_bucket_num(
                        &mut *meta_page,
                        old_recent_entries_num * 2,
                    );
                    <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_recent(
                        &mut *meta_page,
                        &entries,
                    );
                    drop(meta_page);
                }

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn delete_inner(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), CuckooAccessMethodError> {
        let old_result = self.recent().delete(&key, &pkey, ts);
        match old_result {
            Ok((old_ts, old_val)) => {
                // Insert the old record into the history chain
                log_warn!(
                    "old_ts: {:?}, old_val: {:?}",
                    old_ts,
                    str::from_utf8(&old_val[..])
                );
                // self.history_hash_table
                //     .insert(&key, &pkey, old_ts, ts, &old_val)
                let history_insert_res = self
                    .history()
                    .insert(&key, &pkey, old_ts, ts, &old_val);
                match history_insert_res {
                    Ok(rehash_flag) => {
                        if rehash_flag {
                            let (meta_page_id, meta_frame_id) = {
                                (
                                    self.meta_page_id,
                                    self.meta_frame_id
                                        .load(std::sync::atomic::Ordering::Acquire),
                                )
                            };
                            let page_frame_key = PageFrameKey::new_with_frame_id(
                                self.c_key,
                                meta_page_id,
                                meta_frame_id,
                            );
                            let mut meta_page = self.write_page(page_frame_key);

                            let entries = self.history().get_all_bucket_page_ids();
                            let old_history_entries_num =
                                <Page as MvccHashJoinCuckooMetaPage>::get_history_bucket_num(
                                    &*&meta_page,
                                );
                            if old_history_entries_num < entries.len() {
                                assert_eq!(old_history_entries_num * 2, entries.len());
                                <Page as MvccHashJoinCuckooMetaPage>::set_history_bucket_num(
                                    &mut *meta_page,
                                    old_history_entries_num * 2,
                                );
                                <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_history(
                                    &mut *meta_page,
                                    &entries,
                                );
                            }
                            drop(meta_page);
                        }
                        Ok(())
                    }
                    Err(e) => {
                        panic!("should not happen! err: {:?}", e);
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn scan_inner(&self, ts: Timestamp) -> MyScanIter<T> {
        let recent_scan_iter = self.recent().scan(ts);
        let history_scan_iter = self.history().scan(ts);
        let scan_iter = MyScanIter::new(history_scan_iter, recent_scan_iter);
        scan_iter
    }

    pub fn scan_key_inner(&self, ts: Timestamp, key: &[u8]) -> MyScanKeyIter<T> {
        let recent_scan_iter = self.recent().scan_key(ts, key);
        let history_scan_iter = self.history().scan_key(ts, key);
        let scan_iter = MyScanKeyIter::new(history_scan_iter, recent_scan_iter);
        scan_iter
    }

    pub fn delta_scan_inner(&self, from_ts: Timestamp, to_ts: Timestamp) -> MyDeltaScanIter<T> {
        todo!()
    }

    fn write_page(&self, page_key: PageFrameKey) -> FrameWriteGuard {
        loop {
            let page = self.mem_pool.get_page_for_write(page_key);
            match page {
                Ok(page) => {
                    return page;
                }
                Err(MemPoolStatus::FrameWriteLatchGrantFailed) => {
                    log_warn!(
                        "Exclusive page latch grant failed: {:?}. Will retry",
                        page_key
                    );
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
}

/*
    <Recent Bucket Num> <History Bucket Num> [Recent Page Id ...] [History Page Id...]

*/
pub trait MvccHashJoinCuckooMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);
    fn set_history_bucket_num(&mut self, num_buckets: usize);
    fn set_recent_bucket_num(&mut self, num_buckets: usize);
    fn get_recent_bucket_num(&self) -> usize;
    fn get_history_bucket_num(&self) -> usize;

    fn get_recent_bucket_entry(&self, index: usize) -> PageId;
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId);
    fn get_history_bucket_entry(&self, index: usize) -> PageId;
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId);

    fn read_all_entries_recent(&self) -> Vec<PageId>;
    fn write_all_entries_recent(&mut self, entries: &[PageId]);

    fn read_all_entries_history(&self) -> Vec<PageId>;
    fn write_all_entries_history(&mut self, entries: &[PageId]);
}

impl MvccHashJoinCuckooMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE * 2 + (num_buckets * BUCKET_ENTRY_SIZE) * 2;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "Page size is insufficient for the number of buckets",
        );
        self.set_recent_bucket_num(num_buckets);
        self.set_history_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn set_recent_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn set_history_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn get_recent_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_history_bucket_num(&self) -> usize {
        let bytes = &self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_recent_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let recent_pid = PageId::from_be_bytes(bytes[0..PAGE_ID_SIZE].try_into().unwrap());

        recent_pid
    }
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE].copy_from_slice(&entry.to_be_bytes());
    }
    fn get_history_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE)
            + recent_num_buckets * BUCKET_ENTRY_SIZE
            + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let history_pid = PageId::from_be_bytes(bytes[0..PAGE_ID_SIZE].try_into().unwrap());

        history_pid
    }
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE)
            + recent_num_buckets * BUCKET_ENTRY_SIZE
            + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE].copy_from_slice(&entry.to_be_bytes());
    }

    fn read_all_entries_recent(&self) -> Vec<PageId> {
        let num_buckets = self.get_recent_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_recent_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_recent(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_recent_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_recent_bucket_entry(index, entry);
        }
    }

    fn read_all_entries_history(&self) -> Vec<PageId> {
        let num_buckets = self.get_history_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_history_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_history(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_history_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_history_bucket_entry(index, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_in_mem_pool;

    #[test]
    fn test_meta_page_init() {
        let mut page = Page::new_empty();
        let num_buckets = 10;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);
        let stored_num_buckets = page.get_recent_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);

        let stored_num_buckets = page.get_history_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_num() {
        let mut page = Page::new_empty();
        let num_buckets = 15;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);
        page.set_recent_bucket_num(num_buckets + 2);
        let stored_num_buckets = page.get_recent_bucket_num();
        assert_eq!(stored_num_buckets, num_buckets + 2);
    }

    #[test]
    fn test_meta_page_set_and_get_bucket_entry() {
        let mut page = Page::new_empty();
        let num_buckets = 5;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);

        for index in 0..num_buckets {
            let entry = (index as u32 + 100) as PageId;
            page.set_recent_bucket_entry(index, &entry);
        }

        for index in 0..num_buckets {
            let entry = (index as u32 + 200) as PageId;
            page.set_history_bucket_entry(index, &entry);
        }

        for index in 0..num_buckets {
            let entry = page.get_recent_bucket_entry(index);
            assert_eq!(entry, index as u32 + 100);
            let entry = page.get_history_bucket_entry(index);
            assert_eq!(entry, index as u32 + 200);
        }
    }

    #[test]
    fn test_meta_page_read_and_write_all_entries() {
        let mut page = Page::new_empty();
        let num_buckets = 8;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);

        let mut recent_entries = Vec::new();
        let mut history_entries = Vec::new();
        for index in 0..num_buckets {
            let entry = (index as u32 + 500) as PageId;
            recent_entries.push(entry);
            history_entries.push(entry + 100);
        }

        // Write all entries
        page.write_all_entries_recent(&recent_entries);
        page.write_all_entries_history(&history_entries);

        // Read all entries
        let entries = page.read_all_entries_recent();
        assert_eq!(entries, recent_entries);

        let entries = page.read_all_entries_history();
        assert_eq!(entries, history_entries);
    }

    #[test]
    #[should_panic(expected = "Bucket index out of bounds")]
    fn test_meta_page_get_bucket_entry_out_of_bounds() {
        let mut page = Page::new_empty();
        let num_buckets = 3;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);

        // This should panic because index is equal to num_buckets
        let _entry = page.get_recent_bucket_entry(num_buckets);
    }

    #[test]
    #[should_panic(expected = "Bucket index out of bounds")]
    fn test_meta_page_get_bucket_entry_out_of_bounds2() {
        let mut page = Page::new_empty();
        let num_buckets = 3;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);

        // This should panic because index is equal to num_buckets
        let _entry = page.get_history_bucket_entry(num_buckets);
    }

    #[test]
    #[should_panic(expected = "Page size is insufficient for the number of buckets")]
    fn test_meta_page_init_too_many_buckets() {
        let mut page = Page::new_empty();
        let num_buckets = (AVAILABLE_PAGE_SIZE - BUCKET_NUM_SIZE) / BUCKET_ENTRY_SIZE + 1;
        <Page as MvccHashJoinCuckooMetaPage>::init(&mut page, num_buckets);
    }

    const SLOT_KEY_PREFIX_SIZE: usize = 8;
    const SLOT_PKEY_PREFIX_SIZE: usize = 8;
    const SLOT_SIZE: usize = 40;
    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        SLOT_SIZE as u32 + remain_key_size as u32 + remain_pkey_size as u32 + val.len() as u32
    }

    #[test]
    fn simple_insert_cuckoo() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new(c_key, mem_pool);
        hash_join_table
            .insert_inner(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();
        let get_result = hash_join_table.get_inner(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn many_inserts_until_rehash() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new_with_bucket_num(c_key, mem_pool, 1);

        let pair_space_need = space_need(&vec![1], &vec![1], &vec![1]);
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            let i = i as u8;
            hash_join_table
                .insert_inner(vec![i], vec![i], 1, 1, vec![i])
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result =
                hash_join_table.get_inner(&(vec![i as u8])[..], &(vec![i as u8])[..], 1);
            assert_eq!(get_result.unwrap().unwrap(), &[i as u8]);
        }
    }

    #[test]
    fn test_concurrent_inserts_and_reads_cuckoo() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(HashJoinTable::new_with_bucket_num(c_key, mem_pool, 1));

        let hash_join_table_clone = hash_join_table.clone();
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (0..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                hash_join_table_clone
                    .insert_inner(key, pkey, i as u64, 1, value)
                    .unwrap();
            }
        });

        for i in (1..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table
                .insert_inner(key, pkey, i as u64, 1, value)
                .unwrap();
        }

        // Read entries while inserts are happening
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get_inner(&key, &pkey, i as u64);
        }

        handle.join().unwrap();
        log_warn!("FINISH JOIN!!!!!!!!!!");
        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let expected_value = format!("value{}", i).into_bytes();
                let retrieved_val = hash_join_table_clone
                    .get_inner(&key, &pkey, i as u64)
                    .unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = hash_join_table.get_inner(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();
    }

    #[test]
    fn simple_update_cuckoo_same_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new(c_key, mem_pool);
        hash_join_table
            .insert_inner(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .update_inner(vec![1], vec![1], 1, 1, vec![2])
            .unwrap();

        let get_result = hash_join_table.get_inner(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);

        let get_result = hash_join_table.get_inner(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);
    }

    #[test]
    fn simple_update_cuckoo_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new(c_key, mem_pool);
        hash_join_table
            .insert_inner(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .update_inner(vec![1], vec![1], 2, 1, vec![2])
            .unwrap();

        let get_result = hash_join_table.get_inner(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);

        let get_result = hash_join_table.get_inner(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn concurrent_inserts_and_update() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(HashJoinTable::new_with_bucket_num(c_key, mem_pool, 1));

        let hash_join_table_clone = hash_join_table.clone();

        // 1..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table
                .insert_inner(key, pkey, 1, 1, value)
                .unwrap();
        }

        // 1..1000..2 updates
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i * 2 + 10000).into_bytes();
                hash_join_table_clone
                    .update_inner(key, pkey, 2, 1, value)
                    .unwrap();
            }
        });

        // Read entries while inserts are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get_inner(&key, &pkey, 2);
        }

        // 0..1000..2 updates
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i * 2 + 10000).into_bytes();
            hash_join_table
                .update_inner(key, pkey, 2 as u64, 1, value)
                .unwrap();
        }

        handle.join().unwrap();

        log_warn!("FINISH JOIN!!!!!!!!!!");
        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
                let retrieved_val = hash_join_table_clone.get_inner(&key, &pkey, 2).unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
            let retrieved_val = hash_join_table.get_inner(&key, &pkey, 2).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = hash_join_table.get_inner(&key, &pkey, 1).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    #[test]
    fn simple_delete_cuckoo_same_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new(c_key, mem_pool);
        hash_join_table
            .insert_inner(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        let del_result = hash_join_table.delete_inner(&(vec![1])[..], &(vec![1])[..], 0, 1);
        assert_eq!(
            del_result.err(),
            Some(CuckooAccessMethodError::KeyFoundButInvalidTimestamp)
        );

        hash_join_table
            .delete_inner(&(vec![1])[..], &(vec![1])[..], 1, 1)
            .unwrap();

        let get_result = hash_join_table.get_inner(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 2);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        // duplicate delete
        let del_result = hash_join_table.delete_inner(&(vec![1])[..], &(vec![1])[..], 1, 1);
        assert_eq!(del_result.err(), Some(CuckooAccessMethodError::KeyNotFound));
    }

    #[test]
    fn simple_delete_cuckoo_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = HashJoinTable::new(c_key, mem_pool);
        hash_join_table
            .insert_inner(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .delete_inner(&(vec![1])[..], &(vec![1])[..], 2, 1)
            .unwrap();

        let get_result = hash_join_table.get_inner(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 2);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get_inner(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn concurrent_inserts_and_delete() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(HashJoinTable::new_with_bucket_num(c_key, mem_pool, 1));

        let hash_join_table_clone = hash_join_table.clone();

        // 0..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table
                .insert_inner(key, pkey, 1, 1, value)
                .unwrap();
        }

        // 1..1000..2 deletes
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                hash_join_table_clone
                    .delete_inner(&key[..], &pkey[..], 2, 1)
                    .unwrap();
            }
        });

        // Read entries while deletes are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get_inner(&key, &pkey, 2);
        }

        // 0..1000..2 deletes
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table
                .delete_inner(&key, &pkey, 2 as u64, 1)
                .unwrap();
        }

        handle.join().unwrap();

        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after deletes are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let get_result = hash_join_table_clone.get_inner(&key, &pkey, 2);
                assert_eq!(get_result.unwrap(), None);
            }
        });
        // Verify all entries after deletes are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let get_result: Result<Option<Vec<u8>>, CuckooAccessMethodError> = hash_join_table.get_inner(&key, &pkey, 2);
            assert_eq!(get_result.unwrap(), None);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key: Vec<u8> = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let get_result = hash_join_table.get_inner(&key, &pkey, 1);
            assert_eq!(get_result.unwrap().unwrap(), expected_value);
        }
    }

    #[test]
    fn test_insert_and_scan() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(HashJoinTable::new_with_bucket_num(c_key, mem_pool, 1));

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table
                .insert_inner(key, pkey, 1, 1, value)
                .unwrap();
        }

        let scan_iter = hash_join_table.scan_inner(1);
        let mut cnt = 0;
        for pair in scan_iter {
            cnt += 1;
            let (key, pkey, value) = pair;
            assert_eq!(&key[3..], &pkey[4..]);
            assert_eq!(&pkey[4..], &value[5..]);
        }
        assert_eq!(cnt, 100);
    }
}

#[test]
fn haa() {
    let f = tempfile().unwrap();
    let mut bufw = BufWriter::new(&f);
    bufw.write_all(&format!("hahaha").into_bytes()).unwrap();
    bufw.flush().unwrap();
    drop(bufw);
    let mut bufr = BufReader::new(f);
    bufr.seek(std::io::SeekFrom::Start(0)).unwrap();
    let mut by = Vec::new();
    let a = bufr.fill_buf().unwrap().is_empty();
    assert_eq!(a, false);
    bufr.read_to_end(&mut by).unwrap();
    log_warn!("read {:?}", String::from_utf8(by).unwrap());
    let a = bufr.fill_buf().unwrap().is_empty();
    assert_eq!(a, true);
}
