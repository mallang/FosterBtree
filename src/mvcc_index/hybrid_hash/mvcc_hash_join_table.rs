use std::{
    marker::PhantomData,
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

use crate::{
    bp::{ContainerKey, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{
        hybrid_hash::hash_join_table_common::DEFAULT_NUM_BUCKETS, Delta, MvccEntry, MvccIndex,
        Timestamp, TxId,
    },
    page::PageId,
    prelude::AccessMethodError,
};

use super::{
    hash_join_table_common::MvccHashJoinMetaPage,
    hybrid_hash_table::hybrid_hash_sub_table::{
        AllSubTableDeltaScanner, AllSubTableMvccEntryScanner, AllSubTableOneVersionAllKeyScanner,
        AllSubTableOneVersionOneKeyScanner, DHashSubTable, SubTableDeltaScannerOption,
        SubTableSimpleScannerOption,
    },
};

mod iterator {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{
        bp::MemPool,
        mvcc_index::{
            hybrid_hash::{
                hash_join_table_common::HashTableAccessMethodError,
                hybrid_hash_table::hybrid_hash_sub_table::{
                    AllSubTableDeltaScanner, AllSubTableMvccEntryScanner,
                    AllSubTableOneVersionAllKeyScanner, SubTableDeltaScannerOption,
                    SubTableSimpleScannerOption,
                },
            },
            Delta, DeltaEntry, MvccEntry, MvccIndex,
        },
    };

    use super::{OpenAddrHashTable, TableStruct};

    pub struct MvccDeltaScanner<T, MvccIdx>
    where
        T: MemPool + 'static,
        MvccIdx: MvccIndex<T, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        ite: AllSubTableDeltaScanner<T>,
        is_end: bool,
        _data: PhantomData<MvccIdx>,
        _data2: PhantomData<T>,
    }

    impl<T> Iterator for MvccDeltaScanner<T, OpenAddrHashTable<T>>
    where
        T: MemPool + 'static,
    {
        type Item = (
            <OpenAddrHashTable<T> as MvccIndex<T>>::Key,
            <OpenAddrHashTable<T> as MvccIndex<T>>::PKey,
            Delta<<OpenAddrHashTable<T> as MvccIndex<T>>::Value>,
        );
        fn next(&mut self) -> Option<Self::Item> {
            if self.is_end {
                return None;
            }
            let res = self.ite.next();
            if let Some(res) = res {
                Some((res.key, res.pkey, res.value_delta))
            } else {
                self.is_end = true;
                None
            }
        }
    }

    impl<T> MvccDeltaScanner<T, OpenAddrHashTable<T>>
    where
        T: MemPool + 'static,
    {
        pub fn new(table: &Arc<TableStruct<T>>, option: SubTableDeltaScannerOption) -> Self {
            let ite = table.scan_delta(option).into_iter();
            Self {
                ite,
                is_end: false,
                _data: Default::default(),
                _data2: Default::default(),
            }
        }
    }

    // pub struct MvccEntryScanner<T, MvccIdx>
    // where
    //     T: MemPool + 'static,
    //     MvccIdx: MvccIndex<T, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    // {
    //     ite: AllSubTableMvccEntryScanner<T>,
    //     is_end: bool,
    //     _data: PhantomData<MvccIdx>,
    //     _data2: PhantomData<T>,
    // }
    // impl<T, MvccIdx> Iterator for MvccEntryScanner<T, MvccIdx>
    // where
    //     T: MemPool + 'static,
    //     MvccIdx: MvccIndex<T, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    // {
    //     type Item = (MvccIdx::Key, MvccIdx::PKey, MvccIdx::Value);
    //     fn next(&mut self) -> Option<Self::Item> {
    //         if self.is_end {
    //             return None;
    //         }
    //         let res = self.ite.next();
    //         if let Some(res) = res {
    //             Some((res.key, res.pkey, res.value))
    //         } else {
    //             self.is_end = true;
    //             None
    //         }
    //     }
    // }

    // impl<T, MvccIdx> MvccEntryScanner<T, MvccIdx>
    // where
    //     T: MemPool + 'static,
    //     MvccIdx: MvccIndex<T, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    // {
    //     pub fn new(table: &Arc<TableStruct<T>>) -> Self {
    //         let ite = table.scan_mvcc_entries().into_iter();
    //         Self {
    //             ite,
    //             is_end: false,
    //             _data: Default::default(),
    //             _data2: Default::default(),
    //         }
    //     }
    // }
}

use iterator::*;

type TableStruct<T> = DHashSubTable<T>;
pub struct OpenAddrHashTable<T: MemPool + 'static> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    meta: Arc<(PageId, AtomicU32)>,

    hash_table: Arc<TableStruct<T>>,
}

impl<T: MemPool + 'static> MvccIndex<T> for OpenAddrHashTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = AccessMethodError;
    // type Iter = Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>;
    // type DeltaIter = Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>;
    // type ScanKeyIter = Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>;
    // type ScanAllIter = Box<dyn Iterator<Item = MvccEntry> + Send>;
    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new(c_key, mem_pool))
    }

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        _tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.hash_table.insert(&key, &pkey, ts, &value)
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.hash_table.get(key.as_ref(), pkey.as_ref(), ts)
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
        _tx_id: TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.hash_table.update(&key, &pkey, ts, &value).unwrap();
        Ok(())
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        _tx_id: TxId,
    ) -> Result<(), Self::Error> {
        self.hash_table
            .delete(key.as_ref(), pkey.as_ref(), ts)
            .unwrap();
        Ok(())
    }

    fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let ret = Box::new(AllSubTableOneVersionAllKeyScanner::<T>::new(
            &self.hash_table,
            ts,
        ));
        Ok(ret)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        let option = SubTableDeltaScannerOption {
            small_ts: from_ts,
            large_ts: to_ts,
        };
        let ret = Box::new(
            AllSubTableDeltaScanner::<T>::new(&self.hash_table, option)
                .into_iter()
                .map(|x| (x.key, x.pkey, x.value_delta)),
        );
        Ok(ret)
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let option = (ts, key.clone());
        let ret = Box::new(AllSubTableOneVersionOneKeyScanner::<T>::new(
            &self.hash_table,
            option,
        ));
        Ok(ret)
    }

    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        self.hash_table.get_keys(&key, ts)
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        self.hash_table.garbage_collect(safe_ts)?;
        Ok(())
    }

    // fn scan_all(&self) -> Result<Self::ScanAllIter, Self::Error> {
    //     Ok(Box::new(self.scan_all_inner().unwrap()))
    // }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let ret = Box::new(AllSubTableMvccEntryScanner::<T>::new(&self.hash_table));
        Ok(ret)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl<T: MemPool> OpenAddrHashTable<T> {
    pub fn test_rehash(&self) {
        self.hash_table.test_singlethread_rehash();
    }

    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEFAULT_NUM_BUCKETS)
    }

    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        let meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        let meta = Arc::new((meta_page_id, meta_frame_id));

        let hash_table = Arc::new(TableStruct::new_with_bucket_num(
            c_key,
            mem_pool.clone(),
            num_buckets,
        ));

        // <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_recent(
        //     &mut *meta_page,
        //     &recent_page_ids,
        // );
        // <Page as MvccHashJoinCuckooMetaPage>::write_all_entries_history(
        //     &mut *meta_page,
        //     &history_page_ids,
        // );

        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta,
            hash_table,
        }
    }

    fn scan_all_inner(
        &self,
    ) -> core::result::Result<AllSubTableMvccEntryScanner<T>, AccessMethodError> {
        let scan_iter = self.hash_table.scan_mvcc_entries();
        Ok(scan_iter)
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

    pub fn dump_all_entry(&self) {
        self.hash_table.dbg_dump_all_entry();
    }
}

#[cfg(test)]
mod test_ops {
    use super::*;
    use crate::bp::get_in_mem_pool;
    use crate::mvcc_index::hybrid_hash::hash_join_table_common::{
        BUCKET_ENTRY_SIZE, BUCKET_NUM_SIZE,
    };
    use crate::page::{Page, PageId, AVAILABLE_PAGE_SIZE};
    use core::str;
    const SLOT_KEY_PREFIX_SIZE: usize = 8;
    const SLOT_PKEY_PREFIX_SIZE: usize = 8;

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        (16 + val.len()) as u32
    }

    #[test]
    fn simple_insert() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::new(c_key, mem_pool);
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();
        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn many_inserts_until_rehash() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16);

        let pair_space_need = space_need(
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
            &format!("{:06}", 1).as_bytes().to_vec(),
        );
        let pairs_num_rehash = AVAILABLE_PAGE_SIZE as u32 / pair_space_need + 2;
        for i in 0..pairs_num_rehash {
            hash_join_table
                .insert(
                    format!("{:06}", i).as_bytes().to_vec(),
                    format!("{:06}", i).as_bytes().to_vec(),
                    1,
                    1,
                    format!("{:06}", i).as_bytes().to_vec(),
                )
                .unwrap();
        }

        for i in 0..pairs_num_rehash {
            let get_result = hash_join_table.get(
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                &(format!("{:06}", i).as_bytes().to_vec())[..],
                1,
            );
            assert_eq!(
                get_result.unwrap().unwrap(),
                format!("{:06}", i).as_bytes().to_vec()
            );
        }
    }

    #[test]
    fn test_many_inserts_and_reads() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        for i in (0..1000).into_iter() {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let value = format!("value__{}", i).into_bytes();
            hash_join_table
                .insert(key, pkey, i as u64, 1, value)
                .unwrap();
        }

        // log_warn!("FINISH JOIN!!!!!!!!!!");

        // Verify all entries after insertions are complete
        for i in 0..1000 {
            log_warn!("get {i}");
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    #[test]
    fn test_concurrent_inserts_and_reads() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (0..1000).into_iter().step_by(2) {
                let key = format!("key__{}", i).into_bytes();
                let pkey = format!("pkey__{}", i).into_bytes();
                let value = format!("value__{}", i).into_bytes();
                hash_join_table_clone
                    .insert(key, pkey, i as u64, 1, value)
                    .unwrap();
            }
        });

        for i in (1..1000).into_iter().step_by(2) {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let value = format!("value__{}", i).into_bytes();
            hash_join_table
                .insert(key, pkey, i as u64, 1, value)
                .unwrap();
        }

        // Read entries while inserts are happening
        for i in 0..1000 {
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, i as u64);
        }

        handle.join().unwrap();
        // log_warn!("FINISH JOIN!!!!!!!!!!");
        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key__{}", i).into_bytes();
                let pkey = format!("pkey__{}", i).into_bytes();
                let expected_value = format!("value__{}", i).into_bytes();
                let retrieved_val = hash_join_table_clone.get(&key, &pkey, i as u64).unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            log_warn!("get {i}");
            let key = format!("key__{}", i).into_bytes();
            let pkey = format!("pkey__{}", i).into_bytes();
            let expected_value = format!("value__{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, i as u64).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();
    }

    #[test]
    fn simple_update_same_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table: OpenAddrHashTable<crate::prelude::InMemPool> =
            OpenAddrHashTable::new(c_key, mem_pool);
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .update(vec![1], vec![1], 1, 1, vec![2])
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);
    }

    #[test]
    fn simple_update_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::new(c_key, mem_pool);
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .update(vec![1], vec![1], 2, 1, vec![2])
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap().unwrap(), &[2]);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn concurrent_inserts_and_update() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 1..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 1..1000..2 updates
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let value = format!("value{}", i * 2 + 10000).into_bytes();
                hash_join_table_clone
                    .update(key, pkey, 2, 1, value)
                    .unwrap();
            }
        });

        // Read entries while inserts are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, 2);
        }

        // 0..1000..2 updates
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i * 2 + 10000).into_bytes();
            hash_join_table
                .update(key, pkey, 2 as u64, 1, value)
                .unwrap();
        }

        handle.join().unwrap();

        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after insertions are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
                let retrieved_val = hash_join_table_clone.get(&key, &pkey, 2).unwrap();
                assert_eq!(retrieved_val.unwrap(), expected_value);
            }
        });
        // Verify all entries after insertions are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i * 2 + 10000).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, 2).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let retrieved_val = hash_join_table.get(&key, &pkey, 1).unwrap();
            assert_eq!(retrieved_val.unwrap(), expected_value);
        }
    }

    #[test]
    fn simple_delete_same_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::new(c_key, mem_pool);
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        let del_result = hash_join_table.delete(&(vec![1])[..], &(vec![1])[..], 0, 1);
        assert_eq!(del_result.ok(), Some(()));

        hash_join_table
            .delete(&(vec![1])[..], &(vec![1])[..], 1, 1)
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        // duplicate delete
        let del_result = hash_join_table.delete(&(vec![1])[..], &(vec![1])[..], 1, 1);
        assert_eq!(del_result.err(), None);
    }

    #[test]
    fn simple_delete_different_timestamp() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::new(c_key, mem_pool);
        hash_join_table
            .insert(vec![1], vec![1], 1, 1, vec![1])
            .unwrap();

        hash_join_table
            .delete(&(vec![1])[..], &(vec![1])[..], 2, 1)
            .unwrap();

        let get_result = hash_join_table.get(&[2], &[1], 1);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 0);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 2);
        assert_eq!(get_result.unwrap(), None);

        let get_result = hash_join_table.get(&[1], &[1], 1);
        assert_eq!(get_result.unwrap().unwrap(), &[1]);
    }

    #[test]
    fn concurrent_inserts_and_delete() {
        use std::thread;

        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 0..1000 inserts
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 1..1000..2 deletes
        let handle = thread::spawn(move || {
            // Insert entries in a separate thread
            for i in (1..1000).into_iter().step_by(2) {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                hash_join_table_clone
                    .delete(&key[..], &pkey[..], 2, 1)
                    .unwrap();
            }
        });

        // Read entries while deletes are happening
        for i in (0..1000).into_iter().step_by(10) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            // It's possible that the key hasn't been inserted yet
            let _ = hash_join_table.get(&key, &pkey, 2);
        }

        // 0..1000..2 deletes
        for i in (0..1000).into_iter().step_by(2) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table.delete(&key, &pkey, 2 as u64, 1).unwrap();
        }

        handle.join().unwrap();

        let hash_join_table_clone = hash_join_table.clone();

        let handle = thread::spawn(move || {
            // Verify all entries after deletes are complete
            for i in 0..1000 {
                let key = format!("key{}", i).into_bytes();
                let pkey = format!("pkey{}", i).into_bytes();
                let get_result = hash_join_table_clone.get(&key, &pkey, 2);
                assert_eq!(get_result.unwrap(), None);
            }
        });
        // Verify all entries after deletes are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let get_result: Result<Option<Vec<u8>>, AccessMethodError> =
                hash_join_table.get(&key, &pkey, 2);
            assert_eq!(get_result.unwrap(), None);
        }

        handle.join().unwrap();

        for i in 0..1000 {
            let key: Vec<u8> = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let get_result = hash_join_table.get(&key, &pkey, 1);
            assert_eq!(get_result.unwrap().unwrap(), expected_value);
        }
    }

    #[test]
    fn test_insert_and_scan() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        let scan_iter = hash_join_table.scan_all().unwrap();
        let mut cnt = 0;
        for pair in scan_iter {
            cnt += 1;
            let (key, pkey, value) = (pair.key, pair.pkey, pair.value);
            assert_eq!(&key[3..], &pkey[4..]);
            assert_eq!(&pkey[4..], &value[5..]);
        }
        assert_eq!(cnt, 100);
    }

    #[test]
    fn test_insert_and_get_keys() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        // 1..100 inserts
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }
        for i in (0..100).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let a = hash_join_table.scan_key(&key, 2);
            let t = a.unwrap().collect::<Vec<_>>();

            for m in t {
                log_warn!("{:?} {:?}", String::from_utf8(m.0), String::from_utf8(m.1));
            }
        }
    }

    #[test]
    fn test_double_update() {
        // Initialize the hash join table using the MvccIndex trait
        let mem_pool = get_in_mem_pool(); // You need to implement or import this function
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = OpenAddrHashTable::create(c_key, mem_pool.clone()).unwrap();

        let data_num = 1000 as usize;
        let data = (0..data_num)
            .into_iter()
            .map(|i| {
                (
                    format!("key_{:06}", i).as_bytes().to_vec(),
                    format!("pkey_{:06}", i).as_bytes().to_vec(),
                    format!("value_{:06}", i).as_bytes().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let value_new = (0..data_num)
            .into_iter()
            .map(|i| format!("new_value_{:06}", i).as_bytes().to_vec())
            .collect::<Vec<_>>();
        let value_new_new = value_new
            .clone()
            .into_iter()
            .map(|mut ve| {
                ve.push(233);
                ve
            })
            .collect::<Vec<_>>();

        // BENCH HASH_JOIN_TABLE UPDATE

        // Load data into the hash join table
        for (key, pkey, value) in &data {
            hash_join_table
                .insert(key.clone(), pkey.clone(), 0, 0, value.clone())
                .unwrap();
        }

        {
            let data_clone = data.clone();
            let value_new_clone = value_new.clone();

            for ((key, pkey, _value), new_value) in
                (data_clone).into_iter().zip((value_new_clone).into_iter())
            {
                // UPDATE data
                hash_join_table.update(key, pkey, 1, 0, new_value).unwrap();
            }
        }

        {
            let data_clone = data.clone();
            let value_new_new_clone = value_new_new.clone();

            for ((key, pkey, _value), new_value) in (data_clone)
                .into_iter()
                .zip((value_new_new_clone).into_iter())
            {
                // UPDATE data
                hash_join_table.update(key, pkey, 2, 0, new_value).unwrap();
            }
        }

        for ((key, pkey, _value), new_value) in (&data).iter().zip((&value_new_new).iter()) {
            let a = hash_join_table.get(key, pkey, 2).unwrap();
            assert_eq!(a.as_ref().unwrap(), new_value);
        }
    }

    #[ignore = "not implemented yet"]
    #[test]
    fn test_garbage_collect() {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let hash_join_table = Arc::new(OpenAddrHashTable::new_with_bucket_num(c_key, mem_pool, 16));

        let hash_join_table_clone = hash_join_table.clone();

        // 0..1000 inserts at ts 1
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            hash_join_table.insert(key, pkey, 1, 1, value).unwrap();
        }

        // 0..1000 deletes at ts 2
        for i in (0..1000).into_iter().step_by(1) {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            hash_join_table_clone
                .delete(&key[..], &pkey[..], 2, 1)
                .unwrap();
        }

        // Verify all entries after deletes are complete
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let get_result = hash_join_table_clone.get(&key, &pkey, 2);
            assert_eq!(get_result.unwrap(), None);
        }

        for i in 0..1000 {
            let key: Vec<u8> = format!("key{}", i).into_bytes();
            let pkey = format!("pkey{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let get_result = hash_join_table.get(&key, &pkey, 1);
            assert_eq!(get_result.unwrap().unwrap(), expected_value);
        }

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            log_warn!(
                "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
                str::from_utf8(&item.key),
                str::from_utf8(&item.pkey),
                str::from_utf8(&item.value),
                item.start_ts,
                item.end_ts
            );
        }
        assert_eq!(item_count, 2000);
        hash_join_table.garbage_collect(1).unwrap();

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            // log_warn!(
            //     "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
            //     str::from_utf8(&item.key),
            //     str::from_utf8(&item.pkey),
            //     str::from_utf8(&item.value),
            //     item.start_ts,
            //     item.end_ts
            // );
        }
        assert_eq!(item_count, 1000);

        hash_join_table.garbage_collect(2).unwrap();

        let mut scan_all_iter = hash_join_table.scan_all().unwrap();
        let mut item_count = 0;
        while let Some(item) = scan_all_iter.next() {
            item_count += 1;
            log_warn!(
                "item key: {:?}, item pkey: {:?}, item val: {:?}, item start ts: {}, end ts: {}",
                str::from_utf8(&item.key),
                str::from_utf8(&item.pkey),
                str::from_utf8(&item.value),
                item.start_ts,
                item.end_ts
            );
        }
        assert_eq!(item_count, 0);
    }
}
