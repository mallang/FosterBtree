use core::panic;
use std::sync::Arc;

use crate::{
    bp::{ContainerKey, MemPool},
    log_warn,
    mvcc_index::{hash_common::DEFAULT_BUCKET_NUM, MvccEntry, MvccIndex},
    prelude::AccessMethodError,
};

use super::{
    iterators::{LinearSubTableKeyScanner, LinearSubTableScanner},
    linear_sub_table::LinearSubTable,
};

pub struct LinearHashTable<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    recent: Arc<LinearSubTable<T>>,
    history: Arc<LinearSubTable<T>>,
}

impl<T: MemPool + 'static> LinearHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_num: usize) -> Self {
        let recent =
            LinearSubTable::new_with_bucket_num(mem_pool.clone(), c_key.clone(), bucket_num);
        let history =
            LinearSubTable::new_with_bucket_num(mem_pool.clone(), c_key.clone(), bucket_num);
        Self {
            mem_pool,
            c_key,
            recent: Arc::new(recent),
            history: Arc::new(history),
        }
    }
}

impl<T: MemPool + 'static> MvccIndex<T> for LinearHashTable<T> {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = AccessMethodError;

    fn create(c_key: ContainerKey, mem_pool: Arc<T>) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new_with_bucket_num(
            c_key,
            mem_pool,
            DEFAULT_BUCKET_NUM,
        ))
    }

    fn create_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        bucket_num: usize,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self::new_with_bucket_num(c_key, mem_pool, bucket_num))
    }

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        match self.recent.insert(&entry) {
            Ok(_) => Ok(()),
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let recent_result = self.recent.get(key, pkey, ts);
        let get_entry_res = match recent_result {
            Ok(entry) => Ok(entry),
            Err(AccessMethodError::KeyNotFound)
            | Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_result = self.history.get_history(key, pkey, ts);
                match history_result {
                    Ok(entry) => Ok(entry),
                    Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
                    Err(e) => panic!("unexpected error: {:?}", e),
                }
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        };

        match get_entry_res {
            Ok(entry) => Ok(Some(entry.value)),
            Err(AccessMethodError::KeyNotFound) => Ok(None),
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    fn get_read_repair(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.get(key, pkey, ts)
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        match self.recent.update(entry.pkey(), &entry) {
            Ok(mut old_res) => {
                old_res.set_end_ts(&ts);
                self.history.insert_history(&mut old_res).unwrap();
                return Ok(());
            }
            Err(AccessMethodError::KeyNotFound) => {
                return Err(AccessMethodError::KeyNotFound);
            }
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    fn update_write_repair(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
    ) -> Result<(), Self::Error> {
        match self.recent.delete(key, pkey, ts) {
            Ok(mut old_entry) => {
                old_entry.set_end_ts(&ts);
                self.history.insert_history(&mut old_entry).unwrap();
                return Ok(());
            }
            Err(
                AccessMethodError::KeyFoundButInvalidTimestamp | AccessMethodError::KeyNotFound,
            ) => return Ok(()),
            Err(e) => {
                panic!("unexpected error: {:?}", e);
            }
        }
    }

    fn scan(
        &self,
        ts: crate::prelude::Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let recent_iter = LinearSubTableScanner::new(self.recent.clone(), Some(ts));
        Ok(Box::new(
            recent_iter.into_iter().map(|e| (e.key, e.pkey, e.value)),
        ))
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let recent_iter = LinearSubTableScanner::new(self.recent.clone(), None);
        let history_iter = LinearSubTableScanner::new(self.history.clone(), None);
        let iter = Box::new(recent_iter.chain(history_iter));
        Ok(iter)
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: crate::prelude::Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let recent_iter = LinearSubTableKeyScanner::new(self.recent.clone(), Some(ts), key.clone());
        let history_iter =
            LinearSubTableKeyScanner::new(self.history.clone(), Some(ts), key.clone());
        let iter = Box::new(recent_iter.chain(history_iter).map(|e| (e.pkey, e.value)));
        Ok(iter)
    }

    fn scan_key_vec(
        &self,
        key: &Self::Key,
        ts: crate::prelude::Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        todo!()
    }

    fn delta_scan(
        &self,
        from_ts: crate::prelude::Timestamp,
        to_ts: crate::prelude::Timestamp,
    ) -> Result<
        Box<
            dyn Iterator<Item = (Self::Key, Self::PKey, crate::mvcc_index::Delta<Self::Value>)>
                + Send,
        >,
        Self::Error,
    > {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn garbage_collect(&self, safe_ts: crate::prelude::Timestamp) -> Result<(), Self::Error> {
        todo!()
    }
}

mod tests {
    use std::sync::Arc;

    use crate::{
        bp::{ContainerKey, InMemPool},
        mvcc_index::MvccIndex,
    };

    use super::LinearHashTable;

    fn test_basic_index_ops<I>(index: &I) -> Result<(), I::Error>
    where
        I: MvccIndex<InMemPool, Key = Vec<u8>, PKey = Vec<u8>, Value = Vec<u8>>,
    {
        // 1) Insert some entries (key, pkey, ts, tx_id, value)
        index.insert(
            b"key1".to_vec(),
            b"pkey1".to_vec(),
            100,
            1,
            b"value1".to_vec(),
        )?;
        index.insert(
            b"key2".to_vec(),
            b"pkey2".to_vec(),
            100,
            1,
            b"value2".to_vec(),
        )?;
        index.insert(
            b"key1".to_vec(),
            b"pkey3".to_vec(),
            150,
            1,
            b"value3".to_vec(),
        )?;

        // 2) Get an entry at a specific timestamp
        let got = index.get(b"key1", b"pkey1", 100)?;
        assert_eq!(
            got,
            Some(b"value1".to_vec()),
            "Should find value1 at ts=100"
        );

        // 3) Update an existing entry
        index.update(
            b"key1".to_vec(),
            b"pkey3".to_vec(),
            150,
            2, // new transaction ID
            b"value3_updated".to_vec(),
        )?;

        // 4) Delete an entry
        index.delete(b"key2", b"pkey2", 100, 2)?;

        // 5) Now scan at ts=200
        let mut scan_iter = index.scan(200)?;
        let mut scanned = Vec::new();
        while let Some((key, pkey, value)) = scan_iter.next() {
            // println!("Scanned: key={:?}, pkey={:?}, value={:?}", key, pkey, value);
            scanned.push((key, pkey, value));
        }

        // 6) Verify we see "value1" for key1/pkey1, "value3_updated" for key1/pkey3,
        //    and do *not* see key2/pkey2.
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey1" && v == b"value1"),
            "Should still have key1/pkey1/value1"
        );
        assert!(
            scanned
                .iter()
                .any(|(k, pk, v)| k == b"key1" && pk == b"pkey3" && v == b"value3_updated"),
            "Should see updated value3 for key1/pkey3"
        );
        assert!(
            !scanned
                .iter()
                .any(|(k, pk, _)| k == b"key2" && pk == b"pkey2"),
            "Deleted key2/pkey2 should not appear at ts=200"
        );

        // Done
        Ok(())
    }

    #[test]
    fn test_linear_hash_table() {
        let pool = Arc::new(InMemPool::new());
        let index = LinearHashTable::create(ContainerKey::new(1, 1), pool).unwrap();
        test_basic_index_ops(&index).unwrap();
    }
}
