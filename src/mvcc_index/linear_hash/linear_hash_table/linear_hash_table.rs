use core::panic;
use std::{collections::BTreeMap, sync::Arc};

use crate::{
    bp::{ContainerKey, MemPool},
    log_warn,
    mvcc_index::{hash_common::DEFAULT_BUCKET_NUM, Delta, MvccEntry, MvccIndex},
    prelude::{AccessMethodError, Timestamp},
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
        ts: Timestamp,
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
        ts: Timestamp,
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
        ts: Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        self.get(key, pkey, ts)
    }

    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: Timestamp,
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
        ts: Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        self.update(key, pkey, ts, tx_id, value)
    }

    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
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
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let recent_iter = LinearSubTableScanner::new(self.recent.clone(), Some(ts));
        let history_iter = LinearSubTableScanner::new(self.history.clone(), Some(ts));
        Ok(Box::new(
            recent_iter
                .into_iter()
                .map(|e| (e.key, e.pkey, e.value))
                .chain(history_iter.into_iter().map(|e| (e.key, e.pkey, e.value))),
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
        ts: Timestamp,
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
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        Ok(self.scan_key(key, ts)?.into_iter().collect())
    }

    fn scan_key_vec_read_repair(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        self.scan_key_vec(key, ts)
    }

    fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        let mut map = BTreeMap::<Vec<u8>, (Vec<u8>, Delta<Vec<u8>>)>::new();
        let to = self.scan(to_ts)?;
        for entry in to {
            map.insert(entry.1, (entry.0, Delta::Inserted(entry.2)));
        }

        let from = self.scan(from_ts)?;
        for entry in from {
            log_warn!(
                "from ts : {} get entry: {:?}",
                from_ts,
                String::from_utf8(entry.0.clone())
            );
            let e = map.get_mut(&entry.1);
            if let Some(map_entry) = e {
                if map_entry.1.get_value().unwrap() == &entry.2 {
                    map.remove(&entry.1);
                } else {
                    map_entry.1 = Delta::Updated(map_entry.1.get_value().unwrap().to_vec());
                }
            } else {
                map.insert(entry.1, (entry.0, Delta::Deleted));
            }
        }
        Ok(Box::new(map.into_iter().map(|(pk, kv)| (kv.0, pk, kv.1))))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        todo!()
    }

    fn split_at_ts(&self, ts: Timestamp) -> Result<(), Self::Error> {
        Ok(())
    }
}
