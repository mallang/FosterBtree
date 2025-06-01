use core::panic;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::{AtomicBool, AtomicU64}, Arc, Mutex},
};

use crate::{
    bp::{ContainerKey, MemPool},
    log_warn,
    mvcc_index::{
        hash_common::DEFAULT_BUCKET_NUM, hash_join_page::record::RecordRef, Delta, MvccEntry,
        MvccIndex,
    },
    prelude::{AccessMethodError, Timestamp},
};

use super::{
    iterators::{LinearSubTableKeyScanner, LinearSubTableScanner},
    linear_sub_table::LinearSubTable,
};

pub struct LinearBulkUpdate {
    pub update_entries: HashMap<Vec<u8>, Vec<u8>>,
    pub old_entries: Vec<MvccEntry>,
}

pub struct LinearHashTable<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    recent: Arc<LinearSubTable<T>>,
    history: Arc<LinearSubTable<T>>,
    largest_txn_ts: AtomicU64,
    is_bulk_update: AtomicBool,
    bulk_update: Mutex<LinearBulkUpdate>,
}

impl<T: MemPool + 'static> LinearHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_num: usize) -> Self {
        let recent =
            LinearSubTable::new_with_bucket_num(mem_pool.clone(), c_key.clone(), bucket_num);
        let history =
            LinearSubTable::new_with_bucket_num(mem_pool.clone(), c_key.clone(), bucket_num * 2);
        Self {
            mem_pool,
            c_key,
            recent: Arc::new(recent),
            history: Arc::new(history),
            largest_txn_ts: AtomicU64::new(0),
            is_bulk_update: AtomicBool::new(false),
            bulk_update: Mutex::new(LinearBulkUpdate {
                update_entries: HashMap::new(),
                old_entries: Vec::new(),
            }),
        }
    }

    fn set_largest_txn_ts(&self, ts: Timestamp) {
        self.largest_txn_ts
            .store(ts, std::sync::atomic::Ordering::SeqCst);
    }

    fn bulk_update(&self) -> Result<(), AccessMethodError> {
        let mut bulk_update = self.bulk_update.lock().unwrap();
        self.recent.bulk_update_recent(self.largest_txn_ts.load(std::sync::atomic::Ordering::Acquire), &mut bulk_update)?;

        for entry in &bulk_update.old_entries {
            let history_rec = RecordRef::new(&entry.key(), &entry.pkey(), &entry.value());
            self.history
               .insert_history(&history_rec, entry.start_ts(), entry.end_ts())
               .unwrap();
        }

        bulk_update.update_entries.clear();
        bulk_update.old_entries.clear();
        Ok(())
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
        self.set_largest_txn_ts(ts);
        let rec = RecordRef::new(&key, &pkey, &value);
        match self.recent.insert(&rec, ts, Timestamp::MAX) {
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
        self.set_largest_txn_ts(ts);
        // let entry = MvccEntry::new_with_tx_id(key, pkey, value, ts, u64::MAX, tx_id);
        let rec = RecordRef::new(&key, &pkey, &value);

        if self.is_bulk_update.load(std::sync::atomic::Ordering::Acquire) {
            let mut bulk_update = self.bulk_update.lock().unwrap();
            bulk_update.update_entries.insert(pkey, value);
            return Ok(());
        } else {
            match self.recent.update(&rec, ts, Timestamp::MAX) {
                Ok(mut old_res) => {
                    old_res.set_end_ts(&ts);
                    let history_rec = RecordRef::new(&old_res.key(), &old_res.pkey(), &old_res.value());
                    self.history
                        .insert_history(&history_rec, old_res.start_ts(), old_res.end_ts())
                        .unwrap();
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
        self.set_largest_txn_ts(ts);
        match self.recent.delete(key, pkey, ts) {
            Ok(mut old_entry) => {
                old_entry.set_end_ts(&ts);
                let old_rec =
                    RecordRef::new(&old_entry.key(), &old_entry.pkey(), &old_entry.value());
                self.history
                    .insert_history(&old_rec, old_entry.start_ts(), old_entry.end_ts())
                    .unwrap();
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
        let history_iter = if ts
            >= self
                .largest_txn_ts
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            Box::new(vec![].into_iter()) as Box<dyn Iterator<Item = MvccEntry>>
        } else {
            Box::new(LinearSubTableScanner::new(self.history.clone(), Some(ts)))
                as Box<dyn Iterator<Item = MvccEntry>>
        };

        Ok(Box::new(
            recent_iter
                .into_iter()
                .map(|e| (e.key, e.pkey, e.value))
                .chain(history_iter.into_iter().map(|e| (e.key, e.pkey, e.value)))
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let recent_iter = LinearSubTableScanner::new(self.recent.clone(), None);
        let history_iter = LinearSubTableScanner::new(self.history.clone(), None);
        let iter = Box::new(
            recent_iter
                .chain(history_iter)
                .collect::<Vec<_>>()
                .into_iter(),
        );
        Ok(iter)
    }

    fn scan_key(
        &self,
        key: &Self::Key,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        let recent_iter = LinearSubTableKeyScanner::new(self.recent.clone(), Some(ts), key.clone());
        let history_iter = if ts
            >= self
                .largest_txn_ts
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            Box::new(vec![].into_iter()) as Box<dyn Iterator<Item = MvccEntry>>
        } else {
            Box::new(LinearSubTableKeyScanner::new(
                self.history.clone(),
                Some(ts),
                key.clone(),
            ))
        };

        let iter = Box::new(
            recent_iter
                .chain(history_iter)
                .map(|e| (e.pkey, e.value))
                .collect::<Vec<_>>()
                .into_iter(),
        );
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

    fn scan_read_repair(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        self.scan(ts)
    }

    fn delta_scan_read_repair(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Self::Key, Self::PKey, Delta<Self::Value>)> + Send>,
        Self::Error,
    > {
        self.delta_scan(from_ts, to_ts)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn garbage_collect(&self, safe_ts: Timestamp) -> Result<(), Self::Error> {
        self.history.history_garbage_collect(safe_ts)
    }

    fn split_at_ts(&self, ts: Timestamp) -> Result<(), Self::Error> {
        Ok(())
    }

    fn bulk_update_end(&self) -> Result<(), Self::Error> {
        self.is_bulk_update.store(false, std::sync::atomic::Ordering::Release);
        self.bulk_update()?;
        Ok(())
    }
    fn bulk_update_start(&self) -> Result<(), Self::Error> {
        self.is_bulk_update.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
}
