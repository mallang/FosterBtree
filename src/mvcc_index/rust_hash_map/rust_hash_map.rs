use std::{
    collections::HashMap,
    sync::{Mutex, RwLock},
};

use crate::{
    bp::MemPool,
    mvcc_index::{MvccEntry, MvccIndex},
};

pub struct MvccRustHashMap {
    table: RwLock<HashMap<Vec<u8>, Vec<MvccEntry>>>,
}

impl<T: MemPool + 'static> MvccIndex<T> for MvccRustHashMap {
    type Key = Vec<u8>;
    type PKey = Vec<u8>;
    type Value = Vec<u8>;
    type Error = crate::mvcc_index::AccessMethodError;

    fn create(
        c_key: crate::prelude::ContainerKey,
        mem_pool: std::sync::Arc<T>,
    ) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        Ok(Self {
            table: RwLock::new(HashMap::new()),
        })
    }

    fn insert(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let mut table = self.table.write().unwrap();
        let entries = table.entry(key.clone()).or_insert_with(Vec::new);
        // End the previous version if exists for the same pkey
        if let Some(last_entry) = entries
            .iter_mut()
            .rev()
            .find(|e| e.pkey == pkey && e.end_ts == u64::MAX)
        {
            last_entry.end_ts = ts;
        }
        // Add the new version
        let new_entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), ts, u64::MAX);
        entries.push(new_entry);
        Ok(())
    }
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
    ) -> Result<Option<Self::Value>, Self::Error> {
        let table = self.table.read().unwrap();
        if let Some(entries) = table.get(key) {
            if let Some(entry) = entries
                .iter()
                .rev()
                .find(|e| e.pkey == pkey && e.start_ts <= ts && e.end_ts > ts)
            {
                return Ok(Some(entry.value.clone()));
            }
        }
        Ok(None)
    }
    fn update(
        &self,
        key: Self::Key,
        pkey: Self::PKey,
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
        value: Self::Value,
    ) -> Result<(), Self::Error> {
        let mut table = self.table.write().unwrap();
        let entries = table.entry(key.clone()).or_insert_with(Vec::new);
        // End the previous version if exists for the same pkey
        if let Some(last_entry) = entries
            .iter_mut()
            .rev()
            .find(|e| e.pkey == pkey && e.end_ts == u64::MAX)
        {
            last_entry.end_ts = ts;
        }
        // Add the new version
        let new_entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), ts, u64::MAX);
        entries.push(new_entry);
        Ok(())
    }
    fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: crate::prelude::Timestamp,
        tx_id: crate::mvcc_index::TxId,
    ) -> Result<(), Self::Error> {
        let mut table = self.table.write().unwrap();
        let entries = table.entry(key.to_vec()).or_insert_with(Vec::new);
        // End the previous version if exists for the same pkey
        if let Some(last_entry) = entries
            .iter_mut()
            .rev()
            .find(|e| e.pkey == pkey && e.end_ts == u64::MAX)
        {
            last_entry.end_ts = ts;
        }
        Ok(())
    }
    fn scan(
        &self,
        ts: crate::prelude::Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Self::PKey, Self::Value)> + Send>, Self::Error>
    {
        let table = self.table.read().unwrap();
        let result: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = table
            .iter()
            .flat_map(|(key, entries)| {
                entries
                    .iter()
                    .filter(move |e| {
                        e.start_ts <= ts && e.end_ts > ts
                            || (ts == u64::MAX && e.end_ts == u64::MAX)
                    })
                    .map(move |e| (key.clone(), e.pkey.clone(), e.value.clone()))
            })
            .collect();
        Ok(Box::new(result.into_iter()))
    }
    fn scan_all(&self) -> Result<Box<dyn Iterator<Item = MvccEntry> + Send>, Self::Error> {
        let table = self.table.read().unwrap();
        let result: Vec<MvccEntry> = table
            .iter()
            .flat_map(|(_, entries)| entries.iter().cloned())
            .collect();
        Ok(Box::new(result.into_iter()))
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
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
    fn garbage_collect(&self, safe_ts: crate::prelude::Timestamp) -> Result<(), Self::Error> {
        todo!()
    }
    fn get_key(
        &self,
        key: &Self::Key,
        ts: crate::prelude::Timestamp,
    ) -> Result<Vec<(Self::PKey, Self::Value)>, Self::Error> {
        todo!()
    }
    fn scan_key(
        &self,
        key: &Self::Key,
        ts: crate::prelude::Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Self::PKey, Self::Value)> + Send>, Self::Error> {
        todo!()
    }
}
