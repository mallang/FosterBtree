use std::{
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    bp::{ContainerId, DatabaseId, MemPool},
    mvcc_index::{hash_join::mvcc_hash_join::MvccHashJoinTable, TxId, TxInfo},
};

use super::{
    ContainerOptions, DBOptions, ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait,
};

pub struct MvccHashStorage<T: MemPool> {
    containers: UnsafeCell<Vec<Arc<MvccHashJoinTable<T>>>>,
    tx_table: HashMap<TxId, TxInfo>,
}

impl<T: MemPool> Default for MvccHashStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: MemPool> MvccHashStorage<T> {
    pub fn new() -> Self {
        Self {
            containers: UnsafeCell::new(Vec::new()),
            tx_table: HashMap::new(),
        }
    }
}

unsafe impl<T: MemPool> Sync for MvccHashStorage<T> {}
unsafe impl<T: MemPool> Send for MvccHashStorage<T> {}

impl<T: MemPool> TxnStorageTrait for MvccHashStorage<T> {
    type TxnHandle = TxId;
    type IteratorHandle = MvccHashIterator;

    fn open_db(&self, options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
        Ok(0)
    }

    fn close_db(&self, db_id: &DatabaseId) -> Result<(), TxnStorageStatus> {
        Ok(())
    }

    fn delete_db(&self, db_id: &DatabaseId) -> Result<(), TxnStorageStatus> {
        let containers = unsafe { &mut *self.containers.get() };
        containers.clear();
        Ok(())
    }

    fn create_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        options: ContainerOptions,
    ) -> Result<ContainerId, TxnStorageStatus> {
        todo!()
    }

    fn delete_container(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
        c_id: &ContainerId,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn list_containers(
        &self,
        txn: &Self::TxnHandle,
        db_id: &DatabaseId,
    ) -> Result<HashSet<ContainerId>, TxnStorageStatus> {
        todo!()
    }

    fn begin_txn(
        &self,
        db_id: &DatabaseId,
        options: TxnOptions,
    ) -> Result<Self::TxnHandle, TxnStorageStatus> {
        todo!()
    }

    fn commit_txn(
        &self,
        txn: &Self::TxnHandle,
        async_commit: bool,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn abort_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn wait_for_txn(&self, txn: &Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn drop_txn(&self, txn: Self::TxnHandle) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn num_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
    ) -> Result<usize, TxnStorageStatus> {
        todo!()
    }

    fn check_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<bool, TxnStorageStatus> {
        todo!()
    }

    fn get_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<Vec<u8>, TxnStorageStatus> {
        todo!()
    }

    fn insert_value(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn insert_values(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn delete_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn scan_range(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        options: ScanOptions,
    ) -> Result<Self::IteratorHandle, TxnStorageStatus> {
        todo!()
    }

    fn iter_next(
        &self,
        iter: &Self::IteratorHandle,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, TxnStorageStatus> {
        todo!()
    }

    fn drop_iterator_handle(&self, iter: Self::IteratorHandle) -> Result<(), TxnStorageStatus> {
        todo!()
    }

    fn update_value<K: AsRef<[u8]>>(
        &self,
        txn: &Self::TxnHandle,
        c_id: &ContainerId,
        key: K,
        value: Vec<u8>,
    ) -> Result<(), TxnStorageStatus> {
        todo!()
    }
}

pub enum MvccHashIterator {
    Dummy,
}
