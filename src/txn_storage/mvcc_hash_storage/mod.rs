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
pub enum MvccHashIterator {
    Dummy,
}
