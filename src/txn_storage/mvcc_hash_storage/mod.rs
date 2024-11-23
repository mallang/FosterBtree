use std::{
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    bp::{ContainerId, DatabaseId, MemPool},
    mvcc_index::{
        hash_join::mvcc_hash_join::MvccHashJoinTable, MvccIndex, Timestamp, TxId, TxInfo,
    },
};

use super::{
    ContainerOptions, DBOptions, ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait,
};

pub struct MvccHashStorage<T: MemPool, M: MvccIndex<T>> {
    containers: UnsafeCell<Vec<Arc<M>>>,
    tx_table: HashMap<TxId, TxInfo>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: MemPool, M: MvccIndex<T>> Default for MvccHashStorage<T, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: MemPool, M: MvccIndex<T>> MvccHashStorage<T, M> {
    pub fn new() -> Self {
        Self {
            containers: UnsafeCell::new(Vec::new()),
            tx_table: HashMap::new(),
            phantom: std::marker::PhantomData,
        }
    }
}

unsafe impl<T: MemPool, M: MvccIndex<T>> Sync for MvccHashStorage<T, M> {}
unsafe impl<T: MemPool, M: MvccIndex<T>> Send for MvccHashStorage<T, M> {}
pub enum MvccHashIterator {
    Dummy,
}

// impl<T: MemPool, M: MvccIndex<T>> TxnStorageTrait for MvccHashStorage<T, M> {
//     type TxnHandle = (TxId, Timestamp);
//     type IteratorHandle = MvccHashIterator;

//     fn open_db(&self, _options: DBOptions) -> Result<DatabaseId, TxnStorageStatus> {
//         Ok(DatabaseId::new(0))
//     }

//     fn close_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
//         Ok(())
//     }

//     fn delete_db(&self, _db_id: DatabaseId) -> Result<(), TxnStorageStatus> {
//         Ok(())
//     }

//     fn create_container(
//         &self,
//         _db_id: DatabaseId,
//         _options: ContainerOptions,
//     ) -> Result<ContainerId, TxnStorageStatus> {
//         Ok(ContainerId::new(0))
//     }

//     fn delete_container(
//         &self,
//         _db_id: DatabaseId,
//         _c_id: ContainerId,
//     ) -> Result<(), TxnStorageStatus> {
//         Ok(())
//     }

//     fn list_containers(&self, _db_id: DatabaseId) -> Result<HashSet<ContainerId>, TxnStorageStatus> {
//         Ok(HashSet::new())
//     }

//     fn get_value<K: AsRef<[u8]>>(
//             &self,
//             txn: &Self::TxnHandle,
//             c_id: ContainerId,
//             key: K,
//         ) -> Result<Vec<u8>, TxnStorageStatus> {
//         let containers = unsafe { &*self.containers.get() };
//         let storage = containers[c_id as usize].as_ref();
//         let ts = txn.1;
//         storage.get(key.as_ref())
//     }
// }
