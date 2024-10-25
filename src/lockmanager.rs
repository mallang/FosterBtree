use std::collections::HashMap;
mod crustyerror {
    use super::ids::TransactionId;

    use std::fmt;
    use std::io;

    pub fn c_err(s: &str) -> CrustyError {
        CrustyError::CrustyError(s.to_string())
    }

    /// Custom error type.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CrustyError {
        /// IO Errors.
        IOError(String),
        /// Serialization errors.
        SerializationError(String),
        /// Custom errors.
        CrustyError(String),
        /// Validation errors.
        ValidationError(String),
        /// Execution errors.
        ExecutionError(String),
        /// Transaction aborted or committed.
        TransactionNotActive,
        /// Invalid insert or update
        InvalidMutationError(String),
        /// Transaction Rollback
        TransactionRollback(TransactionId),
    }

    impl fmt::Display for CrustyError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "{}",
                match self {
                    CrustyError::ValidationError(s) => format!("Validation Error: {}", s),
                    CrustyError::ExecutionError(s) => format!("Execution Error: {}", s),
                    CrustyError::CrustyError(s) => format!("Crusty Error: {}", s),
                    CrustyError::IOError(s) => s.to_string(),
                    CrustyError::SerializationError(s) => s.to_string(),
                    CrustyError::TransactionNotActive =>
                        String::from("Transaction Not Active Error"),
                    CrustyError::InvalidMutationError(s) => format!("InvalidMutationError {}", s),
                    CrustyError::TransactionRollback(tid) =>
                        format!("Transaction Rolledback {:?}", tid),
                }
            )
        }
    }

    // Implement std::convert::From for AppError; from io::Error
    impl From<io::Error> for CrustyError {
        fn from(error: io::Error) -> Self {
            CrustyError::IOError(error.to_string())
        }
    }
}

use crustyerror::*;

mod ids {
    /// Permissions for locks.
    pub enum Permissions {
        ReadOnly,
        ReadWrite,
    }

    use std::{fmt, sync::atomic::AtomicU64};

    use serde::{Deserialize, Serialize};

    static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);
    pub type TidType = u64;

    /// Implementation of transaction id.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct TransactionId {
        /// Id of transaction.
        id: TidType,
    }

    impl TransactionId {
        /// Creates a new transaction id.
        pub fn new() -> Self {
            Self {
                id: TXN_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            }
        }

        /// Returns the transaction id.
        pub fn id(&self) -> u64 {
            self.id
        }
    }

    impl Default for TransactionId {
        fn default() -> Self {
            TransactionId::new()
        }
    }

    pub type ContainerId = u16;
    pub type SegmentId = u8;
    pub type PageId = u32;
    pub type SlotId = u16;
    /// Holds information to find a record or value's bytes in a storage manager.
    /// Depending on storage manager (SM), various elements may be used.
    /// For example a disk-based SM may use pages to store the records, where
    /// a main-memory based storage manager may not.
    /// It is up to a particular SM to determine how and when to use
    #[derive(PartialEq, Clone, Copy, Eq, Hash, Serialize, Deserialize)]
    pub struct ValueId {
        /// The source of the value. This could represent a table, index, or other data structure.
        /// All values stored must be associated with a container that is created by the storage manager.
        pub container_id: ContainerId,
        /// An optional segment or partition ID
        pub segment_id: Option<SegmentId>,
        /// An optional page id
        pub page_id: Option<PageId>,
        /// An optional slot id. This could represent a physical or logical ID.
        pub slot_id: Option<SlotId>,
    }

    impl ValueId {
        pub fn new(container_id: ContainerId) -> Self {
            ValueId {
                container_id,
                segment_id: None,
                page_id: None,
                slot_id: None,
            }
        }

        pub fn new_page(container_id: ContainerId, page_id: PageId) -> Self {
            ValueId {
                container_id,
                segment_id: None,
                page_id: Some(page_id),
                slot_id: None,
            }
        }

        pub fn new_slot(container_id: ContainerId, page_id: PageId, slot_id: SlotId) -> Self {
            ValueId {
                container_id,
                segment_id: None,
                page_id: Some(page_id),
                slot_id: Some(slot_id),
            }
        }
    }

    impl fmt::Debug for ValueId {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let mut buf: String = format!("<c_id:{}", self.container_id);
            if self.segment_id.is_some() {
                buf.push_str(",seg_id:");
                buf.push_str(&self.segment_id.unwrap().to_string());
            }
            if self.page_id.is_some() {
                buf.push_str(",p_id:");
                buf.push_str(&self.page_id.unwrap().to_string());
            }
            if self.slot_id.is_some() {
                buf.push_str(",slot_id:");
                buf.push_str(&self.slot_id.unwrap().to_string());
            }
            buf.push('>');
            write!(f, "{}", buf)
        }
    }
}

use ids::*;
pub use ids::{Permissions, TransactionId, ValueId};

/// Implementation of a lock.
pub struct Lock {
    /// Page that the lock is for.
    _vid: ValueId,
    /// Transactions that hold the lock.
    txn: Vec<TransactionId>,
    /// Type of lock.
    lock_type: Permissions,
}

/// Implementation of the lock manager.
pub struct LockManager {
    /// Mapping from page id to lock for the page.
    pid_lock: HashMap<ValueId, Lock>,
    /// Mapping from transaction to vector of pages locked by the transaction.
    txn_locks: HashMap<TransactionId, Vec<ValueId>>,
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LockManager {
    /// Initialize a new lock manager.
    pub fn new() -> Self {
        Self {
            pid_lock: HashMap::new(),
            txn_locks: HashMap::new(),
        }
    }

    /// Resets the lockmanager.
    ///
    /// Mainly used for testing the buffer pool.
    pub fn clear(&mut self) {
        self.pid_lock = HashMap::new();
        self.txn_locks = HashMap::new();
    }

    /// Returns true if lock acquired, else false.
    ///
    /// # Arguments
    ///
    /// * `tid` - Id of the transaction that wants to lock the page.
    /// * `pid` - Id of the page to lock.
    /// * `perm` - Type of lock to get.
    pub fn acquire_lock(&mut self, tid: TransactionId, pid: ValueId, perm: Permissions) -> bool {
        // First time seeing txn, initial tracking for ValueIds
        self.txn_locks.entry(tid).or_default();

        #[allow(clippy::map_entry)]
        if !(self.pid_lock.contains_key(&pid)) {
            // No lock exists on this page.
            let lock = Lock {
                _vid: pid,
                txn: vec![tid],
                lock_type: perm,
            };
            self.pid_lock.insert(pid, lock);
            self.txn_locks.get_mut(&tid).unwrap().push(pid);
            true
        } else {
            // Lock exists.
            let lock = self.pid_lock.get_mut(&pid).unwrap();
            match perm {
                Permissions::ReadOnly => match lock.lock_type {
                    Permissions::ReadWrite => lock.txn.contains(&tid),
                    Permissions::ReadOnly => {
                        if lock.txn.contains(&tid) {
                            true
                        } else {
                            lock.txn.push(tid);
                            self.txn_locks.get_mut(&tid).unwrap().push(pid);
                            true
                        }
                    }
                },
                Permissions::ReadWrite => {
                    if lock.txn.len() == 1 && lock.txn.contains(&tid) {
                        //We already have the lock.
                        if let Permissions::ReadOnly = lock.lock_type {
                            lock.lock_type = Permissions::ReadWrite
                        }
                        true
                    } else {
                        // Theoretically, could starve writer.
                        false
                    }
                }
            }
        }
    }

    /// Returns all the pages locked by the transaction.
    ///
    /// # Arguments
    ///
    /// * `tid` - Id of transaction to get the locks it holds.
    pub fn page_ids(&self, tid: TransactionId) -> Vec<ValueId> {
        match self.txn_locks.get(&tid) {
            Some(v) => v.clone(),
            None => Vec::new(),
        }
    }

    /// Releases all the locks of a transaction.
    ///
    /// # Arguments
    ///
    /// * `tid` - Id of the transaction to release all the locks for.
    pub fn release_locks(&mut self, tid: TransactionId) -> Result<(), CrustyError> {
        let mut pages_to_release = Vec::new();

        if let Some(pages) = self.txn_locks.get(&tid) {
            for page in pages {
                pages_to_release.push(*page);
            }
        }

        if !pages_to_release.is_empty() {
            for p in pages_to_release.iter() {
                self.release_lock(tid, *p)?;
            }
            Ok(())
        } else {
            Err(CrustyError::CrustyError(format!(
                "Releasing locks from unknown TXN {}",
                tid.id()
            )))
        }
    }

    /// Releases the lock on the page of the transaction.
    ///
    /// # Arguments
    ///
    /// * `tid` - Id of the transaction to release the lock for.
    /// * `pid` - Id of the page to release.
    pub fn release_lock(&mut self, tid: TransactionId, pid: ValueId) -> Result<(), CrustyError> {
        match self.txn_locks.get_mut(&tid) {
            Some(pages) => {
                if let Some(lock) = self.pid_lock.get_mut(&pid) {
                    if lock.txn.contains(&tid) {
                        if lock.txn.len() == 1 {
                            // Only one holding txn.
                            self.pid_lock.remove(&pid);
                        } else {
                            lock.txn.retain(|&x| x != tid);
                        }
                    } else {
                        return Err(CrustyError::CrustyError(format!(
                            "TXN  {} does not hold lock on {:?}",
                            tid.id(),
                            pid
                        )));
                    }
                } else {
                    return Err(CrustyError::CrustyError(format!(
                        "No locks on ValueId {:?}",
                        pid
                    )));
                }
                pages.retain(|&p| p != pid);
                Ok(())
            }
            None => Err(CrustyError::CrustyError(format!(
                "Releasing lock from unknown TXN {}",
                tid.id()
            ))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use ids::ValueId;
    use std::sync::{Arc, Barrier, RwLock};
    use std::thread;

    #[test]
    fn test_acquire_lock() {
        // Set up page, txns, and lock manager.
        let pid = ValueId::new_page(1, 1);
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();
        let mut lm = LockManager::new();

        // Both threads read.
        assert!(lm.acquire_lock(txn1, pid, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn2, pid, Permissions::ReadOnly));

        // Try to write without releasing read.
        assert!(!lm.acquire_lock(txn1, pid, Permissions::ReadWrite));
        assert!(!lm.acquire_lock(txn2, pid, Permissions::ReadWrite));
    }

    #[test]
    fn test_page_ids() {
        // Set up page, txns, and lock manager.
        let pid1 = ValueId::new_page(1, 1);
        let pid2 = ValueId::new_page(2, 2);
        let txn = TransactionId::new();
        let mut lm = LockManager::new();

        // Read both pages.
        assert!(lm.acquire_lock(txn, pid1, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn, pid2, Permissions::ReadWrite));

        assert_eq!(lm.page_ids(txn).len(), 2);
    }

    #[test]
    fn test_release_lock() {
        // Set up page, txns, and lock manager.
        let pid = ValueId::new_page(1, 1);
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();
        let mut lm = LockManager::new();

        // Both threads read.
        assert!(lm.acquire_lock(txn1, pid, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn2, pid, Permissions::ReadOnly));

        // Both threads release.
        lm.release_lock(txn1, pid).unwrap();
        lm.release_lock(txn2, pid).unwrap();
        assert_eq!(lm.page_ids(txn1).len(), 0);
        assert_eq!(lm.page_ids(txn2).len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_release_missing_lock() {
        // Set up page, txns, and lock manager.
        let pid = ValueId::new_page(1, 1);
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();
        let mut lm = LockManager::new();

        // txn1 read.
        assert!(lm.acquire_lock(txn1, pid, Permissions::ReadOnly));

        // Both threads release, but txn2 never had lock.
        lm.release_lock(txn1, pid).unwrap();
        lm.release_lock(txn2, pid).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_release_missing_page() {
        // Set up page, txns, and lock manager.
        let pid1 = ValueId::new_page(1, 1);
        let pid2 = ValueId::new_page(2, 2);
        let txn = TransactionId::new();
        let mut lm = LockManager::new();

        // Txn acquire lock on pid1.
        assert!(lm.acquire_lock(txn, pid1, Permissions::ReadOnly));

        // Txn release, but never had lock on pid2.
        lm.release_lock(txn, pid2).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_release_missing_txn() {
        // Set up page, txns, and lock manager.
        let pid = ValueId::new_page(1, 1);
        let txn = TransactionId::new();
        let mut lm = LockManager::new();

        // Txn release, but never had lock.
        lm.release_lock(txn, pid).unwrap();
    }

    #[test]
    fn test_release_locks() {
        // Set up page, txns, and lock manager.
        let pid1 = ValueId::new_page(1, 1);
        let pid2 = ValueId::new_page(2, 2);
        let txn = TransactionId::new();
        let mut lm = LockManager::new();

        // Both threads read.
        assert!(lm.acquire_lock(txn, pid1, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn, pid2, Permissions::ReadWrite));

        // Both threads release.
        lm.release_locks(txn).unwrap();
        assert_eq!(lm.page_ids(txn).len(), 0);
    }

    #[test]
    #[should_panic]
    fn test_release_locks_missing_txn() {
        // Set up page, txns, and lock manager.
        let txn = TransactionId::new();
        let mut lm = LockManager::new();

        // Txn release, but never had lock.
        lm.release_locks(txn).unwrap();
    }

    #[test]
    fn test_upgrade_lock() {
        // Set up page, txns, and lock manager.
        let pid = ValueId::new_page(1, 1);
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();
        let mut lm = LockManager::new();

        // Both threads read.
        assert!(lm.acquire_lock(txn1, pid, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn2, pid, Permissions::ReadOnly));

        // Try to write without releasing read.
        assert!(!lm.acquire_lock(txn1, pid, Permissions::ReadWrite));
        assert!(!lm.acquire_lock(txn2, pid, Permissions::ReadWrite));

        // txn2 releases to allow txn1 to write.
        lm.release_lock(txn2, pid).unwrap();
        assert_eq!(lm.page_ids(txn2).len(), 0);
        assert!(lm.acquire_lock(txn1, pid, Permissions::ReadWrite));
    }

    #[test]
    fn test_lock_manager() -> Result<(), CrustyError> {
        let pid1 = ValueId::new_page(1, 1);
        let pid2 = ValueId::new_page(1, 2);
        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();

        let mut lm = LockManager::new();
        assert!(lm.acquire_lock(txn1, pid1, Permissions::ReadWrite));
        assert!(lm.acquire_lock(txn1, pid1, Permissions::ReadWrite));
        assert!(!lm.acquire_lock(txn2, pid1, Permissions::ReadWrite));
        lm.release_locks(txn1)?;
        assert!(lm.acquire_lock(txn2, pid1, Permissions::ReadWrite));
        lm.release_locks(txn2)?;
        if lm.release_locks(txn2).is_ok() {
            unreachable!("Should not get ok");
        }

        assert!(lm.acquire_lock(txn1, pid2, Permissions::ReadOnly));
        assert!(lm.acquire_lock(txn2, pid2, Permissions::ReadOnly));
        Ok(())
    }

    #[test]
    #[allow(clippy::unnecessary_wraps)]
    fn test_conc_lm() -> Result<(), CrustyError> {
        let pid1 = ValueId::new_page(1, 1);
        let pid2 = ValueId::new_page(1, 2);

        let txn1 = TransactionId::new();
        let txn2 = TransactionId::new();

        let lm = Arc::new(RwLock::new(LockManager::new()));
        let l1 = Arc::clone(&lm);
        let l2 = Arc::clone(&lm);

        let barrier = Arc::new(Barrier::new(2));
        let b1 = Arc::clone(&barrier);
        let b2 = Arc::clone(&barrier);

        let txn1 = thread::spawn(move || {
            {
                let mut _lm = l1.write().unwrap();
                assert!(_lm.acquire_lock(txn1, pid1, Permissions::ReadWrite));
                assert!(_lm.acquire_lock(txn1, pid1, Permissions::ReadWrite));
                assert!(_lm.acquire_lock(txn1, pid2, Permissions::ReadOnly));
                b1.wait();
            }
            {
                b1.wait();
                let mut lm = l1.write().unwrap();
                if lm.release_locks(txn1).is_err() {
                    unreachable!("Should not fail");
                }
                b1.wait();
            }
        });
        let txn2 = thread::spawn(move || {
            {
                b2.wait();
                let mut _lm = l2.write().unwrap();
                assert!(!_lm.acquire_lock(txn2, pid1, Permissions::ReadWrite));
                assert!(_lm.acquire_lock(txn2, pid2, Permissions::ReadOnly));
            }
            {
                b2.wait();
                b2.wait();
                let mut _lm = l2.write().unwrap();
                assert!(_lm.acquire_lock(txn2, pid1, Permissions::ReadWrite));
            }
        });
        txn1.join().unwrap();
        txn2.join().unwrap();
        Ok(())
    }
}
