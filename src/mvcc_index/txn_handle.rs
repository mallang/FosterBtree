/// a read committed txn handle
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::bp::{ContainerKey, MemPool};

use super::{hashtable_mu::mvcc_hash_join_table::MvccHashJoinTable, Delta, MvccIndex, Timestamp};

mod watermark {
    use std::collections::BTreeMap;

    pub struct Watermark {
        readers: BTreeMap<u64, usize>,
    }

    impl Default for Watermark {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Watermark {
        pub fn new() -> Self {
            Self {
                readers: BTreeMap::new(),
            }
        }

        pub fn add_reader(&mut self, ts: u64) {
            let entry = self.readers.entry(ts).or_default();
            *entry += 1;
        }

        pub fn remove_reader(&mut self, ts: u64) {
            assert!(self.readers.contains_key(&ts));
            let entry = self.readers.entry(ts).and_modify(|x| *x -= 1).or_default();
            if *entry == 0 {
                self.readers.remove(&ts);
            }
        }

        pub fn watermark(&self) -> Option<u64> {
            if self.readers.is_empty() {
                None
            } else {
                Some(self.readers.iter().map(|entry| *entry.0).min().unwrap())
            }
        }

        pub fn num_retained_snapshots(&self) -> usize {
            self.readers.len()
        }
    }
}

use mvcctxn::{MvccInner, Transaction};
use watermark::Watermark;

mod mvcctxn {
    use crate::{bp::MemPool, log_warn};

    use super::{Delta, MvccIndex, TxnMvccHashTable, Watermark};
    use anyhow::Result;
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        ops::Bound,
        sync::{atomic::AtomicBool, Arc, Mutex},
    };
    pub struct CommittedTxnData {
        pub key_hashes: HashSet<u32>,
    }

    pub struct MvccInner {
        pub commit_lock: Mutex<()>,
        pub ts: Arc<Mutex<(u64, Watermark)>>,
        pub committed_txns: Arc<Mutex<BTreeMap<u64, CommittedTxnData>>>,
    }

    unsafe impl Sync for MvccInner {}

    impl MvccInner {
        pub fn new(initial_ts: u64) -> Self {
            Self {
                commit_lock: Mutex::new(()),
                ts: Arc::new(Mutex::new((initial_ts, Watermark::new()))),
                committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }

        pub fn latest_commit_ts(&self) -> u64 {
            self.ts.lock().unwrap().0
        }

        pub fn update_commit_ts(&self, ts: u64) {
            self.ts.lock().unwrap().0 = ts
        }

        pub fn watermark(&self) -> u64 {
            let ts = self.ts.lock().unwrap();
            ts.1.watermark().unwrap_or(ts.0)
        }

        // /// maybe arc is not necessary
        // pub fn new_txn<T: MemPool, M: MvccIndex<T>>(
        //     &self,
        //     inner: Arc<TxnMvccHashTable<T, M>>,
        // ) -> Arc<Transaction<T, M>> {
        //     let mut ts = self.ts.lock().unwrap();
        //     let ats = ts.0;
        //     ts.1.add_reader(ats);
        //     let txn = Transaction {
        //         begin_ts: ts.0,
        //         txn_hash_table: inner,
        //         committed: false.into(),
        //     };
        //     Arc::new(txn)
        // }
    }

    /// thread_local transaction (serializable) \
    ///
    pub struct Transaction<T: MemPool, M: MvccIndex<T>> {
        pub(super) begin_ts: u64,
        pub(super) txn_hash_table: Arc<TxnMvccHashTable<T, M>>,
        pub(super) committed: AtomicBool,
    }

    const READ_COMMITTED_TS: u64 = crate::mvcc_index::Timestamp::MAX;

    impl<T: MemPool, InnerIndex: MvccIndex<T>> Transaction<T, InnerIndex> {
        pub fn get(
            &self,
            key: &InnerIndex::Key,
            pkey: &InnerIndex::PKey,
        ) -> Result<Option<InnerIndex::Value>> {
            if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
                panic!("can NOT get in a committed txn");
            }
            let value = self
                .txn_hash_table
                .hash_table_inner
                .get(key, pkey, READ_COMMITTED_TS)?;
            Ok(value)
        }

        /// only insert in local storage, \
        /// since all pkey are inserted only once \
        /// so insert will never cause duplicate pkey error
        pub fn insert(
            &self,
            key: InnerIndex::Key,
            pkey: InnerIndex::PKey,
            value: InnerIndex::Value,
        ) -> Result<()> {
            if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
                panic!("can NOT insert in a committed txn");
            }
            self
                .txn_hash_table
                .hash_table_inner
                .insert(key, pkey, self.begin_ts, 0, value)?;
            Ok(())
        }

        /// IF not found neither in local storage, nor in inner mvccindex, \
        /// THEN update should do nothing \
        /// ELSE update in local_storage \
        /// both cases return ok
        ///
        pub fn update(
            &self,
            key: InnerIndex::Key,
            pkey: InnerIndex::PKey,
            value: InnerIndex::Value,
        ) -> Result<()> {
            if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
                panic!("can NOT update in a committed txn");
            }
            self
                .txn_hash_table
                .hash_table_inner
                .update(key, pkey, self.begin_ts, 0, value)?;

            Ok(())
        }

        /// IF not found neither in local storage, nor in inner mvccindex, \
        /// THEN delete should do nothing \
        /// ELSE delete in local_storage \
        /// both cases return ok
        ///
        pub fn delete(&self, key: InnerIndex::Key, pkey: InnerIndex::PKey) -> Result<()> {
            if self.committed.load(std::sync::atomic::Ordering::SeqCst) {
                panic!("can NOT update in a committed txn");
            }
            self
                .txn_hash_table
                .hash_table_inner
                .delete(key, pkey, self.begin_ts, 0)?;

            Ok(())
        }

        /// if commit succ, return committed_ts.ok \
        /// if commit fail (not serializable), return anyhow::Error \
        ///
        pub fn commit(&self) -> Result<u64> {
            self.committed
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .expect("can NOT commit in a committed txn");

            Ok(self.begin_ts)
        }
    }
}

pub struct TxnMvccHashTable<T: MemPool, M: MvccIndex<T>> {
    hash_table_inner: Arc<M>,
    mvcc: MvccInner,
    phantom: std::marker::PhantomData<T>,
}

impl<T: MemPool> TxnMvccHashTable<T, MvccHashJoinTable<T>> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Arc<Self> {
        Arc::new(Self {
            hash_table_inner: Arc::new(MvccHashJoinTable::new(c_key, mem_pool)),
            mvcc: MvccInner::new(1),
            phantom: std::marker::PhantomData,
        })
    }

    fn txn_new(self: &Arc<Self>, write_ts: Timestamp) -> Transaction<T, MvccHashJoinTable<T>> {
        let txn = Transaction {
            begin_ts: write_ts,
            txn_hash_table: self.clone(),
            committed: false.into(),
        };
        txn
    }
}
