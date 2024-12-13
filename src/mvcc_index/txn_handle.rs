/// a read committed txn handle

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::bp::MemPool;

use super::{Delta, MvccIndex};

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

        /// maybe arc is not necessary
        pub fn new_txn<T: MemPool, M: MvccIndex<T>>(
            &self,
            inner: Arc<TxnMvccHashTable<T, M>>,
        ) -> Arc<Transaction<T, M>> {
            let mut ts = self.ts.lock().unwrap();
            let ats = ts.0;
            ts.1.add_reader(ats);
            let txn = Transaction {
                begin_ts: ts.0,
                txn_hash_table: inner,
                local_storage: Mutex::new(HashMap::new()),
                committed: false.into(),
                write_key_hashes: Mutex::new(HashSet::new()),
            };
            Arc::new(txn)
        }
    }

    /// thread_local transaction (serializable) \
    ///
    pub struct Transaction<T: MemPool, M: MvccIndex<T>> {
        pub(super) begin_ts: u64,
        pub(super) txn_hash_table: Arc<TxnMvccHashTable<T, M>>,
        pub(super) local_storage: Mutex<HashMap<(M::Key, M::PKey), Delta<M::Value>>>,
        pub(super) committed: AtomicBool,
        pub(super) write_key_hashes: Mutex<HashSet<u32>>,
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
            if let Some(value) = self
                .local_storage
                .lock()
                .unwrap()
                .get(&(key.clone(), pkey.clone()))
            {
                let local_get_res = match value.clone() {
                    Delta::Inserted(x) | Delta::Updated(x) => Ok(Some(x)),
                    Delta::Deleted => Ok(None),
                };
                return local_get_res;
            }

            let value = self
                .txn_hash_table
                .inner_hash_table()
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
            self.write_key_hashes
                .lock()
                .unwrap()
                .insert(farmhash::hash32(&pkey.as_ref()));
            self.local_storage
                .lock()
                .unwrap()
                .insert((key, pkey), Delta::Inserted(value));
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
            let mut key_hashes = self.write_key_hashes.lock().unwrap();
            key_hashes.insert(farmhash::hash32(&pkey.as_ref()));

            let local_find_result = {
                match self
                    .local_storage
                    .lock()
                    .unwrap()
                    .get(&(key.clone(), pkey.clone()))
                {
                    Some(Delta::Inserted(_) | Delta::Updated(_)) => true,
                    Some(Delta::Deleted) => false,
                    _ => self
                        .txn_hash_table
                        .inner_hash_table()
                        .get(&key, &pkey, self.begin_ts)?
                        .is_some(),
                }
            };
            if local_find_result {
                self.local_storage
                    .lock()
                    .unwrap()
                    .insert((key, pkey), Delta::Updated(value));
            }

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
            let mut key_hashes = self.write_key_hashes.lock().unwrap();
            key_hashes.insert(farmhash::hash32(&pkey.as_ref()));

            let local_find_result = {
                match self
                    .local_storage
                    .lock()
                    .unwrap()
                    .get(&(key.clone(), pkey.clone()))
                {
                    Some(Delta::Inserted(_) | Delta::Updated(_)) => true,
                    Some(Delta::Deleted) => false,
                    _ => self
                        .txn_hash_table
                        .inner_hash_table()
                        .get(&key, &pkey, self.begin_ts)?
                        .is_some(),
                }
            };
            if local_find_result {
                self.local_storage
                    .lock()
                    .unwrap()
                    .insert((key, pkey), Delta::Deleted);
            }

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

            let commit_lk = self.txn_hash_table.mvcc.commit_lock.lock().unwrap();

            let txn_key_hash = self.write_key_hashes.lock().unwrap();

            if txn_key_hash.is_empty() {
                // only read
                log_warn!("only read!");
                return Ok(self.begin_ts);
            }

            let committed_ts = self.txn_hash_table.mvcc.latest_commit_ts() + 1;

            let has_overlap = {
                let committed_txns_lock = self.txn_hash_table.mvcc.committed_txns.lock().unwrap();
                let committed_txns = committed_txns_lock
                    .range((Bound::Excluded(self.begin_ts), Bound::Excluded(committed_ts)));
                committed_txns
                    .into_iter()
                    .map(|(_, committed_txn_data)| {
                        committed_txn_data
                            .key_hashes
                            .intersection(&txn_key_hash)
                            .count()
                    })
                    .sum::<usize>()
                    > 0
            };
            if !has_overlap {
                self.txn_hash_table
                    .mvcc
                    .committed_txns
                    .lock()
                    .unwrap()
                    .insert(
                        committed_ts,
                        CommittedTxnData {
                            key_hashes: txn_key_hash.clone(),
                        },
                    );
            } else {
                anyhow::bail!("serializable check failed");
            }
            self.txn_hash_table.mvcc.update_commit_ts(committed_ts);
            drop(commit_lk);

            for (k_pk, delta) in self.local_storage.lock().unwrap().iter() {
                let (k, pk) = k_pk.clone();

                match delta.clone() {
                    Delta::Inserted(v) => {
                        self.txn_hash_table
                            .inner_hash_table()
                            .insert(k, pk, committed_ts, 0, v)?;
                    }
                    Delta::Updated(v) => {
                        self.txn_hash_table
                            .inner_hash_table()
                            .update(k, pk, committed_ts, 0, v)?;
                    }
                    Delta::Deleted => {
                        self.txn_hash_table
                            .inner_hash_table()
                            .delete(&k, &pk, committed_ts, 0)?;
                    }
                }
            }

            Ok(committed_ts)
        }
    }

    impl<T: MemPool, M: MvccIndex<T>> Drop for Transaction<T, M> {
        fn drop(&mut self) {
            self.txn_hash_table
                .mvcc
                .ts
                .lock()
                .unwrap()
                .1
                .remove_reader(self.begin_ts);
        }
    }
}

pub struct TxnMvccHashTable<T: MemPool, M: MvccIndex<T>> {
    hash_table_inner: Arc<M>,
    mvcc: MvccInner,
    phantom: std::marker::PhantomData<T>,
}

impl<T: MemPool, M: MvccIndex<T>> TxnMvccHashTable<T, M> {
    pub fn inner_hash_table(&self) -> &M {
        &self.hash_table_inner
    }
    pub fn new(hash_table: &Arc<M>) -> Arc<Self> {
        Arc::new(Self {
            hash_table_inner: hash_table.clone(),
            mvcc: MvccInner::new(1),
            phantom: std::marker::PhantomData,
        })
    }

    fn txn_new(self: &Arc<Self>) -> Transaction<T, M> {
        let mut ts = self.mvcc.ts.lock().unwrap();
        let read_ts = ts.0;
        ts.1.add_reader(read_ts);
        let txn = Transaction {
            begin_ts: read_ts,
            txn_hash_table: self.clone(),
            local_storage: Mutex::new(HashMap::new()),
            committed: false.into(),
            write_key_hashes: Mutex::new(HashSet::new()),
        };
        txn
    }
}

pub trait TxnStorage<T: MemPool, M: MvccIndex<T>>: Send + Sync {
    fn txn_new(self: &Arc<Self>) -> Transaction<T, M>;
}

impl<T: MemPool, M: MvccIndex<T>> TxnStorage<T, M> for TxnMvccHashTable<T, M> {
    fn txn_new(self: &Arc<Self>) -> Transaction<T, M> {
        self.txn_new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        bp::{get_in_mem_pool, ContainerKey, InMemPool}, log_warn, mvcc_index::MvccIndex
    };
    use anyhow::Result;

    use super::{super::hash_join::mvcc_hash_join::MvccHashJoinTable, TxnMvccHashTable};
    // use super::{super::hashtable_mu::mvcc_hash_join_cuckoo::MvccHashJoinTable, TxnMvccHashTable};
    const _: () = {
        fn assert_send<T: Send>() {}
        let _ = assert_send::<TxnMvccHashTable<InMemPool, MvccHashJoinTable<InMemPool>>>;

        fn assert_sync<T: Sync>() {}
        let _ = assert_sync::<TxnMvccHashTable<InMemPool, MvccHashJoinTable<InMemPool>>>;
    };

    #[test]
    fn test_txn_create() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();
        assert!(txn.commit().is_ok());
        Ok(())
    }

    #[test]
    fn test_txn_insert_local_storage_visible() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();
        txn.insert(key.clone(), pkey.clone(), value.clone())
            .unwrap();

        let get_res = txn.get(&key, &pkey);
        assert_eq!(get_res.unwrap().unwrap(), value);

        Ok(())
    }

    #[test]
    fn test_txn_update_local_storage_visible() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();
        txn.insert(key.clone(), pkey.clone(), value.clone())
            .unwrap();

        let new_value = b"new_value".to_vec();
        txn.update(key.clone(), pkey.clone(), new_value.clone())
            .unwrap();

        let get_res = txn.get(&key, &pkey);
        assert_eq!(get_res.unwrap().unwrap(), new_value);

        Ok(())
    }

    #[test]
    fn test_txn_delete_local_storage_visible() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();
        txn.insert(key.clone(), pkey.clone(), value.clone())
            .unwrap();

        txn.delete(key.clone(), pkey.clone()).unwrap();

        let get_res = txn.get(&key, &pkey);
        assert_eq!(get_res.unwrap(), None);

        Ok(())
    }

    #[test]
    fn test_txn_insert_into_hash_table() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();
        txn.insert(key.clone(), pkey.clone(), value.clone())
            .unwrap();
        let committed_ts = txn.commit().unwrap();

        let hash_table_get_result = <MvccHashJoinTable<InMemPool> as MvccIndex<InMemPool>>::get(
            &table_inner,
            &key,
            &pkey,
            committed_ts,
        );
        assert!(hash_table_get_result.is_ok());
        assert_eq!(hash_table_get_result.unwrap().unwrap(), value);

        Ok(())
    }

    #[test]
    fn test_txn_update_into_hash_table() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        // txn timestamp start from 1, so 0 can be used in prefill
        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();

        table_inner
            .insert(key.clone(), pkey.clone(), 0, 0, value.clone())
            .unwrap();
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        let new_value = b"new_value".to_vec();
        txn.update(key.clone(), pkey.clone(), new_value.clone())
            .unwrap();
        let committed_ts = txn.commit().unwrap();
        assert!(committed_ts > 0);

        let hash_table_get_result = <MvccHashJoinTable<InMemPool> as MvccIndex<InMemPool>>::get(
            &table_inner,
            &key,
            &pkey,
            committed_ts,
        );
        assert!(hash_table_get_result.is_ok());
        assert_eq!(hash_table_get_result.unwrap().unwrap(), new_value);

        Ok(())
    }

    #[test]
    fn test_txn_delete_into_hash_table() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        // txn timestamp start from 1, so 0 can be used in prefill
        let key = b"key".to_vec();
        let pkey = b"pkey".to_vec();
        let value = b"value".to_vec();

        table_inner
            .insert(key.clone(), pkey.clone(), 0, 0, value.clone())
            .unwrap();
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);
        let txn = txn_hash_table.txn_new();

        txn.delete(key.clone(), pkey.clone()).unwrap();
        let committed_ts = txn.commit().unwrap();
        assert!(committed_ts > 0);

        let hash_table_get_result = <MvccHashJoinTable<InMemPool> as MvccIndex<InMemPool>>::get(
            &table_inner,
            &key,
            &pkey,
            committed_ts,
        );
        assert!(hash_table_get_result.is_ok());
        assert_eq!(hash_table_get_result.unwrap(), None);

        Ok(())
    }

    #[test]
    fn test_txn_read_committed_6() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));

        let prefill_entries = vec![
            (b"key1".to_vec(), b"pkey1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"pkey2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"pkey3".to_vec(), b"value3".to_vec()),
        ];
        // txn timestamp start from 1, so 0 can be used in prefill
        for entry in &prefill_entries {
            let entry = entry.clone();
            table_inner.insert(entry.0, entry.1, 0, 0, entry.2).unwrap();
        }

        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let txn1_key = b"key_txn1".to_vec();
        let txn1_pkey = b"pkey_txn1".to_vec();
        let txn1_value = b"value_txn1".to_vec();

        let txn1_update_value = b"new_value_txn1".to_vec();

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        // insert a new key in txn1
        txn1.insert(txn1_key.clone(), txn1_pkey.clone(), txn1_value.clone())
            .unwrap();
        // update old key to new value in txn1
        for old_entry in &prefill_entries {
            let old_entry = old_entry.clone();
            txn1.update(old_entry.0, old_entry.1, txn1_update_value.clone())
                .unwrap();
        }

        // check consistency of TXN1
        assert_eq!(
            txn1.get(&txn1_key, &txn1_pkey).unwrap().as_ref().unwrap(),
            &txn1_value
        );
        for old_entry in &prefill_entries {
            let old_entry = old_entry.clone();
            let txn1_get_result = txn1.get(&old_entry.0, &old_entry.1).unwrap();
            assert_eq!(txn1_get_result.as_ref().unwrap(), &txn1_update_value);
        }

        // check repeatable read of TXN2 before commit of TXN1
        let txn2_get_result: Option<Vec<u8>> = txn2.get(&txn1_key, &txn1_pkey).unwrap();
        assert_eq!(txn2_get_result, None);

        for old_entry in &prefill_entries {
            let txn2_get_result = txn2.get(&old_entry.0, &old_entry.1).unwrap();
            assert_eq!(txn2_get_result.as_ref().unwrap(), &old_entry.2);
        }

        txn1.commit().unwrap();
        // check repeatable read of TXN2 after commit of TXN1
        let txn2_get_result = txn2.get(&txn1_key, &txn1_pkey).unwrap();
        assert_eq!(txn2_get_result, Some(txn1_value));

        for old_entry in &prefill_entries {
            let txn2_get_result = txn2.get(&old_entry.0, &old_entry.1).unwrap();
            assert_eq!(txn2_get_result.as_ref().unwrap(), &txn1_update_value);
        }

        Ok(())
    }

    #[test]
    fn test_txn_read_committed_1() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));

        let prefill_entries = vec![
            (b"key1".to_vec(), b"pkey1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"pkey2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"pkey3".to_vec(), b"value3".to_vec()),
        ];
        // txn timestamp start from 1, so 0 can be used in prefill
        for entry in &prefill_entries {
            let entry = entry.clone();
            table_inner.insert(entry.0, entry.1, 0, 0, entry.2).unwrap();
        }

        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let conflict_key = b"key_txn1".to_vec();
        let conflict_pkey = b"pkey_txn1".to_vec();
        let conflict_value = b"value_txn1".to_vec();

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        // insert a new key in txn1
        txn1.insert(
            conflict_key.clone(),
            conflict_pkey.clone(),
            conflict_value.clone(),
        )
        .unwrap();
        txn2.update(conflict_key, conflict_pkey, b"txn2_update".to_vec())
            .unwrap();

        txn1.commit().unwrap();

        // txn2 can not be committed
        // because it should update succ after commit of txn1,
        // but in its view, it will not update successfully
        let txn2_commit_result = txn2.commit();
        assert!(txn2_commit_result.is_err());

        Ok(())
    }

    #[test]
    fn test_txn_read_committed_2() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));

        let prefill_entries = vec![
            (b"key1".to_vec(), b"pkey1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"pkey2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"pkey3".to_vec(), b"value3".to_vec()),
        ];
        // txn timestamp start from 1, so 0 can be used in prefill
        for entry in &prefill_entries {
            let entry = entry.clone();
            table_inner.insert(entry.0, entry.1, 0, 0, entry.2).unwrap();
        }

        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let conflict_key = b"key_txn1".to_vec();
        let conflict_pkey = b"pkey_txn1".to_vec();
        let conflict_value = b"value_txn1".to_vec();

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        // insert a new key in txn1
        txn1.insert(
            conflict_key.clone(),
            conflict_pkey.clone(),
            conflict_value.clone(),
        )
        .unwrap();
        txn2.delete(conflict_key, conflict_pkey).unwrap();

        txn1.commit().unwrap();

        // txn2 can not be committed
        // because it should delete succ after commit of txn1,
        // but in its view, it will not deletesuccessfully
        let txn2_commit_result = txn2.commit();
        assert!(txn2_commit_result.is_err());

        Ok(())
    }

    #[test]
    fn test_txn_read_committed_3() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));

        let prefill_entries = vec![
            (b"key1".to_vec(), b"pkey1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"pkey2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"pkey3".to_vec(), b"value3".to_vec()),
        ];
        // txn timestamp start from 1, so 0 can be used in prefill
        for entry in &prefill_entries {
            let entry = entry.clone();
            table_inner.insert(entry.0, entry.1, 0, 0, entry.2).unwrap();
        }

        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let conflict_key1 = b"conflict1".to_vec();
        let conflict_pkey1 = b"conflict1".to_vec();
        let conflict_value1 = b"conflict1".to_vec();

        let conflict_key2 = b"conflict2".to_vec();
        let conflict_pkey2 = b"conflict2".to_vec();
        let conflict_value2 = b"conflict2".to_vec();

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        txn1.get(&conflict_key2, &conflict_pkey2).unwrap();
        txn1.insert(
            conflict_key1.clone(),
            conflict_pkey1.clone(),
            conflict_value1.clone(),
        )
        .unwrap();
        txn1.commit().unwrap();

        // txn2 should read committed value
        let get_res = txn2.get(&conflict_key1, &conflict_pkey1).unwrap();
        assert!(get_res.is_some());
        assert_eq!(get_res.unwrap(), conflict_value1);
        txn2.insert(
            conflict_key2.clone(),
            conflict_pkey2.clone(),
            conflict_value2.clone(),
        )
        .unwrap();



        // txn2 can be committed
        let txn2_commit_result = txn2.commit();
        assert!(txn2_commit_result.is_ok());
        Ok(())
    }

    #[test]
    fn test_txn_read_committed_4() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));

        let prefill_entries = vec![
            (b"key1".to_vec(), b"pkey1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"pkey2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"pkey3".to_vec(), b"value3".to_vec()),
        ];
        // txn timestamp start from 1, so 0 can be used in prefill
        for entry in &prefill_entries {
            let entry = entry.clone();
            table_inner.insert(entry.0, entry.1, 0, 0, entry.2).unwrap();
        }

        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let conflict_key1 = b"conflict1".to_vec();
        let conflict_pkey1 = b"conflict1".to_vec();
        let conflict_value1 = b"conflict1".to_vec();

        let conflict_key2 = b"conflict2".to_vec();
        let conflict_pkey2 = b"conflict2".to_vec();
        let conflict_value2 = b"conflict2".to_vec();

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        txn1.get(&conflict_key2, &conflict_pkey2).unwrap();
        txn1.insert(
            conflict_key1.clone(),
            conflict_pkey1.clone(),
            conflict_value1.clone(),
        )
        .unwrap();

        txn1.insert(
            conflict_key2.clone(),
            conflict_pkey2.clone(),
            conflict_value2.clone(),
        )
        .unwrap();

        txn2.get(&conflict_key1, &conflict_pkey1).unwrap();
        txn2.get(&conflict_key2, &conflict_pkey2).unwrap();

        let txn1_commit_res = txn1.commit();
        let txn2_commit_res = txn2.commit();
        // txn1 and txn2 can both commit, since txn2 is read_only
        // BUT txn2's commit ts must < txn1.commit_ts,
        // EVEN txn2 commit after txn1
        assert!(txn1_commit_res.is_ok());
        assert!(txn2_commit_res.is_ok());
        assert!(txn1_commit_res.unwrap() > txn2_commit_res.unwrap());

        Ok(())
    }

    #[test]
    fn test_txn_read_committed_5() -> Result<()> {
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(100, 100);

        let table_inner = Arc::new(MvccHashJoinTable::new(c_key, mem_pool.clone()));
        let txn_hash_table = TxnMvccHashTable::new(&table_inner);

        let txn1 = txn_hash_table.txn_new();
        let txn2 = txn_hash_table.txn_new();

        txn1.insert(b"test1".to_vec(), b"test1".to_vec(), b"233".to_vec())
            .unwrap();
        txn2.insert(b"test2".to_vec(), b"test2".to_vec(), b"233".to_vec())
            .unwrap();

        let txn3 = txn_hash_table.txn_new();
        assert!(txn3
            .get(&b"test1".to_vec(), &b"test1".to_vec())
            .unwrap()
            .is_none());
        assert!(txn3
            .get(&b"test2".to_vec(), &b"test2".to_vec())
            .unwrap()
            .is_none());
        txn1.commit().unwrap();
        txn2.commit().unwrap();

        assert!(txn3
            .get(&b"test1".to_vec(), &b"test1".to_vec())
            .unwrap()
            .is_some());
        assert!(txn3
            .get(&b"test2".to_vec(), &b"test2".to_vec())
            .unwrap()
            .is_some());

        drop(txn3);

        let txn4 = txn_hash_table.txn_new();
        assert_eq!(
            txn4.get(&b"test1".to_vec(), &b"test1".to_vec())
                .unwrap()
                .unwrap(),
            b"233".to_vec()
        );
        assert_eq!(
            txn4.get(&b"test2".to_vec(), &b"test2".to_vec())
                .unwrap()
                .unwrap(),
            b"233".to_vec()
        );

        txn4.update(b"test2".to_vec(), b"test2".to_vec(), b"2333".to_vec())
            .unwrap();
        assert_eq!(
            txn4.get(&b"test1".to_vec(), &b"test1".to_vec())
                .unwrap()
                .unwrap(),
            b"233".to_vec()
        );
        assert_eq!(
            txn4.get(&b"test2".to_vec(), &b"test2".to_vec())
                .unwrap()
                .unwrap(),
            b"2333".to_vec()
        );

        txn4.delete(b"test2".to_vec(), b"test2".to_vec()).unwrap();
        assert_eq!(
            txn4.get(&b"test1".to_vec(), &b"test1".to_vec())
                .unwrap()
                .unwrap(),
            b"233".to_vec()
        );
        assert_eq!(
            txn4.get(&b"test2".to_vec(), &b"test2".to_vec()).unwrap(),
            None
        );

        Ok(())
    }
}
