use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use crate::{
    bp::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_debug, log_info, log_warn,
    mvcc_index::{
        hash_common::{fix_frame_id2, write_page, KVWithTs, RowDelta, StatCollector},
        hash_join_heap_chain::HeapHashChain,
        hash_join_page::HashJoinPage,
        Delta, MvccEntry, TxId,
    },
    prelude::{AccessMethodError, Timestamp},
};

#[derive(Debug, Clone)]
pub struct ChainBucketBulkUpdate {
    pub updated_entries: HashMap<Vec<u8>, Vec<u8>>, // pkey, (new_value)
    pub old_entries: Vec<MvccEntry>,
}

pub struct DualChainBucket<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    recent_chain: Arc<HeapHashChain<T>>,
    history_chain: Arc<HeapHashChain<T>>,

    bulk_update: Mutex<ChainBucketBulkUpdate>,
}

impl<T: MemPool + 'static> DualChainBucket<T> {
    pub fn collect_page_num(&self) -> usize {
        self.recent_chain.collect_page_num() + self.history_chain.collect_page_num()
    }
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let recent_chain = Arc::new(HeapHashChain::new(c_key, mem_pool.clone()));
        let history_chain = Arc::new(HeapHashChain::new(c_key, mem_pool.clone()));

        Self {
            c_key,
            mem_pool,
            recent_chain,
            history_chain,
            bulk_update: Mutex::new(ChainBucketBulkUpdate {
                updated_entries: HashMap::new(),
                old_entries: Vec::new(),
            }),
        }
    }

    /// Construct from two pre-allocated pages (bulk alloc path).
    pub fn new_from_pages(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        recent_page: FrameWriteGuard,
        history_page: FrameWriteGuard,
    ) -> Self {
        let recent_chain = Arc::new(HeapHashChain::new_from_page(c_key, mem_pool.clone(), recent_page));
        let history_chain = Arc::new(HeapHashChain::new_from_page(c_key, mem_pool.clone(), history_page));

        Self {
            c_key,
            mem_pool,
            recent_chain,
            history_chain,
            bulk_update: Mutex::new(ChainBucketBulkUpdate {
                updated_entries: HashMap::new(),
                old_entries: vec![],
            }),
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        self.recent_chain.insert(entry)
    }

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let recent_result = self.recent_chain.chain_get(pkey, ts);

        match recent_result {
            Ok(entry) => Ok(entry),
            Err(AccessMethodError::KeyNotFound)
            | Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_entry = self.history_chain.chain_get(pkey, ts);
                match history_entry {
                    Ok(entry) => Ok(entry),
                    Err(e) => Err(e),
                }
            }
            Err(e) => panic!("unreachable err: {:?}", e),
        }
    }

    pub fn update(
        &self,
        pkey: &[u8],
        entry: &MvccEntry,
        is_bulk_update: bool,
    ) -> Result<(), AccessMethodError> {
        if is_bulk_update {
            let mut bulk = self.bulk_update.lock().unwrap();
            bulk.updated_entries
                .insert(pkey.to_vec(), entry.value().to_vec());
            return Ok(());
        }
        let old_result = self.recent_chain.chain_update_recent(pkey, entry);
        match old_result {
            Ok(old_entry) => {
                self.history_chain.history_insert(&old_entry)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn do_bulk_update(&self, new_start_ts: Timestamp) -> Result<(), AccessMethodError> {
        let mut bulk = self.bulk_update.lock().unwrap();
        self.recent_chain.chain_bulk_update_collect_old_entries(
            self.recent_chain.first_key(),
            &mut bulk,
            new_start_ts,
        )?;
        self.history_chain
            .chain_bulk_update_history_entries(&bulk)?;
        bulk.old_entries.clear();
        bulk.updated_entries.clear();
        Ok(())
    }

    pub fn delete(&self, pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
        let old_result = self.recent_chain.delete(pkey, ts);
        match old_result {
            Ok(mut old_entry) => {
                old_entry.set_end_ts(ts);
                self.history_chain.history_insert(&old_entry)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        self.history_chain.gc_truncate_entries_before_ts(ts)
    }

    pub fn stat(&self) -> String {
        // Obtain stats from both chains.
        let recent_stat = self.recent_chain.stat();
        let history_stat = self.history_chain.stat();
        // Format a combined report.
        format!(
            "=== SecondBucket Stats ===\nRecent Chain:\n{}\nHistory Chain:\n{}",
            recent_stat, history_stat
        )
    }

    pub fn scan_key_into(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        res: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        self.recent_chain
            .chain_scan_key(search_key, ts, res)
            .unwrap();
        if !self.history_chain.is_empty() {
            self.history_chain
                .chain_scan_key(search_key, ts, res)
                .unwrap();
        }
    }

    pub fn scan_into_vec(
        &self,
        ts: Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        self.recent_chain.chain_scan_into_vec(ts, results)?;
        if !self.history_chain.is_empty() {
            self.history_chain.chain_scan_into_vec(ts, results)?;
        }
        Ok(())
    }

    pub fn scan_into_vec_recent_ignore_ts(
        &self,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        self.recent_chain
            .chain_scan_into_vec_ignore_ts(results)
            .unwrap();
        Ok(())
    }

    pub fn delta_scan(
        &self,
        from: Timestamp,
        to: Timestamp,
        results: &mut Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>,
    ) -> Result<(), AccessMethodError> {
        let mut delta_map = HashMap::<Vec<u8>, RowDelta>::new();
        self.recent_chain
            .scan_delta_into(from, to, &mut delta_map)?;
        self.history_chain
            .scan_delta_into(from, to, &mut delta_map)?;

        results.extend(delta_map.into_iter().filter_map(|(pk, from_to_delta)| {
            let (from_kv, to_kv) = from_to_delta.split();
            if &to_kv == &KVWithTs::default() {
                // both invalid
                None
            } else if &from_kv == &KVWithTs::default() {
                // from is invalid but to is valid
                Some((
                    to_kv.get_k().to_vec(),
                    pk,
                    Delta::Inserted(to_kv.get_v().to_vec()),
                ))
            } else {
                // both is valid
                if from_kv.get_v() == to_kv.get_v() {
                    // no change
                    None
                } else {
                    Some((
                        to_kv.get_k().to_vec(),
                        pk,
                        Delta::Updated(to_kv.get_v().to_vec()),
                    ))
                }
            }
        }));

        Ok(())
    }

    pub fn scan_all(&self, results: &mut Vec<MvccEntry>) -> Result<(), AccessMethodError> {
        results.extend(self.recent_chain.scan_all()?);
        results.extend(self.history_chain.scan_all()?);
        Ok(())
    }

    pub fn collect_space_stat(&self, stat: &mut StatCollector) -> Result<(), AccessMethodError> {
        self.recent_chain.collect_space_statistics(stat);
        self.history_chain.collect_space_statistics(stat);
        Ok(())
    }
}
