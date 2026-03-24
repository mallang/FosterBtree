/// IVMH (IVM-style Hash Table) — IVM baseline for MVHT comparison.
///
/// Maintains a *single current-state* hash table with in-place updates (O(|Δ|)).
/// When a snapshot or historical probe is requested, the current table is rebuilt
/// from accumulated state.  Delta scans diff two materialized snapshots (like SNAP).
///
/// Compared to SNAP:  update is O(|Δ|) instead of O(|R|),
///                     but history/delta still requires full rebuild + diff.
/// Compared to MVHT:  no multi-version retention, no native delta scan.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{
        hash_common::StatCollector,
        hash_join_page::record::RecordRef,
        Delta,
    },
    naive_hash_index::{
        naive_hash_table::{hash_join_table::NaiveHashTable, SingleTsHashTable},
        HeapHashChain,
    },
    prelude::{AccessMethodError, Timestamp},
    mvcc_index::hash_common::KVWithTs,
};

/// A single current-state record.
#[derive(Clone)]
struct CurrentRec {
    key: Vec<u8>,
    pkey: Vec<u8>,
    value: Vec<u8>,
}

pub struct IvmHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    bucket_count: usize,

    /// Current state: pkey → CurrentRec  (latest version of every record)
    current_state: std::cell::RefCell<HashMap<Vec<u8>, CurrentRec>>,

    /// Materialized snapshots for scan/probe at specific timestamps
    snapshots: std::cell::RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,
}

impl<T: MemPool + 'static> IvmHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        Self {
            c_key,
            mem_pool,
            bucket_count,
            current_state: std::cell::RefCell::new(HashMap::new()),
            snapshots: std::cell::RefCell::new(HashMap::new()),
        }
    }

    // -----------------------------------------------------------------------
    // In-place updates on current state (O(|Δ|))
    // -----------------------------------------------------------------------

    /// Insert a new record into current state.
    pub fn add_insert_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let rec = CurrentRec {
            key: k.to_vec(),
            pkey: pk.to_vec(),
            value: v.to_vec(),
        };
        self.current_state.borrow_mut().insert(pk.to_vec(), rec);
    }

    /// Update an existing record in current state (in-place overwrite).
    pub fn add_update_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        // Same as insert — overwrites previous version.
        self.add_insert_rec(k, pk, v);
    }

    /// Delete a record from current state.
    pub fn add_delete_rec(&self, pk: &[u8]) {
        self.current_state.borrow_mut().remove(pk);
    }

    // -----------------------------------------------------------------------
    // Snapshot materialisation (O(|R|))
    // -----------------------------------------------------------------------

    /// Materialise the current state as a snapshot at timestamp `ts`.
    /// Returns the time taken to build the snapshot.
    pub fn mark_ts(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        let state = self.current_state.borrow();
        for rec in state.values() {
            table
                .insert(RecordRef::new(&rec.key, &rec.pkey, &rec.value))
                .unwrap();
        }
        let duration = start.elapsed();
        self.snapshots.borrow_mut().insert(ts, table);
        duration
    }

    /// Build a snapshot from an explicit set of records (for initial load).
    pub fn build_table_from_recs_and_ts(
        &self,
        ts: Timestamp,
        recs: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        let start = Instant::now();
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        for (k, pk, v) in &recs {
            table.insert(RecordRef::new(k, pk, v)).unwrap();
        }
        let duration = start.elapsed();
        self.snapshots.borrow_mut().insert(ts, table);
        duration
    }

    // -----------------------------------------------------------------------
    // Reads — delegate to materialised snapshots
    // -----------------------------------------------------------------------

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        let snaps = self.snapshots.borrow();
        if let Some(table) = snaps.get(&ts) {
            table.get(k, pk).unwrap()
        } else {
            None
        }
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let snaps = self.snapshots.borrow();
        let mut result = vec![];
        if let Some(table) = snaps.get(&ts) {
            table.scan_key_vec(key, &mut result).unwrap();
            Ok(result)
        } else {
            Ok(vec![])
        }
    }

    /// Scan key against the *current* (not materialised) state.
    /// Avoids a full rebuild when only current-snapshot probes are needed.
    pub fn scan_key_vec_current(
        &self,
        key: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let state = self.current_state.borrow();
        state
            .values()
            .filter(|rec| rec.key == key)
            .map(|rec| (rec.pkey.clone(), rec.value.clone()))
            .collect()
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let snaps = self.snapshots.borrow();
        let mut res = vec![];
        if let Some(table) = snaps.get(&ts) {
            res.extend(table.scan().unwrap());
        }
        Ok(Box::new(res.into_iter()))
    }

    // -----------------------------------------------------------------------
    // Delta scan — diff two materialised snapshots (O(|R|))
    // -----------------------------------------------------------------------

    pub fn delta_scan(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)> + Send>,
        AccessMethodError,
    > {
        assert!(from_ts < to_ts, "from_ts must be less than to_ts");
        let tables = self.snapshots.borrow();
        assert!(tables.contains_key(&from_ts), "from_ts snapshot does not exist");
        assert!(tables.contains_key(&to_ts), "to_ts snapshot does not exist");

        let from = tables.get(&from_ts).unwrap();
        let to = tables.get(&to_ts).unwrap();

        let mut delta_map = HashMap::new();
        for i in 0..self.bucket_count {
            let from_bucket = from.get_chain(i);
            let to_bucket = to.get_chain(i);
            HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map)?;
        }

        let result: Vec<_> = delta_map
            .into_iter()
            .filter_map(|(pk, from_to_delta)| {
                let (from_kv, to_kv) = from_to_delta.split();
                if to_kv == KVWithTs::default() {
                    None
                } else if from_kv == KVWithTs::default() {
                    Some((
                        to_kv.get_k().to_vec(),
                        pk,
                        Delta::Inserted(to_kv.get_v().to_vec()),
                    ))
                } else if from_kv.get_v() == to_kv.get_v() {
                    None
                } else {
                    Some((
                        to_kv.get_k().to_vec(),
                        pk,
                        Delta::Updated(to_kv.get_v().to_vec()),
                    ))
                }
            })
            .collect();
        Ok(Box::new(result.into_iter()))
    }

    // -----------------------------------------------------------------------
    // Garbage collection — drop snapshots no longer needed
    // -----------------------------------------------------------------------

    /// Remove snapshots with timestamp <= `ts` to free memory.
    pub fn garbage_collect(&self, ts: Timestamp) {
        self.snapshots.borrow_mut().retain(|&k, _| k > ts);
    }

    // -----------------------------------------------------------------------
    // Stats
    // -----------------------------------------------------------------------

    pub fn collect_space_stat_into_collector(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        let snaps = self.snapshots.borrow();
        for table in snaps.values() {
            table.collect_space_stat(&mut stat);
        }
        stat
    }

    pub fn print_stats(&self) {
        let state = self.current_state.borrow();
        println!("IVMH current state: {} records", state.len());
        let snaps = self.snapshots.borrow();
        println!("IVMH materialised snapshots: {}", snaps.len());
        println!("IVMH bucket count: {}", self.bucket_count);
    }
}
