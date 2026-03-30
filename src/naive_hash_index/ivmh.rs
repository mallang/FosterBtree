/// IVMH (IVM-style Hash Table) — IVM baseline for MVHT comparison.
///
/// Maintains a *single page-based current-state NaiveHashTable* with in-place updates.
/// Updates are O(|Δ|): find pkey in the hash chain and overwrite the value in-place
/// (same size) or delete + reinsert (different size).
///
/// Because current_table IS a NaiveHashTable (identical page structure to SNAP snapshots),
/// J2 probe can read directly from current_table — no mark_ts rebuild needed before probing.
///
/// mark_ts() is still available for historical snapshots: it scans current_table and
/// copies all records into a new NaiveHashTable snapshot (O(|R|), same as SNAP).
///
/// Compared to SNAP:  update is O(|Δ|) in-place instead of O(|R|) full rebuild;
///                    J2 build is 0 ms (no mark_ts needed after updates).
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

pub struct IvmHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    bucket_count: usize,

    /// Page-based hash table for the current state.
    /// Always up-to-date: inserts and updates are applied in-place (O(|Δ|)).
    /// Probe reads go directly here for recent timestamps — no mark_ts needed.
    current_table: NaiveHashTable<T>,

    /// Materialized snapshots at specific timestamps (for historical reads / delta scan).
    snapshots: std::cell::RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    /// The most recent timestamp passed to mark_ts().
    latest_mark_ts: std::cell::RefCell<Option<Timestamp>>,
}

impl<T: MemPool + 'static> IvmHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        let current_table = NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_count);
        Self {
            c_key,
            mem_pool,
            bucket_count,
            current_table,
            snapshots: std::cell::RefCell::new(HashMap::new()),
            latest_mark_ts: std::cell::RefCell::new(None),
        }
    }

    // -----------------------------------------------------------------------
    // Writes — O(|Δ|) in-place on current_table
    // -----------------------------------------------------------------------

    /// Insert a new record into current_table.
    pub fn add_insert_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .insert(RecordRef::new(k, pk, v))
            .unwrap();
    }

    /// Update an existing record in current_table (in-place page update).
    pub fn add_update_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .update(RecordRef::new(k, pk, v))
            .unwrap();
    }

    // -----------------------------------------------------------------------
    // Snapshot materialisation (O(|R|)) — for historical reads
    // -----------------------------------------------------------------------

    /// Materialise current_table as a snapshot at timestamp `ts`.
    /// Scans current_table and copies all records into a new NaiveHashTable.
    pub fn mark_ts(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        let snapshot = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        let records: Vec<_> = self.current_table.scan().unwrap().collect();
        for (k, pk, v) in records {
            snapshot.insert(RecordRef::new(&k, &pk, &v)).unwrap();
        }
        let duration = start.elapsed();
        self.snapshots.borrow_mut().insert(ts, snapshot);
        let mut lts = self.latest_mark_ts.borrow_mut();
        if lts.map_or(true, |prev| ts > prev) {
            *lts = Some(ts);
        }
        duration
    }

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    fn is_recent(&self, ts: Timestamp) -> bool {
        self.latest_mark_ts.borrow().map_or(true, |lts| ts > lts)
    }

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        {
            let snaps = self.snapshots.borrow();
            if let Some(table) = snaps.get(&ts) {
                return table.get(k, pk).unwrap();
            }
        }
        if self.is_recent(ts) {
            // Recent: read directly from current_table (page-based, O(bucket)).
            self.current_table.get(k, pk).unwrap()
        } else {
            self.mark_ts(ts);
            let snaps = self.snapshots.borrow();
            snaps.get(&ts).and_then(|t| t.get(k, pk).unwrap())
        }
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        {
            let snaps = self.snapshots.borrow();
            if let Some(table) = snaps.get(&ts) {
                let mut result = vec![];
                table.scan_key_vec(key, &mut result).unwrap();
                return Ok(result);
            }
        }
        if self.is_recent(ts) {
            // Recent: probe current_table directly — page-based, O(bucket).
            let mut result = vec![];
            self.current_table.scan_key_vec(key, &mut result).unwrap();
            Ok(result)
        } else {
            self.mark_ts(ts);
            let snaps = self.snapshots.borrow();
            let mut result = vec![];
            if let Some(table) = snaps.get(&ts) {
                table.scan_key_vec(key, &mut result).unwrap();
            }
            Ok(result)
        }
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        {
            let snaps = self.snapshots.borrow();
            if let Some(table) = snaps.get(&ts) {
                let res: Vec<_> = table.scan().unwrap().collect();
                return Ok(Box::new(res.into_iter()));
            }
        }
        if self.is_recent(ts) {
            // Recent: scan current_table directly.
            let res: Vec<_> = self.current_table.scan().unwrap().collect();
            Ok(Box::new(res.into_iter()))
        } else {
            self.mark_ts(ts);
            let snaps = self.snapshots.borrow();
            let res: Vec<_> = snaps
                .get(&ts)
                .map(|t| t.scan().unwrap().collect())
                .unwrap_or_default();
            Ok(Box::new(res.into_iter()))
        }
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

        if !self.snapshots.borrow().contains_key(&from_ts) {
            self.mark_ts(from_ts);
        }
        if !self.snapshots.borrow().contains_key(&to_ts) {
            self.mark_ts(to_ts);
        }

        let tables = self.snapshots.borrow();
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
    // Garbage collection
    // -----------------------------------------------------------------------

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
        let snaps = self.snapshots.borrow();
        println!("IVMH current_table bucket count: {}", self.bucket_count);
        println!("IVMH materialised snapshots: {}", snaps.len());
    }
}
