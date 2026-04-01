/// IVMH (IVM-style Hash Table) — IVM baseline for MVHT comparison.
///
/// Maintains:
/// - a base-table MVCC model used to materialize historical snapshots
/// - a single page-based current-state NaiveHashTable updated in place
///
/// This matches the paper's intended semantics: the base table exists
/// independently, while the derived hash state keeps only the latest state.

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{
        hash_common::{KVWithTs, StatCollector},
        hash_join_page::record::RecordRef,
        Delta,
    },
    naive_hash_index::{
        naive_hash_table::{hash_join_table::NaiveHashTable, SingleTsHashTable},
        HeapBaseMvccTable, HeapHashChain,
    },
    prelude::{AccessMethodError, Timestamp},
};

pub struct IvmHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    bucket_count: usize,

    base_table: HeapBaseMvccTable,
    next_auto_ts: Cell<Timestamp>,

    /// Latest derived hash state.
    current_table: NaiveHashTable<T>,

    /// Materialized snapshots for retained historical timestamps.
    snapshots: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    /// The most recent timestamp passed to mark_ts().
    latest_mark_ts: Cell<Option<Timestamp>>,
}

impl<T: MemPool + 'static> IvmHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        let current_table =
            NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_count);
        Self {
            c_key,
            mem_pool,
            bucket_count,
            base_table: HeapBaseMvccTable::new(),
            next_auto_ts: Cell::new(1),
            current_table,
            snapshots: RefCell::new(HashMap::new()),
            latest_mark_ts: Cell::new(None),
        }
    }

    pub fn prepare_insert_base(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.base_table.insert_at_ts(k, pk, v, 0);
    }

    pub fn prepare_insert_base_at_ts(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.insert_at_ts(k, pk, v, ts);
    }

    pub fn prepare_update_base(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.update_at_ts(k, pk, v, ts);
    }

    pub fn insert_current(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .insert(RecordRef::new(k, pk, v))
            .unwrap();
    }

    pub fn update_current(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .update(RecordRef::new(k, pk, v))
            .unwrap();
    }

    /// Backward-compatible helper for older benchmarks: update both base and
    /// current derived state together.
    pub fn add_insert_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.prepare_insert_base(k, pk, v);
        self.insert_current(k, pk, v);
    }

    /// Backward-compatible helper for older benchmarks: update both base and
    /// current derived state together with an auto-assigned timestamp.
    pub fn add_update_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let ts = self.next_auto_ts.get();
        self.prepare_update_base(k, pk, v, ts);
        self.next_auto_ts.set(ts + 1);
        self.update_current(k, pk, v);
    }

    pub fn populate_current_from_iter<I, K, P, V>(&self, rows: I) -> Duration
    where
        I: IntoIterator<Item = (K, P, V)>,
        K: AsRef<[u8]>,
        P: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let start = Instant::now();
        for (k, pk, v) in rows {
            self.insert_current(k.as_ref(), pk.as_ref(), v.as_ref());
        }
        start.elapsed()
    }

    fn build_snapshot_from_iter<I, K, P, V>(&self, rows: I) -> (Arc<NaiveHashTable<T>>, Duration)
    where
        I: IntoIterator<Item = (K, P, V)>,
        K: AsRef<[u8]>,
        P: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let start = Instant::now();
        let snapshot = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        for (k, pk, v) in rows {
            snapshot
                .insert(RecordRef::new(k.as_ref(), pk.as_ref(), v.as_ref()))
                .unwrap();
        }
        (snapshot, start.elapsed())
    }

    fn build_snapshot_from_base(&self, ts: Timestamp) -> Arc<NaiveHashTable<T>> {
        let snapshot = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        for (k, pk, v) in self.base_table.scan_as_of(ts) {
            snapshot.insert(RecordRef::new(&k, &pk, &v)).unwrap();
        }
        snapshot
    }

    pub fn mark_ts(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        let snapshot = self.build_snapshot_from_base(ts);
        let duration = start.elapsed();
        self.snapshots.borrow_mut().insert(ts, snapshot);
        if self.latest_mark_ts.get().map_or(true, |prev| ts > prev) {
            self.latest_mark_ts.set(Some(ts));
        }
        duration
    }

    pub fn cache_snapshot_from_iter_and_ts<I, K, P, V>(&self, ts: Timestamp, rows: I) -> Duration
    where
        I: IntoIterator<Item = (K, P, V)>,
        K: AsRef<[u8]>,
        P: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let (snapshot, duration) = self.build_snapshot_from_iter(rows);
        self.snapshots.borrow_mut().insert(ts, snapshot);
        if self.latest_mark_ts.get().map_or(true, |prev| ts > prev) {
            self.latest_mark_ts.set(Some(ts));
        }
        duration
    }

    fn is_recent(&self, ts: Timestamp) -> bool {
        self.latest_mark_ts.get().map_or(true, |lts| ts > lts)
    }

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        if let Some(table) = self.snapshots.borrow().get(&ts) {
            return table.get(k, pk).unwrap();
        }
        if self.is_recent(ts) {
            self.current_table.get(k, pk).unwrap()
        } else {
            None
        }
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        if let Some(table) = self.snapshots.borrow().get(&ts) {
            let mut result = vec![];
            table.scan_key_vec(key, &mut result).unwrap();
            return Ok(result);
        }
        if self.is_recent(ts) {
            let mut result = vec![];
            self.current_table.scan_key_vec(key, &mut result).unwrap();
            Ok(result)
        } else {
            Err(AccessMethodError::InvalidTimestamp)
        }
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        if let Some(table) = self.snapshots.borrow().get(&ts) {
            let res: Vec<_> = table.scan().unwrap().collect();
            return Ok(Box::new(res.into_iter()));
        }
        if self.is_recent(ts) {
            let res: Vec<_> = self.current_table.scan().unwrap().collect();
            Ok(Box::new(res.into_iter()))
        } else {
            Err(AccessMethodError::InvalidTimestamp)
        }
    }

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
        let from = tables.get(&from_ts).ok_or(AccessMethodError::InvalidTimestamp)?;
        let to = tables.get(&to_ts).ok_or(AccessMethodError::InvalidTimestamp)?;

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

    pub fn garbage_collect(&self, ts: Timestamp) {
        self.snapshots.borrow_mut().retain(|&k, _| k > ts);
    }

    pub fn collect_space_stat_into_collector(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        self.current_table.collect_space_stat(&mut stat);
        for table in self.snapshots.borrow().values() {
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
