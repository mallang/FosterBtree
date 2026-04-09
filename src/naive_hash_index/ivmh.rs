/// IVMH (IVM-style Hash Table) — IVM baseline for MVHT comparison.
///
/// Maintains:
/// - a base-table MVCC model used to materialize historical snapshots
/// - a single page-based current-state NaiveHashTable updated in place
///
/// This matches the paper's intended semantics: the base table exists
/// independently, while the derived hash state keeps only the latest state.
/// Old readable snapshots are materialized lazily from the base heap when a
/// historical scan/join/delta first needs them.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
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
        HeapBaseMvccTable, HeapHashChain, SnapshotStat,
    },
    prelude::{AccessMethodError, Timestamp},
};

pub struct IvmHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    bucket_count: usize,

    base_table: HeapBaseMvccTable,
    next_auto_ts: Cell<Timestamp>,
    current_ts: Cell<Timestamp>,

    /// Latest derived hash state.
    current_table: NaiveHashTable<T>,

    /// Materialized historical snapshots, built lazily from the base heap.
    snapshots: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    /// Readable timestamps published by the benchmark. Historical accesses may
    /// materialize these on demand from the base heap.
    readable_timestamps: RefCell<HashSet<Timestamp>>,

    snapshots_built_total: Cell<usize>,
    snapshot_reads_total: Cell<usize>,
    snapshot_cache_hits_total: Cell<usize>,
    snapshot_cache_misses_total: Cell<usize>,
    current_reads_total: Cell<usize>,
    readable_timestamps_published: Cell<usize>,
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
            current_ts: Cell::new(0),
            current_table,
            snapshots: RefCell::new(HashMap::new()),
            readable_timestamps: RefCell::new(HashSet::new()),
            snapshots_built_total: Cell::new(0),
            snapshot_reads_total: Cell::new(0),
            snapshot_cache_hits_total: Cell::new(0),
            snapshot_cache_misses_total: Cell::new(0),
            current_reads_total: Cell::new(0),
            readable_timestamps_published: Cell::new(0),
        }
    }

    pub fn prepare_insert_base(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.base_table.insert_at_ts(k, pk, v, 0);
        self.current_ts.set(self.current_ts.get().max(0));
    }

    pub fn prepare_insert_base_at_ts(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.insert_at_ts(k, pk, v, ts);
        self.current_ts.set(self.current_ts.get().max(ts));
    }

    pub fn prepare_update_base(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.update_at_ts(k, pk, v, ts);
        self.current_ts.set(self.current_ts.get().max(ts));
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

    pub fn populate_current_from_base(&self, ts: Timestamp) -> Duration {
        let rows = self.base_table.scan_as_of(ts);
        self.populate_current_from_iter(
            rows.iter()
                .map(|(k, pk, v)| (k.as_slice(), pk.as_slice(), v.as_slice())),
        )
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

    fn is_current_or_future(&self, ts: Timestamp) -> bool {
        ts >= self.current_ts.get()
    }

    fn is_readable_historical(&self, ts: Timestamp) -> bool {
        self.readable_timestamps.borrow().contains(&ts)
    }

    fn get_or_build_snapshot(
        &self,
        ts: Timestamp,
    ) -> Result<Arc<NaiveHashTable<T>>, AccessMethodError> {
        if let Some(table) = self.snapshots.borrow().get(&ts) {
            self.snapshot_cache_hits_total
                .set(self.snapshot_cache_hits_total.get() + 1);
            self.snapshot_reads_total
                .set(self.snapshot_reads_total.get() + 1);
            return Ok(table.clone());
        }
        if !self.is_readable_historical(ts) {
            return Err(AccessMethodError::InvalidTimestamp);
        }
        self.snapshot_cache_misses_total
            .set(self.snapshot_cache_misses_total.get() + 1);
        let snapshot = self.build_snapshot_from_base(ts);
        self.snapshots_built_total
            .set(self.snapshots_built_total.get() + 1);
        self.snapshot_reads_total
            .set(self.snapshot_reads_total.get() + 1);
        self.snapshots.borrow_mut().insert(ts, snapshot.clone());
        Ok(snapshot)
    }

    pub fn mark_ts(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        self.readable_timestamps.borrow_mut().insert(ts);
        self.readable_timestamps_published
            .set(self.readable_timestamps_published.get() + 1);
        start.elapsed()
    }

    pub fn ensure_snapshot_materialized(&self, ts: Timestamp) -> Duration {
        if self.is_current_or_future(ts) {
            return Duration::default();
        }
        if self.snapshots.borrow().contains_key(&ts) {
            self.snapshot_cache_hits_total
                .set(self.snapshot_cache_hits_total.get() + 1);
            return Duration::default();
        }
        if !self.is_readable_historical(ts) {
            return Duration::default();
        }
        self.snapshot_cache_misses_total
            .set(self.snapshot_cache_misses_total.get() + 1);
        let start = Instant::now();
        let snapshot = self.build_snapshot_from_base(ts);
        self.snapshots_built_total
            .set(self.snapshots_built_total.get() + 1);
        self.snapshots.borrow_mut().insert(ts, snapshot);
        start.elapsed()
    }

    pub fn cache_current_as_snapshot(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        let snapshot = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        for (k, pk, v) in self.current_table.scan().unwrap() {
            snapshot.insert(RecordRef::new(&k, &pk, &v)).unwrap();
        }
        let duration = start.elapsed();
        self.snapshots_built_total
            .set(self.snapshots_built_total.get() + 1);
        self.snapshots.borrow_mut().insert(ts, snapshot);
        self.readable_timestamps.borrow_mut().insert(ts);
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
        self.snapshots_built_total
            .set(self.snapshots_built_total.get() + 1);
        self.snapshots.borrow_mut().insert(ts, snapshot);
        self.readable_timestamps.borrow_mut().insert(ts);
        duration
    }

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        if self.is_current_or_future(ts) {
            self.current_reads_total
                .set(self.current_reads_total.get() + 1);
            self.current_table.get(k, pk).unwrap()
        } else {
            self.get_or_build_snapshot(ts)
                .ok()
                .and_then(|table| table.get(k, pk).unwrap())
        }
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        if self.is_current_or_future(ts) {
            self.current_reads_total
                .set(self.current_reads_total.get() + 1);
            let mut result = vec![];
            self.current_table.scan_key_vec(key, &mut result).unwrap();
            return Ok(result);
        }
        let table = self.get_or_build_snapshot(ts)?;
        let mut result = vec![];
        table.scan_key_vec(key, &mut result).unwrap();
        Ok(result)
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        if self.is_current_or_future(ts) {
            self.current_reads_total
                .set(self.current_reads_total.get() + 1);
            let res: Vec<_> = self.current_table.scan().unwrap().collect();
            return Ok(Box::new(res.into_iter()));
        }
        let table = self.get_or_build_snapshot(ts)?;
        let res: Vec<_> = table.scan().unwrap().collect();
        Ok(Box::new(res.into_iter()))
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
        let from = self.get_or_build_snapshot(from_ts)?;
        let mut delta_map = HashMap::new();
        if self.is_current_or_future(to_ts) {
            for i in 0..self.bucket_count {
                let from_bucket = from.get_chain(i);
                let to_bucket = self.current_table.get_chain(i);
                HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map)?;
            }
        } else {
            let to = self.get_or_build_snapshot(to_ts)?;
            for i in 0..self.bucket_count {
                let from_bucket = from.get_chain(i);
                let to_bucket = to.get_chain(i);
                HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map)?;
            }
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

    pub fn delta_scan_from_snapshot_to_current(
        &self,
        from_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)> + Send>,
        AccessMethodError,
    > {
        let from = self.get_or_build_snapshot(from_ts)?;

        let mut delta_map = HashMap::new();
        for i in 0..self.bucket_count {
            let from_bucket = from.get_chain(i);
            let to_bucket = self.current_table.get_chain(i);
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

    pub fn advance_readable_epoch_and_collect_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>, AccessMethodError> {
        self.mark_ts(to_ts);
        let deltas: Vec<_> = self.delta_scan_from_snapshot_to_current(from_ts)?.collect();
        Ok(deltas)
    }

    pub fn garbage_collect(&self, ts: Timestamp) {
        self.snapshots.borrow_mut().retain(|&k, _| k > ts);
        self.readable_timestamps.borrow_mut().retain(|&k| k > ts);
    }

    pub fn collect_space_stat_into_collector(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        self.current_table.collect_space_stat(&mut stat);
        for table in self.snapshots.borrow().values() {
            table.collect_space_stat(&mut stat);
        }
        let current_valid_space = self
            .current_table
            .scan()
            .unwrap()
            .map(|entry| entry.0.len() + entry.1.len() + entry.2.len())
            .sum::<usize>();
        stat.inc_valid_space(current_valid_space);
        stat
    }

    pub fn print_stats(&self) {
        let snaps = self.snapshots.borrow();
        println!("IVMH current_table bucket count: {}", self.bucket_count);
        println!("IVMH materialised snapshots: {}", snaps.len());
    }

    pub fn collect_snapshot_stat(&self) -> SnapshotStat {
        SnapshotStat {
            readable_timestamps_published: self.readable_timestamps_published.get(),
            retained_snapshots: self.snapshots.borrow().len(),
            snapshots_built_total: self.snapshots_built_total.get(),
            snapshot_reads_total: self.snapshot_reads_total.get(),
            snapshot_cache_hits_total: self.snapshot_cache_hits_total.get(),
            snapshot_cache_misses_total: self.snapshot_cache_misses_total.get(),
            current_reads_total: self.current_reads_total.get(),
        }
    }
}
