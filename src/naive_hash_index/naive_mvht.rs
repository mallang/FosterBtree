use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
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

pub struct NaiveMvHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,
    bucket_count: usize,

    /// Base-table MVCC state, modeled separately from derived snapshots.
    base_table: HeapBaseMvccTable,
    next_auto_ts: Cell<Timestamp>,

    /// Retained derived hash-table snapshots.
    naivetables: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    build_table_stats: RefCell<BTreeMap<Timestamp, Duration>>,
    scan_delta_stats: RefCell<BTreeMap<(Timestamp, Timestamp), Duration>>,
    table_space_stats: RefCell<BTreeMap<Timestamp, usize>>,
}

impl<T: MemPool + 'static> NaiveMvHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        Self {
            c_key,
            mem_pool,
            bucket_count,
            base_table: HeapBaseMvccTable::new(),
            next_auto_ts: Cell::new(1),
            naivetables: RefCell::new(HashMap::new()),
            build_table_stats: RefCell::new(BTreeMap::new()),
            scan_delta_stats: RefCell::new(BTreeMap::new()),
            table_space_stats: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn add_insert_rec_at_ts(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.insert_at_ts(k, pk, v, ts);
    }

    pub fn add_update_rec_at_ts(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        self.base_table.update_at_ts(k, pk, v, ts);
    }

    pub fn add_insert_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.add_insert_rec_at_ts(k, pk, v, 0);
    }

    pub fn add_update_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let ts = self.next_auto_ts.get();
        self.add_update_rec_at_ts(k, pk, v, ts);
        self.next_auto_ts.set(ts + 1);
    }

    fn build_table_at_ts(&self, ts: Timestamp) -> Arc<NaiveHashTable<T>> {
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        for (k, pk, v) in self.base_table.scan_as_of(ts) {
            table.insert(RecordRef::new(&k, &pk, &v)).unwrap();
        }
        table
    }

    fn build_table_from_iter<I, K, P, V>(&self, rows: I) -> (Arc<NaiveHashTable<T>>, Duration)
    where
        I: IntoIterator<Item = (K, P, V)>,
        K: AsRef<[u8]>,
        P: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let start = Instant::now();
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));

        for (k, pk, v) in rows {
            table
                .insert(RecordRef::new(k.as_ref(), pk.as_ref(), v.as_ref()))
                .unwrap();
        }
        (table, start.elapsed())
    }

    fn build_table_from_recs(
        &self,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> (Arc<NaiveHashTable<T>>, Duration) {
        self.build_table_from_iter(vec_updates)
    }

    pub fn build_table_from_base_and_ts(&self, ts: Timestamp) -> Duration {
        let start = Instant::now();
        let cur_table = self.build_table_at_ts(ts);
        let duration = start.elapsed();
        self.naivetables.borrow_mut().insert(ts, cur_table);
        self.build_table_stats.borrow_mut().insert(ts, duration);
        duration
    }

    pub fn mark_ts(&self, ts: Timestamp) -> Duration {
        self.build_table_from_base_and_ts(ts)
    }

    pub fn build_table_from_recs_and_ts(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        let (cur_table, duration) = self.build_table_from_recs(vec_updates);
        self.naivetables.borrow_mut().insert(ts, cur_table);
        self.build_table_stats.borrow_mut().insert(ts, duration);
        duration
    }

    pub fn build_table_from_iter_and_ts<I, K, P, V>(&self, ts: Timestamp, rows: I) -> Duration
    where
        I: IntoIterator<Item = (K, P, V)>,
        K: AsRef<[u8]>,
        P: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let (cur_table, duration) = self.build_table_from_iter(rows);
        self.naivetables.borrow_mut().insert(ts, cur_table);
        self.build_table_stats.borrow_mut().insert(ts, duration);
        duration
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
        let start = Instant::now();
        let mut result = Vec::new();
        let mut delta_map = HashMap::new();
        let tables = self.naivetables.borrow();
        let from = tables.get(&from_ts).ok_or(AccessMethodError::InvalidTimestamp)?;
        let to = tables.get(&to_ts).ok_or(AccessMethodError::InvalidTimestamp)?;
        for i in 0..self.bucket_count {
            let from_bucket = from.get_chain(i);
            let to_bucket = to.get_chain(i);
            HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map)?;
        }

        result.extend(delta_map.into_iter().filter_map(|(pk, from_to_delta)| {
            let (from_kv, to_kv) = from_to_delta.split();
            if &to_kv == &KVWithTs::default() {
                None
            } else if &from_kv == &KVWithTs::default() {
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
        }));
        self.scan_delta_stats
            .borrow_mut()
            .insert((from_ts, to_ts), start.elapsed());
        Ok(Box::new(result.into_iter()))
    }

    pub fn advance_readable_epoch_and_collect_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>, AccessMethodError> {
        self.build_table_from_base_and_ts(to_ts);
        self.delta_scan(from_ts, to_ts).map(|iter| iter.collect())
    }

    fn collect_space_stats(&self) {
        let tables = self.naivetables.borrow();
        for (ts, table) in tables.iter() {
            self.table_space_stats
                .borrow_mut()
                .insert(*ts, table.collect_page_num());
        }
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let tables = self.naivetables.borrow();
        let entry = if let Some(entry) = tables.get(&ts) {
            entry
        } else if let Some(latest_ts) = tables.keys().max().copied() {
            if ts > latest_ts {
                tables.get(&latest_ts).unwrap()
            } else {
                return Err(AccessMethodError::InvalidTimestamp);
            }
        } else {
            return Err(AccessMethodError::InvalidTimestamp);
        };
        let res: Vec<_> = entry.scan().unwrap().collect();
        Ok(Box::new(res.into_iter()))
    }

    pub fn print_stats(&self) {
        let build_stats = self.build_table_stats.borrow();
        let scan_stats = self.scan_delta_stats.borrow();

        println!("Build Table Stats:");
        for (ts, duration) in build_stats.iter() {
            println!("Timestamp: {}, Duration: {:?}", ts, duration);
        }

        println!("Scan Delta Stats:");
        for ((from_ts, to_ts), duration) in scan_stats.iter() {
            println!(
                "(From: {}, To: {}), Duration: {:?}",
                from_ts, to_ts, duration
            );
        }

        self.collect_space_stats();
        for (ts, space) in self.table_space_stats.borrow().iter() {
            println!("Timestamp: {}, Total Page Num: {}", ts, space);
        }

        println!("Bucket Count: {}", self.bucket_count);
    }

    pub fn garbage_collect(&self, ts: Timestamp) {
        self.naivetables.borrow_mut().retain(|&k, _| k > ts);
    }

    pub fn collect_space_stat_into_collector(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        let tables = self.naivetables.borrow();
        for table in tables.values() {
            table.collect_space_stat(&mut stat);
        }

        if let Some(max_ts) = tables.keys().max().copied() {
            let most_recent_table = tables.get(&max_ts).unwrap();
            let valid_space = most_recent_table
                .scan()
                .unwrap()
                .map(|entry| entry.0.len() + entry.1.len() + entry.2.len())
                .sum::<usize>();
            stat.inc_valid_space(valid_space);
        }

        stat
    }

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        let tables = self.naivetables.borrow();
        if let Some(table) = tables.get(&ts) {
            return table.get(k, pk).unwrap();
        }
        if let Some(latest_ts) = tables.keys().max().copied() {
            if ts > latest_ts {
                return tables.get(&latest_ts).and_then(|table| table.get(k, pk).unwrap());
            }
        }
        None
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let tables = self.naivetables.borrow();
        let table = if let Some(table) = tables.get(&ts) {
            table
        } else if let Some(latest_ts) = tables.keys().max().copied() {
            if ts > latest_ts {
                tables.get(&latest_ts).unwrap()
            } else {
                return Err(AccessMethodError::InvalidTimestamp);
            }
        } else {
            return Err(AccessMethodError::InvalidTimestamp);
        };
        let mut result = vec![];
        table.scan_key_vec(key, &mut result).unwrap();
        Ok(result)
    }
}
