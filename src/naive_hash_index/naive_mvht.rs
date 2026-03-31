use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{
        hash_common::{KVWithTs, StatCollector},
        hash_join_page::record::{Record, RecordRef},
        Delta,
    },
    naive_hash_index::{
        naive_hash_table::{hash_join_table::NaiveHashTable, SingleTsHashTable},
        HeapHashChain,
    },
    prelude::{AccessMethodError, Timestamp},
};

struct UpdatesAtTs {
    pub update_recs: Vec<Record>,
}

struct KeyAndValue {
    pub k: Vec<u8>,
    pub v: Vec<u8>,
}

pub struct NaiveMvHashTable<T: MemPool + 'static> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    bucket_count: usize,

    current_table: NaiveHashTable<T>,

    naivetables: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    build_table_stats: RefCell<BTreeMap<Timestamp, std::time::Duration>>,
    scan_delta_stats: RefCell<BTreeMap<(Timestamp, Timestamp), std::time::Duration>>,
    table_space_stats: RefCell<BTreeMap<Timestamp, usize>>,
}

impl<T: MemPool + 'static> NaiveMvHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        let current_table =
            NaiveHashTable::new_with_bucket_num(c_key, mem_pool.clone(), bucket_count);
        Self {
            c_key,
            mem_pool,
            bucket_count,
            current_table,
            naivetables: RefCell::new(HashMap::new()),
            build_table_stats: RefCell::new(BTreeMap::new()),
            scan_delta_stats: RefCell::new(BTreeMap::new()),
            table_space_stats: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn add_insert_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .insert(RecordRef::new(k, pk, v))
            .unwrap();
    }

    pub fn add_update_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        self.current_table
            .update(RecordRef::new(k, pk, v))
            .unwrap();
    }

    fn build_table_until_now(&self) -> Arc<NaiveHashTable<T>> {
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        let records: Vec<_> = self.current_table.scan().unwrap().collect();
        for (k, pk, v) in records {
            table.insert(RecordRef::new(&k, &pk, &v)).unwrap();
        }
        table
    }

    fn build_table_from_recs(
        &self,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> (Arc<NaiveHashTable<T>>, Duration) {
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));

        let start = Instant::now();

        for (k, pk, v) in vec_updates {
            let rec = Record::new(k.clone(), pk.clone(), v.clone());
            table
                .insert(RecordRef::new(rec.key(), rec.pkey(), rec.val()))
                .unwrap();
        }

        (table, Instant::now() - start)
    }

    // new: build and mark the current table with a timestamp
    pub fn mark_ts(&self, ts: Timestamp) {
        let cur_table = self.build_table_until_now();
        self.naivetables.borrow_mut().insert(ts, cur_table);
    }

    pub fn build_table_from_recs_and_ts(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        let (cur_table, duration) = self.build_table_from_recs(vec_updates);
        self.naivetables.borrow_mut().insert(ts, cur_table);
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
        let mut result = Vec::new();
        let mut delta_map = HashMap::new();
        let tables = self.naivetables.borrow();
        assert!(
            tables.contains_key(&from_ts),
            "from_ts table does not exist"
        );
        assert!(tables.contains_key(&to_ts), "to_ts table does not exist");
        let from = tables.get(&from_ts).unwrap();
        let to = tables.get(&to_ts).unwrap();
        for i in 0..self.bucket_count {
            let from_bucket = from.get_chain(i);
            let to_bucket = to.get_chain(i);

            HeapHashChain::scan_deltas(&from_bucket, &to_bucket, &mut delta_map)?;
        }

        result.extend(delta_map.into_iter().filter_map(|(pk, from_to_delta)| {
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
        Ok(Box::new(result.into_iter()))
    }

    fn collect_space_stats(&self) {
        let tables = self.naivetables.borrow();
        for entry in tables.iter() {
            let (ts, table) = entry;
            let total_page_num = table.collect_page_num();
            self.table_space_stats
                .borrow_mut()
                .insert(*ts, total_page_num);
        }
    }

    pub fn scan(
        &self,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let tables = self.naivetables.borrow();
        if let Some(entry) = tables.get(&ts) {
            let res: Vec<_> = entry.scan().unwrap().collect();
            return Ok(Box::new(res.into_iter()));
        }
        let max_ts = tables.keys().max().copied();
        if let Some(latest_ts) = max_ts {
            if ts > latest_ts {
                let res: Vec<_> = tables.get(&latest_ts).unwrap().scan().unwrap().collect();
                return Ok(Box::new(res.into_iter()));
            }
        }
        Err(AccessMethodError::InvalidTimestamp)
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

    /// Remove snapshots with timestamp <= `ts` to free memory.
    pub fn garbage_collect(&self, ts: Timestamp) {
        self.naivetables.borrow_mut().retain(|&k, _| k > ts);
    }

    pub fn collect_space_stat_into_collector(&self) -> StatCollector {
        let mut stat = StatCollector::new();
        let tables = self.naivetables.borrow();
        for entry in tables.iter() {
            let (_ts, table) = entry;
            table.collect_space_stat(&mut stat);
        }

        let max_ts = tables.keys().max().unwrap();
        let most_recent_table = tables.get(max_ts).unwrap();
        let valid_space = most_recent_table
            .scan()
            .unwrap()
            .map(|entry| entry.0.len() + entry.1.len() + entry.2.len())
            .sum::<usize>();

        stat.inc_valid_space(valid_space);

        stat
    }

    pub fn get_key(&self, k: &[u8], pk: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        let tables = self.naivetables.borrow();
        if let Some(table) = tables.get(&ts) {
            table.get(k, pk).unwrap()
        } else {
            let max_ts = tables.keys().max().copied();
            max_ts.and_then(|latest_ts| {
                if ts > latest_ts {
                    tables.get(&latest_ts).and_then(|table| table.get(k, pk).unwrap())
                } else {
                    None
                }
            })
        }
    }

    pub fn scan_key_vec(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let tables = self.naivetables.borrow();
        if let Some(table) = tables.get(&ts) {
            let mut result = vec![];
            table.scan_key_vec(key, &mut result).unwrap();
            return Ok(result);
        }
        let max_ts = tables.keys().max().copied();
        if let Some(latest_ts) = max_ts {
            if ts > latest_ts {
                let mut result = vec![];
                tables
                    .get(&latest_ts)
                    .unwrap()
                    .scan_key_vec(key, &mut result)
                    .unwrap();
                return Ok(result);
            }
        }
        Err(AccessMethodError::InvalidTimestamp)
    }
}
