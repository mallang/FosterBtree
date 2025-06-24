use std::{cell::RefCell, collections::{BTreeMap, HashMap}, sync::Arc};

use crate::{bp::{ContainerKey, MemPool}, mvcc_index::{hash_common::KVWithTs, hash_join_page::record::{Record, RecordRef}, Delta}, naive_hash_index::{naive_hash_table::{hash_join_table::NaiveHashTable, SingleTsHashTable}, HeapHashChain}, prelude::{AccessMethodError, Timestamp}};

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

    updates_at_ts: RefCell<BTreeMap<Timestamp, UpdatesAtTs>>,
    initial: RefCell<BTreeMap<Vec<u8>, KeyAndValue>>,

    naivetables: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    build_table_stats: RefCell<BTreeMap<Timestamp,  std::time::Duration>>,
    scan_delta_stats: RefCell<BTreeMap<(Timestamp, Timestamp), std::time::Duration>>,
    table_space_stats: RefCell<BTreeMap<Timestamp, usize>>,
}

impl<T: MemPool + 'static> NaiveMvHashTable<T> {
    pub fn new_with_bucket_num(
        c_key: ContainerKey,
        mem_pool: Arc<T>,
        bucket_count: usize,
    ) -> Self {
        Self {
            c_key,
            mem_pool,
            bucket_count,
            updates_at_ts: RefCell::new(BTreeMap::new()),
            initial: RefCell::new(BTreeMap::new()),
            naivetables: RefCell::new(HashMap::new()),
            build_table_stats: RefCell::new(BTreeMap::new()),
            scan_delta_stats: RefCell::new(BTreeMap::new()),
            table_space_stats: RefCell::new(BTreeMap::new()),
        }
    }
    fn build_a_table<'a>(&self, ite: impl Iterator<Item = RecordRef<'a>>) -> Arc<NaiveHashTable<T>> {
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(self.c_key, self.mem_pool.clone(), self.bucket_count));
        for item in ite {
            table.insert(item).unwrap();
        }
        return table;
    }

    pub fn add_insert_rec(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let mut t = self.initial.borrow_mut();
        t.insert(pk.to_vec(), KeyAndValue { k: k.to_vec(), v: v.to_vec() });
    }

    pub fn add_update_rec(&self, k: &[u8], pk: &[u8], v: &[u8], ts: Timestamp) {
        let mut t = self.updates_at_ts.borrow_mut();
        let updates = t.entry(ts).or_insert_with(|| UpdatesAtTs { update_recs: Vec::new() });
        updates.update_recs.push(Record::new(k.to_vec(), pk.to_vec(), v.to_vec()));
    }

    pub fn build_tables(&self, ts_slice: &[Timestamp]) {
        let mut table = self.initial.borrow_mut();
        {
            let ite = table.iter().map(|(pk, kv)| {
                RecordRef::new(&kv.k, &kv.v, &pk)
            });
            // TODO: generate stats for the table
            let start_time = std::time::Instant::now();
            let table = self.build_a_table(ite);
            let duration = start_time.elapsed();

            self.naivetables.borrow_mut().insert(0, table);
            self.build_table_stats.borrow_mut().insert(0, duration);
        }

        let updates = self.updates_at_ts.borrow();
        let max_updates_ts = updates.keys().max().cloned().unwrap();
        for (ts, updates_at_ts) in updates.iter() {
            for update_rec in &updates_at_ts.update_recs {
                if let Some(v) = table.get_mut(update_rec.pkey()) {
                    v.k = update_rec.key().to_vec();
                    v.v = update_rec.val().to_vec();
                }
            }
            if (!ts_slice.contains(ts)) &&  (*ts < max_updates_ts) {
                continue;
            }

            let ite = table.iter().map(|(pk, kv)| {
                RecordRef::new(&kv.k, &kv.v, &pk)
            });
            // TODO: generate stats for the table
            let start_time = std::time::Instant::now();
            let table = self.build_a_table(ite);
            let duration = start_time.elapsed();

            self.naivetables.borrow_mut().insert(*ts, table);

            self.build_table_stats.borrow_mut().insert(*ts, duration);
        }
    }

    fn delta_scan(
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

    pub fn delta_scan_tables(
        &self,
        from_ts: Timestamp,
        mut to_ts: Timestamp,
    ) -> Result<
        Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)> + Send>,
        AccessMethodError,
    > {
        let max_update_ts = self.updates_at_ts.borrow().keys().max().cloned().unwrap();
        if to_ts > max_update_ts {
            to_ts = max_update_ts;
        }
        let start_time = std::time::Instant::now();
        let result = self.delta_scan(from_ts, to_ts);
        let duration = start_time.elapsed();
        if let Ok(_) = result {
            self.scan_delta_stats.borrow_mut().insert((from_ts, to_ts), duration);
        } else {
            panic!()
        }
        result
    }

    fn collect_space_stats(&self) {
        let tables = self.naivetables.borrow();
        for entry in tables.iter() {
            let (ts, table) = entry;
            let total_page_num = table.collect_page_num();
            self.table_space_stats.borrow_mut().insert(*ts, total_page_num);
        }
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
            println!("(From: {}, To: {}), Duration: {:?}", from_ts, to_ts, duration);
        }

        self.collect_space_stats();
        for (ts, space) in self.table_space_stats.borrow().iter() {
            println!("Timestamp: {}, Total Page Num: {}", ts, space);
        }

        println!("Bucket Count: {}", self.bucket_count);
    }
}

