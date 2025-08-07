use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{
        hash_common::KVWithTs,
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

    naivetables: RefCell<HashMap<Timestamp, Arc<NaiveHashTable<T>>>>,

    build_table_stats: RefCell<BTreeMap<Timestamp, std::time::Duration>>,
    scan_delta_stats: RefCell<BTreeMap<(Timestamp, Timestamp), std::time::Duration>>,
    table_space_stats: RefCell<BTreeMap<Timestamp, usize>>,

    vec_updates: RefCell<Vec<Record>>,
    map_current_recs: RefCell<HashMap<Vec<u8>, usize>>, // map from pkey to index in vec_updates
}

impl<T: MemPool + 'static> NaiveMvHashTable<T> {
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, bucket_count: usize) -> Self {
        let mut vec_updates = Vec::<Record>::new();
        // vec_updates.reserve(150000);
        Self {
            c_key,
            mem_pool,
            bucket_count,
            naivetables: RefCell::new(HashMap::new()),
            build_table_stats: RefCell::new(BTreeMap::new()),
            scan_delta_stats: RefCell::new(BTreeMap::new()),
            table_space_stats: RefCell::new(BTreeMap::new()),
            vec_updates: RefCell::new(vec_updates),
            map_current_recs: RefCell::new(HashMap::new()),
        }
    }

    pub fn add_insert_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let mut vec_updates = self.vec_updates.borrow_mut();
        vec_updates.push(Record::new(k.to_vec(), pk.to_vec(), v.to_vec()));
        let mut map_current_recs = self.map_current_recs.borrow_mut();
        map_current_recs.insert(pk.to_vec(), vec_updates.len() - 1);
    }

    pub fn add_update_rec_new(&self, k: &[u8], pk: &[u8], v: &[u8]) {
        let mut vec_updates = self.vec_updates.borrow_mut();
        vec_updates.push(Record::new(k.to_vec(), pk.to_vec(), v.to_vec()));
        let mut map_current_recs = self.map_current_recs.borrow_mut();
        map_current_recs.insert(pk.to_vec(), vec_updates.len() - 1);
    }

    fn build_table_until_now(&self) -> Arc<NaiveHashTable<T>> {
        // let start = std::time::Instant::now();
        let table = Arc::new(NaiveHashTable::new_with_bucket_num(
            self.c_key,
            self.mem_pool.clone(),
            self.bucket_count,
        ));
        let cur_table = self.map_current_recs.borrow();
        let vec_updates = self.vec_updates.borrow();
        for (_, idx) in cur_table.iter() {
            let rec = &vec_updates[*idx];
            table
                .insert(RecordRef::new(&rec.key(), &rec.pkey(), &rec.val()))
                .unwrap();
        }

        // let elapsed = start.elapsed();
        // println!("time elapsed: {:?}, row count: {:?}", elapsed, cur_table.len());
        table
    }

    // new: build and mark the current table with a timestamp
    pub fn mark_ts(&self, ts: Timestamp) {
        let cur_table = self.build_table_until_now();
        self.naivetables.borrow_mut().insert(ts, cur_table);
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
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError> {
        let tables = self.naivetables.borrow();
        let mut res = vec![];
        if let Some(entry) = tables.get(&ts) {
            res.extend(entry.scan().unwrap())
        }
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
}
