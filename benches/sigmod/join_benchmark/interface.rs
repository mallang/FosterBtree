use std::time::Duration;

use fbtree::{
    bp::MemPool,
    mvcc_index::{
        dual_heap_hash::chained_hash_table::ChainedHashTable, hash_common::StatCollector,
        hash_heap::hash_heap_table::HeapHashTable,
        ts_partitioned::ts_partitioned_table::TsPartitionedTable, Delta, MvccIndex,
    },
    naive_hash_index::NaiveMvHashTable,
    prelude::{AccessMethodError, Timestamp},
};

pub type BoxMVIndex = Box<dyn MultiVersionJoinTable>;

pub trait MultiVersionJoinTable {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]);
    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp);
    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp);
    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>>;
    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn mark_ts(&self, ts: u64);
    fn after_mark_ts(&self, ts: Timestamp);
    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>;
    fn garbage_collect(&self, ts: Timestamp);
    fn collect_space_stat(&self) -> StatCollector;

    fn insert_naive(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration;
}

/*
    CHAIN
*/
impl<T: MemPool + 'static> MultiVersionJoinTable for ChainedHashTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)> {
        <Self as MvccIndex<_>>::scan_key_vec(&self, join_key, ts).unwrap()
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        <Self as MvccIndex<_>>::get(self, key, pkey, ts).unwrap()
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update_write_repair(
            self,
            key.to_vec(),
            pkey.to_vec(),
            ts,
            0,
            value.to_vec(),
        )
        .unwrap();
    }

    fn mark_ts(&self, ts: u64) {}

    fn after_mark_ts(&self, ts: u64) {
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan(
        &self,
        ts: Timestamp,
        _is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        <Self as MvccIndex<_>>::scan(self, ts)
    }

    fn garbage_collect(&self, ts: Timestamp) {
        let _ = <Self as MvccIndex<_>>::garbage_collect(&self, ts);
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
    }

    fn insert_naive(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        unimplemented!()
    }
}

// HEAP
impl<T: MemPool + 'static> MultiVersionJoinTable for HeapHashTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)> {
        <Self as MvccIndex<_>>::scan_key_vec(&self, join_key, ts).unwrap()
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        <Self as MvccIndex<_>>::get(self, key, pkey, ts).unwrap()
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update_write_repair(
            self,
            key.to_vec(),
            pkey.to_vec(),
            ts,
            0,
            value.to_vec(),
        )
        .unwrap();
    }

    fn mark_ts(&self, ts: u64) {}

    fn after_mark_ts(&self, ts: u64) {
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan(
        &self,
        ts: Timestamp,
        _is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        <Self as MvccIndex<_>>::scan(&self, ts)
    }

    fn garbage_collect(&self, ts: Timestamp) {
        let _ = <Self as MvccIndex<_>>::garbage_collect(&self, ts);
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
    }

    fn insert_naive(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        unimplemented!()
    }
}

/*
    TS PARTITION
*/
impl<T: MemPool + 'static> MultiVersionJoinTable for TsPartitionedTable<T> {
    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)> {
        <Self as MvccIndex<_>>::scan_key_vec(&self, join_key, ts).unwrap()
    }

    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        <Self as MvccIndex<_>>::get(self, key, pkey, ts).unwrap()
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update_write_repair(
            self,
            key.to_vec(),
            pkey.to_vec(),
            ts,
            0,
            value.to_vec(),
        )
        .unwrap();
    }

    fn mark_ts(&self, ts: u64) {
        <Self as MvccIndex<_>>::split_at_ts(self, ts).unwrap();
    }

    fn after_mark_ts(&self, ts: u64) {
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan(
        &self,
        ts: Timestamp,
        _is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        <Self as MvccIndex<_>>::scan(&self, ts)
    }

    fn garbage_collect(&self, ts: Timestamp) {
        <Self as MvccIndex<_>>::garbage_collect(&self, ts).unwrap();
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
    }

    fn insert_naive(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        unimplemented!()
    }
}

// NAIVE
impl<T: MemPool + 'static> MultiVersionJoinTable for NaiveMvHashTable<T> {
    fn after_mark_ts(&self, _ts: Timestamp) {}

    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        NaiveMvHashTable::add_insert_rec_at_ts(&self, key, pkey, value, 0);
    }

    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)> {
        NaiveMvHashTable::scan_key_vec(&self, join_key, ts).unwrap()
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        NaiveMvHashTable::get_key(self, key, pkey, ts)
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        NaiveMvHashTable::add_update_rec_at_ts(&self, key, pkey, value, ts);
    }

    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        // Naive table doesn't support write repair, just use regular update
        NaiveMvHashTable::add_update_rec_at_ts(&self, key, pkey, value, ts);
    }

    fn mark_ts(&self, ts: u64) {
        NaiveMvHashTable::mark_ts(&self, ts);
    }

    fn scan(
        &self,
        ts: Timestamp,
        _is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        NaiveMvHashTable::scan(&self, ts)
    }

    fn garbage_collect(&self, _ts: Timestamp) {}

    fn collect_space_stat(&self) -> StatCollector {
        NaiveMvHashTable::collect_space_stat_into_collector(&self)
    }

    fn insert_naive(
        &self,
        ts: Timestamp,
        vec_updates: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    ) -> Duration {
        NaiveMvHashTable::build_table_from_recs_and_ts(&self, ts, vec_updates)
    }
}
