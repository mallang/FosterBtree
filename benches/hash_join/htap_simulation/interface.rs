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

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Update,
    UpdateWR,
    DeltaScan,
    MarkTs,
    InitLoad,
    Scan,
    GbgCollect,
    Probe,
    RecentScan,
    HistoryScan,
}

pub trait MultiVersionJoinTable {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]);
    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp);
    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>>;
    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)>;
    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp);
    fn mark_ts(&self, ts: u64);
    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
        is_read_repair: bool,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>>;
    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>;
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError>;
    fn end_txs(&self, optype: OperationType) -> Result<(), AccessMethodError>;
    fn garbage_collect(&self, ts: Timestamp);

    fn after_mark_ts(&self, ts: Timestamp);

    fn collect_space_stat(&self) -> StatCollector;
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
        // for cache warm-up
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
        is_read_repair: bool,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        <Self as MvccIndex<T>>::delta_scan(self, from_ts, to_ts).unwrap()
    }

    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR || optype == OperationType::Update {
            let _ = <Self as MvccIndex<T>>::bulk_update_start(&self);
        }
        Ok(())
    }
    fn end_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR || optype == OperationType::Update {
            let _ = <Self as MvccIndex<T>>::bulk_update_end(&self);
        }
        Ok(())
    }

    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let iter = if is_read_repair {
            <Self as MvccIndex<_>>::scan_read_repair(self, ts)
        } else {
            <Self as MvccIndex<_>>::scan(self, ts)
        };
        iter
    }

    fn garbage_collect(&self, ts: Timestamp) {
        let _ = <Self as MvccIndex<_>>::garbage_collect(&self, ts);
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
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
        // for cache warm-up
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
        is_read_repair: bool,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        if is_read_repair {
            <Self as MvccIndex<T>>::delta_scan_read_repair(&self, from_ts, to_ts).unwrap()
        } else {
            <Self as MvccIndex<T>>::delta_scan(&self, from_ts, to_ts).unwrap()
        }
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR {
            let _ = <Self as MvccIndex<_>>::bulk_update_start(&self);
        }
        Ok(())
    }
    fn end_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR {
            let _ = <Self as MvccIndex<_>>::bulk_update_end(&self);
        }
        Ok(())
    }

    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let iter: Result<
            Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>,
            AccessMethodError,
        > = if is_read_repair {
            <Self as MvccIndex<_>>::scan_read_repair(&self, ts)
        } else {
            <Self as MvccIndex<_>>::scan(&self, ts)
        };
        iter
    }

    fn garbage_collect(&self, ts: Timestamp) {
        let _ = <Self as MvccIndex<_>>::garbage_collect(&self, ts);
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
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
        let _ = <Self as MvccIndex<_>>::update_write_repair(
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
        // for cache warm-up
        let _ = <Self as MvccIndex<_>>::scan(self, ts).unwrap();
    }

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
        is_read_repair: bool,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        if is_read_repair {
            <Self as MvccIndex<T>>::delta_scan_read_repair(self, from_ts, to_ts).unwrap()
        } else {
            <Self as MvccIndex<T>>::delta_scan(self, from_ts, to_ts).unwrap()
        }
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR {
            let _ = <Self as MvccIndex<T>>::bulk_update_start(&self);
        }
        Ok(())
    }
    fn end_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        if optype == OperationType::UpdateWR {
            let _ = <Self as MvccIndex<T>>::bulk_update_end(&self);
        }
        Ok(())
    }

    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        let iter = if is_read_repair {
            <Self as MvccIndex<_>>::scan_read_repair(&self, ts)
        } else {
            <Self as MvccIndex<_>>::scan(&self, ts)
        };
        iter
    }

    fn garbage_collect(&self, ts: Timestamp) {
        <Self as MvccIndex<_>>::garbage_collect(&self, ts).unwrap();
    }

    fn collect_space_stat(&self) -> StatCollector {
        <Self as MvccIndex<_>>::collect_space_stat(&self)
    }
}

// NAIVE
impl<T: MemPool + 'static> MultiVersionJoinTable for NaiveMvHashTable<T> {
    fn after_mark_ts(&self, ts: Timestamp) {}
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        NaiveMvHashTable::add_insert_rec_new(&self, key, pkey, value);
    }

    fn probe(&self, join_key: &[u8], ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>)> {
        let a = NaiveMvHashTable::scan_key_vec(&self, join_key, ts).unwrap();
        a
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<Vec<u8>> {
        NaiveMvHashTable::get_key(self, key, pkey, ts)
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        NaiveMvHashTable::add_update_rec_new(&self, key, pkey, value);
    }

    fn mark_ts(&self, ts: u64) {
        NaiveMvHashTable::mark_ts(&self, ts);
    }

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
        is_read_repair: bool,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        Box::new(NaiveMvHashTable::delta_scan(self, from_ts, to_ts).unwrap())
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        Ok(())
    }
    fn end_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        Ok(())
    }
    fn scan(
        &self,
        ts: Timestamp,
        is_read_repair: bool,
    ) -> Result<Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Vec<u8>)> + Send>, AccessMethodError>
    {
        NaiveMvHashTable::scan(&self, ts)
    }
    fn update_write_repair(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        NaiveMvHashTable::add_update_rec_new(&self, key, pkey, value);
    }
    fn garbage_collect(&self, ts: Timestamp) {}

    fn collect_space_stat(&self) -> StatCollector {
        NaiveMvHashTable::collect_space_stat_into_collector(&self)
    }
}
