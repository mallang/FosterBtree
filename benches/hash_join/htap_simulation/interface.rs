use fbtree::{
    bp::MemPool,
    mvcc_index::{
        dual_heap_hash::chained_hash_table::ChainedHashTable,
        hash_heap::hash_heap_table::HeapHashTable,
        ts_partitioned::ts_partitioned_table::TsPartitionedTable, Delta, MvccIndex,
    },
    naive_hash_index::NaiveMvHashTable,
    prelude::{AccessMethodError, Timestamp},
};

pub type BoxMVIndex = Box<dyn MultiVersionJoinTable>;

#[derive(Debug, Clone)]
pub enum OperationType {
    Update,
    DeltaScan,
    MarkTs,
    InitialLoad,
}

pub trait MultiVersionJoinTable {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]);
    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp);
    fn mark_ts(&self, ts: u64);
    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>>;
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError>;
    fn end_txs(&self) -> Result<(), AccessMethodError>;
}

impl<T: MemPool + 'static> MultiVersionJoinTable for ChainedHashTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn mark_ts(&self, ts: u64) {}

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        <Self as MvccIndex<T>>::delta_scan(self, from_ts, to_ts).unwrap()
    }

    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        todo!()
    }
    fn end_txs(&self) -> Result<(), AccessMethodError> {
        todo!()
    }
}

impl<T: MemPool + 'static> MultiVersionJoinTable for HeapHashTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn mark_ts(&self, ts: u64) {}

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        <Self as MvccIndex<T>>::delta_scan(self, from_ts, to_ts).unwrap()
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        Ok(())
    }
    fn end_txs(&self) -> Result<(), AccessMethodError> {
        Ok(())
    }
}

impl<T: MemPool + 'static> MultiVersionJoinTable for TsPartitionedTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        <Self as MvccIndex<_>>::insert(self, key.to_vec(), pkey.to_vec(), 0, 0, value.to_vec())
            .unwrap();
    }

    fn update(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        <Self as MvccIndex<_>>::update(self, key.to_vec(), pkey.to_vec(), ts, 0, value.to_vec())
            .unwrap();
    }

    fn mark_ts(&self, ts: u64) {}

    fn scan_delta(
        &self,
        from_ts: Timestamp,
        to_ts: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        <Self as MvccIndex<T>>::delta_scan(self, from_ts, to_ts).unwrap()
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        todo!()
    }
    fn end_txs(&self) -> Result<(), AccessMethodError> {
        todo!()
    }
}

impl<T: MemPool + 'static> MultiVersionJoinTable for NaiveMvHashTable<T> {
    fn insert(&self, key: &[u8], pkey: &[u8], value: &[u8]) {
        NaiveMvHashTable::add_insert_rec_new(&self, key, pkey, value);
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
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        Box::new(NaiveMvHashTable::delta_scan(self, from_ts, to_ts).unwrap())
    }
    fn begin_txs(&self, optype: OperationType) -> Result<(), AccessMethodError> {
        Ok(())
    }
    fn end_txs(&self) -> Result<(), AccessMethodError> {
        Ok(())
    }
}
