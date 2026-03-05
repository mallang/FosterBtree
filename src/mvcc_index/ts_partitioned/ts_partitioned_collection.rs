use core::panic;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
    time::Instant,
};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    log_warn,
    mvcc_index::{
        hash_common::{
            read_repair_btree, read_repair_vec, write_page, KVWithTs, MvccEntryLoc, RowDelta,
            StatCollector,
        },
        hash_join_heap_chain::HeapHashChain,
        hash_join_page::HashJoinPage,
        Delta, MvccEntry,
    },
    page::Page,
    prelude::{AccessMethodError, Timestamp},
};

pub struct TimestampPartitionCollection<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    partitions: Vec<TimestampPartition<T>>,
}

pub struct TimestampPartition<T: MemPool> {
    range: (Timestamp, Timestamp), // [start, end)
    chain: Arc<HeapHashChain<T>>,
}

impl<T: MemPool + 'static> TimestampPartition<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, range: (Timestamp, Timestamp)) -> Self {
        Self {
            range,
            chain: Arc::new(HeapHashChain::new(c_key, mem_pool)),
        }
    }

    pub fn get_range(&self) -> &(Timestamp, Timestamp) {
        &self.range
    }

    pub fn set_max_start_ts(&mut self, new_ts: Timestamp) {
        self.range.1 = new_ts;
    }

    pub fn chain(&self) -> &Arc<HeapHashChain<T>> {
        &self.chain
    }
}

impl<T: MemPool + 'static> TimestampPartitionCollection<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let partitions = vec![TimestampPartition::new(
            c_key,
            mem_pool.clone(),
            (0, Timestamp::MAX),
        )];
        Self {
            mem_pool,
            c_key,
            partitions,
        }
    }

    pub fn partitions(&self) -> &Vec<TimestampPartition<T>> {
        &self.partitions
    }

    pub fn split_last_partition_at(&mut self, new_ts: Timestamp) -> Result<(), AccessMethodError> {
        let last_idx = self.partitions.len().saturating_sub(1);
        let last_partition = &mut self.partitions[last_idx];
        last_partition.set_max_start_ts(new_ts - 1);

        // if &new_ts <= start || &new_ts >= end {
        //     panic!(
        //         "Invalid timestamp for partition split, start & end ts: ({} {}), new_ts: {}",
        //         start, end, new_ts
        //     );
        // }

        let new_partition =
            TimestampPartition::new(self.c_key, self.mem_pool.clone(), (new_ts, Timestamp::MAX));

        self.partitions.push(new_partition);

        Ok(())
    }

    pub fn insert(&self, ts: Timestamp, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let partition = self.partitions.last().unwrap();

        partition.chain.insert(entry)
    }

    pub fn get_no_repair(
        &self,
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 {
                match p.chain.get_no_repair(pkey, &ts) {
                    Ok(entry) => return Ok(entry),
                    Err(AccessMethodError::KeyNotFound) => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        Err(AccessMethodError::KeyNotFound)
    }

    pub fn get_read_repair(
        &self,
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut versions: BTreeMap<u64, (MvccEntryLoc, bool)> = BTreeMap::new();

        // MUST reverse order
        let mut ret = Err(AccessMethodError::KeyNotFound);
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 {
                match p.chain.get_read_repair(pkey, &ts, &mut versions) {
                    Ok(entry) => {
                        ret = Ok(entry);
                        break;
                    }
                    Err(AccessMethodError::KeyNotFound) => continue,
                    Err(e) => {
                        ret = Err(e);
                        break;
                    }
                }
            }
        }

        read_repair_btree(&self.mem_pool, &versions, self.c_key);
        ret
    }

    pub fn update(&self, ts: Timestamp, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let partition = self.partitions.last().unwrap();

        partition.chain.update_no_repair(entry.pkey(), entry)
    }

    pub fn update_write_repair(
        &self,
        ts: Timestamp,
        entry: &MvccEntry,
    ) -> Result<(), AccessMethodError> {
        let mut repaired = false;
        for partition in self.partitions.iter().take(self.partitions.len() - 1) {
            match partition
                .chain
                .update_write_repair_ts_partition_except_last(entry)
            {
                Ok(_) => {
                    repaired = true;
                    break;
                }
                Err(AccessMethodError::RepairedNotFound) => continue,
                Err(e) => return Err(e),
            }
        }
        self.partitions
            .last()
            .unwrap()
            .chain
            .update_write_repair_ts_partition_last(entry, repaired)?;
        Ok(())
    }

    pub fn delete(&self, ts: Timestamp, pkey: &[u8]) -> Result<(), AccessMethodError> {
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 {
                if p.chain.delete(pkey, &ts).is_ok() {
                    return Ok(());
                }
                continue;
            }
        }
        Ok(())
    }

    pub fn scan_with_key(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut best_candidates = HashMap::new();
        for p in self.partitions.iter() {
            if ts >= p.range.0 && !p.chain.is_empty() {
                let partition_scanner = p.chain.scan_key_vec_read_repair(key, &ts, None)?;
                // Iterate over all entries from the chain.
                for entry in partition_scanner {
                    let (pkey, value) = entry;
                    best_candidates.entry(pkey).or_insert(value);
                }
            }
        }
        Ok(best_candidates.into_iter().collect())
    }

    /// Fast path for single-key lookup after write repair (or read repair completion).
    /// Since repair guarantees at most one valid version per pkey at a given ts,
    /// we can skip HashMap dedup and directly collect into a Vec.
    pub fn scan_with_key_write_repair(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut result = Vec::new();
        for p in self.partitions.iter() {
            if ts >= p.range.0 && !p.chain.is_empty() {
                p.chain.chain_scan_key(key, &ts, &mut result)?;
            }
        }
        Ok(result)
    }

    pub fn scan_with_key_read_repair(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut best_candidates: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut versions_map = HashMap::new();
        // iterate in natural order
        for p in self.partitions.iter() {
            if ts >= p.range.0 && !p.chain.is_empty() {
                let partition_scanner =
                    p.chain
                        .scan_key_vec_read_repair(key, &ts, Some(&mut versions_map))?;
                // Iterate over all entries from the chain.
                for entry in partition_scanner {
                    best_candidates.insert(entry.0, entry.1);
                }
            }
        }

        for versions in versions_map.into_values() {
            read_repair_vec(&self.mem_pool, &versions, self.c_key);
        }

        Ok(best_candidates.into_iter().collect())
    }

    pub fn scan_all(&self) -> Result<Vec<MvccEntry>, AccessMethodError> {
        let mut res = vec![];
        for p in self.partitions.iter() {
            let partition_scanner: crate::mvcc_index::hash_join_heap_chain::HeapChainScanner<
                '_,
                T,
            > = p.chain.scan_all()?;
            res.extend(partition_scanner);
        }
        Ok(res)
    }

    pub fn scan_delta_read_repair(
        &self,
        from: Timestamp,
        to: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        let mut delta_map = HashMap::<Vec<u8>, RowDelta>::new();
        let mut versions_map = HashMap::new();
        for p in self.partitions.iter() {
            p.chain
                .scan_delta_read_repair(from, to, &mut delta_map, &mut versions_map);
        }

        for versions in versions_map.values() {
            read_repair_vec(&self.mem_pool, versions, self.c_key);
        }

        Box::new(delta_map.into_iter().filter_map(|(pk, from_to_delta)| {
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
        }))
    }

    pub fn scan_delta(
        &self,
        from: Timestamp,
        to: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        let mut delta_map = HashMap::<Vec<u8>, RowDelta>::new();
        for p in self.partitions.iter() {
            let (_min_start_ts, max_start_ts) = p.get_range().to_owned();
            if from > max_start_ts {
                continue;
            }
            p.chain.scan_delta(from, to, &mut delta_map);
        }

        Box::new(delta_map.into_iter().filter_map(|(pk, from_to_delta)| {
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
        }))
    }

    pub fn garbage_collect(&self, ts: Timestamp) -> Result<(), AccessMethodError> {
        let mut best_map = HashMap::new();
        for part in &self.partitions {
            part.chain.gc_collect_versions(&ts, &mut best_map)?;
        }

        for map in best_map.into_values() {
            read_repair_vec(&self.mem_pool, &map, self.c_key);
        }

        for part in &self.partitions {
            part.chain.gc_truncate_entries_before_ts(&ts)?;
        }

        Ok(())
    }

    pub fn collect_space_stat(&self, stat: &mut StatCollector) {
        for part in &self.partitions {
            part.chain.collect_space_statistics(stat);
        }
    }
}
