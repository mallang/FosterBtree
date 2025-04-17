use core::panic;
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    log_warn,
    mvcc_index::{
        hash_common::{write_page, KVWithTs, MvccEntryLoc, RowDelta},
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

impl<T: MemPool> TimestampPartition<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>, range: (Timestamp, Timestamp)) -> Self {
        Self {
            range,
            chain: Arc::new(HeapHashChain::new(c_key, mem_pool)),
        }
    }

    pub fn get_range(&self) -> (Timestamp, Timestamp) {
        self.range
    }
}

impl<T: MemPool> TimestampPartitionCollection<T> {
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

    pub fn split_last_partition_at(&mut self, new_ts: Timestamp) -> Result<(), AccessMethodError> {
        let last_idx = self.partitions.len().saturating_sub(1);
        let last_partition = &self.partitions[last_idx];
        let (start, end) = last_partition.get_range();

        if new_ts <= start || new_ts >= end {
            panic!("Invalid timestamp for partition split");
        }

        let new_partition =
            TimestampPartition::new(self.c_key, self.mem_pool.clone(), (new_ts, end));

        self.partitions.push(new_partition);

        Ok(())
    }

    pub fn insert(&self, ts: Timestamp, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let partition = self
            .partitions
            .iter()
            .rev()
            .find(|p| ts >= p.range.0 && ts < p.range.1)
            .unwrap();

        partition.chain.insert(entry)
    }

    pub fn get_no_repair(
        &self,
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<MvccEntry, AccessMethodError> {
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
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
        let mut versions: BTreeMap<u64, MvccEntryLoc> = BTreeMap::new();

        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
                match p.chain.get_read_repair(pkey, &ts, &mut versions) {
                    Ok(entry) => {
                        self.read_repair(&versions);
                        return Ok(entry);
                    }
                    Err(AccessMethodError::KeyNotFound) => continue,
                    Err(e) => {
                        self.read_repair(&versions);
                        return Err(e);
                    }
                }
            }
        }

        self.read_repair(&versions);
        Err(AccessMethodError::KeyNotFound)
    }

    pub fn read_repair(&self, versions: &BTreeMap<u64, MvccEntryLoc>) {
        for (ts, loc) in versions.iter() {
            let next_entry = versions.range((Excluded(*ts), Unbounded)).next();
            if let Some((next_ts, _)) = next_entry {
                let page_key = PageFrameKey::new(self.c_key, loc.page_id());
                let mut current_page = write_page(&*self.mem_pool, page_key);
                let mut slot = <Page as HashJoinPage>::slot(&*current_page, loc.slot_id() as usize);
                slot.set_end_ts(*next_ts);
                <Page as HashJoinPage>::set_slot(&mut current_page, loc.slot_id() as usize, &slot);
            }
        }
    }

    pub fn update(&self, ts: Timestamp, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let partition = self
            .partitions
            .iter()
            .rev()
            .find(|p| ts >= p.range.0 && ts < p.range.1)
            .unwrap();

        partition.chain.update_no_repair(entry.pkey(), entry)
    }

    pub fn delete(&self, ts: Timestamp, pkey: &[u8]) -> Result<(), AccessMethodError> {
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
                if p.chain.delete(pkey, &ts).is_ok() {
                    return Ok(());
                }
                continue;
            }
        }
        Ok(())
    }

    pub fn scan_unique(&self, ts: Timestamp) -> Result<Vec<MvccEntry>, AccessMethodError> {
        let mut best_candidates = HashMap::new();
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
                let partition_scanner = p.chain.scan_unique(ts).unwrap();
                // Iterate over all entries from the chain.
                for entry in partition_scanner {
                    log_warn!("ts parti scan: get entry: {:?}", entry);
                    let pkey = entry.pkey().to_vec();
                    best_candidates
                        .entry(pkey)
                        .and_modify(|existing: &mut MvccEntry| {
                            // Replace with this candidate if it has a higher start_ts.
                            if entry.start_ts() > existing.start_ts() {
                                *existing = entry.clone();
                            }
                        })
                        .or_insert(entry);
                }
            }
        }
        Ok(best_candidates.into_values().collect())
    }

    pub fn scan_with_key(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut best_candidates = HashMap::new();
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
                let partition_scanner = p.chain.scan_key_vec(key, &ts);
                // Iterate over all entries from the chain.
                for entry in partition_scanner {
                    let (pkey, value) = entry;
                    best_candidates.entry(pkey).or_insert(value);
                }
            }
        }
        Ok(best_candidates.into_iter().collect())
    }

    pub fn scan_with_key_read_repair(
        &self,
        ts: Timestamp,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut best_candidates: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for p in self.partitions.iter().rev() {
            if ts >= p.range.0 && ts < p.range.1 {
                let partition_scanner = p.chain.scan_key_vec_read_repair(key, &ts);
                // Iterate over all entries from the chain.
                for entry in partition_scanner {
                    let (pkey, value) = entry;
                    best_candidates.entry(pkey).or_insert(value);
                }
            }
        }
        Ok(best_candidates.into_iter().collect())
    }

    pub fn scan_all(&self) -> Result<Vec<MvccEntry>, AccessMethodError> {
        // let mut best_candidates = HashMap::new();
        // for p in self.partitions.iter().rev() {
        //     let partition_scanner = p.chain.scan_all()?;
        //     // Iterate over all entries from the chain.
        //     for entry in partition_scanner {
        //         let (pkey, value) = entry;
        //         best_candidates
        //             .entry(pkey)
        //             .or_insert(value);
        //     }
        // }
        // Ok(best_candidates.into_iter().collect())
        todo!()
    }

    pub fn scan_delta(
        &self,
        from: Timestamp,
        to: Timestamp,
    ) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>, Delta<Vec<u8>>)>> {
        let mut delta_map = HashMap::<Vec<u8>, RowDelta>::new();
        for p in self.partitions.iter() {
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

    // pub fn garbage_collect(&mut self, watermark: Timestamp) {
    //     self.partitions.retain(|p| p.range.1 > watermark);
    //     for part in &mut self.partitions {
    //         part.chain.garbage_collect(&watermark).ok();
    //     }
    // }
}
