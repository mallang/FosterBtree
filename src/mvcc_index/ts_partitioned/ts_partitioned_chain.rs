use std::sync::Arc;

use crate::{
    bp::{ContainerKey, MemPool},
    mvcc_index::{hash_join::chained_hash_history_chain::ChainedHashHistoryChain, MvccEntry},
    prelude::{AccessMethodError, Timestamp},
};

pub struct TimestampPartitionedChain<T: MemPool> {
    mem_pool: Arc<T>,
    c_key: ContainerKey,

    partitions: Vec<TimestampPartition<T>>,
    partition_bounds: Vec<Timestamp>, // start_ts of each partition
}

pub struct TimestampPartition<T: MemPool> {
    range: (Timestamp, Timestamp), // [start, end)
    chain: Vec<ChainedHashHistoryChain<T>>,
}

impl<T: MemPool> TimestampPartitionedChain<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self {
            mem_pool,
            c_key,
            partitions: Vec::new(),
            partition_bounds: vec![0],
        }
    }

    pub fn find_partition_index(&self, ts: Timestamp) -> Option<usize> {
        match self
            .partition_bounds
            .binary_search_by(|probe| probe.cmp(&ts))
        {
            Ok(idx) => Some(idx),
            Err(idx) => {
                if idx > 0 {
                    Some(idx - 1)
                } else {
                    None
                }
            }
        }
    }

    pub fn get_partition_mut(&mut self, ts: Timestamp) -> Option<&mut TimestampPartition<T>> {
        self.find_partition_index(ts)
            .map(|i| &mut self.partitions[i])
    }

    pub fn split_last_partition_at(&mut self, new_ts: Timestamp) -> Result<(), String> {
        let last_idx = self.partitions.len().saturating_sub(1);
        let last_partition = &self.partitions[last_idx];
        let (start, end) = last_partition.range;

        if new_ts <= start || new_ts >= end {
            return Err("new_ts must be strictly within the last partition range".into());
        }

        self.partitions[last_idx].range = (start, new_ts);

        let new_partition = TimestampPartition {
            range: (new_ts, Timestamp::MAX),
            chain: Vec::new(),
        };

        self.partition_bounds.push(new_ts);
        self.partitions.push(new_partition);

        Ok(())
    }

    pub fn insert(&mut self, ts: Timestamp, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let part = self
            .get_partition_mut(ts)
            .ok_or(AccessMethodError::Other("No matching partition".into()))?;

        let chain = part
            .chain
            .first_mut()
            .ok_or(AccessMethodError::Other("Empty chain in partition".into()))?;

        chain.insert(entry)
    }
    // pub fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
    //     let ts = entry.start_ts();
    //     match self.find_or_create_partition(ts)? {
    //         Some(partition) => partition.chain.insert(entry),
    //         None => Err(AccessMethodError::TsPartitionNotFound),
    //     }
    // }

    // pub fn probe(&self, pkey: &[u8], ts: Timestamp) -> Option<MvccEntry> {
    //     if let Some(partition) = self.find_partition(ts) {
    //         partition.chain.get_history(pkey, &ts).ok()
    //     } else {
    //         None
    //     }
    // }

    // pub fn garbage_collect(&mut self, watermark: Timestamp) {
    //     self.partitions.retain(|p| p.range.1 > watermark);
    //     for part in &mut self.partitions {
    //         part.chain.garbage_collect(&watermark).ok();
    //     }
    // }

    // fn find_partition(&self, ts: Timestamp) -> Option<&TimestampPartition<T>> {
    //     self.partitions
    //         .iter()
    //         .find(|p| ts >= p.range.0 && ts < p.range.1)
    // }

    // fn find_or_create_partition(
    //     &mut self,
    //     ts: Timestamp,
    // ) -> Result<&mut TimestampPartition<T>, AccessMethodError> {
    //     if let Some(idx) = self
    //         .partitions
    //         .iter()
    //         .position(|p| ts >= p.range.0 && ts < p.range.1)
    //     {
    //         Ok(&mut self.partitions[idx])
    //     } else {
    //         let new_start = ts - (ts % MAX_PARTITION_LIFETIME);
    //         let new_end = new_start + MAX_PARTITION_LIFETIME;
    //         let new_partition = TimestampPartition {
    //             range: (new_start, new_end),
    //             chain: ChainedHashHistoryChain::new(...), // create new chain
    //         };
    //         self.partitions.push(new_partition);
    //         Ok(self.partitions.last_mut().unwrap())
    //     }
    // }
}
