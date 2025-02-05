use std::{sync::Arc, time::Duration};

use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::TxId,
    prelude::{AccessMethodError, Timestamp},
};

use super::{
    mvcc_hash_join_history_chain::MvccHashJoinHistoryChain,
    mvcc_hash_join_recent_chain::MvccHashJoinRecentChain,
};

pub struct SecondTableBucket<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    recent_chain: Arc<MvccHashJoinRecentChain<T>>,
    history_chain: Arc<MvccHashJoinHistoryChain<T>>,
}

impl<T: MemPool> SecondTableBucket<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let recent_chain = Arc::new(MvccHashJoinRecentChain::new(c_key, mem_pool.clone()));
        let history_chain = Arc::new(MvccHashJoinHistoryChain::new(c_key, mem_pool.clone()));

        Self {
            c_key,
            mem_pool,
            recent_chain,
            history_chain,
        }
    }

    pub fn insert(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        self.recent_chain.insert(key, pkey, ts, tx_id, val)
    }

    pub fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
        let recent_val = self.recent_chain.get(key, pkey, ts);
        match recent_val {
            Ok(val) => Ok(val),
            Err(AccessMethodError::KeyNotFound)
            | Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_val = self.history_chain.get(key, pkey, *ts);
                match history_val {
                    Ok(val) => Ok(val),
                    Err(AccessMethodError::KeyNotFound) => Err(AccessMethodError::KeyNotFound),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn update(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        let old_result = self.recent_chain.update(key, pkey, ts, tx_id, val);
        match old_result {
            Ok((old_ts, old_val)) => {
                self.history_chain
                    .insert(key, pkey, old_ts, *ts, &old_val)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn delete(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: &Timestamp,
        tx_id: &TxId,
    ) -> Result<(), AccessMethodError> {
        let old_result = self.recent_chain.delete(key, pkey, ts, tx_id);
        match old_result {
            Ok((old_ts, old_val)) => {
                self.history_chain
                    .insert(key, pkey, old_ts, *ts, &old_val)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Read page with given PageFrameKey
    fn read_page(&self, page_key: PageFrameKey) -> FrameReadGuard {
        loop {
            let page = self.mem_pool.get_page_for_read(page_key);
            match page {
                Ok(page) => return page,
                Err(MemPoolStatus::FrameReadLatchGrantFailed) => {
                    log_warn!("Shared page latch grant failed: {:?}. Will retry", page_key);
                    std::hint::spin_loop();
                }
                Err(MemPoolStatus::CannotEvictPage) => {
                    log_warn!("All frames are latched and cannot evict page to read the page: {:?}. Will retry", page_key);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }
    }

    pub fn get_recent_chain(&self) -> Arc<MvccHashJoinRecentChain<T>> {
        self.recent_chain.clone()
    }

    pub fn get_history_chain(&self) -> Arc<MvccHashJoinHistoryChain<T>> {
        self.history_chain.clone()
    }
}
