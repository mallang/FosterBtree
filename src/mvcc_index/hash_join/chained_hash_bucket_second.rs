use std::{sync::Arc, time::Duration};

use dashmap::mapref::entry;

use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{MvccEntry, TxId},
    prelude::{AccessMethodError, Timestamp},
};

use super::{
    chained_hash_history_chain::ChainedHashHistoryChain,
    chained_hash_recent_chain::ChainedHashRecentChain,
};

pub struct SecondBucket<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    recent_chain: Arc<ChainedHashRecentChain<T>>,
    history_chain: Arc<ChainedHashHistoryChain<T>>,
}

impl<T: MemPool> SecondBucket<T> {
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        let recent_chain = Arc::new(ChainedHashRecentChain::new(c_key, mem_pool.clone()));
        let history_chain = Arc::new(ChainedHashHistoryChain::new(c_key, mem_pool.clone()));

        Self {
            c_key,
            mem_pool,
            recent_chain,
            history_chain,
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        self.recent_chain.insert(entry)
    }

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let recent_result = self.recent_chain.get(pkey, ts);
        match recent_result {
            Ok(entry) => Ok(entry),
            Err(AccessMethodError::KeyNotFound)
            | Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_entry = self.history_chain.get(pkey, ts);
                match history_entry {
                    Ok(entry) => Ok(entry),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn update(&self, pkey: &[u8], entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let old_result = self.recent_chain.update(pkey, entry);
        match old_result {
            Ok(mut old_entry) => {
                old_entry.set_end_ts(&entry.end_ts());
                self.history_chain.insert(&old_entry)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn delete(&self, pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
        let old_result = self.recent_chain.delete(pkey, ts);
        match old_result {
            Ok(mut old_entry) => {
                old_entry.set_end_ts(ts);
                self.history_chain.insert(&old_entry)?;
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

    pub fn recent_chain(&self) -> &Arc<ChainedHashRecentChain<T>> {
        &self.recent_chain
    }

    pub fn history_chain(&self) -> &Arc<ChainedHashHistoryChain<T>> {
        &self.history_chain
    }
}
