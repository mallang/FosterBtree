use std::{
    collections::HashMap, sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    }, time::{Duration, Instant}
};

use dashmap::mapref::entry;

use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{MvccEntry, TxId},
    prelude::{AccessMethodError, Timestamp},
};

use super::{
    chained_hash_bucket_first::ChainBucketBulkUpdate, chained_hash_history_chain::ChainedHashHistoryChain, chained_hash_recent_chain::ChainedHashRecentChain
};

pub static RECENT_GET_TOTAL_NS: AtomicU64 = AtomicU64::new(0);
pub static HISTORY_GET_TOTAL_NS: AtomicU64 = AtomicU64::new(0);

pub static RECENT_GET_COUNT: AtomicU64 = AtomicU64::new(0);
pub static HISTORY_GET_COUNT: AtomicU64 = AtomicU64::new(0);

pub struct SecondBucket<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    recent_chain: Arc<ChainedHashRecentChain<T>>,
    history_chain: Arc<ChainedHashHistoryChain<T>>,

    bulk_update: HashMap<Vec<u8>, Vec<u8>>,
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
            bulk_update: HashMap::new(),
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        self.recent_chain.insert(entry)
    }

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let recent_start = Instant::now();
        let recent_result = self.recent_chain.get(pkey, ts);
        let recent_duration = recent_start.elapsed().as_nanos() as u64;
        // Accumulate the duration globally
        RECENT_GET_TOTAL_NS.fetch_add(recent_duration, Ordering::Relaxed);
        RECENT_GET_COUNT.fetch_add(1, Ordering::Relaxed);
        match recent_result {
            Ok(entry) => Ok(entry),
            Err(AccessMethodError::KeyNotFound)
            | Err(AccessMethodError::KeyFoundButInvalidTimestamp) => {
                let history_start = Instant::now();
                let history_entry = self.history_chain.get(pkey, ts);
                let history_duration = history_start.elapsed().as_nanos() as u64;
                // Accumulate the history chain duration globally
                HISTORY_GET_TOTAL_NS.fetch_add(history_duration, Ordering::Relaxed);
                HISTORY_GET_COUNT.fetch_add(1, Ordering::Relaxed);
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
            Ok(old_entry) => {
                self.history_chain.insert(&old_entry)?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn bulk_update(&self, bulk: &mut ChainBucketBulkUpdate, new_start_ts: Timestamp) -> Result<(), AccessMethodError> {
        self.recent_chain.do_bulk_update(bulk, new_start_ts)?;
        for old_entry in bulk.old_entries.iter() {
            self.history_chain.insert(old_entry)?;
        }
        Ok(())
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

    pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        self.history_chain.garbage_collect(ts)
    }

    pub fn recent_chain(&self) -> &Arc<ChainedHashRecentChain<T>> {
        &self.recent_chain
    }

    pub fn history_chain(&self) -> &Arc<ChainedHashHistoryChain<T>> {
        &self.history_chain
    }

    pub fn stat(&self) -> String {
        // Obtain stats from both chains.
        let recent_stat = self.recent_chain.stat();
        let history_stat = self.history_chain.stat();
        // Format a combined report.
        format!(
            "=== SecondBucket Stats ===\nRecent Chain:\n{}\nHistory Chain:\n{}",
            recent_stat, history_stat
        )
    }

    pub fn scan_key_into(&self, search_key: &[u8], ts: &Timestamp, results: &mut Vec<MvccEntry>) {
        self.recent_chain.scan_key_into(search_key, ts, results);
        self.history_chain.scan_key_into(search_key, ts, results);
    }

    pub fn scan_into_vec(
        &self,
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        self.recent_chain.scan_into_vec(ts, results)?;
        self.history_chain.scan_into_vec(ts, results)
    }

    pub fn scan_into_vec_recent(
        &self,
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) -> Result<(), AccessMethodError> {
        self.recent_chain.scan_into_vec(ts, results)?;
        Ok(())
    }
}

// test code
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::get_in_mem_pool;
    use crate::mvcc_index::MvccEntry;

    #[test]
    fn test_second_bucket_insert_get_update() {
        // Assume get_in_mem_pool() and ContainerKey are available.
        // Create a mem pool and a SecondBucket.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = SecondBucket::new(c_key, mem_pool);

        // --- Step 1: Insert a recent value ---
        let key = b"key-001".to_vec();
        let pkey = b"pkey-001".to_vec();

        // Insert a recent version that is valid from 100.
        let initial_value = b"initial-value".to_vec();
        let initial_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            initial_value.clone(),
            100,
            u64::MAX,
        );
        bucket
            .insert(&initial_entry)
            .expect("Insert recent value failed");

        // Verify that a get query within [100,150) returns the inserted recent value.
        let fetched_initial = bucket
            .get(&pkey, &120)
            .expect("Get failed for initial recent value");
        assert_eq!(fetched_initial.value(), initial_value.as_slice());
        assert_eq!(fetched_initial.start_ts(), 100);
        assert_eq!(fetched_initial.end_ts(), u64::MAX);

        // --- Step 2: Update the value ---
        // We now update the same primary key with a new version that starts later.
        // For example, the new version is valid from 200.
        // According to the SecondBucket::update implementation, the update is applied to the recent chain
        // and the old recent version is inserted into the history chain with its end_ts updated to the new entry’s end_ts.
        let updated_value = b"updated-value".to_vec();
        let update_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            updated_value.clone(),
            200,
            u64::MAX,
        );
        bucket.update(&pkey, &update_entry).expect("Update failed");

        // --- Step 3: Verify get() behavior for different timestamps ---
        // For a query timestamp within the old version's interval (e.g., ts = 120),
        // the recent chain should not have a valid entry; so get() falls back to the history chain.
        // The history chain should return the old version with its end_ts adjusted.
        let fetched_history = bucket
            .get(&pkey, &120)
            .expect("Get failed for historical version");
        assert_eq!(
            fetched_history.value(),
            initial_value.as_slice(),
            "Historical value mismatch: expected initial value"
        );
        // According to our update implementation, the old version’s end_ts is set to the new entry's end_ts.
        assert_eq!(
            fetched_history.start_ts(),
            100,
            "Historical start_ts mismatch"
        );
        assert_eq!(fetched_history.end_ts(), 200, "Historical end_ts mismatch");

        // For a query timestamp within the new version's valid range (e.g., ts = 220),
        // get() should return the new recent version.
        let fetched_recent = bucket
            .get(&pkey, &220)
            .expect("Get failed for updated recent version");
        assert_eq!(
            fetched_recent.value(),
            updated_value.as_slice(),
            "Recent value mismatch: expected updated value"
        );
        assert_eq!(fetched_recent.start_ts(), 200, "Recent start_ts mismatch");
        assert_eq!(fetched_recent.end_ts(), u64::MAX, "Recent end_ts mismatch");
    }

    #[test]
    fn test_second_bucket_insert_update_delete_get() {
        // Assume that get_in_mem_pool() and ContainerKey are available.
        // Create a mem pool and a SecondBucket.
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = SecondBucket::new(c_key, mem_pool);

        // --- Step 1: Insert a recent value ---
        let key = b"key-001".to_vec();
        let pkey = b"pkey-001".to_vec();

        // Insert a recent version valid from 100 until u64::MAX.
        let initial_value = b"initial-value".to_vec();
        let recent_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            initial_value.clone(),
            100,
            u64::MAX,
        );
        bucket
            .insert(&recent_entry)
            .expect("Insert recent value failed");

        // Verify that a get query at ts = 120 returns the recent value.
        let fetched_recent = bucket
            .get(&pkey, &120)
            .expect("Get failed for recent value");
        assert_eq!(fetched_recent.value(), initial_value.as_slice());
        assert_eq!(fetched_recent.start_ts(), 100);
        assert_eq!(fetched_recent.end_ts(), u64::MAX);

        // --- Step 2: Update the value ---
        // Update the same primary key with a new version starting at 200 (to u64::MAX).
        // According to the update implementation, the old recent version is removed from the recent chain
        // and inserted into the history chain with its end_ts set to 200.
        let updated_value = b"updated-value".to_vec();
        let update_entry = MvccEntry::new(
            key.clone(),
            pkey.clone(),
            updated_value.clone(),
            200,
            u64::MAX,
        );
        bucket.update(&pkey, &update_entry).expect("Update failed");

        // After the update:
        // - The recent chain should now contain the new version valid from 200 to u64::MAX.
        // - The history chain should contain the old version, now adjusted to [100,200].
        let history_version = bucket
            .get(&pkey, &150)
            .expect("Get failed for historical version");
        assert_eq!(
            history_version.value(),
            initial_value.as_slice(),
            "Historical value mismatch: expected initial value"
        );
        assert_eq!(
            history_version.start_ts(),
            100,
            "Historical start_ts mismatch"
        );
        assert_eq!(history_version.end_ts(), 200, "Historical end_ts mismatch");

        let recent_updated = bucket
            .get(&pkey, &220)
            .expect("Get failed for updated recent version");
        assert_eq!(
            recent_updated.value(),
            updated_value.as_slice(),
            "Recent value mismatch: expected updated value"
        );
        assert_eq!(recent_updated.start_ts(), 200, "Recent start_ts mismatch");
        assert_eq!(recent_updated.end_ts(), u64::MAX, "Recent end_ts mismatch");

        // --- Step 3: Delete the recent value ---
        // Delete the recent version using a deletion timestamp within its valid range.
        // The delete method removes the recent version and inserts it into the history chain with its end_ts set to the deletion ts.
        let deletion_ts: Timestamp = 250;
        bucket.delete(&pkey, &deletion_ts).expect("Delete failed");

        // After deletion:
        // - The recent chain should no longer have a valid version for this pkey.
        // - The history chain should now have two versions for this pkey:
        //     a) [100,200] with the initial value.
        //     b) [200,250] with the updated value.
        // Verify that a get query at ts = 150 returns the historical version with the initial value.
        let fetched_history = bucket
            .get(&pkey, &150)
            .expect("Get failed for historical version after delete");
        assert_eq!(
            fetched_history.value(),
            initial_value.as_slice(),
            "Historical value mismatch after delete"
        );
        assert_eq!(
            fetched_history.start_ts(),
            100,
            "Historical start_ts mismatch after delete"
        );
        assert_eq!(
            fetched_history.end_ts(),
            200,
            "Historical end_ts mismatch after delete"
        );

        // Verify that a get query at ts = 220 returns the deleted version with the updated value.
        let fetched_deleted = bucket
            .get(&pkey, &220)
            .expect("Get failed for deleted version at ts 220");
        assert_eq!(
            fetched_deleted.value(),
            updated_value.as_slice(),
            "Deleted version value mismatch"
        );
        assert_eq!(
            fetched_deleted.start_ts(),
            200,
            "Deleted version start_ts mismatch"
        );
        assert_eq!(
            fetched_deleted.end_ts(),
            250,
            "Deleted version end_ts mismatch"
        );

        // Finally, a query with a timestamp beyond the deletion range (e.g., ts = 300) should fail.
        let not_found = bucket.get(&pkey, &300);
        assert!(
            not_found.is_err(),
            "Expected get to fail for pkey at ts 300"
        );
    }

    #[ignore = "failed"]
    #[test]
    fn test_second_bucket_random_mixed_ops_half_open_with_logs() {
        use rand::Rng;
        use std::collections::{HashMap, VecDeque};
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        };
        use std::thread;

        //
        // 1. Create a mem pool and a SecondBucket.
        //
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = Arc::new(SecondBucket::new(c_key, mem_pool));

        //
        // 2. Shared data structures:
        //    - ref_state: pkey -> Vec<MvccEntry> (last is "recent" if end_ts==u64::MAX)
        //    - op_log:    pkey -> list of operation strings
        //
        let ref_state: Arc<Mutex<HashMap<Vec<u8>, Vec<MvccEntry>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let op_log: Arc<Mutex<HashMap<Vec<u8>, VecDeque<String>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Helper function to record an operation for a given pkey
        fn log_op(
            op_log: &Arc<Mutex<HashMap<Vec<u8>, VecDeque<String>>>>,
            pkey: &[u8],
            desc: &str,
        ) {
            let mut guard = op_log.lock().unwrap();
            let entry = guard.entry(pkey.to_vec()).or_insert_with(VecDeque::new);
            entry.push_back(desc.to_string());
        }

        //
        // 3. Pre-insert some keys with half‑open intervals [start, u64::MAX)
        //    plus large key/pkey/value.
        //
        let num_initial_keys = 1000;
        for i in 0..num_initial_keys {
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < 30 {
                    s.push('K');
                }
                s.into_bytes()
            };
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < 50 {
                    s.push('P');
                }
                s.into_bytes()
            };
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < 500 {
                value.push(b'V');
            }

            let start_ts = 100 + i as u64;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, u64::MAX);

            // Insert into the bucket
            bucket.insert(&entry).expect("Initial insert failed");
            {
                // Update the reference state
                let mut rs = ref_state.lock().unwrap();
                rs.insert(pkey.clone(), vec![entry.clone()]);
            }
            // Log the operation
            log_op(
                &op_log,
                &pkey,
                &format!("Initial Insert [start={}, end=MAX]", start_ts),
            );
        }
        println!("Bucket stat after initial inserts:\n{}", bucket.stat());

        //
        // 4. Worker threads for random ops (insert, update, delete, get)
        //
        let new_key_counter = Arc::new(AtomicUsize::new(num_initial_keys));
        const NUM_WORKER_THREADS: usize = 1;
        const NUM_ITERATIONS: usize = 1000;
        let mut handles = Vec::new();

        for t_id in 0..NUM_WORKER_THREADS {
            let bucket_clone = bucket.clone();
            let ref_state_clone = ref_state.clone();
            let op_log_clone = op_log.clone();
            let new_key_counter_clone = new_key_counter.clone();

            let handle = thread::spawn(move || {
                let mut rng = rand::thread_rng();

                for iter_idx in 0..NUM_ITERATIONS {
                    let op = rng.gen_range(0..4); // 0=insert,1=update,2=delete,3=get

                    match op {
                        0 => {
                            // Insert a brand-new key
                            let idx = new_key_counter_clone.fetch_add(1, Ordering::SeqCst);
                            let key = {
                                let mut s = format!("key-{:03}", idx);
                                while s.len() < 30 {
                                    s.push('K');
                                }
                                s.into_bytes()
                            };
                            let pkey = {
                                let mut s = format!("pkey-{:03}", idx);
                                while s.len() < 50 {
                                    s.push('P');
                                }
                                s.into_bytes()
                            };
                            let mut value = format!("value-{:03}", idx).into_bytes();
                            while value.len() < 500 {
                                value.push(b'V');
                            }

                            let entry = MvccEntry::new(key, pkey.clone(), value, 100, u64::MAX);
                            if bucket_clone.insert(&entry).is_ok() {
                                {
                                    let mut rs = ref_state_clone.lock().unwrap();
                                    rs.insert(pkey.clone(), vec![entry.clone()]);
                                }
                                log_op(
                                    &op_log_clone,
                                    &pkey,
                                    &format!(
                                        "Insert [start=100, end=MAX] t_id={} iter={}",
                                        t_id, iter_idx
                                    ),
                                );
                            }
                        }
                        1 => {
                            // Update
                            let mut rs = ref_state_clone.lock().unwrap();
                            if rs.is_empty() {
                                continue;
                            }
                            let keys: Vec<_> = rs.keys().cloned().collect();
                            let pkey = &keys[rng.gen_range(0..keys.len())];
                            let versions = rs.get_mut(pkey).unwrap();
                            if let Some(last) = versions.last() {
                                if last.end_ts() != u64::MAX {
                                    continue;
                                }
                                let new_start = last.start_ts() + rng.gen_range(1..100);
                                let mut updated_value =
                                    format!("updated-{}", rng.gen::<u32>()).into_bytes();
                                while updated_value.len() < 500 {
                                    updated_value.push(b'U');
                                }

                                let update_entry = MvccEntry::new(
                                    last.key().to_vec(),
                                    last.pkey().to_vec(),
                                    updated_value.clone(),
                                    new_start,
                                    u64::MAX,
                                );
                                if bucket_clone.update(pkey, &update_entry).is_ok() {
                                    let mut old = versions.pop().unwrap();
                                    old.set_end_ts(&new_start);
                                    versions.push(old.clone());
                                    versions.push(update_entry.clone());
                                    log_op(&op_log_clone, pkey, &format!(
                                        "Update old->end={} new->[start={},end=MAX], t_id={}, iter={}",
                                        new_start, new_start, t_id, iter_idx
                                    ));
                                }
                            }
                        }
                        2 => {
                            // Delete
                            let mut rs = ref_state_clone.lock().unwrap();
                            if rs.is_empty() {
                                continue;
                            }
                            let keys: Vec<_> = rs.keys().cloned().collect();
                            let pkey = &keys[rng.gen_range(0..keys.len())];
                            let versions = rs.get_mut(pkey).unwrap();
                            if let Some(last) = versions.last() {
                                if last.end_ts() != u64::MAX {
                                    continue;
                                }
                                let del_ts = last.start_ts() + rng.gen_range(1..100);
                                if bucket_clone.delete(pkey, &del_ts).is_ok() {
                                    let mut old_recent = versions.pop().unwrap();
                                    old_recent.set_end_ts(&del_ts);
                                    versions.push(old_recent.clone());
                                    log_op(
                                        &op_log_clone,
                                        pkey,
                                        &format!(
                                            "Delete => old->end={}, t_id={}, iter={}",
                                            del_ts, t_id, iter_idx
                                        ),
                                    );
                                }
                            }
                        }
                        3 => {
                            // Get
                            let rs = ref_state_clone.lock().unwrap();
                            if rs.is_empty() {
                                continue;
                            }
                            let keys: Vec<_> = rs.keys().cloned().collect();
                            let pkey = &keys[rng.gen_range(0..keys.len())];
                            let versions = rs.get(pkey).unwrap();
                            let chosen = &versions[rng.gen_range(0..versions.len())];
                            let start = chosen.start_ts();
                            let end = chosen.end_ts();
                            if start < end {
                                // pick query_ts in [start, end), skipping boundary
                                let query_ts = if end == u64::MAX {
                                    // simplest approach: use start
                                    start
                                } else if end == start + 1 {
                                    start
                                } else {
                                    let adj_end = end.saturating_sub(1);
                                    start + (adj_end - start) / 2
                                };
                                let _ = bucket_clone.get(pkey, &query_ts);
                                log_op(
                                    &op_log_clone,
                                    pkey,
                                    &format!(
                                        "Get at ts={}, interval=[{},{}), t_id={}, iter={}",
                                        query_ts, start, end, t_id, iter_idx
                                    ),
                                );
                            }
                        }
                        _ => {}
                    }
                }
            });
            handles.push(handle);
        }

        for h in handles {
            h.join().unwrap();
        }

        println!("Bucket stat after random ops:\n{}", bucket.stat());

        //
        // 5. Final verification
        //
        let rs = ref_state.lock().unwrap();
        for (pkey_bytes, versions) in rs.iter() {
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                // skip if empty or degenerate
                if start >= end {
                    continue;
                }
                let query_ts = if end == u64::MAX {
                    // just pick start
                    start
                } else if end == start + 1 {
                    start
                } else {
                    let adj_end = end.saturating_sub(1);
                    start + (adj_end - start) / 2
                };
                if query_ts >= start && query_ts < end {
                    // We expect a valid version
                    let fetched = bucket.get(pkey_bytes, &query_ts);
                    match fetched {
                        Ok(res) => {
                            if res.start_ts() != start
                                || res.end_ts() != end
                                || res.value() != version.value()
                            {
                                // Mismatch => print operation logs for debugging
                                let pkey_str = String::from_utf8_lossy(&pkey_bytes);
                                eprintln!(
                                    "Mismatch for pkey=\"{}\" at ts={}: expected [start={},end={}), got [start={},end={}), value_len={}, expecting {}",
                                    pkey_str, query_ts, start, end, res.start_ts(), res.end_ts(),
                                    res.value().len(), version.value().len()
                                );
                                // Print logs
                                let op_log_guard = op_log.lock().unwrap();
                                if let Some(loglist) = op_log_guard.get(pkey_bytes) {
                                    eprintln!("Operation logs for pkey=\"{}\":", pkey_str);
                                    for (idx, line) in loglist.iter().enumerate() {
                                        eprintln!("  {}: {}", idx, line);
                                    }
                                }
                                panic!(
                                    "Version mismatch for pkey=\"{}\", see logs above",
                                    pkey_str
                                );
                            }
                        }
                        Err(AccessMethodError::KeyNotFound) => {
                            // We expected a version, got KeyNotFound => dump logs
                            let pkey_str = String::from_utf8_lossy(&pkey_bytes);
                            eprintln!(
                                "Expected version [start={},end={}) for pkey=\"{}\" at ts={}, got KeyNotFound",
                                start, end, pkey_str, query_ts
                            );
                            let op_log_guard = op_log.lock().unwrap();
                            if let Some(loglist) = op_log_guard.get(pkey_bytes) {
                                eprintln!("Operation logs for pkey=\"{}\":", pkey_str);
                                for (idx, line) in loglist.iter().enumerate() {
                                    eprintln!("  {}: {}", idx, line);
                                }
                            }
                            panic!(
                                "Expected version but got KeyNotFound for pkey=\"{}\"",
                                pkey_str
                            );
                        }
                        Err(e) => {
                            let pkey_str = String::from_utf8_lossy(&pkey_bytes);
                            eprintln!(
                                "Unexpected error for pkey=\"{}\" at ts={}: {:?}",
                                pkey_str, query_ts, e
                            );
                            let op_log_guard = op_log.lock().unwrap();
                            if let Some(loglist) = op_log_guard.get(pkey_bytes) {
                                eprintln!("Operation logs for pkey=\"{}\":", pkey_str);
                                for (idx, line) in loglist.iter().enumerate() {
                                    eprintln!("  {}: {}", idx, line);
                                }
                            }
                            panic!(
                                "Unexpected error for pkey=\"{}\" at ts={}",
                                pkey_str, query_ts
                            );
                        }
                    }
                } else {
                    // outside [start,end) => KeyNotFound is correct
                    if let Ok(res) = bucket.get(pkey_bytes, &query_ts) {
                        let pkey_str = String::from_utf8_lossy(pkey_bytes);
                        eprintln!(
                            "Got version unexpectedly for pkey=\"{}\" at ts={}, but interval is [{},{}). res=[start={},end={})",
                            pkey_str, query_ts, start, end, res.start_ts(), res.end_ts()
                        );
                        let op_log_guard = op_log.lock().unwrap();
                        if let Some(loglist) = op_log_guard.get(pkey_bytes) {
                            eprintln!("Operation logs for pkey=\"{}\":", pkey_str);
                            for (idx, line) in loglist.iter().enumerate() {
                                eprintln!("  {}: {}", idx, line);
                            }
                        }
                        panic!(
                            "Got version unexpectedly for pkey=\"{}\" at ts={}",
                            pkey_str, query_ts
                        );
                    }
                }
            }
        }
    }

    #[ignore = "failed"]
    #[test]
    fn test_second_bucket_single_thread_insert_update_history() {
        use std::collections::HashMap;

        // 1) Set up your mem pool, container key, and instantiate SecondBucket
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = SecondBucket::new(c_key, mem_pool);

        // For half‑open intervals, we treat a version’s validity as [start, end).
        // The last version is "recent" with end_ts == u64::MAX. When we update,
        // we fix the old version’s end_ts to the new version’s start_ts.

        // 2) We'll store a reference state: pkey -> list of versions (Vec<MvccEntry>)
        // The last version in each list is "recent" if it has end_ts==u64::MAX.
        let mut ref_state: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();

        // We define the number of initial keys and the number of updates we do for each key
        let num_keys = 500;
        let num_updates_per_key = 3;

        // 3) Insert step
        // For each key:
        //  - build key/pkey with sizes: key_size=50, pkey_size=100
        //  - build value with size=1000
        //  - create a version: [start=some, end=u64::MAX], insert into bucket, store in ref_state
        for i in 0..num_keys {
            // Key
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < 50 {
                    s.push('K');
                }
                s.into_bytes()
            };
            // Pkey
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < 100 {
                    s.push('P');
                }
                s.into_bytes()
            };
            // Value
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < 1000 {
                value.push(b'V');
            }

            // We'll define start_ts in some range, e.g., 100+i
            let start_ts = 100 + i as u64;
            let end_ts = u64::MAX;

            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            // Insert into the bucket
            bucket.insert(&entry).expect("Initial insert failed");

            // Put in our reference state
            ref_state.insert(pkey.clone(), vec![entry]);
        }

        println!(
            "Bucket stat after initial inserts / before update:\n{}",
            bucket.stat()
        );

        // 4) Update step
        // For each key, we apply multiple updates. Each update:
        //  - picks the last version in ref_state (which should have end_ts=MAX),
        //  - sets that version's end_ts to new_start,
        //  - pushes a new "recent" version with [new_start, MAX],
        //  - we also enlarge the value by 1000 each time, and pick new_start > old.start_ts
        for i in 0..num_keys {
            // We'll reconstruct the pkey we built, or we can keep track in a vector.
            // Here we do the same logic we used in the insert step:
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < 100 {
                    s.push('P');
                }
                s.into_bytes()
            };
            // We'll do `num_updates_per_key` updates for this single key
            for u in 0..num_updates_per_key {
                let versions = ref_state.get_mut(&pkey).unwrap();
                let old_recent = versions.last().unwrap().clone(); // clone so we can manipulate
                if old_recent.end_ts() != u64::MAX {
                    // no recent version => skip or panic
                    panic!("Unexpected: old recent version does not have end_ts == MAX");
                }
                // new start_ts
                let new_start = old_recent.start_ts() + 10 + (u as u64) * 10;
                // or any logic ensuring new_start > old_recent.start_ts

                // build a bigger value (old size + 1000)
                let mut new_value = old_recent.value().to_vec();
                // let's enlarge it by 1000 each update
                for _ in 0..1000 {
                    new_value.push(b'U');
                }

                // The updated entry
                let updated_entry = MvccEntry::new(
                    old_recent.key().to_vec(),
                    old_recent.pkey().to_vec(),
                    new_value.clone(),
                    new_start,
                    u64::MAX,
                );

                // Actually call update on the bucket
                bucket
                    .update(&old_recent.pkey(), &updated_entry)
                    .expect("Update failed");

                // Now fix the old recent version's end_ts to new_start
                let mut old_recent_mut = versions.pop().unwrap();
                old_recent_mut.set_end_ts(&new_start);
                versions.push(old_recent_mut);

                // Push the new "recent" version
                versions.push(updated_entry);
            }
        }

        // 5) Final verification
        // We verify that for every version in ref_state, a query timestamp in [start, end)
        // returns that version. We'll skip if start >= end (which means an empty interval).
        for (pkey, versions) in &ref_state {
            // For each version in ascending order
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                // skip if the interval is empty or degenerate
                if start >= end {
                    continue;
                }
                // pick a query_ts in [start, end), half‑open => skip boundary
                // simplest approach: if end == start+1 => pick start
                // if end == u64::MAX => pick start
                // else pick midpoint
                let query_ts = if end == u64::MAX {
                    // we can just pick start
                    start
                } else if end == start + 1 {
                    start
                } else {
                    let adj_end = end.saturating_sub(1);
                    start + (adj_end - start) / 2
                };

                if query_ts >= start && query_ts < end {
                    // we expect a version
                    let fetched = bucket.get(pkey, &query_ts);
                    match fetched {
                        Ok(res) => {
                            // verify it matches
                            assert_eq!(
                                res.start_ts(),
                                start,
                                "start_ts mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                            assert_eq!(
                                res.end_ts(),
                                end,
                                "end_ts mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                            assert_eq!(
                                res.value(),
                                version.value(),
                                "value mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                        }
                        Err(AccessMethodError::KeyNotFound) => {
                            panic!(
                                "Expected version [start={}, end={}) for pkey='{}' at ts={}, got KeyNotFound",
                                start, end, String::from_utf8_lossy(pkey), query_ts
                            );
                        }
                        Err(e) => {
                            panic!(
                                "Unexpected error for pkey='{}' at ts={}: {:?}",
                                String::from_utf8_lossy(pkey),
                                query_ts,
                                e
                            );
                        }
                    }
                } else {
                    // outside [start, end) => we expect KeyNotFound
                    if let Ok(res) = bucket.get(pkey, &query_ts) {
                        panic!(
                            "Got a version unexpectedly for pkey='{}' at ts={}, but interval is [{},{}) => start={},end={},value_len={}",
                            String::from_utf8_lossy(pkey), query_ts, start, end,
                            res.start_ts(), res.end_ts(), res.value().len()
                        );
                    }
                }
            }
        }

        println!("Bucket stat after update:\n{}", bucket.stat());
    }
    
    #[ignore = "failed"]
    #[test]
    fn test_second_bucket_multi_thread_insert_update_history() {
        use std::collections::HashMap;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        };
        use std::thread;

        // 1) Create mem pool + SecondBucket
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = Arc::new(SecondBucket::new(c_key, mem_pool));

        // We'll treat each version as [start_ts, end_ts), with the last version having end_ts == u64::MAX.

        // 2) Shared reference state: pkey -> vec of versions
        let ref_state: Arc<Mutex<HashMap<Vec<u8>, Vec<MvccEntry>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // We'll define the total number of keys and updates
        let total_keys = 1000;
        let updates_per_key = 3;

        // 3) Multi-threaded insertion
        // Suppose we spawn multiple threads that each insert a portion of the keys.
        let num_insert_threads = 1;
        let keys_per_thread = (total_keys + num_insert_threads - 1) / num_insert_threads;

        let mut insert_handles = Vec::new();
        for t_id in 0..num_insert_threads {
            let bucket_clone = bucket.clone();
            let ref_state_clone = ref_state.clone();

            let handle = thread::spawn(move || {
                // We'll compute the key range for this thread
                let start_idx = t_id * keys_per_thread;
                let end_idx = std::cmp::min(start_idx + keys_per_thread, total_keys);

                for i in start_idx..end_idx {
                    // Build a 50-byte key
                    let key = {
                        let mut s = format!("key-{:03}", i);
                        while s.len() < 50 {
                            s.push('K');
                        }
                        s.into_bytes()
                    };
                    // Build a 100-byte pkey
                    let pkey = {
                        let mut s = format!("pkey-{:03}", i);
                        while s.len() < 100 {
                            s.push('P');
                        }
                        s.into_bytes()
                    };
                    // Build a 1000-byte value
                    let mut value = format!("value-{:03}", i).into_bytes();
                    while value.len() < 1000 {
                        value.push(b'V');
                    }

                    let start_ts = 100 + i as u64;
                    let end_ts = u64::MAX;
                    let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

                    // Insert
                    bucket_clone.insert(&entry).expect("Insert failed");

                    // Update reference state
                    let mut guard = ref_state_clone.lock().unwrap();
                    guard.insert(pkey.clone(), vec![entry]);
                }
            });
            insert_handles.push(handle);
        }

        // Wait for all insert threads
        for h in insert_handles {
            h.join().unwrap();
        }

        println!(
            "Bucket stat after multi-thread initial inserts:\n{}",
            bucket.stat()
        );

        // 4) Multi-threaded updates
        // We spawn multiple threads again, each updates a portion of the keys.
        // Or we can have them all update all keys at random to create more concurrency collisions.
        let num_update_threads = 1;
        let keys_per_thread_update = (total_keys + num_update_threads - 1) / num_update_threads;

        let mut update_handles = Vec::new();
        for t_id in 0..num_update_threads {
            let bucket_clone = bucket.clone();
            let ref_state_clone = ref_state.clone();

            let handle = thread::spawn(move || {
                // We'll compute the key range for this thread
                let start_idx = t_id * keys_per_thread_update;
                let end_idx = std::cmp::min(start_idx + keys_per_thread_update, total_keys);

                for i in start_idx..end_idx {
                    for u in 0..updates_per_key {
                        // Rebuild the same pkey
                        let pkey = {
                            let mut s = format!("pkey-{:03}", i);
                            while s.len() < 100 {
                                s.push('P');
                            }
                            s.into_bytes()
                        };

                        // We'll lock the ref_state, read the last version, and build an update
                        let mut guard = ref_state_clone.lock().unwrap();
                        let versions = guard.get_mut(&pkey).unwrap();

                        let old_recent = versions.last().unwrap().clone();
                        if old_recent.end_ts() != u64::MAX {
                            // if no recent version, skip
                            continue;
                        }

                        // pick new_start
                        let new_start = old_recent.start_ts() + 10 + (u as u64) * 10;

                        // build bigger value: old size + 1000
                        let mut new_value = old_recent.value().to_vec();
                        for _ in 0..1000 {
                            new_value.push(b'U');
                        }

                        let updated_entry = MvccEntry::new(
                            old_recent.key().to_vec(),
                            old_recent.pkey().to_vec(),
                            new_value,
                            new_start,
                            u64::MAX,
                        );

                        // Actually update in the bucket
                        if bucket_clone
                            .update(&old_recent.pkey(), &updated_entry)
                            .is_ok()
                        {
                            // fix old version's end_ts
                            let mut oldv = versions.pop().unwrap();
                            oldv.set_end_ts(&new_start);
                            versions.push(oldv);
                            // push new recent
                            versions.push(updated_entry);
                        }
                    }
                }
            });
            update_handles.push(handle);
        }

        for h in update_handles {
            h.join().unwrap();
        }

        println!("Bucket stat after multi-thread updates:\n{}", bucket.stat());

        // 5) Single-thread final verification
        // We'll read the final reference state and for each version, pick a query_ts in [start, end).
        {
            let guard = ref_state.lock().unwrap();
            for (pkey, versions) in guard.iter() {
                for version in versions {
                    let start = version.start_ts();
                    let end = version.end_ts();
                    if start >= end {
                        continue;
                    }
                    // pick a query timestamp strictly < end
                    let query_ts = if end == u64::MAX {
                        start
                    } else if end == start + 1 {
                        start
                    } else {
                        let adj_end = end.saturating_sub(1);
                        start + (adj_end - start) / 2
                    };

                    if query_ts >= start && query_ts < end {
                        // expect a version
                        match bucket.get(pkey, &query_ts) {
                            Ok(fetched) => {
                                assert_eq!(
                                    fetched.start_ts(),
                                    start,
                                    "start_ts mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                                assert_eq!(
                                    fetched.end_ts(),
                                    end,
                                    "end_ts mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                                assert_eq!(
                                    fetched.value(),
                                    version.value(),
                                    "value mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                            }
                            Err(AccessMethodError::KeyNotFound) => {
                                panic!(
                                    "Expected version [start={}, end={}) for pkey='{}' at ts={}, got KeyNotFound",
                                    start, end, String::from_utf8_lossy(pkey), query_ts
                                );
                            }
                            Err(e) => {
                                panic!(
                                    "Unexpected error for pkey='{}' at ts={}: {:?}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts,
                                    e
                                );
                            }
                        }
                    } else {
                        // outside [start, end) => expect KeyNotFound
                        if let Ok(fetched) = bucket.get(pkey, &query_ts) {
                            panic!(
                                "Got version unexpectedly for pkey='{}' at ts={}, but interval is [{},{}). Found [start={},end={},value_len={}]",
                                String::from_utf8_lossy(pkey), query_ts, start, end,
                                fetched.start_ts(), fetched.end_ts(), fetched.value().len()
                            );
                        }
                    }
                }
            }
        }

        println!(
            "Multi-threaded test complete: inserted and updated {} keys successfully",
            total_keys
        );
    }

    #[ignore = "failed"]
    #[test]
    fn test_second_bucket_precreated_insert_and_update() {
        use std::collections::HashMap;

        // 1) Create a mem pool and a SecondBucket
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = SecondBucket::new(c_key, mem_pool);

        //
        // Configurable parameters
        //
        let num_keys = 1000; // how many unique keys we test
        let num_updates_per_key = 5; // how many updates we do for each key
        let key_size = 50;
        let pkey_size = 100;
        let initial_value_size = 1000;
        let update_value_growth = 1000;

        //
        // 2) Stage 1: Pre-create insert entries
        //    We'll store them in a vector: one insert per key.
        //
        let mut insert_entries = Vec::new();
        for i in 0..num_keys {
            // Build a large key
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < key_size {
                    s.push('K');
                }
                s.into_bytes()
            };
            // Build a large pkey
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            // Build a 1000-byte value
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < initial_value_size {
                value.push(b'V');
            }

            let start_ts = 100 + i as u64;
            let end_ts = u64::MAX;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            insert_entries.push(entry);
        }

        //
        // 3) Stage 2: Pre-create update entries
        //    For each inserted key, we'll generate multiple updates. We need to record which
        //    old "recent" version it's updating, so we can fix that in the reference state
        //    afterward. For simplicity, we assume each update applies to the "most recent" version
        //    so far. We'll store the updates in a structure, or simply store them in the order
        //    we intend to apply them.
        //
        //    We'll store them in a vector of (pkey, old_start_ts, new_entry).
        //    `old_start_ts` helps us identify which old version to set end_ts for in the reference.
        //
        let mut update_entries = Vec::new();

        for i in 0..num_keys {
            // We'll update the same key multiple times
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            let old_start_ts = 100 + i as u64; // the initial version's start
                                               // We'll keep track of the "current" start for each subsequent update
            let mut current_start = old_start_ts;

            for u in 0..num_updates_per_key {
                // new start is bigger
                let new_start = current_start + 10 + (u as u64) * 10;

                // We'll build a new, bigger value
                let mut updated_value = format!("updated-{}-{:03}", i, u).into_bytes();
                while updated_value.len() < (initial_value_size + (u + 1) * update_value_growth) {
                    updated_value.push(b'U');
                }

                // The new version's key is the same as the old
                // We must retrieve that from insert_entries or store them in some map.
                // But let's replicate the logic for building the same key for i:
                let key = {
                    let mut s = format!("key-{:03}", i);
                    while s.len() < key_size {
                        s.push('K');
                    }
                    s.into_bytes()
                };

                let new_entry =
                    MvccEntry::new(key, pkey.clone(), updated_value, new_start, u64::MAX);

                update_entries.push((pkey.clone(), current_start, new_entry.clone()));

                // next time we do an update, the old "recent" version is this new version
                // i.e. the new start becomes the old for next iteration
                current_start = new_start;
            }
        }

        // We'll sort update_entries if we want them in strict ascending order, but if we
        // plan to apply them in that order we can just keep them as is.

        //
        // 4) Actually Insert everything
        //
        // We'll maintain a reference state: pkey -> Vec<MvccEntry> (the last is recent).
        let mut ref_state: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();

        // 4a) Insert the initial entries
        for entry in &insert_entries {
            bucket.insert(entry).expect("insert failed");
            // in ref_state, we store: ref_state[pkey] = [entry]
            let pkey = entry.pkey().to_vec();
            ref_state.insert(pkey, vec![entry.clone()]);
        }

        println!("Bucket stat after initial inserts:\n{}", bucket.stat());

        // 4b) Apply all updates in the order we created them
        for (pkey, old_start_ts, new_entry) in &update_entries {
            // We'll call update
            bucket.update(pkey, new_entry).expect("update failed");
            // Now fix the old version in ref_state: find the last version that has start_ts==old_start_ts
            // with end_ts==u64::MAX
            let versions = ref_state.get_mut(pkey).unwrap();
            let idx = versions
                .iter()
                .rposition(|v| v.start_ts() == *old_start_ts && v.end_ts() == u64::MAX)
                .expect("No matching old recent version found in ref_state for update");
            // fix that old version's end_ts
            let mut old_version = versions.remove(idx);
            old_version.set_end_ts(&new_entry.start_ts());
            versions.insert(idx, old_version.clone());

            // push the new version at the end
            versions.push(new_entry.clone());
        }

        println!("Bucket stat after updates:\n{}", bucket.stat());

        //
        // 5) Final verification
        // For each version in the reference, pick a query timestamp in [start, end).
        // If end_ts==u64::MAX, we skip the boundary. If start>=end, skip.
        //
        for (pkey, versions) in &ref_state {
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                if start >= end {
                    // empty interval => skip
                    continue;
                }
                // pick a query_ts in [start, end)
                let query_ts = if end == u64::MAX {
                    // simplest approach: just pick start
                    start
                } else if end == start + 1 {
                    start
                } else {
                    let adjusted_end = end.saturating_sub(1);
                    start + (adjusted_end - start) / 2
                };

                if query_ts >= start && query_ts < end {
                    // expect a version
                    match bucket.get(pkey, &query_ts) {
                        Ok(fetched) => {
                            assert_eq!(
                                fetched.start_ts(),
                                start,
                                "start_ts mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                            assert_eq!(
                                fetched.end_ts(),
                                end,
                                "end_ts mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                            assert_eq!(
                                fetched.value(),
                                version.value(),
                                "value mismatch for pkey='{}', query_ts={}",
                                String::from_utf8_lossy(pkey),
                                query_ts
                            );
                        }
                        Err(AccessMethodError::KeyNotFound) => {
                            panic!(
                                "Expected version [start={},end={}) for pkey='{}' at ts={}, got KeyNotFound",
                                start, end, String::from_utf8_lossy(pkey), query_ts
                            );
                        }
                        Err(e) => {
                            panic!(
                                "Unexpected error for pkey='{}' at ts={}: {:?}",
                                String::from_utf8_lossy(pkey),
                                query_ts,
                                e
                            );
                        }
                    }
                } else {
                    // outside the interval => KeyNotFound is correct
                    if let Ok(res) = bucket.get(pkey, &query_ts) {
                        panic!(
                            "Got version unexpectedly for pkey='{}' at ts={}, interval=[{},{}). [start={},end={},value_len={}]",
                            String::from_utf8_lossy(pkey),
                            query_ts,
                            start, end,
                            res.start_ts(), res.end_ts(), res.value().len()
                        );
                    }
                }
            }
        }

        println!(
            "Test completed: inserted {} keys, each updated {} times, all verified!",
            num_keys, num_updates_per_key
        );
    }

    #[ignore = "failed"]
    #[test]
    fn test_second_bucket_multi_thread_precreated() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use std::thread;

        //
        // 1) Setup: create mem pool, container key, SecondBucket
        //
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let bucket = Arc::new(SecondBucket::new(c_key, mem_pool));

        // We define how many keys, how many updates per key
        let num_keys = 500;
        let num_updates_per_key = 5;
        let key_size = 50;
        let pkey_size = 100;
        let initial_value_size = 1000;
        let update_value_growth = 100;

        //
        // 2) Precreate all insert entries
        //
        let mut insert_entries = Vec::new();
        for i in 0..num_keys {
            // key
            let key = {
                let mut s = format!("key-{:03}", i);
                while s.len() < key_size {
                    s.push('K');
                }
                s.into_bytes()
            };
            // pkey
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            // value
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < initial_value_size {
                value.push(b'V');
            }

            let start_ts = 100 + i as u64;
            let end_ts = u64::MAX;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            insert_entries.push(entry);
        }

        //
        // 3) Precreate all update entries
        //    We'll store them in a vector of (pkey, old_start_ts, new_entry).
        //    We'll do `num_updates_per_key` updates for each key.
        //
        let mut update_entries = Vec::new();
        for i in 0..num_keys {
            let pkey = {
                let mut s = format!("pkey-{:03}", i);
                while s.len() < pkey_size {
                    s.push('P');
                }
                s.into_bytes()
            };
            // The old start for the initial version is 100 + i
            let mut current_start = 100 + i as u64;
            for u in 0..num_updates_per_key {
                let new_start = current_start + 10 + (u as u64) * 10;
                // build bigger value
                let mut new_value = format!("updated-{}-{:03}", i, u).into_bytes();
                let target_len = initial_value_size + (u + 1) * update_value_growth;
                while new_value.len() < target_len {
                    new_value.push(b'U');
                }

                // rebuild the same key we used in the insert
                let key = {
                    let mut s = format!("key-{:03}", i);
                    while s.len() < key_size {
                        s.push('K');
                    }
                    s.into_bytes()
                };

                let new_entry = MvccEntry::new(key, pkey.clone(), new_value, new_start, u64::MAX);

                update_entries.push((pkey.clone(), current_start, new_entry.clone()));

                // next iteration
                current_start = new_start;
            }
        }

        //
        // 4) We'll maintain a global reference state: pkey -> Vec<MvccEntry>
        //    We'll wrap it in Arc<Mutex<...>> for multi-threaded usage.
        //
        let ref_state: Arc<Mutex<HashMap<Vec<u8>, Vec<MvccEntry>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        //
        // 5) Multi-threaded Insert
        //    We'll partition the insert_entries among threads
        //
        let num_insert_threads = 1;
        let chunk_size = (insert_entries.len() + num_insert_threads - 1) / num_insert_threads;

        let mut insert_handles = Vec::new();
        for t_id in 0..num_insert_threads {
            let bucket_clone = bucket.clone();
            let ref_state_clone = ref_state.clone();
            let start_idx = t_id * chunk_size;
            let end_idx = std::cmp::min(start_idx + chunk_size, insert_entries.len());

            let entries_slice = insert_entries[start_idx..end_idx].to_vec();

            let handle = thread::spawn(move || {
                for entry in entries_slice {
                    bucket_clone.insert(&entry).expect("Insert failed");
                    let pkey = entry.pkey().to_vec();
                    // store in reference
                    let mut guard = ref_state_clone.lock().unwrap();
                    guard.insert(pkey, vec![entry.clone()]);
                }
            });
            insert_handles.push(handle);
        }

        for h in insert_handles {
            h.join().unwrap();
        }

        println!("Bucket stat after multi-thread inserts:\n{}", bucket.stat());

        //
        // 6) Multi-threaded Updates
        //    We'll do the same partition logic for update_entries
        //
        let num_update_threads = 1;
        let chunk_size_updates =
            (update_entries.len() + num_update_threads - 1) / num_update_threads;

        let mut update_handles = Vec::new();
        for t_id in 0..num_update_threads {
            let bucket_clone = bucket.clone();
            let ref_state_clone = ref_state.clone();
            let start_idx = t_id * chunk_size_updates;
            let end_idx = std::cmp::min(start_idx + chunk_size_updates, update_entries.len());

            let updates_slice = update_entries[start_idx..end_idx].to_vec();

            let handle = thread::spawn(move || {
                for (pkey, old_start_ts, new_entry) in updates_slice {
                    // call update
                    if bucket_clone.update(&pkey, &new_entry).is_ok() {
                        // fix old version in ref_state
                        let mut guard = ref_state_clone.lock().unwrap();
                        let versions = guard.get_mut(&pkey).unwrap();
                        // find the version with start_ts == old_start_ts and end_ts==MAX
                        let idx = versions
                            .iter()
                            .rposition(|v| v.start_ts() == old_start_ts && v.end_ts() == u64::MAX)
                            .expect("No matching old version in ref_state");
                        let mut old = versions.remove(idx);
                        old.set_end_ts(&new_entry.start_ts());
                        versions.insert(idx, old);
                        // push the new one
                        versions.push(new_entry.clone());
                    }
                }
            });
            update_handles.push(handle);
        }

        for h in update_handles {
            h.join().unwrap();
        }

        println!("Bucket stat after multi-thread updates:\n{}", bucket.stat());

        //
        // 7) Final single-thread verification
        //    For each version in ref_state, pick a query_ts in [start, end)
        //
        {
            let guard = ref_state.lock().unwrap();
            for (pkey, versions) in guard.iter() {
                for version in versions {
                    let start = version.start_ts();
                    let end = version.end_ts();
                    if start >= end {
                        continue; // skip empty
                    }
                    // pick a query_ts
                    let query_ts = if end == u64::MAX {
                        start
                    } else if end == start + 1 {
                        start
                    } else {
                        let adj_end = end.saturating_sub(1);
                        start + (adj_end - start) / 2
                    };

                    if query_ts >= start && query_ts < end {
                        // we expect a version
                        let fetched = bucket.get(pkey, &query_ts);
                        match fetched {
                            Ok(res) => {
                                assert_eq!(
                                    res.start_ts(),
                                    start,
                                    "start_ts mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                                assert_eq!(
                                    res.end_ts(),
                                    end,
                                    "end_ts mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                                assert_eq!(
                                    res.value(),
                                    version.value(),
                                    "value mismatch for pkey='{}', query_ts={}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts
                                );
                            }
                            Err(AccessMethodError::KeyNotFound) => {
                                panic!(
                                    "Expected version [start={},end={}) for pkey='{}' at ts={}, got KeyNotFound",
                                    start, end, String::from_utf8_lossy(pkey), query_ts
                                );
                            }
                            Err(e) => {
                                panic!(
                                    "Unexpected error for pkey='{}' at ts={}: {:?}",
                                    String::from_utf8_lossy(pkey),
                                    query_ts,
                                    e
                                );
                            }
                        }
                    } else {
                        // outside [start, end) => we expect KeyNotFound
                        if let Ok(res) = bucket.get(pkey, &query_ts) {
                            panic!(
                                "Got a version unexpectedly for pkey='{}' at ts={}, interval=[{},{}). Found [start={},end={},value_len={}]",
                                String::from_utf8_lossy(pkey), query_ts, start, end,
                                res.start_ts(), res.end_ts(), res.value().len()
                            );
                        }
                    }
                }
            }
        }

        println!(
            "Multi-thread test completed: {} keys inserted, each updated multiple times",
            num_keys
        );
    }
}
