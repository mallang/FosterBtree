use dashmap::mapref::entry;

use super::{
    chained_hash_bucket_second::SecondBucket, chained_hash_history_chain::ChainedHashHistoryChain,
    chained_hash_recent_chain::ChainedHashRecentChain,
    Timestamp,
};

use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, MemPoolStatus, PageFrameKey},
    log_warn,
    mvcc_index::{MvccEntry, TxId},
    page::PageId,
    prelude::AccessMethodError,
    mvcc_index::hash_join_page::ChainedHashMetaPage,
};

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};

pub const DEAFAULT_SECOND_BUCKET_NUM: usize = 1;

pub struct FirstBucket<T: MemPool> {
    c_key: ContainerKey,
    mem_pool: Arc<T>,

    meta_page_id: PageId,
    meta_frame_id: AtomicU32,

    bucket_count: usize,
    bucket_entries: Vec<Arc<SecondBucket<T>>>,
}

impl<T: MemPool> FirstBucket<T> {
    /// Creates a new hash join table with the default number of buckets.
    pub fn new(c_key: ContainerKey, mem_pool: Arc<T>) -> Self {
        Self::new_with_bucket_num(c_key, mem_pool, DEAFAULT_SECOND_BUCKET_NUM)
    }

    /// Creates a new hash join table with a specified number of buckets.
    pub fn new_with_bucket_num(c_key: ContainerKey, mem_pool: Arc<T>, num_buckets: usize) -> Self {
        // TODO: (JUN) Need to do something with the meta page
        let mut meta_page = mem_pool.create_new_page_for_write(c_key).unwrap();
        let meta_page_id = meta_page.get_id();
        let meta_frame_id = AtomicU32::new(meta_page.frame_id());
        ChainedHashMetaPage::init(&mut *meta_page, num_buckets);
        ChainedHashMetaPage::set_bucket_num(&mut *meta_page, num_buckets);

        let mut bucket_entries: Vec<Arc<SecondBucket<T>>> = Vec::with_capacity(num_buckets);
        for i in 0..num_buckets {
            let bucket_entry = SecondBucket::new(c_key, mem_pool.clone());
            // MvccHashJoinMetaPage::set_bucket_entry(&mut *meta_page, i, &entry);
            bucket_entries.push(Arc::new(bucket_entry));
        }
        drop(meta_page);

        Self {
            mem_pool,
            c_key,
            meta_page_id,
            meta_frame_id,
            bucket_count: num_buckets,
            bucket_entries,
        }
    }

    pub fn insert(&self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(entry.pkey());
        let bucket = &self.bucket_entries[index];

        bucket.insert(entry)
    }

    pub fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.get(pkey, ts)
    }

    pub fn update(&self, pkey: &[u8], entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.update(pkey, entry)
    }

    pub fn delete(&self, pkey: &[u8], ts: &Timestamp) -> Result<(), AccessMethodError> {
        let index = self.get_bucket_index(pkey);
        let bucket = &self.bucket_entries[index];

        bucket.delete(pkey, ts)
    }

    pub fn garbage_collect(&self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        for bucket in &self.bucket_entries {
            bucket.garbage_collect(ts)?;
        }
        Ok(())
    }

    fn get_bucket_index(&self, key: &[u8]) -> usize {
        if self.bucket_count == 1 {
            return 0;
        }
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.bucket_count
    }

    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub fn bucket_entries(&self, idx: usize) -> &Arc<SecondBucket<T>> {
        &self.bucket_entries[idx]
    }

    pub fn recent_chain(&self, idx: usize) -> &Arc<ChainedHashRecentChain<T>> {
        self.bucket_entries[idx].recent_chain()
    }

    pub fn history_chain(&self, idx: usize) -> &Arc<ChainedHashHistoryChain<T>> {
        self.bucket_entries[idx].history_chain()
    }

    /// Returns a human‑readable status string for the FirstBucket.
    /// It prints the stat() for each SecondBucket and then a summary for both the recent and history chains.
    pub fn stat(&self) -> String {
        let mut stat_str = String::new();
        stat_str.push_str("=== FirstBucket Stats ===\n\n");

        // These accumulators hold overall metrics for recent and history chains.
        let mut recent_total_pages = 0;
        let mut recent_total_kvs = 0;
        let mut recent_usage_sum = 0.0;
        let mut recent_max_usage: f64 = 0.0;
        let mut recent_min_usage = f64::MAX;

        let mut history_total_pages = 0;
        let mut history_total_kvs = 0;
        let mut history_usage_sum = 0.0;
        let mut history_max_usage: f64 = 0.0;
        let mut history_min_usage = f64::MAX;

        // Iterate over each SecondBucket.
        for (i, bucket) in self.bucket_entries.iter().enumerate() {
            // stat_str.push_str(&format!("--- SecondBucket {} ---\n", i));
            // stat_str.push_str(&bucket.stat());
            // stat_str.push('\n');

            // Recent chain summary from this SecondBucket.
            let (r_pages, r_kvs, r_usage, r_max, r_min) = bucket.recent_chain().summary_metrics();
            recent_total_pages += r_pages;
            recent_total_kvs += r_kvs;
            recent_usage_sum += r_usage;
            if r_pages > 0 {
                recent_max_usage = recent_max_usage.max(r_max);
                recent_min_usage = recent_min_usage.min(r_min);
            }

            // History chain summary from this SecondBucket.
            let (h_pages, h_kvs, h_usage, h_max, h_min) = bucket.history_chain().summary_metrics();
            history_total_pages += h_pages;
            history_total_kvs += h_kvs;
            history_usage_sum += h_usage;
            if h_pages > 0 {
                history_max_usage = history_max_usage.max(h_max);
                history_min_usage = history_min_usage.min(h_min);
            }
        }

        // Compute average usage per page if any pages exist.
        let recent_avg_usage = if recent_total_pages > 0 {
            recent_usage_sum / recent_total_pages as f64
        } else {
            0.0
        };
        let history_avg_usage = if history_total_pages > 0 {
            history_usage_sum / history_total_pages as f64
        } else {
            0.0
        };

        // Append the overall summary.
        stat_str.push_str("\n=== Summary ===\n");
        stat_str.push_str(&format!(
            "Recent Chain: {} pages, {} kv count, avg usage: {:.2}%, max usage: {:.2}%, min usage: {:.2}%\n",
            recent_total_pages, recent_total_kvs, recent_avg_usage, recent_max_usage, recent_min_usage
        ));
        stat_str.push_str(&format!(
            "History Chain: {} pages, {} kv count, avg usage: {:.2}%, max usage: {:.2}%, min usage: {:.2}%\n",
            history_total_pages, history_total_kvs, history_avg_usage, history_max_usage, history_min_usage
        ));

        stat_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bp::{get_in_mem_pool, ContainerKey},
        mvcc_index::MvccEntry,
    };

    // Helper function to generate a fixed number of MVCC entries with given sizes.
    // Each entry has:
    //   - key of fixed length `key_size` (padded with 'K'),
    //   - primary key of fixed length `pkey_size` (padded with 'P'),
    //   - value of fixed length `value_size` (padded with 'V'),
    //   - start_ts starting at `start_base` (incremented by one per entry),
    //   - end_ts = u64::MAX.
    fn generate_fixed_mvcc_entries(
        num: usize,
        key_size: usize,
        pkey_size: usize,
        value_size: usize,
        start_base: u64,
    ) -> Vec<MvccEntry> {
        let mut entries = Vec::with_capacity(num);
        for i in 0..num {
            // Build a key of exactly `key_size` bytes.
            let mut key = format!("key-{:03}", i);
            while key.len() < key_size {
                key.push('K');
            }
            let key_bytes = key.into_bytes();

            // Build a primary key of exactly `pkey_size` bytes.
            let mut pkey = format!("pkey-{:03}", i);
            while pkey.len() < pkey_size {
                pkey.push('P');
            }
            let pkey_bytes = pkey.into_bytes();

            // Build a value of exactly `value_size` bytes.
            let mut value = format!("value-{:03}", i).into_bytes();
            while value.len() < value_size {
                value.push(b'V');
            }

            let start_ts = start_base + i as u64;
            let end_ts = u64::MAX; // recent entry, valid from start_ts to infinity (half‑open: [start_ts, u64::MAX))
            let entry = MvccEntry::new(key_bytes, pkey_bytes, value, start_ts, end_ts);
            entries.push(entry);
        }
        entries
    }

    #[test]
    fn test_first_bucket_insert_get_fixed() {
        // 1. Set up mem pool, container key, and instantiate FirstBucket.
        //    (Adjust get_in_mem_pool() and ContainerKey as needed.)
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let first_bucket = FirstBucket::new(c_key, mem_pool);

        // 2. Generate a fixed set of MVCC entries.
        let num_entries = 1000;
        let key_size = 50;
        let pkey_size = 100;
        let value_size = 1000;
        let start_base = 100; // starting timestamps will be 100, 101, 102, ...
        let entries =
            generate_fixed_mvcc_entries(num_entries, key_size, pkey_size, value_size, start_base);

        // 3. Insert each generated entry into the FirstBucket.
        for entry in &entries {
            first_bucket.insert(entry).expect("Insert failed");
        }

        // Optionally print the overall bucket stat.
        println!("FirstBucket stat after inserts:\n{}", first_bucket.stat());

        // 4. For each inserted entry, perform a get() using a query timestamp within the valid interval.
        //    Since each inserted version is [start_ts, u64::MAX), we can safely use entry.start_ts() as a query.
        for entry in &entries {
            let query_ts = entry.start_ts();
            let fetched = first_bucket
                .get(entry.pkey(), &query_ts)
                .expect("Get failed");
            assert_eq!(
                fetched.key(),
                entry.key(),
                "Key mismatch for pkey='{}'",
                String::from_utf8_lossy(entry.pkey())
            );
            assert_eq!(
                fetched.pkey(),
                entry.pkey(),
                "PKey mismatch for pkey='{}'",
                String::from_utf8_lossy(entry.pkey())
            );
            assert_eq!(
                fetched.value(),
                entry.value(),
                "Value mismatch for pkey='{}'",
                String::from_utf8_lossy(entry.pkey())
            );
            assert_eq!(
                fetched.start_ts(),
                entry.start_ts(),
                "start_ts mismatch for pkey='{}'",
                String::from_utf8_lossy(entry.pkey())
            );
            assert_eq!(
                fetched.end_ts(),
                entry.end_ts(),
                "end_ts mismatch for pkey='{}'",
                String::from_utf8_lossy(entry.pkey())
            );
        }

        println!(
            "Test passed: All {} entries inserted and retrieved successfully.",
            num_entries
        );
    }

    #[test]
    fn test_first_bucket_single_thread_insert_update_get() {
        use std::collections::HashMap;

        // -------------------------------------------------------------------
        // Helper: Build a fixed-length string
        // -------------------------------------------------------------------
        fn fixed_length_str(base: &str, target_len: usize, pad: char) -> String {
            let mut s = base.to_string();
            while s.len() < target_len {
                s.push(pad);
            }
            s.truncate(target_len);
            s
        }

        // -------------------------------------------------------------------
        // 1) Set up mem pool, container key, and create a FirstBucket.
        // Assume get_in_mem_pool() and ContainerKey are available.
        // -------------------------------------------------------------------
        let mem_pool = get_in_mem_pool();
        let c_key = ContainerKey::new(0, 0);
        let first_bucket = FirstBucket::new(c_key, mem_pool);

        // -------------------------------------------------------------------
        // 2) Create a reference state to record all versions.
        //    Map from primary key (Vec<u8>) to a vector of versions (MvccEntry)
        //    The last element in the vector should be the “recent” version (with end_ts == u64::MAX).
        // -------------------------------------------------------------------
        let mut ref_state: HashMap<Vec<u8>, Vec<MvccEntry>> = HashMap::new();

        // -------------------------------------------------------------------
        // 3) Insert initial entries.
        //    Each entry has key size = 50, pkey size = 100, value size = 1000.
        //    start_ts for key i is (start_base + i) with end_ts = u64::MAX.
        // -------------------------------------------------------------------
        let num_keys = 1000;
        let key_size = 50;
        let pkey_size = 100;
        let value_size = 1000;
        let start_base = 100;

        for i in 0..num_keys {
            let key = fixed_length_str(&format!("key-{:03}", i), key_size, 'K').into_bytes();
            let pkey = fixed_length_str(&format!("pkey-{:03}", i), pkey_size, 'P').into_bytes();
            let value = fixed_length_str(&format!("value-{:03}", i), value_size, 'V').into_bytes();
            let start_ts = start_base + i as u64;
            let end_ts = u64::MAX;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            // Insert into FirstBucket
            first_bucket.insert(&entry).expect("Initial insert failed");
            // Record in our reference state
            ref_state.insert(pkey, vec![entry]);
        }

        println!("FirstBucket stat after inserts:\n{}", first_bucket.stat());

        // -------------------------------------------------------------------
        // 4) Update step: For each key, perform a number of updates.
        //    Each update creates a new recent version and fixes the old version's end_ts.
        //    In this example, we perform num_updates updates per key.
        //    Each update enlarges the value by 1000 bytes.
        // -------------------------------------------------------------------
        let num_updates = 5;
        for (pkey, versions) in ref_state.iter_mut() {
            for u in 0..num_updates {
                // Get the current recent version (should have end_ts == u64::MAX)
                let old_recent = versions.last().unwrap().clone();
                if old_recent.end_ts() != u64::MAX {
                    panic!(
                        "No recent version for key '{}'",
                        String::from_utf8_lossy(pkey)
                    );
                }
                // Choose a new start_ts > old_recent.start_ts.
                let new_start = old_recent.start_ts() + 10 + (u as u64) * 10;
                // Build an updated value by extending the old value by 1000 'U' characters.
                let mut new_value = old_recent.value().to_vec().clone();
                for _ in 0..100 {
                    new_value.push(b'U');
                }
                let updated_entry = MvccEntry::new(
                    old_recent.key().to_vec().clone(),
                    old_recent.pkey().to_vec().clone(),
                    new_value,
                    new_start,
                    u64::MAX,
                );
                // Call update on the FirstBucket.
                first_bucket
                    .update(&old_recent.pkey(), &updated_entry)
                    .expect("Update failed");
                // Update the reference state:
                // Remove the old recent version, set its end_ts to new_start,
                // then push the new version.
                let mut old_version = versions.pop().unwrap();
                old_version.set_end_ts(&new_start);
                versions.push(old_version);
                versions.push(updated_entry);
            }
        }

        println!("FirstBucket stat after updates:\n{}", first_bucket.stat());

        // -------------------------------------------------------------------
        // 5) Final verification:
        // For each version in our reference state, pick a query timestamp in [start, end)
        // and verify that first_bucket.get(pkey, query_ts) returns the expected version.
        // For half‑open intervals:
        //   - If end == u64::MAX or the interval length is 1 (i.e. end == start + 1), we pick start.
        //   - Otherwise, we choose the midpoint in [start, end - 1].
        // -------------------------------------------------------------------
        for (pkey, versions) in ref_state.iter() {
            for version in versions {
                let start = version.start_ts();
                let end = version.end_ts();
                if start >= end {
                    continue; // skip degenerate intervals
                }
                let query_ts = if end == u64::MAX || end == start + 1 {
                    start
                } else {
                    let adjusted_end = end.saturating_sub(1);
                    start + (adjusted_end - start) / 2
                };
                let fetched = first_bucket.get(pkey, &query_ts).expect(&format!(
                    "Get failed for key '{}' at ts={}",
                    String::from_utf8_lossy(pkey),
                    query_ts
                ));
                assert_eq!(
                    fetched.start_ts(),
                    start,
                    "start_ts mismatch for key '{}'",
                    String::from_utf8_lossy(pkey)
                );
                assert_eq!(
                    fetched.end_ts(),
                    end,
                    "end_ts mismatch for key '{}'",
                    String::from_utf8_lossy(pkey)
                );
                assert_eq!(
                    fetched.value(),
                    version.value(),
                    "value mismatch for key '{}'",
                    String::from_utf8_lossy(pkey)
                );
            }
        }

        println!(
            "Test passed: All {} keys inserted, updated, and verified successfully.",
            num_keys
        );
    }
}
