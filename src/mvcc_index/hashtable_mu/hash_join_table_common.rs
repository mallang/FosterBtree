use std::sync::Arc;

use crate::{
    bp::MemPool,
    mvcc_index::{MvccEntry, Timestamp},
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

pub(crate) const PAGE_ID_SIZE: usize = std::mem::size_of::<PageId>();
pub(crate) const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>();
pub(crate) const BUCKET_ENTRY_SIZE: usize = PAGE_ID_SIZE;
pub(crate) const DEFAULT_NUM_BUCKETS: usize = 256;

mod access_err {
    use core::fmt;

    use crate::bp::MemPoolStatus;

    #[derive(Debug, PartialEq)]
    pub enum HashTableAccessMethodError {
        KeyNotFound,
        KeyFoundButInvalidTimestamp, // For MVCC
        PageReadLatchFailed,
        PageWriteLatchFailed,
        RecordTooLarge,
        MemPoolStatus(MemPoolStatus),
        OutOfSpace,              // For ReadOptimizedPage
        HashPageOutOfSpace(u32), // new_hash_size
        AcquireLockFailed,       // for multi-page acq
        Other(String),
    }

    impl fmt::Display for HashTableAccessMethodError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                HashTableAccessMethodError::KeyNotFound => write!(f, "Key not found"),
                HashTableAccessMethodError::KeyFoundButInvalidTimestamp => {
                    write!(f, "Key found but invalid timestamp")
                }
                HashTableAccessMethodError::PageReadLatchFailed => {
                    write!(f, "Page read latch failed")
                }
                HashTableAccessMethodError::PageWriteLatchFailed => {
                    write!(f, "Page write latch failed")
                }
                HashTableAccessMethodError::RecordTooLarge => write!(f, "Record too large"),
                HashTableAccessMethodError::MemPoolStatus(status) => {
                    write!(f, "MemPool status: {:?}", status)
                }
                HashTableAccessMethodError::OutOfSpace => write!(f, "Out of space"),
                HashTableAccessMethodError::Other(msg) => write!(f, "{}", msg),
                HashTableAccessMethodError::HashPageOutOfSpace(u32) => {
                    write!(f, "iterate failed!")
                }
                HashTableAccessMethodError::AcquireLockFailed => {
                    write!(f, "acquire page lock failed")
                }
            }
        }
    }

    impl std::error::Error for HashTableAccessMethodError {}
}

pub use access_err::HashTableAccessMethodError;

/*
    <Recent Bucket Num> <History Bucket Num> [Recent Page Id ...] [History Page Id...]

*/
pub(crate) trait MvccHashJoinMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);
    fn set_history_bucket_num(&mut self, num_buckets: usize);
    fn set_recent_bucket_num(&mut self, num_buckets: usize);
    fn get_recent_bucket_num(&self) -> usize;
    fn get_history_bucket_num(&self) -> usize;

    fn get_recent_bucket_entry(&self, index: usize) -> PageId;
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId);
    fn get_history_bucket_entry(&self, index: usize) -> PageId;
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId);

    fn read_all_entries_recent(&self) -> Vec<PageId>;
    fn write_all_entries_recent(&mut self, entries: &[PageId]);

    fn read_all_entries_history(&self) -> Vec<PageId>;
    fn write_all_entries_history(&mut self, entries: &[PageId]);

    fn rehash_update_recent(&mut self, entries: &[PageId]);
    fn rehash_update_history(&mut self, entries: &[PageId]);
}

impl MvccHashJoinMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE * 2 + (num_buckets * BUCKET_ENTRY_SIZE) * 2;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "Page size is insufficient for the number of buckets",
        );
        self.set_recent_bucket_num(num_buckets);
        self.set_history_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn set_recent_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn set_history_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }

    fn get_recent_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_history_bucket_num(&self) -> usize {
        let bytes = &self[BUCKET_NUM_SIZE..BUCKET_NUM_SIZE * 2];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn get_recent_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let recent_pid = PageId::from_be_bytes(bytes[0..PAGE_ID_SIZE].try_into().unwrap());

        recent_pid
    }
    fn set_recent_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        assert!(index < recent_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE) + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE].copy_from_slice(&entry.to_be_bytes());
    }
    fn get_history_bucket_entry(&self, index: usize) -> PageId {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE)
            + recent_num_buckets * BUCKET_ENTRY_SIZE
            + index * BUCKET_ENTRY_SIZE;
        let bytes = &self[offset..offset + BUCKET_ENTRY_SIZE];

        let history_pid = PageId::from_be_bytes(bytes[0..PAGE_ID_SIZE].try_into().unwrap());

        history_pid
    }
    fn set_history_bucket_entry(&mut self, index: usize, entry: &PageId) {
        let recent_num_buckets = self.get_recent_bucket_num();
        let history_num_buckets = self.get_history_bucket_num();
        assert!(index < history_num_buckets, "Bucket index out of bounds");

        let offset = (2 * BUCKET_NUM_SIZE)
            + recent_num_buckets * BUCKET_ENTRY_SIZE
            + index * BUCKET_ENTRY_SIZE;
        let bytes = &mut self[offset..offset + BUCKET_ENTRY_SIZE];

        bytes[0..PAGE_ID_SIZE].copy_from_slice(&entry.to_be_bytes());
    }

    fn read_all_entries_recent(&self) -> Vec<PageId> {
        let num_buckets = self.get_recent_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_recent_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_recent(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_recent_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_recent_bucket_entry(index, entry);
        }
    }

    fn read_all_entries_history(&self) -> Vec<PageId> {
        let num_buckets = self.get_history_bucket_num();
        let mut entries = Vec::with_capacity(num_buckets);
        for index in 0..num_buckets {
            entries.push(self.get_history_bucket_entry(index));
        }
        entries
    }

    fn write_all_entries_history(&mut self, entries: &[PageId]) {
        let num_buckets = self.get_history_bucket_num();
        assert!(
            entries.len() == num_buckets,
            "Number of entries does not match number of buckets"
        );
        for (index, entry) in entries.iter().enumerate() {
            self.set_history_bucket_entry(index, entry);
        }
    }

    fn rehash_update_history(&mut self, entries: &[PageId]) {
        // rehash into meta page
        let old_history_entries_num = self.get_history_bucket_num();
        if old_history_entries_num < entries.len() {
            assert_eq!(old_history_entries_num * 2, entries.len());
            self.set_history_bucket_num(old_history_entries_num * 2);
            self.write_all_entries_history(&entries);
        }
    }

    fn rehash_update_recent(&mut self, entries: &[PageId]) {
        // rehash into meta page
        let old_recent_entries_num = self.get_recent_bucket_num();
        if old_recent_entries_num < entries.len() {
            assert_eq!(old_recent_entries_num * 2, entries.len());
            self.set_recent_bucket_num(old_recent_entries_num * 2);
            self.write_all_entries_recent(&entries);
        }
    }
}
