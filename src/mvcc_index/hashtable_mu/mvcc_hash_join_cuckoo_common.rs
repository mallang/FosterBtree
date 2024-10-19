use core::fmt;

use crate::bp::MemPoolStatus;

#[derive(Debug, PartialEq)]
pub enum CuckooAccessMethodError {
    KeyNotFound,
    KeyFoundButInvalidTimestamp, // For MVCC
    KeyDuplicate,
    KeyNotInPageRange, // For Btree
    PageReadLatchFailed,
    PageWriteLatchFailed,
    RecordTooLarge,
    MemPoolStatus(MemPoolStatus),
    OutOfSpace, // For ReadOptimizedPage
    OutOfSpaceForUpdate(Vec<u8>),
    NeedToUpdateMVCC(u64, Vec<u8>), // For MVCC
    InvalidTimestamp, // For MVCC
    CuckooIterateFailed(u32), // new_hash_size
    Other(String),
}

impl fmt::Display for CuckooAccessMethodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CuckooAccessMethodError::KeyNotFound => write!(f, "Key not found"),
            CuckooAccessMethodError::KeyFoundButInvalidTimestamp => write!(f, "Key found but invalid timestamp"),
            CuckooAccessMethodError::KeyDuplicate => write!(f, "Key duplicate"),
            CuckooAccessMethodError::KeyNotInPageRange => write!(f, "Key not in page range"),
            CuckooAccessMethodError::PageReadLatchFailed => write!(f, "Page read latch failed"),
            CuckooAccessMethodError::PageWriteLatchFailed => write!(f, "Page write latch failed"),
            CuckooAccessMethodError::RecordTooLarge => write!(f, "Record too large"),
            CuckooAccessMethodError::MemPoolStatus(status) => write!(f, "MemPool status: {:?}", status),
            CuckooAccessMethodError::OutOfSpace => write!(f, "Out of space"),
            CuckooAccessMethodError::OutOfSpaceForUpdate(key) => write!(f, "Out of space for update: {:?}", key),
            CuckooAccessMethodError::NeedToUpdateMVCC(ts, val) => write!(f, "Need to update MVCC: ts: {}, val: {:?}", ts, val),
            CuckooAccessMethodError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            CuckooAccessMethodError::Other(msg) => write!(f, "{}", msg),
            CuckooAccessMethodError::CuckooIterateFailed(u32) => write!(f, "cuckoo iterate failed!"),
        }
    }
}

impl std::error::Error for CuckooAccessMethodError {}