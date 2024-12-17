mod header {
    use crate::page::{AVAILABLE_PAGE_SIZE, PAGE_SIZE};

    use super::SLOT_SIZE;
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();
    const BASE_HEADER_SIZE: usize = PAGE_SIZE - AVAILABLE_PAGE_SIZE;
    const _: () = assert!(SLOT_SIZE >= PAGE_HEADER_SIZE + BASE_HEADER_SIZE);
    pub const PAGE_HEADER_SIZE_ALIGNED: usize = SLOT_SIZE - BASE_HEADER_SIZE;
    #[derive(Debug)]
    pub struct Header {
        total_bytes_used: u32,
        slot_count: u32,
        rec_start_offset: u32,
    }
    impl Header {
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < PAGE_HEADER_SIZE {
                return Err("Insufficient bytes to form Header".into());
            }

            let mut current_pos = 0;

            let total_bytes_used = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse total_bytes_used")?,
            );
            current_pos += 4;

            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse slot_count")?,
            );
            current_pos += 4;

            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse rec_start_offset")?,
            );

            Ok(Self {
                total_bytes_used,
                slot_count,
                rec_start_offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.rec_start_offset.to_be_bytes());

            bytes
        }

        pub fn new() -> Self {
            Self {
                total_bytes_used: PAGE_HEADER_SIZE_ALIGNED as u32,
                slot_count: 0,
                rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
            }
        }
        pub fn total_bytes_used(&self) -> u32 {
            self.total_bytes_used
        }
        pub fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
            self.total_bytes_used = total_bytes_used;
        }

        pub fn slot_count(&self) -> u32 {
            self.slot_count
        }
        pub fn set_slot_count(&mut self, slot_count: u32) {
            self.slot_count = slot_count;
        }

        pub fn rec_start_offset(&self) -> u32 {
            self.rec_start_offset
        }
        pub fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
            self.rec_start_offset = rec_start_offset;
        }

        pub fn decrement_slot_count(&mut self) {
            self.slot_count -= 1;
        }

        pub fn increment_slot_count(&mut self) {
            self.slot_count += 1;
        }

        pub fn increase_total_bytes_used(&mut self, delta: u32) {
            self.total_bytes_used = self.total_bytes_used + delta;
        }

        pub fn decrease_total_bytes_used(&mut self, delta: u32) {
            self.total_bytes_used = self.total_bytes_used - delta;
        }

        pub fn increase_rec_offset(&mut self, delta: u32) {
            self.rec_start_offset += delta;
        }

        pub fn decrease_rec_offset(&mut self, delta: u32) {
            self.rec_start_offset -= delta;
        }

        pub fn slot_end_offset(&self) -> usize {
            (self.slot_count() as u32 * SLOT_SIZE as u32 + PAGE_HEADER_SIZE_ALIGNED as u32) as usize
        }
    }
}

use core::slice;
use std::cmp::{self, Ordering};

use header::*;

mod slot {
    use crate::mvcc_index::Timestamp;

    pub const DELETE_MARKER_IN_VAL_SIZE: u32 = u32::MAX;
    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq, Default)]
    pub struct Slot {
        // hash key for join
        key_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        // primary key for row
        pkey_size: u32,
        pkey_prefix: [u8; SLOT_PKEY_PREFIX_SIZE],
        // timestamp
        start_ts: Timestamp,
        end_ts: Timestamp,
        // value
        val_size: u32,
        offset: u32,
    }

    impl Slot {
        pub fn is_mark_deleted(&self) -> bool {
            self.val_size == DELETE_MARKER_IN_VAL_SIZE
        }

        pub fn mark_deleted(&mut self) {
            self.val_size = DELETE_MARKER_IN_VAL_SIZE
        }
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < SLOT_SIZE {
                return Err("Insufficient bytes to form Slot".into());
            }

            let mut current_pos = 0;

            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse key_size")?,
            );
            current_pos += 4;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            key_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            let pkey_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse pkey_size")?,
            );
            current_pos += 4;

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            pkey_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            let start_ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let end_ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse val_size")?,
            );
            current_pos += 4;

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse offset")?,
            );

            Ok(Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                start_ts,
                end_ts,
                val_size,
                offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0u8; SLOT_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]
                .copy_from_slice(&self.key_prefix);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.pkey_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]
                .copy_from_slice(&self.pkey_prefix);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.start_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.end_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(
            key: &[u8],
            pkey: &[u8],
            start_ts: Timestamp,
            end_ts: Timestamp,
            val: &[u8],
            offset: usize,
        ) -> Self {
            let key_size = key.len() as u32;
            let pkey_size = pkey.len() as u32;
            let val_size = val.len() as u32;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            key_prefix[..key_prefix_len].copy_from_slice(&key[..key_prefix_len]);

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());
            pkey_prefix[..pkey_prefix_len].copy_from_slice(&pkey[..pkey_prefix_len]);

            Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                start_ts,
                end_ts,
                val_size,
                offset: offset as u32,
            }
        }

        pub fn key_size(&self) -> u32 {
            self.key_size
        }

        pub fn key_prefix(&self) -> &[u8] {
            if self.key_size as usize > SLOT_KEY_PREFIX_SIZE {
                &self.key_prefix
            } else {
                &self.key_prefix[..self.key_size as usize]
            }
        }

        pub fn pkey_size(&self) -> u32 {
            self.pkey_size
        }

        pub fn pkey_prefix(&self) -> &[u8] {
            if self.pkey_size as usize > SLOT_PKEY_PREFIX_SIZE {
                &self.pkey_prefix
            } else {
                &self.pkey_prefix[..self.pkey_size as usize]
            }
        }

        pub fn start_ts(&self) -> Timestamp {
            self.start_ts
        }

        pub fn set_start_ts(&mut self, ts: Timestamp) {
            self.start_ts = ts;
        }

        pub fn end_ts(&self) -> Timestamp {
            self.end_ts
        }

        pub fn set_end_ts(&mut self, ts: Timestamp) {
            self.end_ts = ts;
        }

        pub fn val_size(&self) -> u32 {
            if self.val_size == DELETE_MARKER_IN_VAL_SIZE {
                0
            } else {
                self.val_size
            }
        }

        pub fn set_val_size(&mut self, val_size: usize) {
            self.val_size = val_size as u32;
        }

        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u32) {
            self.offset = offset;
        }

        pub fn get_record_size_of_slot(&self) -> u32 {
            let key_size = self.key_size();
            let pkey_size = self.pkey_size();
            let val_size = self.val_size();

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
            val_size + remain_key_size + remain_pkey_size
        }
    }
}

use slot::*;

mod record {
    use super::slot::{SLOT_KEY_PREFIX_SIZE, SLOT_PKEY_PREFIX_SIZE};

    pub struct Record {
        remain_key: Vec<u8>,
        remain_pkey: Vec<u8>,
        val: Vec<u8>,
    }

    impl Record {
        pub fn from_bytes(bytes: &[u8], key_size: u32, pkey_size: u32, val_size: u32) -> Self {
            let key_size = key_size as usize;
            let pkey_size = pkey_size as usize;
            let val_size = val_size as usize;

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);

            let total_size = remain_key_size + remain_pkey_size + val_size;
            assert!(bytes.len() >= total_size, "Insufficient bytes for Record");

            let mut current_pos = 0;

            let remain_key = bytes[current_pos..current_pos + remain_key_size].to_vec();
            current_pos += remain_key_size;

            let remain_pkey = bytes[current_pos..current_pos + remain_pkey_size].to_vec();
            current_pos += remain_pkey_size;

            let val = bytes[current_pos..current_pos + val_size].to_vec();

            Record {
                remain_key,
                remain_pkey,
                val,
            }
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes =
                Vec::with_capacity(self.remain_key.len() + self.remain_pkey.len() + self.val.len());

            bytes.extend_from_slice(&self.remain_key);
            bytes.extend_from_slice(&self.remain_pkey);
            bytes.extend_from_slice(&self.val);

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], val: &[u8]) -> Self {
            let remain_key = if key.len() > SLOT_KEY_PREFIX_SIZE {
                key[SLOT_KEY_PREFIX_SIZE..].to_vec()
            } else {
                Vec::new()
            };

            let remain_pkey = if pkey.len() > SLOT_PKEY_PREFIX_SIZE {
                pkey[SLOT_PKEY_PREFIX_SIZE..].to_vec()
            } else {
                Vec::new()
            };

            Record {
                remain_key,
                remain_pkey,
                val: val.to_vec(),
            }
        }

        pub fn remain_key(&self) -> &[u8] {
            &self.remain_key
        }

        pub fn remain_pkey(&self) -> &[u8] {
            &self.remain_pkey
        }

        pub fn val(&self) -> &[u8] {
            &self.val
        }

        pub fn update(&mut self, new_val: &[u8]) {
            self.val = new_val.to_vec();
        }

        pub fn update_with_merge(&mut self, new_val: &[u8], merge_fn: fn(&[u8], &[u8]) -> Vec<u8>) {
            self.val = merge_fn(&self.val, new_val);
        }
    }
}
use record::*;

use crate::{
    log_debug, log_warn,
    mvcc_index::{hashtable_mu::hash_join_table_common::HashTableAccessMethodError, Timestamp},
    page::{Page, AVAILABLE_PAGE_SIZE},
};

pub trait DoubleHashPage {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), HashTableAccessMethodError>;

    /// used both in recent and history \
    /// if recent => find one matched(key, pkey) and either return OK or return Err(Ts) \
    /// if history => find if ts not match, then continue;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        is_recent_get: bool,
    ) -> Result<Vec<u8>, HashTableAccessMethodError>;

    fn recent_get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, HashTableAccessMethodError>;
    /// return the free space after compaction.
    fn compact(&mut self) -> u32;

    fn delete_slot_at_id(
        &mut self,
        slot_id: u32,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError>;

    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HashTableAccessMethodError>;

    /* -----------------only recent------------------- */
    /// calculate to find whether the record size is okay or not \
    /// if compaction is not enough -> OutOfSpace \
    /// ONLY RECENT
    fn check_and_update_at_slot_id(
        &mut self,
        slot_id: u32,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError>;

    /// return slot_id if find \
    /// Err(notfound) \
    /// Err(KeyFoundButInvalidTimestamp) \
    /// ONLY RECENT
    fn get_slot_id(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<u32, HashTableAccessMethodError>;

    /// ONLY RECENT
    fn mark_delete_slot_at_id(
        &mut self,
        slot_id: u32,
        end_ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError>;

    // ONLY RECENT
    fn get_delete_mark_slot_id(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<u32, HashTableAccessMethodError>;
    /* -----------------only history------------------- */
    /// ONLY HISTORY
    fn garbage_collect(&mut self, safe_ts: Timestamp);
    /// ONLY HISTORY and recent::delete
    fn insert_deleted(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), HashTableAccessMethodError>;

    /* ------------------helper function------------------------ */
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    fn read_bytes(&self, offset: usize, length: usize) -> &[u8];
    fn get_slot_slice(&self, slot_id: u32) -> &[Slot];
    fn set_slot(&mut self, slot_id: u32, slot: &Slot);
    fn get_slot(&self, slot_id: u32) -> Option<Slot>;

    /// find the first slot idx >= given (key, pkey, ts) \
    /// if (key,pkey) of slot is the same, then return the \
    /// first idx whose end_ts is larger than seek_ts \
    /// if no slot >=, THEN return None
    fn find_slot_idx(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Option<usize> {
        let slots_slice = self.get_slot_slice(0);

        let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
        let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

        let remain_key = if key.len() > SLOT_KEY_PREFIX_SIZE {
            &key[SLOT_KEY_PREFIX_SIZE..]
        } else {
            &[]
        };

        let remain_pkey = if pkey.len() > SLOT_PKEY_PREFIX_SIZE {
            &pkey[SLOT_PKEY_PREFIX_SIZE..]
        } else {
            &[]
        };

        let cmp_seek_key = (
            &key[..key_prefix_len],
            remain_key,
            &pkey[..pkey_prefix_len],
            remain_pkey,
            ts,
        );

        // seek:(key, pkey, ts) , slot: (key, pkey, end_ts)
        let cmp_slot_key = |seek_key: &(&[u8], &[u8], &[u8], &[u8], Timestamp),
                            slot_key: &(&[u8], &[u8], &[u8], &[u8], Timestamp)|
         -> cmp::Ordering {
            if slot_key.0 == seek_key.0 {
                if slot_key.1 == seek_key.1 {
                    if slot_key.2 == seek_key.2 {
                        if slot_key.3 == seek_key.3 {
                            if slot_key.4 <= seek_key.4 {
                                return Ordering::Less;
                            } else {
                                return Ordering::Greater;
                            }
                        } else {
                            return slot_key.3.cmp(&seek_key.3);
                        }
                    } else {
                        return slot_key.2.cmp(&seek_key.2);
                    }
                } else {
                    return slot_key.1.cmp(&seek_key.1);
                }
            } else {
                return slot_key.0.cmp(&seek_key.0);
            }
        };

        let slot_idx = slots_slice.binary_search_by(|slot| {
            // key > pkey > ts
            let (slot_remain_key, slot_remain_pkey, slot_end_ts) =
                self.get_key_pkey_end_ts_with_slot(&slot);
            let slot_key = (
                slot.key_prefix(),
                slot_remain_key,
                slot.pkey_prefix(),
                slot_remain_pkey,
                slot_end_ts,
            );
            cmp_slot_key(&cmp_seek_key, &slot_key)
        });
        assert!(slot_idx.is_err());
        let slot_idx = slot_idx.err().unwrap();

        if slot_idx >= slots_slice.len() {
            return None;
        }
        Some(slot_idx)
        // let slot = self.slot(slot_idx as u32).unwrap();
        // let (slot_key, slot_pkey, slot_val,  slot_start_ts, slot_end_ts) = self.get_key_pkey_val_ts_with_slot(&slot);
        // if slot_key == key && slot_pkey == pkey && ts >= slot_start_ts && ts < slot_end_ts {
        //     Some(slot_idx)
        // } else {
        //     None
        // }
    }

    fn header(&self) -> Header {
        let header_bytes = self.read_bytes(0, PAGE_HEADER_SIZE);
        Header::from_bytes(header_bytes).unwrap()
    }
    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self.write_bytes(0, &header_bytes);
    }

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        SLOT_SIZE as u32 + remain_key_size as u32 + remain_pkey_size as u32 + val.len() as u32
    }
    fn free_space_without_compaction(&self) -> u32 {
        let header = self.header();
        header.rec_start_offset()
            - (header.slot_count() * SLOT_SIZE as u32 + PAGE_HEADER_SIZE_ALIGNED as u32)
    }
    fn free_space_with_compaction(&self) -> u32 {
        let header = self.header();
        AVAILABLE_PAGE_SIZE as u32 - header.total_bytes_used()
    }

    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }

    fn set_slot_count(&mut self, slot_count: u32) {
        let mut header = self.header();
        header.set_slot_count(slot_count);
        self.set_header(&header);
    }

    fn swap_slot(&mut self, i: u32, j: u32) {
        if i == j {
            return;
        }
        assert!(i < j);
        let slot = self.get_slot(j).unwrap();
        self.set_slot(i, &slot);
    }

    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE_ALIGNED as u32 + slot_id as u32 * SLOT_SIZE as u32
    }
    // fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
    //     let slot_offset = self.slot_offset(slot_id);
    //     self.write_bytes(slot_offset as usize, &slot.to_bytes());
    // }

    fn write_record(&mut self, offset: u32, record: &Record) {
        let bytes = record.to_bytes();
        self.write_bytes(offset as usize, &bytes);
    }

    /// return: key, pkey, value, start_ts, end_ts \
    /// WARN: pay attention to when slot is mark deleted
    fn get_key_pkey_end_ts_with_slot<'a>(&'a self, slot: &Slot) -> (&'a [u8], &'a [u8], Timestamp) {
        let ts = slot.end_ts();

        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let pkey_size = slot.pkey_size() as usize;
        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);

        let remain_key_bytes = self.read_bytes(offset, remain_key_size);
        let remain_pkey_bytes = self.read_bytes(offset + remain_key_size, remain_pkey_size);

        (remain_key_bytes, remain_pkey_bytes, ts)
    }

    fn get_value_with_slot_id(&self, slot_id: u32) -> Vec<u8> {
        let slot = self.get_slot(slot_id).expect("Invalid slot_id");
        self.get_value_with_slot(&slot)
    }

    fn get_value_with_slot(&self, slot: &Slot) -> Vec<u8> {
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let pkey_size = slot.pkey_size() as usize;
        let val_size = slot.val_size() as usize;

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        let val_offset = offset + remain_key_size + remain_pkey_size;
        self.read_bytes(val_offset, val_size).to_vec()
    }

    fn get_key_pkey_val_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let slot = self.get_slot(slot_id).expect("Invalid slot_id");
        self.get_key_pkey_val_with_slot(&slot)
    }

    fn get_key_pkey_val_with_slot(&self, slot: &Slot) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let rec_offset = slot.offset();
        let key_size = slot.key_size();
        let pkey_size = slot.pkey_size();
        let val_size = slot.val_size();

        let rec_size = val_size
            + key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let rec_bytes = self.read_bytes(rec_offset as usize, rec_size as usize);
        let record = Record::from_bytes(rec_bytes, key_size, pkey_size, val_size);

        let mut full_key = slot.key_prefix().to_vec();
        full_key.extend_from_slice(record.remain_key());

        let mut full_pkey = slot.pkey_prefix().to_vec();
        full_pkey.extend_from_slice(record.remain_pkey());

        let val = record.val().to_vec();
        (full_key, full_pkey, val)
    }

    /// return: key, pkey, value, start_ts, end_ts \
    /// WARN: pay attention to when slot is mark deleted
    fn get_key_pkey_val_ts_with_slot_id(
        &self,
        slot_id: u32,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp) {
        let slot = self.get_slot(slot_id).expect("Invalid slot_id");
        self.get_key_pkey_val_ts_with_slot(&slot)
    }

    fn get_key_pkey_val_ts_with_slot(
        &self,
        slot: &Slot,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp) {
        let rec_offset = slot.offset();
        let key_size = slot.key_size();
        let pkey_size = slot.pkey_size();
        let val_size = slot.val_size();

        let rec_size = val_size
            + key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let rec_bytes = self.read_bytes(rec_offset as usize, rec_size as usize);
        let record = Record::from_bytes(rec_bytes, key_size, pkey_size, val_size);

        let mut full_key = slot.key_prefix().to_vec();
        full_key.extend_from_slice(record.remain_key());

        let mut full_pkey = slot.pkey_prefix().to_vec();
        full_pkey.extend_from_slice(record.remain_pkey());

        let val = record.val().to_vec();
        (full_key, full_pkey, val, slot.start_ts(), slot.end_ts())
    }

    fn rec_start_offset(&self) -> u32 {
        self.header().rec_start_offset()
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }

    fn increase_total_bytes_used(&mut self, bytes: u32) {
        let mut header = self.header();
        header.set_total_bytes_used(header.total_bytes_used() + bytes);
        self.set_header(&header);
    }

    fn decrement_slot_count(&mut self) {
        let mut header = self.header();
        header.decrement_slot_count();
        self.set_header(&header);
    }

    fn decrease_total_bytes_used(&mut self, num_bytes: u32) {
        let mut header = self.header();
        header.set_total_bytes_used(header.total_bytes_used() - num_bytes);
        self.set_header(&header);
    }

    fn rehash_truncate_for_new_page(&mut self, src_st_id: u32, src_ed_id: u32);

    fn decrease_bytes_for_rehash(&mut self, slot_id: u32) {
        // remove record of the slot
        let slot = self.get_slot(slot_id).unwrap();
        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(
                slot.offset()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                    + slot.val_size(),
            );
        }
        self.decrease_total_bytes_used(
            slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                + slot
                    .pkey_size()
                    .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                + slot.val_size()
                + SLOT_SIZE as u32,
        );

        // log_warn!(
        //     "[decrease_bytes_for_rehash] total_bytes_used for deleted slot page: {:?}",
        //     self.header().total_bytes_used()
        // );
    }

    fn insert_at_slot_id(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
        slot_id: u32,
    ) -> Result<(), HashTableAccessMethodError> {
        let space_need = <Page as DoubleHashPage>::space_need(key, pkey, val);
        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;

        // log_warn!("[REHASH::insert], current_slot_offset: {:?}, current_rec_offset: {:?}, slot_id: {:?}", slot_offset, rec_offset, slot_id);

        let slot = Slot::new(key, pkey, start_ts, end_ts, val, rec_offset as usize);
        let record = Record::new(key, pkey, val);

        self.set_slot(slot_id, &slot);
        self.write_record(rec_offset, &record);
        // header.increment_slot_count();
        header.increase_total_bytes_used(space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);
        // log_warn!(
        //     "insert key{:?} at slot id {:?} slot count{:?}",
        //     key,
        //     slot_id,
        //     self.slot_count()
        // );

        Ok(())
    }
}

impl DoubleHashPage for Page {
    fn rehash_truncate_for_new_page(&mut self, src_st_id: u32, src_ed_id: u32) {
        let src_start_offset = self.slot_offset(src_st_id) as usize;
        let bytes_len = (src_ed_id - src_st_id) as usize * SLOT_SIZE;
        let dst_offset = self.slot_offset(0) as usize;
        self.copy_within(src_start_offset..src_start_offset + bytes_len, dst_offset);

        self.set_slot_count(src_ed_id - src_st_id);
    }

    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), HashTableAccessMethodError> {
        let space_need = <Page as DoubleHashPage>::space_need(key, pkey, val);

        if space_need > self.free_space_with_compaction() {
            log_debug!("should not happen, detect before calling cuckoopage::insert");
            return Err(HashTableAccessMethodError::OutOfSpace);
        }
        if space_need > self.free_space_without_compaction() {
            // DO COMPACT
            let want_free_with_compaction_space = self.free_space_with_compaction();
            let actual_free_with_compaction_space = self.compact();
            assert_eq!(
                want_free_with_compaction_space,
                actual_free_with_compaction_space,
                "page id: {:?}",
                self.get_id(),
            );
        }
        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;

        let slot_offset = self.slot_offset(self.slot_count());
        if rec_offset < slot_offset + SLOT_SIZE as u32 {
            log_debug!("should not happen, detect before calling cuckoopage::insert");
            return Err(HashTableAccessMethodError::OutOfSpace);
        }

        let slot_count = self.slot_count();
        let slot_id = {
            if let Some(idx) = self.find_slot_idx(key, pkey, start_ts) {
                idx as u32
            } else {
                slot_count
            }
        };

        let old_suffix_slot_offset = self.slot_offset(slot_id) as usize;
        self.copy_within(
            old_suffix_slot_offset..slot_offset as usize,
            old_suffix_slot_offset + SLOT_SIZE,
        );
        let slot = Slot::new(key, pkey, start_ts, end_ts, val, rec_offset as usize);
        let record = Record::new(key, pkey, val);

        self.set_slot(slot_id, &slot);
        self.write_record(rec_offset, &record);
        header.increment_slot_count();
        header.increase_total_bytes_used(space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);
        // log_warn!("insert at id: {}, space need: {}, free_space_with_compaction: {} free_space_without_compaction: {}", self.get_id(), space_need, self.free_space_with_compaction(), self.free_space_without_compaction());
        // log_warn!(
        //     "insert key{:?} at slot id {:?} slot count{:?}",
        //     key,
        //     slot_id,
        //     self.slot_count()
        // );

        Ok(())
    }

    fn recent_get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, HashTableAccessMethodError> {
        let slot_idx = self.find_slot_idx(key, pkey, ts);
        if let Some(slot_idx) = slot_idx {
            let slot_idx = slot_idx as u32;
            // log_warn!("get idx: {}\n", slot_idx);
            let slot = self.get_slot(slot_idx).unwrap();
            if slot.key_size() == key.len() as u32
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())]
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    // only find once
                    if ts < slot.end_ts() && ts >= slot.start_ts() {
                        if !slot.is_mark_deleted() {
                            // find and not deleted
                            return Ok(record.val().to_vec());
                        } else {
                            // slot deleted
                            return Err(HashTableAccessMethodError::KeyNotFound);
                        }
                    } else {
                        // find but mismatch ts
                        return Err(HashTableAccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }
        } else {
            // not found
        }
        return Err(HashTableAccessMethodError::KeyNotFound);
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        is_recent_get: bool,
    ) -> Result<Vec<u8>, HashTableAccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count();

        for slot_id in 0..slot_count {
            let slot = self.get_slot(slot_id).unwrap();
            if slot.key_size() == key.len() as u32
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())]
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    // check recent or history get
                    if is_recent_get {
                        // only find once
                        if ts < slot.end_ts() && ts >= slot.start_ts() {
                            if !slot.is_mark_deleted() {
                                // find and not deleted
                                return Ok(record.val().to_vec());
                            } else {
                                // slot deleted
                                return Err(HashTableAccessMethodError::KeyNotFound);
                            }
                        } else {
                            // find but mismatch ts
                            return Err(HashTableAccessMethodError::KeyFoundButInvalidTimestamp);
                        }
                    } else
                    /* history get */
                    {
                        // history get
                        if slot.start_ts() <= ts && ts < slot.end_ts() {
                            if !slot.is_mark_deleted() {
                                return Ok(record.val().to_vec());
                            } else {
                                // deleted during [start_ts, end_ts)
                                return Err(HashTableAccessMethodError::KeyNotFound);
                            }
                        } else {
                            // return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                        }
                    }
                }
            }
        }
        Err(HashTableAccessMethodError::KeyNotFound)
    }

    /// get all pair matching `key` and `ts` \
    /// return (pkey, val) \
    /// ignore deleted \
    ///
    fn get_all(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, HashTableAccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count();

        let mut ret = vec![];
        for slot_id in 0..slot_count {
            let slot = self.get_slot(slot_id).unwrap();
            if slot.key_size() == key.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                let val = record.val();

                if full_key == key {
                    if slot.start_ts() <= ts && ts < slot.end_ts() && !slot.is_mark_deleted() {
                        ret.push((full_pkey, val.to_vec()));
                    }
                }
            }
        }
        return Ok(ret);
    }

    /// delete slot at specific id \
    /// used in both::re-hash and history::garbage_collect() \
    /// returns the old ts and old val
    fn delete_slot_at_id(
        &mut self,
        slot_id: u32,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError> {
        // check if rec_start_offset should change
        let slot = self.get_slot(slot_id).expect("Invalid slot_id");

        let old_ts = slot.start_ts();
        let old_val = self.get_value_with_slot(&slot);

        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(
                slot.offset()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                    + slot.val_size(),
            );
        }
        let dbg_decrease_bytes = slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + slot
                .pkey_size()
                .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
            + slot.val_size();
        self.decrease_total_bytes_used(
            slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                + slot
                    .pkey_size()
                    .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                + slot.val_size(),
        );
        // move afterward slots forward
        if slot_id < self.slot_count() {
            let start_offset = self.slot_offset(slot_id + 1) as usize;
            let end_offset = self.slot_offset(self.slot_count()) as usize;
            self.copy_within(start_offset..end_offset, start_offset - SLOT_SIZE);
        }
        self.decrement_slot_count();
        self.decrease_total_bytes_used(SLOT_SIZE as u32);
        self.write_bytes(
            self.slot_offset(self.slot_count()) as usize,
            [0u8; SLOT_SIZE].as_ref(),
        );
        // log_warn!(
        //     "total_bytes_used for deleted slot page: {:?}",
        //     self.header().total_bytes_used()
        // );
        log_warn!("delete at id: {}, space need: {}, free_space_with_compaction: {} free_space_without_compaction: {}", self.get_id(), dbg_decrease_bytes + SLOT_SIZE as u32, self.free_space_with_compaction(), self.free_space_without_compaction());
        Ok((old_ts, old_val))
    }

    /// mark delete slot at specific id \
    /// ONLY used in recent delete
    fn mark_delete_slot_at_id(
        &mut self,
        slot_id: u32,
        end_ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError> {
        // check if rec_start_offset should change
        let mut slot = self.get_slot(slot_id).expect("Invalid slot_id");

        let old_ts = slot.start_ts();
        let old_val = self.get_value_with_slot(&slot);

        // remove record of the slot
        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(
                slot.offset()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                    + slot.val_size(),
            );
        }
        let dbg_decrease_bytes = slot.val_size();
        self.decrease_total_bytes_used(slot.val_size());

        // mark delete at slot
        slot.mark_deleted();
        slot.set_start_ts(end_ts);
        self.set_slot(slot_id, &slot);
        // log_warn!("mark delete at id: {}, space need: {}, free_space_with_compaction: {} free_space_without_compaction: {}", self.get_id(), dbg_decrease_bytes, self.free_space_with_compaction(), self.free_space_without_compaction());

        // log_warn!(
        //     "[mark delete] total_bytes_used for deleted slot page: {:?}",
        //     self.header().total_bytes_used()
        // );
        Ok((old_ts, old_val))
    }

    /// ONLY RECENT
    fn get_delete_mark_slot_id(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<u32, HashTableAccessMethodError> {
        let slot_idx = self.find_slot_idx(key, pkey, ts);
        if let Some(slot_idx) = slot_idx {
            let slot_idx = slot_idx as u32;
            // log_warn!("get idx: {}\n", slot_idx);
            let slot = self.get_slot(slot_idx).unwrap();
            if slot.key_size() == key.len() as u32
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())]
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    if slot.start_ts() <= ts && slot.is_mark_deleted() {
                        return Ok(slot_idx);
                    } else {
                        return Err(HashTableAccessMethodError::KeyNotFound);
                    }
                }
            }
        } else {
            // not found
        }
        return Err(HashTableAccessMethodError::KeyNotFound);
    }

    /// ONLY RECENT
    /// used in recent::update and recent::delete
    fn get_slot_id(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<u32, HashTableAccessMethodError> {
        let slot_idx = self.find_slot_idx(key, pkey, ts);
        if let Some(slot_idx) = slot_idx {
            let slot_idx = slot_idx as u32;
            // log_warn!("get idx: {}\n", slot_idx);
            let slot = self.get_slot(slot_idx).unwrap();
            if slot.key_size() == key.len() as u32
                && slot.pkey_size() == pkey.len() as u32
                && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())]
            {
                let rec_offset = slot.offset() as usize;
                let rec_size = slot.val_size()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot
                        .pkey_size()
                        .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
                let record_bytes = &self[rec_offset..rec_offset + rec_size as usize];
                let record = Record::from_bytes(
                    record_bytes,
                    slot.key_size(),
                    slot.pkey_size(),
                    slot.val_size(),
                );

                let mut full_key = slot.key_prefix().to_vec();
                full_key.extend_from_slice(record.remain_key());

                let mut full_pkey = slot.pkey_prefix().to_vec();
                full_pkey.extend_from_slice(record.remain_pkey());

                if full_key == key && full_pkey == pkey {
                    if slot.is_mark_deleted() {
                        return Err(HashTableAccessMethodError::KeyNotFound);
                    } else if ts < slot.start_ts() {
                        return Err(HashTableAccessMethodError::KeyFoundButInvalidTimestamp);
                    } else {
                        return Ok(slot_idx);
                    }
                }
            }
        } else {
            // not found
        }
        return Err(HashTableAccessMethodError::KeyNotFound);
    }

    // only recent
    /*
       calculate to find whether the record size is okay or not
       if compaction is not enough -> OutOfSpace
    */
    fn check_and_update_at_slot_id(
        &mut self,
        slot_id: u32,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        ts: Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), HashTableAccessMethodError> {
        let new_rec_size = Self::space_need(key, pkey, val) - SLOT_SIZE as u32;

        let slot = self.get_slot(slot_id).expect("Invalid slot_id");
        let old_val = self.get_value_with_slot(&slot);
        let old_ts = slot.start_ts();
        let old_record_offset = slot.offset();
        let old_val_size = slot.val_size();
        let old_rec_size = slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + slot
                .pkey_size()
                .saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
            + old_val_size;

        // log_warn!(
        //     "[before UPDATE: page_id: {:?}] ref offset: {:?}, slot_end {:?} free_space_without_compaction {:?} free_space_with_compaction {:?}, new_recsize: {:?} old_recsize: {:?}",
        //     self.get_id(),
        //     self.rec_start_offset(),
        //     self.slot_offset(self.slot_count()),
        //     self.free_space_without_compaction(),
        //     self.free_space_with_compaction(),
        //     new_rec_size,
        //     old_rec_size,
        // );

        if self.free_space_without_compaction() < new_rec_size {
            if (self.free_space_with_compaction() as i32)
                < (new_rec_size as i32) - (old_rec_size as i32)
            {
                return Err(HashTableAccessMethodError::OutOfSpace);
            }
            if new_rec_size > old_rec_size {
                // log_warn!("new rec size: {:?}, old: {:?}", new_rec_size, old_rec_size);
                // delete old record and compaction

                // WARNING: inconsistent state
                let dummy_slot = Slot::new(b"", b"", 0, 0, b"", old_record_offset as usize);
                self.set_slot(slot_id, &dummy_slot);
                let want_free_space = self.free_space_with_compaction() + old_rec_size;
                let actual_free_space = self.compact();
                assert_eq!(want_free_space, actual_free_space);
            }
        }
        // log_warn!(
        //     "[before UPDATE, after possible COMPACT] ref offset: {:?}, slot_end {:?} free_space_without_compaction {:?}",
        //     self.rec_start_offset(),
        //     self.slot_offset(self.slot_count()),
        //     self.free_space_without_compaction(),
        // );
        let new_record = Record::new(key, pkey, val);
        let new_rec_offset = {
            if new_rec_size <= old_rec_size || old_record_offset == self.rec_start_offset() {
                // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`

                // log_warn!(
                //     "[case 1&2 PAGEID: {:?}]<OLD>, old_rec offset:{:?} rec start offset: {:?}",
                //     self.get_id(),
                //     old_record_offset,
                //     self.rec_start_offset()
                // );

                let new_rec_offset = old_record_offset + old_rec_size - new_rec_size;
                self.write_record(new_rec_offset, &new_record);
                if new_rec_size < old_rec_size {
                    self.write_bytes(
                        old_record_offset as usize,
                        &vec![0; old_rec_size as usize - new_rec_size as usize],
                    );
                }
                if old_record_offset == self.rec_start_offset() {
                    self.set_rec_start_offset(new_rec_offset as u32);
                }

                // log_warn!(
                //     "[case 1&2 PAGEID: {:?}]<NEW>, new_rec offset:{:?} new rec start offset: {:?}",
                //     self.get_id(),
                //     new_rec_offset,
                //     self.rec_start_offset()
                // );

                new_rec_offset
            } else {
                // log_warn!(
                //     "[case 3 PAGEID: {:?}]<OLD>, old_rec offset:{:?} rec start offset: {:?}",
                //     self.get_id(),
                //     old_record_offset,
                //     self.rec_start_offset()
                // );

                // Case 3: New value is larger and offset doesn't match `rec_start_offset`
                let new_rec_offset = self.rec_start_offset() - new_rec_size;
                self.write_record(new_rec_offset, &new_record);
                self.set_rec_start_offset(new_rec_offset as u32);
                // log_warn!(
                //     "[case 3 PAGEID: {:?}]<NEW>, new_rec offset:{:?} new rec start offset: {:?}",
                //     self.get_id(),
                //     new_rec_offset,
                //     self.rec_start_offset()
                // );

                new_rec_offset
            }
        };

        let new_slot = Slot::new(key, pkey, ts, Timestamp::MAX, val, new_rec_offset as usize);

        self.set_slot(slot_id, &new_slot);

        self.increase_total_bytes_used(new_rec_size as u32);
        self.decrease_total_bytes_used(old_rec_size as u32);
        log_warn!("update at id: {}, (old, new)space need: {:?}, free_space_with_compaction: {} free_space_without_compaction: {}", self.get_id(), (old_rec_size, new_rec_offset), self.free_space_with_compaction(), self.free_space_without_compaction());

        Ok((old_ts, old_val))
    }

    fn compact(&mut self) -> u32 {
        // decreasing order 3, 2, 1
        let slot_offsets_sorted_decreased = {
            // (slot, slot_rec_offset, slot_idx)
            let mut rec_offsets = vec![];

            for slot_id in 0..self.slot_count() {
                let slot = self.get_slot(slot_id).unwrap();
                let rec_offset = slot.offset();
                rec_offsets.push((slot, rec_offset, slot_id));
            }

            rec_offsets.sort_by(|a, b| b.1.cmp(&a.1));

            rec_offsets
        };

        let mut new_rec_offset = AVAILABLE_PAGE_SIZE;

        assert_eq!(
            slot_offsets_sorted_decreased.len() as u32,
            self.slot_count()
        );

        for (mut slot, rec_offset, slot_idx) in slot_offsets_sorted_decreased {
            let old_rec_start_off = rec_offset as usize;
            let old_rec_end_off = (rec_offset + slot.get_record_size_of_slot()) as usize;
            new_rec_offset -= slot.get_record_size_of_slot() as usize;
            self.copy_within(old_rec_start_off..old_rec_end_off, new_rec_offset);
            slot.set_offset(new_rec_offset as u32);
            self.set_slot(slot_idx, &slot);
        }

        let mut header = self.header();
        header.set_rec_start_offset(new_rec_offset as u32);
        self.set_header(&header);
        // log_warn!(
        //     "[COMPACT page-id: {:?}] new rec offset: {:?}, slot_count: {:?}",
        //     self.get_id(),
        //     header.rec_start_offset(),
        //     header.slot_count()
        // );

        self.free_space_without_compaction()
    }

    // only history
    /// delete all record with end_ts <= safe_ts
    fn garbage_collect(&mut self, safe_ts: Timestamp) {
        let hashed_page_old_slot_count = self.slot_count();

        let mut invalid_idxes = std::collections::HashSet::new();

        for slot_idx in (0..hashed_page_old_slot_count).rev() {
            let slot_start_offset_in_page = self.slot_offset(slot_idx) as usize;
            let slot_start_ptr = &self[slot_start_offset_in_page] as *const u8 as *const Slot;
            let end_ts = unsafe { (*slot_start_ptr).end_ts() };
            if end_ts <= safe_ts {
                self.decrease_bytes_for_rehash(slot_idx);
                invalid_idxes.insert(slot_idx);
            }
        }

        let hashed_page_new_slot_count = hashed_page_old_slot_count - invalid_idxes.len() as u32;
        let mut i: u32 = 0;
        for j in 0..hashed_page_old_slot_count {
            if invalid_idxes.contains(&j) {
                continue;
            }
            self.swap_slot(i, j);
            i += 1;
        }
        assert_eq!(i, hashed_page_new_slot_count);
        self.set_slot_count(hashed_page_new_slot_count);
    }
    /// only history and recent::delete \
    /// when deleted is inserted again in recent table \
    /// insert deleted pair in history table
    fn insert_deleted(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), HashTableAccessMethodError> {
        // log_warn!("insert deleted at id: {}", self.get_id());
        let space_need = <Page as DoubleHashPage>::space_need(key, pkey, &vec![]);
        if space_need > self.free_space_with_compaction() {
            log_debug!("should not happen, detect before calling cuckoopage::insert");
            return Err(HashTableAccessMethodError::OutOfSpace);
        }

        if space_need > self.free_space_without_compaction() {
            // DO COMPACT
            let want_free_with_compaction_space = self.free_space_with_compaction();
            let actual_free_with_compaction_space = self.compact();
            assert_eq!(
                want_free_with_compaction_space,
                actual_free_with_compaction_space
            );
        }

        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;
        let slot_offset = self.slot_offset(self.slot_count());

        if rec_offset < slot_offset + SLOT_SIZE as u32 {
            panic!("should not happen, detect before calling cuckoopage::insert");
            // return Err(CuckooAccessMethodError::OutOfSpace);
        }

        let slot_count = self.slot_count();
        let slot_id = {
            if let Some(idx) = self.find_slot_idx(key, pkey, start_ts) {
                idx as u32
            } else {
                slot_count
            }
        };

        let old_suffix_slot_offset = self.slot_offset(slot_id) as usize;
        self.copy_within(
            old_suffix_slot_offset..slot_offset as usize,
            old_suffix_slot_offset + SLOT_SIZE,
        );

        let mut slot = Slot::new(key, pkey, start_ts, end_ts, &vec![], rec_offset as usize);
        slot.mark_deleted();
        let record = Record::new(key, pkey, &vec![]);

        self.set_slot(slot_id, &slot);
        self.write_record(rec_offset, &record);

        header.increment_slot_count();
        header.increase_total_bytes_used(space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);

        Ok(())
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn read_bytes(&self, offset: usize, length: usize) -> &[u8] {
        &self[offset..offset + length]
    }
    fn get_slot_slice(&self, slot_id: u32) -> &[Slot] {
        let slots_start_offset_in_page = self.slot_offset(slot_id) as usize;
        let slots_start_ptr = &self[slots_start_offset_in_page] as *const u8 as *const Slot;
        // assert!(slots_start_ptr.is_aligned());
        let len = self.slot_count();
        assert!(slot_id <= len, "sid {:?}, len{:?}", slot_id, len);
        unsafe { slice::from_raw_parts(slots_start_ptr, (len - slot_id) as usize) }
    }

    fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
        let slots_start_offset_in_page = self.slot_offset(slot_id) as usize;
        let slots_start_ptr = &self[slots_start_offset_in_page] as *const u8 as *mut Slot;
        let len = self.slot_count();
        assert!(slot_id <= len);
        unsafe {
            std::ptr::copy_nonoverlapping(slot as *const Slot, slots_start_ptr, 1);
        }
    }

    fn get_slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let slots_start_offset_in_page = self.slot_offset(slot_id) as usize;
            let slots_start_ptr = &self[slots_start_offset_in_page] as *const u8 as *const Slot;
            let len = self.slot_count();
            assert!(slot_id <= len);
            let mut slot = Slot::default();
            unsafe {
                std::ptr::copy_nonoverlapping(slots_start_ptr, &mut slot as *mut Slot, 1);
            }

            Some(slot)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{Page, AVAILABLE_PAGE_SIZE};

    #[test]
    fn test_insert_and_get_key_pkey_len_less_than_prefix() {
        // Key and PKey lengths less than prefix sizes
        let key = b"key1"; // Length 4
        let pkey = b"pk1"; // Length 3
        let ts: Timestamp = 1;
        let val = b"value1";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts, true).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_len_less_than_prefix_pkey_len_greater_than_prefix() {
        // Key length less than prefix size, PKey length greater than prefix size
        let key = b"key2"; // Length 4
        let pkey = b"primarykey_longer"; // Length > 8
        let ts: Timestamp = 2;
        let val = b"value2";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts, true).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_len_greater_than_prefix_pkey_len_less_than_prefix() {
        // Key length greater than prefix size, PKey length less than prefix size
        let key = b"key_longer_than_prefix"; // Length > 8
        let pkey = b"pk2"; // Length 3
        let ts: Timestamp = 3;
        let val = b"value3";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts, true).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_and_get_key_pkey_len_greater_than_prefix() {
        // Key and PKey lengths greater than prefix sizes
        let key = b"key_longer_than_prefix_size"; // Length > 8
        let pkey = b"primarykey_longer_than_prefix"; // Length > 8
        let ts: Timestamp = 4;
        let val = b"value4";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts, true).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_multiple_entries_with_various_key_pkey_lengths() {
        // Define the entries with keys and pkeys as slices (&[u8])
        let entries: Vec<(&[u8], &[u8], u64, &[u8])> = vec![
            (b"k1", b"p1", 10u64, b"v1"), // Both key and pkey < prefix size
            (b"key_longlong", b"p2", 20u64, b"v2"), // Key > prefix size, pkey < prefix size
            (b"k3", b"primarykey_long", 30u64, b"v3"), // Key < prefix size, pkey > prefix size
            (b"key_very_long", b"primarykey_very_long", 40u64, b"v4"), // Both key and pkey > prefix size
        ];

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);
        // Insert entries
        for (key, pkey, ts, val) in &entries {
            page.insert(key, pkey, *ts, Timestamp::MAX, val).unwrap();
        }

        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = page.get(key, pkey, *ts, true).unwrap();
            assert_eq!(retrieved_val, *val);
        }
    }

    #[test]
    fn test_insert_and_get_with_timestamp_check() {
        // Test that entries with timestamps greater than the query timestamp are not returned
        let key = b"key_test";
        let pkey = b"pkey_test";
        let ts_insert: Timestamp = 100;
        let ts_query: Timestamp = 50; // Less than ts_insert
        let val = b"value_test";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val)
            .unwrap();

        // Attempt to retrieve with earlier timestamp
        let result = page.get(key, pkey, ts_query, true);
        assert!(matches!(
            result,
            Err(HashTableAccessMethodError::KeyFoundButInvalidTimestamp)
        ));

        // Retrieve with correct timestamp
        let retrieved_val = page.get(key, pkey, ts_insert, true).unwrap();
        assert_eq!(retrieved_val, val);
    }

    #[test]
    fn test_insert_duplicate_keys() {
        // Insert entries with the same key and pkey but different timestamps
        let key = b"key_dup";
        let pkey1: &[u8; 8] = b"pkey_dup";
        let pkey2: &[u8; 9] = b"pkey_dup2";
        let val1 = b"value1";
        let val2 = b"value2";
        let ts1: Timestamp = 1;
        let ts2: Timestamp = 2;

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert first entry
        page.insert(key, pkey1, ts1, Timestamp::MAX, val1).unwrap();

        // Insert second entry with a newer timestamp
        page.insert(key, pkey2, ts2, Timestamp::MAX, val2).unwrap();

        // Retrieve with ts1
        let retrieved_val = page.get(key, pkey1, ts1, true).unwrap();
        assert_eq!(retrieved_val, val1);

        // Retrieve with ts2
        let retrieved_val = page.get(key, pkey2, ts2, true).unwrap();
        assert_eq!(retrieved_val, val2);
    }

    #[test]
    fn test_insert_when_page_full() {
        // Fill the page to capacity and attempt to insert another entry
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Use fixed-length keys and pkeys
        let key = b"key_full_full"; // Length 12 bytes
        let pkey = b"pkey_full_full"; // Length 13 bytes
        let val = b"value_full"; // Value size
        let ts: Timestamp = 1;

        let space_per_entry = Page::space_need(key, pkey, val) as usize;
        let available_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE_ALIGNED;
        let page_capacity = available_space / space_per_entry;

        // Insert entries until the page is full
        for _ in 0..page_capacity {
            page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();
        }

        // Attempt to insert one more entry
        let result = page.insert(key, pkey, ts, Timestamp::MAX, val);
        assert!(matches!(
            result,
            Err(HashTableAccessMethodError::OutOfSpace)
        ));
    }
    #[test]
    fn test_update_same_size_value() {
        let key = b"key1";
        let pkey = b"pkey1";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"value1"; // Length 6
        let val_update = b"value2"; // Length 6 (same as val_insert)

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val_insert)
            .unwrap();

        // Update the entry
        let slot_id = page.get_slot_id(key, pkey, ts_update).unwrap();

        let (old_ts, old_val) = page
            .check_and_update_at_slot_id(slot_id, key, pkey, val_update, ts_update)
            .unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update, true).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_smaller_value() {
        let key = b"key2";
        let pkey = b"pkey2";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"value_longer"; // Length 12
        let val_update = b"short"; // Length 5 (smaller than val_insert)

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val_insert)
            .unwrap();

        // Update the entry
        let slot_id = page.get_slot_id(key, pkey, ts_update).unwrap();

        let (old_ts, old_val) = page
            .check_and_update_at_slot_id(slot_id, key, pkey, val_update, ts_update)
            .unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update, true).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_larger_value_enough_space() {
        let key = b"key3";
        let pkey = b"pkey3";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"short"; // Length 5
        let val_update = b"value_is_longer"; // Length 14 (larger than val_insert)

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val_insert)
            .unwrap();

        // Update the entry
        let slot_id = page.get_slot_id(key, pkey, ts_update).unwrap();

        let (old_ts, old_val) = page
            .check_and_update_at_slot_id(slot_id, key, pkey, val_update, ts_update)
            .unwrap();

        // let (old_ts, old_val) = page.update(key, pkey, ts_update, val_update).unwrap();

        // Verify old timestamp and value
        assert_eq!(old_ts, ts_insert);
        assert_eq!(old_val, val_insert);

        // Retrieve the updated entry
        let retrieved_val = page.get(key, pkey, ts_update, true).unwrap();
        assert_eq!(retrieved_val, val_update);
    }

    #[test]
    fn test_update_larger_value_insufficient_space() {
        let key = b"key4";
        let pkey = b"pkey4";
        let ts_insert: Timestamp = 100;
        let ts_update: Timestamp = 200;
        let val_insert = b"val"; // Length 3
        let val_update = vec![b'a'; (AVAILABLE_PAGE_SIZE / 2) as usize]; // Large value

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Fill the page to limit the available space
        page.insert(
            b"key_dummy",
            b"pkey_dummy",
            50,
            Timestamp::MAX,
            &vec![b'b'; (AVAILABLE_PAGE_SIZE / 2) as usize],
        )
        .unwrap();

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val_insert)
            .unwrap();

        // Attempt to update the entry with a larger value
        let slot_id = page.get_slot_id(key, pkey, ts_update).unwrap();

        let result = page.check_and_update_at_slot_id(slot_id, key, pkey, &val_update, ts_update);

        // let result = page.update(key, pkey, ts_update, &val_update);
        assert!(matches!(
            result,
            Err(HashTableAccessMethodError::OutOfSpace)
        ));
    }

    #[test]
    fn test_update_non_existent_key() {
        let key = b"key_nonexistent";
        let pkey = b"pkey_nonexistent";
        let ts_update: Timestamp = 100;
        let val_update = b"value";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Attempt to update a non-existent key
        let slot_id = page.get_slot_id(key, pkey, ts_update);
        assert_eq!(slot_id.err(), Some(HashTableAccessMethodError::KeyNotFound));
        // let result = page.check_and_update_at_slot_id(slot_id, key, pkey, &val_update, ts_update);

        // let result = page.update(key, pkey, ts_update, val_update);
    }

    #[test]
    fn test_update_invalid_timestamp() {
        let key = b"key5";
        let pkey = b"pkey5";
        let ts_insert: Timestamp = 200;
        let ts_update: Timestamp = 100; // Less than ts_insert
        let val_insert = b"value1";
        let val_update = b"value2";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val_insert)
            .unwrap();

        // Attempt to update with an earlier timestamp
        let slot_id = page.get_slot_id(key, pkey, ts_update);
        assert_eq!(
            slot_id.err(),
            Some(HashTableAccessMethodError::KeyFoundButInvalidTimestamp)
        );
        // let result = page.update(key, pkey, ts_update, val_update);
        // assert!(matches!(
        //     result,
        //     Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp)
        // ));
    }

    #[test]
    fn test_delete_and_get_key_pkey_len_less_than_prefix() {
        // Key and PKey lengths less than prefix sizes
        let key = b"key1"; // Length 4
        let pkey = b"pk1"; // Length 3
        let ts: Timestamp = 1;
        let val = b"value1";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Delete the entry
        let slot_id = page.get_slot_id(key, pkey, ts).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // let del_result = page.delete(key, pkey, ts + 1);

        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().1, val);
        assert_eq!(del_result.as_ref().unwrap().0, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts, true);
        assert!(retrieved_res.is_err());
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );
    }

    #[test]
    fn test_insert_and_del_key_len_less_than_prefix_pkey_len_greater_than_prefix() {
        // Key length less than prefix size, PKey length greater than prefix size
        let key = b"key2"; // Length 4
        let pkey = b"primarykey_longer"; // Length > 8
        let ts: Timestamp = 2;
        let val = b"value2";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Delete the entry
        let slot_id = page.get_slot_id(key, pkey, ts + 1).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().1, val);
        assert_eq!(del_result.as_ref().unwrap().0, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts, true);
        assert!(retrieved_res.is_err());
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );
    }

    #[test]
    fn test_insert_and_delete_key_len_greater_than_prefix_pkey_len_less_than_prefix() {
        // Key length greater than prefix size, PKey length less than prefix size
        let key = b"key_longer_than_prefix"; // Length > 8
        let pkey = b"pk2"; // Length 3
        let ts: Timestamp = 3;
        let val = b"value3";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Delete the entry
        let slot_id = page.get_slot_id(key, pkey, ts + 1).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().1, val);
        assert_eq!(del_result.as_ref().unwrap().0, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts, true);
        assert!(retrieved_res.is_err());
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );
    }

    #[test]
    fn test_insert_and_delete_key_pkey_len_greater_than_prefix() {
        // Key and PKey lengths greater than prefix sizes
        let key = b"key_longer_than_prefix_size"; // Length > 8
        let pkey = b"primarykey_longer_than_prefix"; // Length > 8
        let ts: Timestamp = 4;
        let val = b"value4";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();

        // Delete the entry
        let slot_id = page.get_slot_id(key, pkey, ts + 1).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().1, val);
        assert_eq!(del_result.as_ref().unwrap().0, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts, true);
        assert!(retrieved_res.is_err());
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );
    }

    #[test]
    fn test_insert_and_delete_multiple_entries_with_various_key_pkey_lengths() {
        // Define the entries with keys and pkeys as slices (&[u8])
        let entries: Vec<(&[u8], &[u8], u64, &[u8])> = vec![
            (b"k1", b"p1", 10u64, b"v1"), // Both key and pkey < prefix size
            (b"key_longlong", b"p2", 20u64, b"v2"), // Key > prefix size, pkey < prefix size
            (b"k3", b"primarykey_long", 30u64, b"v3"), // Key < prefix size, pkey > prefix size
            (b"key_very_long", b"primarykey_very_long", 40u64, b"v4"), // Both key and pkey > prefix size
        ];

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert entries
        for (key, pkey, ts, val) in &entries {
            page.insert(key, pkey, *ts, Timestamp::MAX, val).unwrap();
        }

        // Delete entries
        for (key, pkey, ts, val) in &entries {
            let slot_id = page.get_slot_id(key, pkey, *ts).unwrap();
            let del_result = page.delete_slot_at_id(slot_id);
            assert_eq!(del_result.unwrap().1, *val);
            // assert_eq!(page.delete(key, pkey, *ts).unwrap().1, *val);
        }

        // Retrieve and verify entries
        for (key, pkey, ts, _val) in &entries {
            let retrieved_res = page.get(key, pkey, *ts, true);
            assert_eq!(
                retrieved_res.err(),
                Some(HashTableAccessMethodError::KeyNotFound)
            );
        }
    }

    #[test]
    fn test_insert_and_delete_with_timestamp_check() {
        // Test that entries with timestamps greater than the query timestamp are not returned
        let key = b"key_test";
        let pkey = b"pkey_test";
        let ts_insert: Timestamp = 100;
        let ts_query: Timestamp = 50; // Less than ts_insert
        let val = b"value_test";

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert the entry
        page.insert(key, pkey, ts_insert, Timestamp::MAX, val)
            .unwrap();

        // Delete the entry
        let slot_id = page.get_slot_id(key, pkey, ts_query);
        // let delete_result = page.delete(key, pkey, ts_query);
        assert_eq!(
            slot_id.err(),
            Some(HashTableAccessMethodError::KeyFoundButInvalidTimestamp)
        );
    }

    #[test]
    fn test_delete_duplicate_keys() {
        // Insert entries with the same key and pkey but different timestamps
        let key = b"key_dup";
        let pkey1: &[u8; 8] = b"pkey_dup";
        let pkey2: &[u8; 9] = b"pkey_dup2";
        let val1 = b"value1";
        let val2 = b"value2";
        let ts1: Timestamp = 1;
        let ts2: Timestamp = 2;

        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert first entry
        page.insert(key, pkey1, ts1, Timestamp::MAX, val1).unwrap();

        // Insert second entry with a newer timestamp
        page.insert(key, pkey2, ts2, Timestamp::MAX, val2).unwrap();

        // Delete the 2
        let slot_id = page.get_slot_id(key, pkey1, ts1).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // page.delete(key, pkey1, ts1).unwrap();
        let slot_id = page.get_slot_id(key, pkey2, ts2).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // page.delete(key, pkey2, ts2).unwrap();

        // Retrieve with ts1
        let retrieved_res = page.get(key, pkey1, ts1, true);
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );

        // Retrieve with ts2
        let retrieved_res = page.get(key, pkey2, ts2, true);
        assert_eq!(
            retrieved_res.err(),
            Some(HashTableAccessMethodError::KeyNotFound)
        );
    }

    #[test]
    fn test_insert_and_delete_when_page_full() {
        // Fill the page to capacity and attempt to insert another entry
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Use fixed-length keys and pkeys
        let key = b"key_full_full"; // Length 12 bytes
        let pkey = b"pkey_full_full"; // Length 13 bytes
        let val = b"value_full"; // Value size
        let ts: Timestamp = 1;

        let space_per_entry = Page::space_need(key, pkey, val) as usize;
        let available_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE_ALIGNED;
        let page_capacity = available_space / space_per_entry;

        // Insert entries until the page is full
        for _ in 0..page_capacity {
            page.insert(key, pkey, ts, Timestamp::MAX, val).unwrap();
        }

        let before_delete_pg_sz = page.header().total_bytes_used();
        // Attempt to insert one more entry
        let slot_id = page.get_slot_id(key, pkey, ts).unwrap();
        let del_result = page.delete_slot_at_id(slot_id);
        // let _result = page.delete(key, pkey, ts);

        assert_eq!(
            space_per_entry as u32,
            before_delete_pg_sz - page.header().total_bytes_used()
        );
        assert!(page.insert(key, pkey, ts, Timestamp::MAX, val).is_ok());
    }

    // -------------UPDATE---------------
    #[test]
    fn test_update_with_smaller_value() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20, 30, 40];
        page.insert(&key, &pkey, ts, Timestamp::MAX, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a smaller value
        let new_value = vec![50, 60];
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result = page.check_and_update_at_slot_id(slot_id, &key, &pkey, &new_value, ts);
        // page.update(&key, &pkey, ts, &new_value)
        //     .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page
            .get(&key, &pkey, ts, true)
            .expect("Failed to get updated value");

        assert_eq!(updated_value, &[50, 60]);
    }

    #[test]
    fn test_update_with_larger_value() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20];
        page.insert(&key, &pkey, ts, Timestamp::MAX, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![30, 40, 50, 60];
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result = page.check_and_update_at_slot_id(slot_id, &key, &pkey, &new_value, ts);
        // page.update(&key, &pkey, ts, &new_value)
        //     .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page
            .get(&key, &pkey, ts, true)
            .expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[30, 40, 50, 60]);
    }

    #[test]
    fn test_update_with_equal_size_value() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20, 30];
        page.insert(&key, &pkey, ts, Timestamp::MAX, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a value of equal size
        let new_value = vec![40, 50, 60];
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result = page.check_and_update_at_slot_id(slot_id, &key, &pkey, &new_value, ts);
        // page.update(&key, &pkey, ts, &new_value)
        //     .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page
            .get(&key, &pkey, ts, true)
            .expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[40, 50, 60]);
    }

    #[test]
    fn test_update_with_large_value_out_of_space() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20];
        page.insert(&key, &pkey, ts, Timestamp::MAX, &initial_value)
            .expect("Failed to insert key-value pair");

        // Attempt to update with a value that is too large
        let large_value = vec![0; AVAILABLE_PAGE_SIZE]; // Value larger than the remaining space
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result =
            page.check_and_update_at_slot_id(slot_id, &key, &pkey, &large_value, ts);
        assert_eq!(
            update_result.err(),
            Some(HashTableAccessMethodError::OutOfSpace)
        )
        // let result = page.update(&key, &pkey, ts, &large_value);
    }

    #[test]
    fn test_insert_multiple_and_update_multiple() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert multiple key-value pairs
        let keys = vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
            vec![10, 11, 12],
        ];
        let values = vec![
            vec![10, 20, 30],
            vec![40, 50, 60],
            vec![70, 80, 90],
            vec![100, 110, 120],
        ];

        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, &pkey, ts, Timestamp::MAX, value)
                .expect("Failed to insert key-value pair");
        }

        // Update the values
        let new_values = vec![
            vec![13, 14, 15],
            vec![16, 17, 18],
            vec![19, 20, 21],
            vec![22, 23, 24],
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            log_warn!("update key {:?}", key);
            let slot_id = page.get_slot_id(key, &pkey, ts).unwrap();
            let update_result = page
                .check_and_update_at_slot_id(slot_id, &key, &pkey, new_value, ts + 1)
                .expect("Failed to update value");
            // page.update(key, &pkey, ts, new_value)
            //     .expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        for (key, expected_value) in keys.iter().zip(new_values.iter()) {
            let retrieved_value = page
                .get(key, &pkey, ts + 1, true)
                .expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_multiple_update_mixed_sizes_and_get() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        // Insert multiple key-value pairs
        let keys = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let values = vec![vec![10, 20, 30], vec![40, 50, 60], vec![70, 80, 90]];

        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, &pkey, ts, Timestamp::MAX, value)
                .expect("Failed to insert key-value pair");
        }

        // Update the values with mixed sizes
        let new_values = vec![
            vec![13, 14],             // Smaller
            vec![16, 17, 18],         // Same size
            vec![19, 20, 21, 22, 23], // Larger
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            let slot_id = page.get_slot_id(key, &pkey, ts).unwrap();
            let update_result = page
                .check_and_update_at_slot_id(slot_id, key, &pkey, new_value, ts)
                .expect("Failed to update value");
            // page.update(key, &pkey, ts, new_value)
            //     .expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        let expected_values = vec![
            vec![13, 14],             // Padded with zeros
            vec![16, 17, 18],         // Same size
            vec![19, 20, 21, 22, 23], // Larger, no padding needed
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page
                .get(key, &pkey, ts, true)
                .expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_update_and_get_mixed() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert initial key-value pairs
        let initial_keys = vec![vec![1, 1, 1], vec![2, 2, 2]];
        let initial_values = vec![vec![11, 12, 13], vec![21, 22, 23]];

        for (key, value) in initial_keys.iter().zip(initial_values.iter()) {
            page.insert(key, &pkey, ts, Timestamp::MAX, value)
                .expect("Failed to insert key-value pair");
        }

        // Update existing and insert new key-value pairs
        let keys_to_update = vec![vec![1, 1, 1], vec![2, 2, 2]];
        let new_values = vec![
            vec![14, 15, 16],
            vec![24, 25], // Smaller, expect padding
        ];

        for (key, new_value) in keys_to_update.iter().zip(new_values.iter()) {
            let slot_id = page.get_slot_id(key, &pkey, ts).unwrap();
            let update_result = page
                .check_and_update_at_slot_id(slot_id, key, &pkey, new_value, ts)
                .expect("Failed to update value");
            // page.update(key, &pkey, ts, new_value)
            //     .expect("Failed to update value");
        }

        // Insert a new key-value pair
        let new_key = vec![3, 3, 3];
        let new_value = vec![31, 32, 33];
        page.insert(&new_key, &pkey, ts, Timestamp::MAX, &new_value)
            .expect("Failed to insert new key-value pair");

        // Verify updates and new insertions
        let retrieved_values = vec![
            vec![14, 15, 16], // Updated
            vec![24, 25],     // Updated with padding
            vec![31, 32, 33], // Newly inserted
        ];

        let all_keys = vec![vec![1, 1, 1], vec![2, 2, 2], vec![3, 3, 3]];

        for (key, expected_value) in all_keys.iter().zip(retrieved_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts, true).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_update_large_keys() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Key sizes larger than SLOT_KEY_PREFIX_SIZE (8 bytes)
        let key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 bytes
        let initial_value = vec![101, 102, 103];

        // Insert a key-value pair
        page.insert(&key, &pkey, ts, Timestamp::MAX, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![104, 105, 106, 107, 108];
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result = page
            .check_and_update_at_slot_id(slot_id, &key, &pkey, &new_value, ts)
            .expect("Failed to update value");
        // page.update(&key, &pkey, ts, &new_value)
        //     .expect("Failed to update value");

        // Retrieve the updated value and verify correctness
        let retrieved_value = page
            .get(&key, &pkey, ts, true)
            .expect("Failed to get updated value");
        assert_eq!(retrieved_value, new_value.as_slice());

        // Update the value with a smaller value
        let smaller_value = vec![109, 110];
        let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
        let update_result = page
            .check_and_update_at_slot_id(slot_id, &key, &pkey, &smaller_value, ts)
            .expect("Failed to update value");
        // page.update(&key, &pkey, ts, &smaller_value)
        //     .expect("Failed to update value");

        // Retrieve the updated value
        let retrieved_value = page
            .get(&key, &pkey, ts, true)
            .expect("Failed to get updated value");
        assert_eq!(retrieved_value, smaller_value.as_slice());
    }

    #[test]
    fn test_insert_update_and_get_with_large_keys() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert multiple key-value pairs with keys larger than 8 bytes
        let keys = vec![
            vec![20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31], // 12 bytes
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],                      // 9 bytes
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],         // 10 bytes
        ];
        let initial_values = vec![
            vec![101, 102],
            vec![201, 202, 203],
            vec![251, 252, 253, 254],
        ];

        for (key, value) in keys.iter().zip(initial_values.iter()) {
            page.insert(key, &pkey, ts, Timestamp::MAX, value)
                .expect("Failed to insert key-value pair");
        }

        // Update some of the values with new data
        let new_values = vec![
            vec![103, 104, 105], // Update with a larger value
            vec![206],           // Update with a smaller value
            vec![208, 209],      // Update with a smaller value
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            let slot_id = page.get_slot_id(&key, &pkey, ts).unwrap();
            let update_result = page
                .check_and_update_at_slot_id(slot_id, key, &pkey, new_value, ts)
                .expect("Failed to update value");
            // page.update(key, &pkey, ts, new_value)
            //     .expect("Failed to update value");
        }

        // Retrieve and verify all values
        let expected_values = vec![
            vec![103, 104, 105], // Updated with a larger value
            vec![206],           // Updated with a smaller value, no padding
            vec![208, 209],      // Updated with a smaller value, no padding
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts, true).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_get_delete_mark() {
        let mut page = Page::new_empty();
        <Page as DoubleHashPage>::init(&mut page);

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert multiple key-value pairs with keys larger than 8 bytes
        let keys = vec![
            vec![1, 2, 1],
            vec![1, 2, 3],
            vec![1, 2, 3, 4],
            vec![1, 2, 4],
            vec![1, 2, 5],
            vec![2, 1, 1],
        ];
        let initial_values = vec![
            vec![1, 2, 1],
            vec![1, 2, 3],
            vec![1, 2, 3, 4],
            vec![1, 2, 4],
            vec![1, 2, 5],
            vec![2, 1, 1],
        ];

        for (key, value) in keys.iter().zip(initial_values.iter()) {
            page.insert(key, &pkey, ts, Timestamp::MAX, value)
                .expect("Failed to insert key-value pair");
        }

        for slot_id in 0..page.slot_count() {
            let slot = page.get_slot(slot_id).unwrap();
            let (k, pk, v) = page.get_key_pkey_val_with_slot(&slot);

            let del_result = page.mark_delete_slot_at_id(slot_id, ts + 1);
            assert!(del_result.is_ok());
            assert_eq!(
                del_result.as_ref().unwrap(),
                &(ts, (&initial_values[slot_id as usize]).clone())
            );
        }

        for i in 0..6 {
            let delete_mark_result =
                <Page as DoubleHashPage>::get_delete_mark_slot_id(&page, &keys[i], &pkey, ts + 1);
            assert!(delete_mark_result.is_ok());
            assert_eq!(delete_mark_result.unwrap(), i as u32);
        }
    }
}
