use crate::{
    access_method::AccessMethodError,
    mvcc_index::{MvccEntry, TxId},
    prelude::{Page, PageId, Timestamp, AVAILABLE_PAGE_SIZE},
};
use core::slice;
use std::{
    collections::{BTreeMap, HashMap},
    result::Result::Ok,
    sync::atomic::{AtomicU64, Ordering},
};
pub const BUCKET_NUM_SIZE: usize = std::mem::size_of::<u64>(); // Size of bucket_num (u64)
pub static HISTORY_SLOT_CMP_CNT: AtomicU64 = AtomicU64::new(0);

mod header {
    use crate::{
        page::{PageId, AVAILABLE_PAGE_SIZE},
        prelude::Timestamp,
    };
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    pub struct Header {
        next_page_id: PageId,
        next_frame_id: u32,
        total_bytes_used: u32, // (PAGE_HEADER_SIZE + slots + records)
        slot_count: u32,
        rec_start_offset: u32,
        //
        min_ts: Timestamp,
        max_ts: Timestamp,
        is_full: u8,
    }

    impl Header {
        pub fn from_bytes(bytes: &[u8]) -> Self {
            let mut current_pos = 0;
            let next_page_id = crate::page::PageId::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<crate::page::PageId>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<crate::page::PageId>();
            let next_frame_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let total_bytes_used = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
            let min_ts = Timestamp::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<Timestamp>();
            let max_ts = Timestamp::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<Timestamp>();
            let is_full = u8::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u8>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u8>();

            Header {
                next_page_id,
                next_frame_id,
                total_bytes_used,
                slot_count,
                rec_start_offset,
                min_ts,
                max_ts,
                is_full,
            }
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;
            bytes[current_pos..current_pos + std::mem::size_of::<crate::page::PageId>()]
                .copy_from_slice(&self.next_page_id.to_be_bytes());
            current_pos += std::mem::size_of::<crate::page::PageId>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.next_frame_id.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.rec_start_offset.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.min_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();
            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.max_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();
            bytes[current_pos..current_pos + std::mem::size_of::<u8>()]
                .copy_from_slice(&self.is_full.to_be_bytes());
            current_pos += std::mem::size_of::<u8>();
            bytes
        }

        pub fn new() -> Self {
            Header {
                next_page_id: PageId::MAX,
                next_frame_id: u32::MAX,
                total_bytes_used: PAGE_HEADER_SIZE as u32,
                slot_count: 0,
                rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
                min_ts: Timestamp::MAX,
                max_ts: Timestamp::MIN,
                is_full: 0,
            }
        }

        pub fn next_page(&self) -> Option<(PageId, u32)> {
            if self.next_page_id == PageId::MAX {
                None
            } else {
                Some((self.next_page_id, self.next_frame_id))
            }
        }

        pub fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
            self.next_page_id = next_page_id;
            self.next_frame_id = frame_id;
        }

        pub fn next_page_id(&self) -> Option<PageId> {
            if self.next_page_id == PageId::MAX {
                None
            } else {
                Some(self.next_page_id)
            }
        }

        pub fn is_full(&self) -> bool {
            self.is_full != 0
        }

        pub fn set_full(&mut self) {
            self.is_full = 1;
        }

        pub fn set_next_page_id(&mut self, next_page_id: PageId) {
            self.next_page_id = next_page_id;
        }

        pub fn next_frame_id(&self) -> u32 {
            self.next_frame_id
        }

        pub fn set_next_frame_id(&mut self, next_frame_id: u32) {
            self.next_frame_id = next_frame_id;
        }

        pub fn total_bytes_used(&self) -> usize {
            self.total_bytes_used as usize
        }

        pub fn set_total_bytes_used(&mut self, total_bytes_used: usize) {
            self.total_bytes_used = total_bytes_used as u32;
        }

        pub fn inc_total_bytes_used(&mut self, bytes: usize) {
            self.total_bytes_used += bytes as u32;
        }

        pub fn dec_total_bytes_used(&mut self, bytes: usize) {
            self.total_bytes_used -= bytes as u32;
        }

        pub fn slot_count(&self) -> usize {
            self.slot_count as usize
        }

        pub fn set_slot_count(&mut self, slot_count: usize) {
            self.slot_count = slot_count as u32;
        }

        pub fn inc_slot_count(&mut self) {
            self.slot_count += 1;
        }

        pub fn dec_slot_count(&mut self) {
            self.slot_count -= 1;
        }

        pub fn rec_start_offset(&self) -> usize {
            self.rec_start_offset as usize
        }

        pub fn set_rec_start_offset(&mut self, rec_start_offset: usize) {
            self.rec_start_offset = rec_start_offset as u32;
        }

        pub fn min_ts(&self) -> Timestamp {
            self.min_ts
        }

        pub fn set_min_ts(&mut self, min_ts: Timestamp) {
            self.min_ts = min_ts;
        }

        pub fn max_ts(&self) -> Timestamp {
            self.max_ts
        }

        pub fn set_max_ts(&mut self, max_ts: Timestamp) {
            self.max_ts = max_ts;
        }
    }
}
use header::*;

pub mod slot {
    use std::fmt::Debug;

    use crate::{mvcc_index::TxId, prelude::Timestamp};

    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(PartialEq, Copy, Clone)]
    #[repr(C)]
    pub struct Slot {
        key_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        pkey_size: u32,
        pkey_prefix: [u8; SLOT_PKEY_PREFIX_SIZE],
        tx_id: TxId,
        start_ts: Timestamp,
        end_ts: Timestamp,
        val_size: u32,
        offset: u32,
    }

    impl Debug for Slot {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Slot")
                .field("key_size", &self.key_size)
                .field(
                    "key_prefix",
                    &std::str::from_utf8(&self.key_prefix).unwrap(),
                )
                .field("pkey_size", &self.pkey_size)
                .field(
                    "pkey_prefix",
                    &std::str::from_utf8(&self.pkey_prefix).unwrap(),
                )
                .field("tx_id", &self.tx_id)
                .field("start_ts", &self.start_ts)
                .field("end_ts", &self.end_ts)
                .field("val_size", &self.val_size)
                .field("offset", &self.offset)
                .finish()
        }
    }

    impl Slot {
        pub unsafe fn unsafe_from_bytes(bytes: &[u8]) -> &Slot {
            &*(bytes.as_ptr() as *const Slot)
        }

        pub unsafe fn unsafe_mut_from_bytes(bytes: &[u8]) -> &mut Slot {
            &mut *(bytes.as_ptr() as *mut Slot)
        }

        pub unsafe fn unsafe_from_bytes_mut(bytes: &[u8]) -> &mut Slot {
            &mut *(bytes.as_ptr() as *mut Slot)
        }

        pub fn from_bytes(bytes: &[u8]) -> Self {
            let mut current_pos = 0;

            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();

            let mut key_prefix: [u8; 8] = [0u8; SLOT_KEY_PREFIX_SIZE];
            key_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            let pkey_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();

            let mut pkey_prefix: [u8; 8] = [0u8; SLOT_PKEY_PREFIX_SIZE];
            pkey_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            let tx_id = TxId::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<TxId>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<TxId>();

            let start_ts = Timestamp::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let end_ts = Timestamp::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );

            Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                tx_id,
                start_ts,
                end_ts,
                val_size,
                offset,
            }
        }

        pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0u8; SLOT_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.key_size.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();

            bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]
                .copy_from_slice(&self.key_prefix);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.pkey_size.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();

            bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]
                .copy_from_slice(&self.pkey_prefix);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + std::mem::size_of::<TxId>()]
                .copy_from_slice(&self.tx_id.to_be_bytes());
            current_pos += std::mem::size_of::<TxId>();

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.start_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.end_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();

            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(
            key: &[u8],
            pkey: &[u8],
            tx_id: TxId,
            start_ts: Timestamp,
            end_ts: Timestamp,
            val: &[u8],
            offset: usize,
        ) -> Self {
            let key_size = key.len() as u32;
            let val_size = val.len() as u32;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            let copy_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            key_prefix[..copy_len].copy_from_slice(&key[..copy_len]);

            let pkey_size = pkey.len() as u32;

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            let copy_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());
            pkey_prefix[..copy_len].copy_from_slice(&pkey[..copy_len]);

            Slot {
                key_size,
                key_prefix,
                pkey_size,
                pkey_prefix,
                tx_id,
                start_ts,
                end_ts,
                val_size,
                offset: offset as u32,
            }
        }

        pub fn key_size(&self) -> usize {
            self.key_size as usize
        }

        pub fn key_prefix(&self) -> &[u8] {
            &self.key_prefix
        }

        pub fn pkey_size(&self) -> usize {
            self.pkey_size as usize
        }

        pub fn pkey_prefix(&self) -> &[u8] {
            &self.pkey_prefix
        }

        pub fn tx_id(&self) -> TxId {
            self.tx_id
        }

        pub fn start_ts(&self) -> Timestamp {
            self.start_ts
        }

        pub fn end_ts(&self) -> Timestamp {
            self.end_ts
        }

        pub fn set_end_ts(&mut self, end_ts: Timestamp) {
            self.end_ts = end_ts;
        }

        pub fn val_size(&self) -> usize {
            self.val_size as usize
        }

        pub fn set_val_size(&mut self, val_size: usize) {
            self.val_size = val_size as u32;
        }

        pub fn offset(&self) -> usize {
            self.offset as usize
        }

        pub fn set_offset(&mut self, offset: usize) {
            self.offset = offset as u32;
        }

        pub fn rec_size(&self) -> usize {
            self.key_size() + self.pkey_size() + self.val_size()
        }
    }
}
use slot::*;

pub mod record {
    #[derive(Debug)]
    pub struct Record {
        key: Vec<u8>,  // hash key
        pkey: Vec<u8>, // primary key
        val: Vec<u8>,
    }

    pub struct RecordRef<'a> {
        pub key: &'a [u8],
        pub pkey: &'a [u8],
        pub val: &'a [u8],
    }

    impl<'a> RecordRef<'a> {
        pub fn new(key: &'a [u8], pkey: &'a [u8], val: &'a [u8]) -> Self {
            RecordRef { key, pkey, val }
        }
        pub fn key(&self) -> &'a [u8] {
            self.key
        }
        pub fn pkey(&self) -> &'a [u8] {
            self.pkey
        }
        pub fn val(&self) -> &'a [u8] {
            self.val
        }
        pub fn size(&self) -> usize {
            self.key.len() + self.pkey.len() + self.val.len()
        }
        pub fn sort_key(&self) -> &[u8] {
            &self.pkey
        }
        pub fn from_bytes(
            bytes: &'a [u8],
            key_size: usize,
            pkey_size: usize,
            val_size: usize,
        ) -> Self {
            let key = &bytes[..key_size];
            let pkey = &bytes[key_size..key_size + pkey_size];
            let val = &bytes[key_size + pkey_size..key_size + pkey_size + val_size];
            Self { key, pkey, val }
        }
    }

    impl Record {
        pub fn _from_bytes(
            bytes: &[u8],
            key_size: usize,
            pkey_size: usize,
            val_size: usize,
        ) -> Self {
            if bytes.len() != key_size + pkey_size + val_size {
                panic!("Invalid record size");
            }
            let key = bytes[..key_size].to_vec();
            let pkey = bytes[key_size..key_size + pkey_size].to_vec();
            let val = bytes[key_size + pkey_size..key_size + pkey_size + val_size].to_vec();
            Record { key, pkey, val }
        }

        pub fn _to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(self.key.len() + self.pkey.len() + self.val.len());

            bytes.extend_from_slice(&self.key);
            bytes.extend_from_slice(&self.pkey);
            bytes.extend_from_slice(&self.val);

            bytes
        }

        pub fn _new(key: &[u8], pkey: &[u8], val: &[u8]) -> Self {
            Record {
                key: key.to_vec(),
                pkey: pkey.to_vec(),
                val: val.to_vec(),
            }
        }

        pub fn key(&self) -> &[u8] {
            &self.key
        }

        pub fn pkey(&self) -> &[u8] {
            &self.pkey
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

        pub fn size(&self) -> usize {
            self.key.len() + self.pkey.len() + self.val.len()
        }

        pub fn sort_key(&self) -> &[u8] {
            &self.pkey
        }
    }
}
use record::*;

use super::hash_common::{KVWithTs, MvccEntryLoc, RowDelta};

pub trait HashJoinPage {
    fn init(&mut self);

    fn insert_heap_no_repair(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError>;
    fn insert_recent_history(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError>;

    fn upsert_history(&mut self, entry: &mut MvccEntry) -> Result<(), AccessMethodError>;
    fn insert_at_slot_id(
        &mut self,
        entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<(), AccessMethodError>;

    fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError>;
    fn heap_get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError>;
    fn heap_get_read_repair(
        &self,
        pkey: &[u8],
        ts: &Timestamp,
        versions: &mut BTreeMap<Timestamp, (MvccEntryLoc, bool)>, // is_need_repair
    ) -> Result<MvccEntry, AccessMethodError>;
    fn get_history(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError>;
    fn get_entry_at_slot_id(&self, slot_id: usize) -> Result<MvccEntry, AccessMethodError>;

    fn update(&mut self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError>;
    fn update_at_slot_id(
        &mut self,
        entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError>;
    fn update_heap_write_repair(
        &mut self,
        entry: &MvccEntry,
        already_inserted: bool,
        already_repaired: bool,
    ) -> Result<(), AccessMethodError>;

    fn delete(&mut self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError>;
    fn delete_at_slot_id(
        &mut self,
        ts: &Timestamp,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError>;

    fn chained_hash_garbage_collect(&mut self, ts: &Timestamp) -> Result<(), AccessMethodError>;
    fn heap_hash_garbage_collect(&mut self, ts: &Timestamp) -> Result<(), AccessMethodError>;
    fn search_slot(&self, sort_key: &[u8]) -> (bool, usize) {
        self.binary_search(sort_key)
        // self.linear_search(sort_key)
    }
    fn binary_search(&self, sort_key: &[u8]) -> (bool, usize); // (found, slot_id)
    fn linear_search(&self, sort_key: &[u8]) -> (bool, usize); // (found, slot_id)
    /// Return slot idx of the first slot whose end_ts is greater than to target_end_ts
    fn binary_search_by_end_ts(&self, target_end_ts: Timestamp) -> usize;

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8];
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    // write vector of bytes into offset, skip using vector to collect bytes
    fn write_bytes_slice(&mut self, offset: usize, bytes: &[&[u8]]);

    fn free_space_before_compaction(&self) -> usize {
        self.header().rec_start_offset() - self.slot_offset(self.slot_count())
    }
    fn free_space_after_compaction(&self) -> usize {
        AVAILABLE_PAGE_SIZE - self.header().total_bytes_used()
    }
    fn require_space(entry: &MvccEntry) -> usize {
        SLOT_SIZE + RecordRef::new(entry.key(), entry.pkey(), entry.value()).size()
    }

    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);
    fn next_page(&self) -> Option<(PageId, u32)> {
        self.header().next_page()
    }
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        let mut header = self.header();
        header.set_next_page(next_page_id, frame_id);
        self.set_header(&header);
    }
    fn set_next_page_frame(&mut self, page_frame: (PageId, u32)) {
        let mut header = self.header();
        header.set_next_page(page_frame.0, page_frame.1);
        self.set_header(&header);
    }
    fn rec_start_offset(&self) -> usize {
        self.header().rec_start_offset()
    }
    fn set_rec_start_offset(&mut self, rec_start_offset: usize) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }
    fn slot_count(&self) -> usize {
        self.header().slot_count()
    }
    fn set_slot_count(&mut self, slot_count: usize) {
        let mut header = self.header();
        header.set_slot_count(slot_count);
        self.set_header(&header);
    }
    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE + self.slot_count() * SLOT_SIZE
    }
    fn total_bytes_used(&self) -> usize {
        self.header().total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: usize) {
        let mut header = self.header();
        header.set_total_bytes_used(total_bytes_used);
        self.set_header(&header);
    }
    fn increase_total_bytes_used(&mut self, bytes: usize) {
        self.set_total_bytes_used(self.total_bytes_used() + bytes);
    }
    fn decrease_total_bytes_used(&mut self, bytes: usize) {
        self.set_total_bytes_used(self.total_bytes_used() - bytes);
    }
    fn increase_slot_count(&mut self) {
        let mut header = self.header();
        header.inc_slot_count();
        self.set_header(&header);
    }
    fn decrease_slot_count(&mut self) {
        let mut header = self.header();
        header.dec_slot_count();
        self.set_header(&header);
    }

    fn slot_offset(&self, slot_id: usize) -> usize {
        PAGE_HEADER_SIZE + slot_id * SLOT_SIZE
    }
    fn slot(&self, slot_id: usize) -> Slot {
        // Slot::from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
        self.unsafe_slot(slot_id).to_owned()
    }

    fn unsafe_slot_mut(&mut self, slot_id: usize) -> &mut Slot {
        // Slot::from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
        unsafe {
            Slot::unsafe_mut_from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
        }
    }

    fn unsafe_slot(&self, slot_id: usize) -> &Slot {
        unsafe { Slot::unsafe_from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE)) }
    }

    fn _record_from_slotid(&self, slot_id: usize) -> Record {
        let slot = self.unsafe_slot(slot_id);
        self._record_from_slot(&slot)
    }
    fn record_ref_from_slotid(&self, slot_id: usize) -> RecordRef {
        let slot = self.unsafe_slot(slot_id);
        self.record_ref_from_slot(&slot)
    }
    fn _record_from_slot(&self, slot: &Slot) -> Record {
        Record::_from_bytes(
            self.read_bytes(slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        )
    }
    fn record_ref_from_slot(&self, slot: &Slot) -> RecordRef {
        RecordRef::from_bytes(
            self.read_bytes(slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        )
    }
    fn set_slot(&mut self, slot_id: usize, slot: &Slot) {
        // self.write_bytes(self.slot_offset(slot_id), &slot.to_bytes());
        let slot_mut_ref = unsafe {
            Slot::unsafe_mut_from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
        };
        *slot_mut_ref = *slot;
    }

    fn _set_record_at_slot_id(&mut self, slot_id: usize, rec: &Record) {
        let slot = self.unsafe_slot(slot_id);
        self.write_bytes(slot.offset(), &rec._to_bytes());
    }

    fn _set_record_at_offset(&mut self, offset: usize, rec: &Record) {
        self.write_bytes(offset, &rec._to_bytes());
    }

    fn set_record_ref_at_offset(&mut self, offset: usize, rec: &RecordRef) {
        // self.write_bytes(offset, &rec.to_bytes());
        let bytes_slice = [rec.key(), rec.pkey(), rec.val()];
        self.write_bytes_slice(offset, &bytes_slice);
    }

    fn insert_slot_at_id(&mut self, slot: &Slot, slot_id: usize);
    fn delete_slot_at_id(&mut self, slot_id: usize);

    fn _insert_rec_at_offset(&mut self, rec: &Record, offset: usize);
    fn insert_rec_ref_at_offset(&mut self, rec: &RecordRef, offset: usize);
    /// Returns `Some(Record)` if the slot's pkey exactly matches `pkey`.
    /// Otherwise returns `None`.
    fn slot_pkey_matches(&self, slot: &Slot, pkey: &[u8]) -> Option<RecordRef>;

    fn get_pkey_from_slot(&self, slot: &Slot) -> &[u8];

    /// Compare the slot’s pkey at slot_id with `search_key`.
    /// Returns Ordering::Less if slot’s pkey < search_key,
    ///         Ordering::Equal if slot’s pkey == search_key,
    ///         Ordering::Greater if slot’s pkey > search_key.
    fn slot_cmp_key(&self, slot_id: usize, search_key: &[u8]) -> std::cmp::Ordering;

    /// Returns a human-readable status string for this page.
    /// - `kv count`: number of key–value pairs (i.e. the slot count)
    /// - `usage`: percentage of the page space used (based on total_bytes_used)
    /// - `free_space_without_compaction`: free space computed via `free_space_before_compaction`
    /// - `free_space_after_compaction`: free space computed via `free_space_after_compaction`
    fn stat(&self) -> String;

    // scan_key for heap, recent, and history

    /// For “heap” pages, we do a linear scan.  But *instead* of returning a Vec,
    /// we pass in a &mut HashMap so we can update the "best version" logic
    /// directly without creating an intermediate Vec.
    fn scan_key_heap_into_best(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError>;

    /// used in scan_delta
    fn scan_delta_heap(
        &self,
        from_ts: &Timestamp,
        to_ts: &Timestamp,
        delta_map: &mut HashMap<Vec<u8>, RowDelta>,
    );

    /// Returns one MvccEntry per pkey for `search_key` visible at `ts`,
    /// using a page sorted by end_ts. O(logN + K) time, skipping old versions.
    fn scan_key_history_into(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    );
    fn scan_key_recent_into(
        &self,
        search_pkey: &[u8],
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    );

    fn scan_recent_into(&self, ts: &Timestamp, results: &mut Vec<MvccEntry>);

    fn scan_history_into(&self, ts: &Timestamp, results: &mut Vec<MvccEntry>);

    fn scan_key_heap_into_best_read_repair(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        best_map: &mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError>;

    fn scan_all_for_gc_read_repair(
        &self,
        best_map: &mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    );

    fn max_end_ts(&self) -> Timestamp;
}

impl HashJoinPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        HashJoinPage::set_header(&mut *self, &header);
    }

    fn insert_recent_history(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let new_rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        let needed_space = SLOT_SIZE + new_rec.size();
        if needed_space > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if needed_space > self.free_space_before_compaction() {
            if needed_space > self.free_space_after_compaction() {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }

        let (found, slot_id) = self.search_slot(new_rec.sort_key());
        if found {
            // unreachable!("no duplicate keys should be inserted");
            return Err(AccessMethodError::KeyDuplicate);
        }
        HashJoinPage::insert_at_slot_id(&mut *self, entry, slot_id)
    }
    fn insert_at_slot_id(
        &mut self,
        entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<(), AccessMethodError> {
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());

        let new_rec_start_offset = HashJoinPage::header(&*self).rec_start_offset() - rec.size();
        let slot = Slot::new(
            entry.key(),
            entry.pkey(),
            0, // tx_id not used now
            entry.start_ts(),
            entry.end_ts(),
            entry.value(),
            new_rec_start_offset,
        );

        HashJoinPage::insert_slot_at_id(&mut *self, &slot, slot_id);
        HashJoinPage::insert_rec_ref_at_offset(&mut *self, &rec, new_rec_start_offset);

        Ok(())
    }

    fn insert_heap_no_repair(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        if SLOT_SIZE + rec.size() > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if SLOT_SIZE + rec.size() > HashJoinPage::free_space_before_compaction(&*self) {
            if SLOT_SIZE + rec.size() > HashJoinPage::free_space_after_compaction(&*self) {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: (JUN) Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }
        self.insert_at_slot_id(entry, self.slot_count())
    }

    fn upsert_history(&mut self, entry: &mut MvccEntry) -> Result<(), AccessMethodError> {
        // 1) Check record size constraints
        let new_rec_ref = RecordRef::new(entry.key(), entry.pkey(), entry.value());
        let needed_space = SLOT_SIZE + new_rec_ref.size();
        if needed_space > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if needed_space > self.free_space_before_compaction() {
            if needed_space > self.free_space_after_compaction() {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }

        // // 2) For any existing slot whose end_ts() > entry.start_ts() (found via binary_search_by_end_ts),
        // //    if that slot’s pkey matches, adjust end_ts either on the old version or the new version.
        // let start_idx = self.binary_search_by_end_ts(entry.start_ts());
        // for idx in start_idx..self.slot_count() {
        //     let slot = self.slot(idx);
        //     // If slot.start_ts() <= new entry’s start_ts,
        //     // we fix the old version’s end_ts to the new entry’s start_ts.
        //     // Else the new version’s end_ts is set to the old slot’s start_ts.
        //     if slot.start_ts() <= entry.start_ts() {
        //         // Use slot_pkey_matches to skip reading record unless prefix is promising.
        //         if let Some(_existing_rec) = self.slot_pkey_matches(&slot, entry.pkey()) {
        //             // The slot’s pkey matches => fix up its end_ts
        //             let mut updated_slot = slot;
        //             updated_slot.set_end_ts(entry.start_ts());
        //             // Remove the slot from idx, then insert it at start_idx to keep it sorted by end_ts
        //             self.delete_slot_at_id(idx);
        //             self.insert_slot_at_id(&updated_slot, start_idx);
        //         }
        //     } else {
        //         // slot.start_ts() > entry.start_ts() => new version’s end_ts = slot.start_ts()
        //         if let Some(_existing_rec) = self.slot_pkey_matches(&slot, entry.pkey()) {
        //             entry.set_end_ts(&slot.start_ts());
        //         }
        //     }
        // }

        // 3) Now insert the new record into the page.
        let new_rec_offset = self.rec_start_offset() - new_rec_ref.size();

        // Build the slot for the new version
        let mut new_slot = Slot::new(
            entry.key(),
            entry.pkey(),
            0, // tx_id not used
            entry.start_ts(),
            entry.end_ts(),
            entry.value(),
            0, // offset => updated below
        );

        // Insert in sorted order by end_ts
        let new_slot_idx = self.binary_search_by_end_ts(new_slot.end_ts());
        new_slot.set_offset(new_rec_offset);

        self.insert_slot_at_id(&new_slot, new_slot_idx);
        self.insert_rec_ref_at_offset(&new_rec_ref, new_rec_offset);

        Ok(())
    }

    fn get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        let slot = self.unsafe_slot(slot_id);
        if *ts < slot.start_ts() || slot.end_ts() <= *ts {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        self.get_entry_at_slot_id(slot_id)
    }
    fn get_history(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let start_idx = self.binary_search_by_end_ts(*ts);
        for idx in start_idx..self.slot_count() {
            // HISTORY_SLOT_CMP_CNT.fetch_add(1, Ordering::Relaxed);
            let slot = self.unsafe_slot(idx);
            if slot.start_ts() <= *ts {
                if let Some(rec) = self.slot_pkey_matches(&slot, pkey) {
                    return Ok(MvccEntry::new(
                        rec.key().to_vec(),
                        rec.pkey().to_vec(),
                        rec.val().to_vec(),
                        slot.start_ts(),
                        slot.end_ts(),
                    ));
                }
            }
        }
        Err(AccessMethodError::KeyNotFound)
    }
    fn get_entry_at_slot_id(&self, slot_id: usize) -> Result<MvccEntry, AccessMethodError> {
        let slot = self.unsafe_slot(slot_id);
        let rec = self.record_ref_from_slotid(slot_id);
        Ok(MvccEntry::new(
            rec.key().to_vec(),
            rec.pkey().to_vec(),
            rec.val().to_vec(),
            slot.start_ts(),
            slot.end_ts(),
        ))
    }

    fn heap_get(&self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let mut best_candidate: Option<(usize, Timestamp)> = None;

        for i in 0..self.slot_count() {
            let slot = self.unsafe_slot(i);
            let start = slot.start_ts();
            if start > *ts {
                // This slot is too new; skip it.
                continue;
            }
            let end = slot.end_ts();
            if end < *ts {
                // This slot is too old; skip it.
                continue;
            }

            // Attempt a cheap pkey check first; if no match, skip it.
            if let Some(_) = self.slot_pkey_matches(slot, pkey) {
                // The slot's pkey is correct. Now let's see if it's valid for this timestamp.
                // we update the best candidate.
                match best_candidate {
                    Some((_, best_start)) if start > best_start => {
                        best_candidate = Some((i, start));
                    }
                    None => {
                        best_candidate = Some((i, start));
                    }
                    _ => {}
                }
            }
        }

        // If we found a candidate, return its MVCC entry.
        if let Some((idx, _)) = best_candidate {
            self.get_entry_at_slot_id(idx)
        } else {
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn heap_get_read_repair(
        &self,
        pkey: &[u8],
        ts: &Timestamp,
        versions: &mut BTreeMap<Timestamp, (MvccEntryLoc, bool)>, // is_need_repair
    ) -> Result<MvccEntry, AccessMethodError> {
        let mut best_candidate: Option<(usize, Timestamp)> = None;

        for i in 0..self.slot_count() {
            // Attempt a cheap pkey check first; if no match, skip it.
            let slot = &self.unsafe_slot(i);
            if let Some(_) = self.slot_pkey_matches(slot, pkey) {
                // The slot's pkey is correct. Now let's see if it's valid for this timestamp.
                let start = slot.start_ts();

                let e = versions
                    .entry(start)
                    .or_insert((MvccEntryLoc::new(self.get_id(), i as u32), true));
                if slot.end_ts() != u64::MAX {
                    e.1 = false;
                }
                if start <= *ts {
                    // If this start_ts is the largest we've seen so far (still ≤ ts),
                    // we update the best candidate.
                    match best_candidate {
                        Some((_, best_start)) if start > best_start => {
                            best_candidate = Some((i, start));
                        }
                        None => {
                            best_candidate = Some((i, start));
                        }
                        _ => {}
                    }
                }
            }
        }

        // If we found a candidate, return its MVCC entry.
        if let Some((idx, _)) = best_candidate {
            self.get_entry_at_slot_id(idx)
        } else {
            Err(AccessMethodError::KeyNotFound)
        }
    }

    fn update(&mut self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        let slot = HashJoinPage::unsafe_slot(&*self, slot_id);
        if slot.start_ts() > entry.start_ts() {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        HashJoinPage::update_at_slot_id(&mut *self, entry, slot_id)
    }
    fn update_at_slot_id(
        &mut self,
        entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError> {
        let old_slot = HashJoinPage::slot(self, slot_id);
        if entry.start_ts() < old_slot.start_ts() {
            let mut old_entry = entry.clone();
            old_entry.set_end_ts(&old_slot.start_ts());
            return Ok(old_entry);
        }

        let mut new_slot = Slot::new(
            entry.key(),
            entry.pkey(),
            0,
            entry.start_ts(),
            entry.end_ts(),
            entry.value(),
            0, // for temporary use
        );
        let new_rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());

        let old_rec = HashJoinPage::record_ref_from_slotid(self, slot_id);

        let old_rec_size = old_rec.size();
        let new_rec_size = new_rec.size();

        let new_rec_offset;

        if old_rec.key() != new_rec.key() {
            return Err(AccessMethodError::KeyNotFound);
        }
        let old_entry = MvccEntry::new(
            old_rec.key().to_vec(),
            old_rec.pkey().to_vec(),
            old_rec.val().to_vec(),
            old_slot.start_ts(),
            entry.start_ts(),
        );
        // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`
        if new_rec_size <= old_rec_size || old_slot.offset() == HashJoinPage::rec_start_offset(self)
        {
            new_rec_offset = old_slot.offset() + old_rec_size - new_rec_size;
            if new_rec_offset < HashJoinPage::slot_end_offset(self) {
                // TODO: Compact the page
                self.delete_slot_at_id(slot_id);
                self.decrease_total_bytes_used(old_rec_size);
                // Reach here means new_rec_size > old_rec_size and offset matches `rec_start_offset`
                self.set_rec_start_offset(self.rec_start_offset() + old_rec_size);
                return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
            }
            self.set_record_ref_at_offset(new_rec_offset, &new_rec);
            if new_rec_size < old_rec_size {
                HashJoinPage::write_bytes(
                    self,
                    old_slot.offset(),
                    &vec![0; old_rec_size - new_rec_size],
                );
            }
            if old_slot.offset() == HashJoinPage::rec_start_offset(self) {
                HashJoinPage::set_rec_start_offset(self, new_rec_offset);
            }
        }
        // Case 3: New value is larger and offset doesn't match `rec_start_offset`
        else {
            if HashJoinPage::slot_end_offset(self) + new_rec_size
                > HashJoinPage::rec_start_offset(self)
            {
                // TODO: Compact the page
                self.delete_slot_at_id(slot_id);
                self.decrease_total_bytes_used(old_rec_size);
                return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
            }
            new_rec_offset = HashJoinPage::rec_start_offset(self) - new_rec_size;
            self.set_record_ref_at_offset(new_rec_offset, &new_rec);
            self.set_rec_start_offset(new_rec_offset);
        }
        new_slot.set_offset(new_rec_offset);
        HashJoinPage::set_slot(self, slot_id, &new_slot);

        HashJoinPage::increase_total_bytes_used(self, new_rec_size);
        HashJoinPage::decrease_total_bytes_used(self, old_rec_size);
        Ok(old_entry)
    }

    fn update_heap_write_repair(
        &mut self,
        entry: &MvccEntry,
        already_inserted: bool,
        already_repaired: bool,
    ) -> Result<(), AccessMethodError> {
        let mut did_repair = already_repaired;
        let mut did_insert = already_inserted;

        let pkey = entry.pkey();
        let st = entry.start_ts();

        // 1. repair
        if !already_repaired {
            for i in 0..self.slot_count() {
                let slot = self.unsafe_slot(i);
                if slot.end_ts() != Timestamp::MAX {
                    continue;
                }
                if let Some(_) = self.slot_pkey_matches(&slot, pkey) {
                    if slot.start_ts() < st {
                        let mut new_slot = *slot;
                        new_slot.set_end_ts(st);
                        self.set_slot(i, &new_slot);
                        did_repair = true;
                        break;
                    }
                }
            }
        }

        // 2. insert
        if !already_inserted {
            match self.insert_heap_no_repair(entry) {
                Ok(_) => {
                    did_insert = true;
                }
                Err(AccessMethodError::OutOfSpace) => { /* skip */ }
                Err(e) => return Err(e),
            }
        }

        // 3. result
        match (did_repair, did_insert) {
            (true, true) => Ok(()),
            (true, false) => Err(AccessMethodError::UpdateReapiredButNotInseted),
            (false, true) => Err(AccessMethodError::UpdateInsertedButNotReapired),
            (false, false) => Err(AccessMethodError::KeyNotFound),
        }
    }

    fn delete(&mut self, pkey: &[u8], ts: &Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        HashJoinPage::delete_at_slot_id(&mut *self, ts, slot_id)
    }
    fn delete_at_slot_id(
        &mut self,
        ts: &Timestamp,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError> {
        let ts = *ts;
        let slot = self.slot(slot_id);
        if slot.start_ts() > ts {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        let rec = self.record_ref_from_slotid(slot_id);
        let rec_size = rec.size();
        let old_entry = MvccEntry::new(
            rec.key().to_vec(),
            rec.pkey().to_vec(),
            rec.val().to_vec(),
            slot.start_ts(),
            slot.end_ts(),
        );
        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(slot.offset() + rec_size);
        }
        HashJoinPage::delete_slot_at_id(self, slot_id);
        HashJoinPage::decrease_total_bytes_used(self, rec_size);

        Ok(old_entry)
    }

    fn chained_hash_garbage_collect(&mut self, ts: &Timestamp) -> Result<(), AccessMethodError> {
        let end_idx = self.binary_search_by_end_ts(*ts);

        let mut total_deleted_rec_size = 0;
        let mut deleted_slots = 0;

        for idx in (0..end_idx).rev() {
            let slot = self.unsafe_slot(idx);
            if slot.end_ts() > *ts {
                panic!("Page should be sorted by end_ts");
            }
            let rec = self.record_ref_from_slotid(idx);
            total_deleted_rec_size += rec.size();
            deleted_slots += 1;
            if slot.offset() == self.rec_start_offset() {
                self.set_rec_start_offset(slot.offset() + rec.size());
            }
        }
        assert_eq!(deleted_slots, end_idx);

        let current_slot_count = self.slot_count();
        if deleted_slots > 0 {
            let src_start = self.slot_offset(deleted_slots);
            let src_end = self.slot_offset(current_slot_count);
            let dest_offset = self.slot_offset(0);
            self.copy_within(src_start..src_end, dest_offset);

            self.set_slot_count(current_slot_count - deleted_slots);
        }

        HashJoinPage::decrease_total_bytes_used(
            self,
            deleted_slots * SLOT_SIZE + total_deleted_rec_size,
        );
        Ok(())
    }

    fn heap_hash_garbage_collect(&mut self, safe_ts: &Timestamp) -> Result<(), AccessMethodError> {
        let mut total_deleted_rec_size = 0;
        let mut deleted_slots = 0;
        let mut next_valid_slot_idx = 0;
        for idx in 0..self.slot_count() as usize {
            let slot = self.slot(idx);
            if slot.end_ts() > *safe_ts {
                // keep
                self.set_slot(next_valid_slot_idx, &slot);
                next_valid_slot_idx += 1;
                continue;
            }
            // delete
            // TODO: I think there maybe some bug on rec_start_offset, but I have no time to fix it.
            let rec = self.record_ref_from_slotid(idx);
            total_deleted_rec_size += rec.size();
            deleted_slots += 1;
            if slot.offset() == self.rec_start_offset() {
                self.set_rec_start_offset(slot.offset() + rec.size());
            }
        }

        let current_slot_count = self.slot_count();
        assert_eq!(deleted_slots, current_slot_count - next_valid_slot_idx);
        if deleted_slots > 0 {
            self.set_slot_count(current_slot_count - deleted_slots);
        }

        HashJoinPage::decrease_total_bytes_used(
            self,
            deleted_slots * SLOT_SIZE + total_deleted_rec_size,
        );
        Ok(())
    }

    fn binary_search(&self, search_key: &[u8]) -> (bool, usize) {
        use std::cmp::Ordering;

        let mut low = 0;
        let mut high = self.slot_count();

        while low < high {
            let mid = (low + high) / 2;

            match self.slot_cmp_key(mid, search_key) {
                Ordering::Less => {
                    // slot pkey < search_key => search the upper half
                    low = mid + 1;
                }
                Ordering::Equal => {
                    // Found exact match
                    return (true, mid);
                }
                Ordering::Greater => {
                    // slot pkey > search_key => search lower half
                    high = mid;
                }
            }
        }
        // If we exit the loop, no exact match was found.
        // 'low' is where 'search_key' could be inserted to keep order.
        (false, low)
    }

    fn linear_search(&self, search_key: &[u8]) -> (bool, usize) {
        for i in 0..self.slot_count() {
            let res = self.slot_cmp_key(i, search_key);
            if res == std::cmp::Ordering::Equal {
                return (true, i);
            }
            if res == std::cmp::Ordering::Greater {
                return (false, i);
            }
        }
        (false, self.slot_count())
    }

    fn binary_search_by_end_ts(&self, target_end_ts: Timestamp) -> usize {
        // Find the first index i such that slot[i].end_ts() > target_end_ts.
        // Standard upper_bound style binary search.
        let mut low = 0;
        let mut high = self.slot_count();
        while low < high {
            let mid = low + (high - low) / 2;
            let mid_end_ts = HashJoinPage::unsafe_slot(self, mid).end_ts();
            if mid_end_ts <= target_end_ts {
                // If the mid value is less than or equal to the target,
                // then the first element greater than target must be to the right.
                low = mid + 1;
            } else {
                // Otherwise, it could be mid or somewhere to the left.
                high = mid;
            }
        }
        // At this point, low == high, and low is the first index where end_ts > target_end_ts.
        // Return false for the boolean flag because we are not considering an "exact match"
        // (even if there were entries equal to target_end_ts, we ignore them).
        low
    }

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        &self[offset..offset + len]
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn header(&self) -> Header {
        Header::from_bytes(&self[0..PAGE_HEADER_SIZE])
    }

    fn set_header(&mut self, header: &Header) {
        HashJoinPage::write_bytes(&mut *self, 0, &header.to_bytes());
    }

    fn insert_slot_at_id(&mut self, slot: &Slot, slot_id: usize) {
        if slot_id < self.slot_count() {
            let start_offset = HashJoinPage::slot_offset(&*self, slot_id);
            let end_offset = HashJoinPage::slot_offset(&*self, self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset + SLOT_SIZE);
        }

        HashJoinPage::set_slot(&mut *self, slot_id, slot);

        self.increase_slot_count();
        self.increase_total_bytes_used(SLOT_SIZE);
    }

    fn delete_slot_at_id(&mut self, slot_id: usize) {
        if slot_id < self.slot_count() {
            let start_offset = HashJoinPage::slot_offset(&*self, slot_id + 1);
            let end_offset = HashJoinPage::slot_offset(&*self, self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset - SLOT_SIZE);
        }

        self.decrease_slot_count();
        self.decrease_total_bytes_used(SLOT_SIZE);
    }

    fn _insert_rec_at_offset(&mut self, rec: &Record, offset: usize) {
        HashJoinPage::write_bytes(&mut *self, offset, &rec._to_bytes());
        self.increase_total_bytes_used(rec.size());
        if offset < self.rec_start_offset() {
            self.set_rec_start_offset(offset);
        }
    }

    fn insert_rec_ref_at_offset(&mut self, rec: &RecordRef, offset: usize) {
        let rec_bytes_slice = [rec.key(), rec.pkey(), rec.val()];
        HashJoinPage::write_bytes_slice(&mut *self, offset, &rec_bytes_slice);
        self.increase_total_bytes_used(rec.size());
        if offset < self.rec_start_offset() {
            self.set_rec_start_offset(offset);
        }
    }

    /// Returns `Some(Record)` if the slot's pkey exactly matches `pkey`.
    /// Otherwise returns `None`.
    fn slot_pkey_matches(&self, slot: &Slot, pkey: &[u8]) -> Option<RecordRef> {
        // 1) Check pkey length first
        if pkey.len() != slot.pkey_size() {
            return None;
        }

        // 2) Compare prefix
        let prefix_len = std::cmp::min(SLOT_PKEY_PREFIX_SIZE, pkey.len());
        let slot_prefix = &slot.pkey_prefix()[..prefix_len];
        let input_prefix = &pkey[..prefix_len];
        if slot_prefix != input_prefix {
            return None;
        }

        // If the entire pkey fits within the prefix, we've already confirmed equality:
        if pkey.len() <= SLOT_PKEY_PREFIX_SIZE {
            // same length, same prefix => match
            // But we still need to read the record to return it.
            let rec_bytes = self.read_bytes(slot.offset(), slot.rec_size());
            let rec = RecordRef::from_bytes(
                rec_bytes,
                slot.key_size(),
                slot.pkey_size(),
                slot.val_size(),
            );
            return Some(rec);
        }

        // 3) pkey is longer than the prefix => compare the remainder.
        let rec = self.record_ref_from_slot(slot);
        if rec.pkey() == pkey {
            Some(rec)
        } else {
            None
        }
    }

    fn get_pkey_from_slot(&self, slot: &Slot) -> &[u8] {
        // 2) Compare prefix
        let prefix_len = std::cmp::min(SLOT_PKEY_PREFIX_SIZE, slot.pkey_size());
        let slot_prefix = &slot.pkey_prefix()[..prefix_len];

        let bytes = self.read_bytes(slot.offset(), slot.rec_size());
        let key_size = slot.key_size();
        let pkey_size = slot.pkey_size();
        &bytes[key_size..key_size + pkey_size]
    }

    /// Compare the slot’s pkey at slot_id with `search_key`.
    /// Returns Ordering::Less if slot’s pkey < search_key,
    ///         Ordering::Equal if slot’s pkey == search_key,
    ///         Ordering::Greater if slot’s pkey > search_key.
    fn slot_cmp_key(&self, slot_id: usize, search_key: &[u8]) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let slot = self.unsafe_slot(slot_id);

        // 1) First compare lengths
        let slot_len = slot.pkey_size();
        let input_len = search_key.len();
        // If they differ, use length to decide “less” or “greater” or keep going.
        // But if your key ordering truly depends on lexical order, you might want
        // to compare length only after comparing prefixes.  Usually, for lexical order:
        //
        //   "abc" < "abcd" because at the first difference we notice "abc" ended.
        //
        // But if you store your keys such that shorter < longer in all cases, you can do:
        if slot_len != input_len {
            // If your desired ordering is purely lexical, you'd do something
            // like comparing the shorter prefix first. For simplicity, let's do:
            return slot_len.cmp(&input_len);
        }

        // 2) Compare the prefix (up to 8 bytes).
        let prefix_len = std::cmp::min(SLOT_PKEY_PREFIX_SIZE, input_len);
        let slot_prefix = &slot.pkey_prefix()[..prefix_len];
        let input_prefix = &search_key[..prefix_len];
        match slot_prefix.cmp(input_prefix) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => {
                // If the entire key fits within the 8-byte prefix, we’re done.
                // The lengths are equal, and the prefix is the same => full match.
                if input_len <= SLOT_PKEY_PREFIX_SIZE {
                    return Ordering::Equal;
                }
                // Otherwise, the keys are longer than 8 bytes => compare remainder
            }
        }

        // 3) We must read the entire pkey from the record area, then compare it to `search_key`.
        let rec = self.record_ref_from_slot(&slot);
        let slot_pkey = rec.pkey();

        slot_pkey.cmp(search_key)
    }

    /// Returns a human-readable status string for this page.
    /// - `kv count`: number of key–value pairs (i.e. the slot count)
    /// - `usage`: percentage of the page space used (based on total_bytes_used)
    /// - `free_space_without_compaction`: free space computed via `free_space_before_compaction`
    /// - `free_space_after_compaction`: free space computed via `free_space_after_compaction`
    fn stat(&self) -> String {
        let slot_count = self.slot_count();
        // Total bytes used in this page, as maintained in the header.
        let used_bytes = self.header().total_bytes_used();
        let usage_percent = (used_bytes as f64 / AVAILABLE_PAGE_SIZE as f64) * 100.0;
        // Free space without compaction: this is the gap between where the records start
        // and the end of the slot area.
        let free_before = HashJoinPage::free_space_before_compaction(self);

        format!(
            "PageId {}: kv count: {}, usage: {:.2}% ({} bytes used / {} total), free_before: {}",
            self.get_id(),
            slot_count,
            usage_percent,
            used_bytes,
            AVAILABLE_PAGE_SIZE,
            free_before,
        )
    }

    /// For “heap” pages, we do a linear scan.  But *instead* of returning a Vec,
    /// we pass in a &mut HashMap so we can update the "best version" logic
    /// directly without creating an intermediate Vec.
    fn scan_key_heap_into_best(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut res: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let ts = *ts;
        // For each slot:
        for slot_idx in 0..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);

            // 1) Compare prefix, etc. (same as your existing logic)
            let slot_key_len = slot.key_size();
            if slot_key_len != search_key.len() {
                continue;
            }
            let prefix_len = std::cmp::min(SLOT_KEY_PREFIX_SIZE, slot_key_len);
            let slot_prefix = &slot.key_prefix()[..prefix_len];
            let input_prefix = &search_key[..prefix_len];
            if slot_prefix != input_prefix {
                continue;
            }

            // 2) If prefix matches, load the record
            let rec = self.record_ref_from_slot(&slot);
            if rec.key() == search_key {
                let start_ts = slot.start_ts();
                if start_ts <= ts {
                    let pkey = rec.pkey();

                    res.insert(pkey.to_vec(), rec.val().to_vec());
                }
            }
        }

        Ok(res.into_iter().collect())
    }

    fn scan_delta_heap(
        &self,
        from_ts: &Timestamp,
        to_ts: &Timestamp,
        delta_map: &mut HashMap<Vec<u8>, RowDelta>,
    ) {
        // For each slot:
        for slot_idx in 0..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);

            let st = slot.start_ts();
            // 2) If prefix matches, load the record

            if &st <= to_ts {
                let rec = self.record_ref_from_slot(&slot);

                let entry = delta_map.get_mut(rec.pkey());
                if let Some(entry_v) = entry {
                    entry_v.to().cmp_and_swap(st, rec.key(), rec.val());
                } else {
                    delta_map.insert(rec.pkey().to_vec(), RowDelta::new());
                }
                if &st <= from_ts {
                    let entry = delta_map.get_mut(rec.pkey());
                    if let Some(entry_v) = entry {
                        entry_v.from().cmp_and_swap(st, rec.key(), rec.val());
                    }
                }
            }
        }
    }

    /// For “heap” pages, we do a linear scan.  But *instead* of returning a Vec,
    /// we pass in a &mut HashMap so we can update the "best version" logic
    /// directly without creating an intermediate Vec.
    fn scan_key_heap_into_best_read_repair(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        versions_map: &mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, AccessMethodError> {
        let mut res: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let page_id = self.get_id();
        let ts = *ts;
        // For each slot:
        for slot_idx in 0..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);

            // 1) Compare prefix, etc. (same as your existing logic)
            let slot_key_len = slot.key_size();
            if slot_key_len != search_key.len() {
                continue;
            }
            let prefix_len = std::cmp::min(SLOT_KEY_PREFIX_SIZE, slot_key_len);
            let slot_prefix = &slot.key_prefix()[..prefix_len];
            let input_prefix = &search_key[..prefix_len];
            if slot_prefix != input_prefix {
                continue;
            }

            // 2) If prefix matches, load the record
            let rec = self.record_ref_from_slot(&slot);
            if rec.key() == search_key {
                let start_ts = slot.start_ts();
                if start_ts <= ts {
                    let pkey = rec.pkey();

                    res.insert(pkey.to_vec(), rec.val().to_vec());

                    match versions_map.get_mut(pkey) {
                        Some(versions_vec_of_pkey) => {
                            versions_vec_of_pkey.push((
                                start_ts,
                                MvccEntryLoc::new(page_id, slot_idx as u32),
                                slot.end_ts() == Timestamp::MAX,
                            ));
                        }
                        None => {
                            let versions_vec_of_pkey =
                                versions_map.entry(pkey.to_vec()).or_insert(Vec::new());
                            versions_vec_of_pkey.push((
                                start_ts,
                                MvccEntryLoc::new(page_id, slot_idx as u32),
                                slot.end_ts() == Timestamp::MAX,
                            ));
                        }
                    }
                }
            }
        }

        Ok(res.into_iter().collect())
    }

    fn scan_all_for_gc_read_repair(
        &self,
        best_map: &mut HashMap<Vec<u8>, Vec<(Timestamp, MvccEntryLoc, bool)>>,
    ) {
        let page_id = self.get_id();

        // For each slot:
        for slot_idx in 0..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);
            if slot.end_ts() != Timestamp::MAX {
                continue;
            }

            let rec = self.record_ref_from_slot(&slot);
            let pk = rec.pkey();

            let get_res = best_map.get_mut(pk);
            if let Some(vec) = get_res {
                vec.push((
                    slot.start_ts(),
                    MvccEntryLoc::new(page_id, slot_idx as u32),
                    slot.end_ts() == Timestamp::MAX,
                ));
            } else {
                let vec = vec![(
                    slot.start_ts(),
                    MvccEntryLoc::new(page_id, slot_idx as u32),
                    slot.end_ts() == Timestamp::MAX,
                )];
                best_map.insert(pk.to_vec(), vec);
            }
        }
    }

    fn scan_key_recent_into(
        &self,
        search_pkey: &[u8],
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) {
        let ts = *ts;
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // 1) Check if this version is visible at time `ts`.
            let st = slot.start_ts();
            let et = slot.end_ts();
            // if !(st <= ts && (et == u64::MAX || ts < et)) {
            if ts < st {
                continue;
            }

            // 2) Compare pkey prefix to avoid reading the full record if mismatch.
            //    (We assume your slot stores the prefix in e.g. `slot.pkey_prefix()`.)
            let slot_key_len = slot.key_size();
            if slot_key_len != search_pkey.len() {
                // If lengths differ, definitely not a match.
                continue;
            }

            // Compare up to 8 bytes (or whatever your prefix size is).
            let prefix_len = std::cmp::min(SLOT_KEY_PREFIX_SIZE, slot_key_len);
            let slot_prefix = &slot.key_prefix()[..prefix_len];
            let input_prefix = &search_pkey[..prefix_len];
            if slot_prefix != input_prefix {
                continue;
            }

            // 3) If the prefix matches, read the entire record to confirm pkey equality.
            let rec = self.record_ref_from_slot(&slot);
            if rec.key() == search_pkey {
                // 4) Finally, build an MvccEntry
                let entry = MvccEntry::new(
                    rec.key().to_vec(),
                    rec.pkey().to_vec(),
                    rec.val().to_vec(),
                    st,
                    et,
                );
                results.push(entry);
            }
        }
    }

    fn scan_recent_into(&self, ts: &Timestamp, results: &mut Vec<MvccEntry>) {
        let ts = *ts;
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // 1) Check if this version is visible at time `ts`.
            let st = slot.start_ts();
            let et = slot.end_ts();
            if ts < st || et <= ts {
                continue;
            }

            // 2) Read the entire record to confirm pkey equality.
            let rec = self.record_ref_from_slot(&slot);
            // 4) Finally, build an MvccEntry
            let entry = MvccEntry::new(
                rec.key().to_vec(),
                rec.pkey().to_vec(),
                rec.val().to_vec(),
                st,
                et,
            );
            results.push(entry);
        }
    }

    fn scan_history_into(&self, ts: &Timestamp, results: &mut Vec<MvccEntry>) {
        let ts = *ts;
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // 1) Check if this version is visible at time `ts`.
            let st = slot.start_ts();
            let et = slot.end_ts();
            if ts < st || et <= ts {
                continue;
            }

            // 2) Read the entire record to confirm pkey equality.
            let rec = self.record_ref_from_slot(&slot);
            // 4) Finally, build an MvccEntry
            let entry = MvccEntry::new(
                rec.key().to_vec(),
                rec.pkey().to_vec(),
                rec.val().to_vec(),
                st,
                et,
            );
            results.push(entry);
        }
    }

    /// Returns one MvccEntry per pkey for `search_key` visible at `ts`,
    /// using a page sorted by end_ts. O(logN + K) time, skipping old versions.
    fn scan_key_history_into(
        &self,
        search_key: &[u8],
        ts: &Timestamp,
        results: &mut Vec<MvccEntry>,
    ) {
        let ts = *ts;

        // 1) skip older slots
        let start_idx = self.binary_search_by_end_ts(ts);
        // let start_idx = 0;

        for slot_idx in start_idx..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);

            // Check if st <= ts
            let st = slot.start_ts();
            let et = slot.end_ts();
            if st <= ts {
                // 2) prefix check
                let slot_key_len = slot.key_size();
                if slot_key_len != search_key.len() {
                    continue;
                }
                let prefix_len = std::cmp::min(SLOT_KEY_PREFIX_SIZE, slot_key_len);
                let slot_prefix = &slot.key_prefix()[..prefix_len];
                let input_prefix = &search_key[..prefix_len];
                if slot_prefix == input_prefix {
                    // read the entire record
                    let rec = self.record_ref_from_slot(&slot);
                    if rec.key() == search_key {
                        results.push(MvccEntry::new(
                            rec.key().to_vec(),
                            rec.pkey().to_vec(),
                            rec.val().to_vec(),
                            st,
                            et,
                        ));
                    }
                }
            }
        }
    }

    fn max_end_ts(&self) -> Timestamp {
        let mut max_ts = Timestamp::MIN;
        for slot_idx in 0..self.slot_count() {
            let slot = self.unsafe_slot(slot_idx);
            let et = slot.end_ts();
            if et > max_ts {
                max_ts = et;
            }

            if et == Timestamp::MAX {
                break;
            }
        }
        max_ts
    }

    fn write_bytes_slice(&mut self, mut offset: usize, bytes_vec: &[&[u8]]) {
        for bytes in bytes_vec {
            self[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
        }
    }
}

pub trait ChainedHashMetaPage {
    /// Initializes the meta page with the specified number of buckets.
    fn init(&mut self, num_buckets: usize);

    /// Retrieves the number of buckets from the meta page.
    fn get_bucket_num(&self) -> usize;

    /// Sets the number of buckets in the meta page.
    fn set_bucket_num(&mut self, num_buckets: usize);
}

impl ChainedHashMetaPage for Page {
    fn init(&mut self, num_buckets: usize) {
        let required_size = BUCKET_NUM_SIZE;
        assert!(
            required_size <= AVAILABLE_PAGE_SIZE,
            "Page size is insufficient for the number of buckets"
        );

        self.set_bucket_num(num_buckets);
        // only set bucket num here cause we need mem_pool to allocate pages
    }

    fn get_bucket_num(&self) -> usize {
        let bytes = &self[..BUCKET_NUM_SIZE];
        u64::from_be_bytes(bytes.try_into().unwrap()) as usize
    }

    fn set_bucket_num(&mut self, num_buckets: usize) {
        let bytes = &mut self[..BUCKET_NUM_SIZE];
        bytes.copy_from_slice(&(num_buckets as u64).to_be_bytes());
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hash;

    use super::*;

    #[test]
    fn test_page_initialization() {
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        assert_eq!(
            HashJoinPage::free_space_before_compaction(&page),
            AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE
        );
        assert_eq!(
            HashJoinPage::free_space_after_compaction(&page),
            AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE
        );
        assert_eq!(HashJoinPage::next_page(&page), None);
    }

    #[test]
    fn test_basic_insert_and_get() {
        // Create a new, empty page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        let i = 1;
        let key = format!("key-large-{:03}-extra", i).into_bytes();
        let pkey = format!("pkey-large-{:03}-extra", i).into_bytes();
        let value = format!("value-large-{:03}-extra", i).into_bytes();

        // Define timestamps. (Here we assume Timestamp is a numeric type like u64.)
        let start_ts: Timestamp = 100;
        let end_ts: Timestamp = 200;

        // Create an MVCC entry.
        let entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), start_ts, end_ts);

        // Insert the entry into the page.
        let insert_result = HashJoinPage::insert_recent_history(&mut page, &entry);
        assert!(insert_result.is_ok(), "Insert failed: {:?}", insert_result);

        // Retrieve the entry by its primary key and the timestamp.
        let fetched_entry_result = HashJoinPage::get(&page, &pkey, &start_ts);
        assert!(
            fetched_entry_result.is_ok(),
            "Failed to get entry: {:?}",
            fetched_entry_result
        );
        let fetched_entry = fetched_entry_result.unwrap();

        // Validate that the retrieved entry matches the inserted one.
        assert_eq!(fetched_entry.key(), entry.key());
        assert_eq!(fetched_entry.pkey(), entry.pkey());
        assert_eq!(fetched_entry.value(), entry.value());
        assert_eq!(fetched_entry.start_ts(), entry.start_ts());
        assert_eq!(fetched_entry.end_ts(), entry.end_ts());

        let slot = page.slot(0);
        let pkey_from_slot = page.get_pkey_from_slot(&slot);
        assert_eq!(pkey_from_slot, pkey);
    }

    #[test]
    fn test_multiple_insert_update_get_delete() {
        // Create and initialize a new empty page.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // --- Multiple Inserts ---
        let num_entries = 10;
        for i in 1..=num_entries {
            let key = format!("key-large-{:03}-extra", i).into_bytes();
            let pkey = format!("pkey-large-{:03}-extra", i).into_bytes();
            let value = format!("value-large-{:03}-extra", i).into_bytes();
            // For simplicity, set timestamps based on i.
            let start_ts: Timestamp = 100 + i as Timestamp;
            let end_ts: Timestamp = 200 + i as Timestamp;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            let res = HashJoinPage::insert_recent_history(&mut page, &entry);
            assert!(
                res.is_ok(),
                "Failed to insert entry for pkey {:?}: {:?}",
                pkey,
                res.err()
            );
        }

        // --- Update an Entry ---
        // For example, update entry i = 5 with a new value and timestamps.
        let update_index = 5;
        let update_pkey = format!("pkey-large-{:03}-extra", update_index).into_bytes();
        let updated_value = format!("updated-value-large-{:03}-extra", update_index).into_bytes();
        let new_start_ts: Timestamp = 150;
        let new_end_ts: Timestamp = 250;
        let update_entry = MvccEntry::new(
            format!("key-large-{:03}-extra", update_index).into_bytes(),
            update_pkey.clone(),
            updated_value.clone(),
            new_start_ts,
            new_end_ts,
        );
        let update_res = HashJoinPage::update(&mut page, &update_pkey, &update_entry);
        assert!(
            update_res.is_ok(),
            "Failed to update entry for pkey {:?}: {:?}",
            update_pkey,
            update_res.err()
        );

        // --- Simulate a Deletion ---
        // Assume deletion is represented by updating the entry to a tombstone.
        // For example, mark entry i = 3 as deleted.
        let delete_index = 3;
        let delete_pkey = format!("pkey-large-{:03}-extra", delete_index).into_bytes();
        let delete_res = HashJoinPage::delete(&mut page, &delete_pkey, &200);
        assert!(
            delete_res.is_ok(),
            "Failed to mark entry as deleted for pkey {:?}: {:?}",
            delete_pkey,
            delete_res.err()
        );

        // --- Get Operations ---

        // 1. Get a normal entry (e.g. i = 1).
        let get_index = 1;
        let get_pkey = format!("pkey-large-{:03}-extra", get_index).into_bytes();
        let get_ts = 100 + get_index as Timestamp;
        let get_res = HashJoinPage::get(&page, &get_pkey, &get_ts);
        assert!(
            get_res.is_ok(),
            "Failed to get entry for pkey {:?}: {:?}",
            get_pkey,
            get_res.err()
        );
        let entry = get_res.unwrap();
        assert_eq!(
            entry.value(),
            format!("value-large-{:03}-extra", get_index).as_bytes(),
            "Retrieved value does not match for pkey {:?}",
            get_pkey
        );

        // 2. Get the updated entry (i = 5).
        let get_updated_res = HashJoinPage::get(&page, &update_pkey, &new_start_ts);
        assert!(
            get_updated_res.is_ok(),
            "Failed to get updated entry for pkey {:?}: {:?}",
            update_pkey,
            get_updated_res.err()
        );
        let updated_entry = get_updated_res.unwrap();
        assert_eq!(
            updated_entry.value(),
            updated_value.as_slice(),
            "Updated value does not match for pkey {:?}",
            update_pkey
        );

        // 3. Attempt to get the deleted entry (i = 3).
        // Here we expect an error (or a “not found” result) because the entry was marked as deleted.
        let get_deleted_res =
            HashJoinPage::get(&page, &delete_pkey, &(100 + delete_index as Timestamp));
        assert!(
            get_deleted_res.is_err(),
            "Expected error when retrieving deleted entry for pkey {:?}",
            String::from_utf8_lossy(&delete_pkey)
        );

        // --- Check Slot Order ---
        // Here we assume that the page implementation uses exactly `num_entries` slots.
        // We iterate over each slot and verify that the primary keys are in non-decreasing order.
        let mut last_pkey: Option<Vec<u8>> = None;
        for slot_id in 0..num_entries {
            let slot_entry = HashJoinPage::get_entry_at_slot_id(&page, slot_id)
                .expect(&format!("Failed to retrieve entry at slot {}", slot_id));
            let current_pkey = slot_entry.pkey().to_vec();
            if let Some(ref last) = last_pkey {
                assert!(
                    last <= &current_pkey,
                    "Slots are not sorted: previous pkey {:?} is greater than current pkey {:?} at slot {}",
                    last,
                    current_pkey,
                    slot_id
                );
            }
            last_pkey = Some(current_pkey);
        }

        // --- Check Free Space After Compaction ---
        let free_before = HashJoinPage::free_space_before_compaction(&page);
        let free_after = HashJoinPage::free_space_after_compaction(&page);
        // It is expected that the free space reported after compaction is at least
        // as high as the free space before compaction.
        assert!(
            free_after >= free_before,
            "Free space after compaction ({}) is less than free space before compaction ({})",
            free_after,
            free_before
        );
    }

    #[test]
    fn test_free_space_calculation() {
        // Compute the base free space when the page is empty.
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        // This will track how much space (in bytes) is used by our entries.
        let mut expected_used: usize = 0;

        // Create a new page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Verify that before any insertions, free space is as expected.
        assert_eq!(
            HashJoinPage::free_space_before_compaction(&page),
            base_free_space
        );
        assert_eq!(
            HashJoinPage::free_space_after_compaction(&page),
            base_free_space
        );

        // --- INSERT OPERATION ---
        // Create an entry with human-readable key, pkey, and value.
        let key = b"key-large-001-extra".to_vec();
        let pkey = b"pkey-large-001-extra".to_vec();
        let value = b"value-large-001-extra".to_vec();
        let insert_entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), 100, 200);

        // Insert the entry into the page.
        let res = HashJoinPage::insert_recent_history(&mut page, &insert_entry);
        assert!(res.is_ok(), "Insert failed: {:?}", res.err());

        // Increase the expected used space.
        expected_used += SLOT_SIZE + key.len() + pkey.len() + value.len();
        let free_after_insert = HashJoinPage::free_space_after_compaction(&page);
        let expected_free_after_insert = base_free_space - expected_used;
        assert_eq!(
            free_after_insert, expected_free_after_insert,
            "After insert, expected free space {} but got {}",
            expected_free_after_insert, free_after_insert
        );

        // --- UPDATE OPERATION ---
        // Now update the deleted entry (currently holding the tombstone) with a new value.
        let new_value = b"new-value-large-001-extra".to_vec();
        let update_entry = MvccEntry::new(key.clone(), pkey.clone(), new_value.clone(), 150, 250);

        let res = HashJoinPage::update(&mut page, &pkey, &update_entry);
        assert!(res.is_ok(), "Update failed: {:?}", res.err());

        // The update frees the space of the tombstone and allocates the new entry.
        // Thus, the net change in used space is: new_value.len() - tombstone_value.len()
        expected_used += new_value.len().saturating_sub(value.len());
        let free_after_update = HashJoinPage::free_space_after_compaction(&page);
        let expected_free_after_update = base_free_space - expected_used;
        assert_eq!(
            free_after_update, expected_free_after_update,
            "After update, expected free space {} but got {}",
            expected_free_after_update, free_after_update
        );

        // --- DELETE OPERATION ---
        let res = HashJoinPage::delete(&mut page, &pkey, &150);
        assert!(res.is_ok(), "Delete (update) failed: {:?}", res.err());

        // For deletion, we assume that the full cost of the original entry is freed.
        expected_used -= SLOT_SIZE + key.len() + pkey.len() + new_value.len();
        let free_after_delete = HashJoinPage::free_space_after_compaction(&page);
        let expected_free_after_delete = base_free_space - expected_used;
        assert_eq!(
            free_after_delete, expected_free_after_delete,
            "After delete, expected free space {} but got {}",
            expected_free_after_delete, free_after_delete
        );
    }

    #[test]
    fn test_free_space_after_compaction_multiple_ops() {
        // Compute the base free space when the page is empty.
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        // This will track the expected used space (in bytes) by all entries.
        let mut expected_used: usize = 0;

        // Create a new page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Verify that initially the free space is as expected.
        assert_eq!(
            HashJoinPage::free_space_after_compaction(&page),
            base_free_space,
            "Initial free space should be {}",
            base_free_space
        );

        // --- MULTIPLE INSERTS ---
        let num_entries = 5;
        // We'll store the inserted entries so that we can later update and delete them.
        let mut entries = Vec::with_capacity(num_entries);

        for i in 0..num_entries {
            // Create human-readable key, primary key, and value.
            let key = format!("key-{:03}", i).into_bytes();
            let pkey = format!("pkey-{:03}", i).into_bytes();
            let value = format!("value-{:03}", i).into_bytes();
            let start_ts = 100 + i as Timestamp;
            let end_ts = 200 + i as Timestamp;
            let entry = MvccEntry::new(key.clone(), pkey.clone(), value.clone(), start_ts, end_ts);

            // Insert the entry.
            let res = HashJoinPage::insert_recent_history(&mut page, &entry);
            assert!(
                res.is_ok(),
                "Insert failed for entry {}: {:?}",
                i,
                res.err()
            );

            // Increase expected used space.
            expected_used += SLOT_SIZE + key.len() + pkey.len() + value.len();

            let free_after_insert = HashJoinPage::free_space_after_compaction(&page);
            let expected_free_after_insert = base_free_space - expected_used;
            assert_eq!(
                free_after_insert, expected_free_after_insert,
                "After insert {}: expected free space {} but got {}",
                i, expected_free_after_insert, free_after_insert
            );

            entries.push(entry);
        }

        // --- MULTIPLE UPDATES ---
        // For demonstration, update each entry with a new value:
        // For even-indexed entries, use a larger value; for odd-indexed ones, a smaller value.
        for i in 0..num_entries {
            let entry = &entries[i];
            let new_value = if i % 2 == 0 {
                format!("updated-larger-value-{:03}", i).into_bytes()
            } else {
                format!("upd-sm-{:03}", i).into_bytes()
            };
            // Create an update entry (keeping key and pkey the same).
            let update_entry = MvccEntry::new(
                entry.key().to_vec(),
                entry.pkey().to_vec(),
                new_value.clone(),
                entry.start_ts(),
                entry.end_ts(),
            );

            let res = HashJoinPage::update(&mut page, entry.pkey(), &update_entry);
            assert!(
                res.is_ok(),
                "Update failed for entry {}: {:?}",
                i,
                res.err()
            );

            // The net change in used space is the difference in value lengths.
            // (Assuming key and pkey remain unchanged.)
            expected_used += new_value.len().saturating_sub(entry.value().len());

            let free_after_update = HashJoinPage::free_space_after_compaction(&page);
            let expected_free_after_update = base_free_space - expected_used;
            assert_eq!(
                free_after_update, expected_free_after_update,
                "After update {}: expected free space {} but got {}",
                i, expected_free_after_update, free_after_update
            );

            // Replace the entry in our vector with the updated one.
            entries[i] = update_entry;
        }

        // --- MULTIPLE DELETES ---
        // Delete entries one by one (here, in reverse order to further test the deletion logic).
        for i in (0..num_entries).rev() {
            let entry = &entries[i];
            // Use the entry's start timestamp for deletion.
            let res = HashJoinPage::delete(&mut page, entry.pkey(), &entry.start_ts());
            assert!(
                res.is_ok(),
                "Delete failed for entry {}: {:?}",
                i,
                res.err()
            );

            // Deletion frees the space used by both the slot and the record.
            expected_used -=
                SLOT_SIZE + entry.key().len() + entry.pkey().len() + entry.value().len();

            let free_after_delete = HashJoinPage::free_space_after_compaction(&page);
            let expected_free_after_delete = base_free_space - expected_used;
            assert_eq!(
                free_after_delete, expected_free_after_delete,
                "After delete {}: expected free space {} but got {}",
                i, expected_free_after_delete, free_after_delete
            );
        }
    }

    #[test]
    fn test_history_insert_and_get() {
        // Compute the base free space when the page is empty.
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        // This will track how much space (in bytes) is used by our history entries.
        let mut expected_used: usize = 0;

        // Create a new page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Verify that initially the free space is as expected.
        assert_eq!(
            HashJoinPage::free_space_after_compaction(&page),
            base_free_space,
            "Initial free space should be {}",
            base_free_space
        );

        // --- HISTORY INSERT OPERATIONS ---
        // In the history chain the sorting is by end_ts.
        // We use the same key and pkey for history, but with different timestamp ranges.
        let key = b"key-history-001".to_vec();
        let pkey = b"pkey-history-001".to_vec();

        // History record 1: valid from 100 to 150.
        let value1 = b"value-history-first".to_vec();
        let mut entry1 = MvccEntry::new(key.clone(), pkey.clone(), value1.clone(), 100, 150);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry1);
        assert!(
            res.is_ok(),
            "insert_history for entry1 failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + value1.len();
        let free_after_insert1 = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_insert1,
            base_free_space - expected_used,
            "After first history insert, expected free space {} but got {}",
            base_free_space - expected_used,
            free_after_insert1
        );

        // History record 2: valid from 150 to 200.
        let value2 = b"value-history-second".to_vec();
        let mut entry2 = MvccEntry::new(key.clone(), pkey.clone(), value2.clone(), 150, 200);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry2);
        assert!(
            res.is_ok(),
            "insert_history for entry2 failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + value2.len();
        let free_after_insert2 = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_insert2,
            base_free_space - expected_used,
            "After second history insert, expected free space {} but got {}",
            base_free_space - expected_used,
            free_after_insert2
        );

        // --- VERIFY SLOT ORDERING FOR HISTORY CHAIN ---
        // Since history entries are ordered by end_ts, slot 0 should have the smaller end_ts.
        let history_entry1 = HashJoinPage::get_entry_at_slot_id(&page, 0)
            .expect("failed to get history entry at slot 0");
        let history_entry2 = HashJoinPage::get_entry_at_slot_id(&page, 1)
            .expect("failed to get history entry at slot 1");
        assert!(
            history_entry1.end_ts() <= history_entry2.end_ts(),
            "History entries are not sorted by end_ts: slot 0 end_ts {} is not <= slot 1 end_ts {}",
            history_entry1.end_ts(),
            history_entry2.end_ts()
        );

        // --- GET HISTORY OPERATIONS ---
        // Query with a timestamp that falls into the range of the first history entry.
        let query_ts1: Timestamp = 120; // In range [100,150)
        let fetched_entry1 = HashJoinPage::get_history(&page, &pkey, &query_ts1)
            .expect("get_history failed for query_ts1");
        assert_eq!(
            fetched_entry1.value(),
            value1.as_slice(),
            "For query_ts1, expected value {:?} but got {:?}",
            value1,
            fetched_entry1.value()
        );
        assert_eq!(fetched_entry1.start_ts(), 100);
        assert_eq!(fetched_entry1.end_ts(), 150);

        // Query with a timestamp that falls into the range of the second history entry.
        let query_ts2: Timestamp = 155; // In range [150,200)
        let fetched_entry2 = HashJoinPage::get_history(&page, &pkey, &query_ts2)
            .expect("get_history failed for query_ts2");
        assert_eq!(
            fetched_entry2.value(),
            value2.as_slice(),
            "For query_ts2, expected value {:?} but got {:?}",
            value2,
            fetched_entry2.value()
        );
        assert_eq!(fetched_entry2.start_ts(), 150);
        assert_eq!(fetched_entry2.end_ts(), 200);

        // Query with a timestamp that does not fall into any history entry.
        let query_ts3: Timestamp = 95; // Before the first history record.
        let res = HashJoinPage::get_history(&page, &pkey, &query_ts3);
        assert!(
            res.is_err(),
            "Expected get_history to fail for query_ts3 ({}), but got {:?}",
            query_ts3,
            res.ok()
        );

        let query_ts4: Timestamp = 200; // 200 is not in [150,200) assuming end_ts is exclusive.
        let res = HashJoinPage::get_history(&page, &pkey, &query_ts4);
        assert!(
            res.is_err(),
            "Expected get_history to fail for query_ts4 ({}), but got {:?}",
            query_ts4,
            res.ok()
        );
    }

    #[test]
    fn test_history_upsert_non_overlapping() {
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut expected_used: usize = 0;

        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Initially, free space should match.
        assert_eq!(
            HashJoinPage::free_space_after_compaction(&page),
            base_free_space,
            "Initial free space should be {}",
            base_free_space
        );

        let key = b"key-history-001".to_vec();
        let pkey = b"pkey-history-001".to_vec();

        // History record 1: valid [100,150), value "value-history-first"
        let value1 = b"value-history-first".to_vec();
        let mut entry1 = MvccEntry::new(key.clone(), pkey.clone(), value1.clone(), 100, 150);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry1);
        assert!(
            res.is_ok(),
            "upsert_history for entry1 failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + value1.len();
        let free_after_entry1 = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_entry1,
            base_free_space - expected_used,
            "After entry1, expected free space {} but got {}",
            base_free_space - expected_used,
            free_after_entry1
        );

        // History record 2: valid [150,200), value "value-history-second"
        let value2 = b"value-history-second".to_vec();
        let mut entry2 = MvccEntry::new(key.clone(), pkey.clone(), value2.clone(), 150, 200);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry2);
        assert!(
            res.is_ok(),
            "upsert_history for entry2 failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + value2.len();
        let free_after_entry2 = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_entry2,
            base_free_space - expected_used,
            "After entry2, expected free space {} but got {}",
            base_free_space - expected_used,
            free_after_entry2
        );

        // --- Verify ordering: history records must be sorted by end_ts ---
        let slot_count = page.slot_count();
        for idx in 0..(slot_count - 1) {
            let current = HashJoinPage::get_entry_at_slot_id(&page, idx)
                .expect("failed to get entry at slot");
            let next = HashJoinPage::get_entry_at_slot_id(&page, idx + 1)
                .expect("failed to get entry at slot");
            assert!(
                current.end_ts() <= next.end_ts(),
                "Ordering violation: slot {} end_ts {} > slot {} end_ts {}",
                idx,
                current.end_ts(),
                idx + 1,
                next.end_ts()
            );
        }

        // --- GET HISTORY OPERATIONS ---
        // Query timestamp 120 is in [100,150)
        let query_ts1: Timestamp = 120;
        let fetched_entry1 = HashJoinPage::get_history(&page, &pkey, &query_ts1)
            .expect("get_history failed for query_ts1");
        assert_eq!(
            fetched_entry1.value(),
            value1.as_slice(),
            "For query_ts1, expected value {:?} but got {:?}",
            value1,
            fetched_entry1.value()
        );
        assert_eq!(fetched_entry1.start_ts(), 100);
        assert_eq!(fetched_entry1.end_ts(), 150);

        // Query timestamp 160 is in [150,200)
        let query_ts2: Timestamp = 160;
        let fetched_entry2 = HashJoinPage::get_history(&page, &pkey, &query_ts2)
            .expect("get_history failed for query_ts2");
        assert_eq!(
            fetched_entry2.value(),
            value2.as_slice(),
            "For query_ts2, expected value {:?} but got {:?}",
            value2,
            fetched_entry2.value()
        );
        assert_eq!(fetched_entry2.start_ts(), 150);
        assert_eq!(fetched_entry2.end_ts(), 200);

        // Query with a timestamp that does not fall into any history entry.
        let query_ts3: Timestamp = 95;
        let res = HashJoinPage::get_history(&page, &pkey, &query_ts3);
        assert!(
            res.is_err(),
            "Expected get_history to fail for query_ts3 ({}), but got {:?}",
            query_ts3,
            res.ok()
        );

        // Query with a timestamp at the exclusive end boundary.
        let query_ts4: Timestamp = 200;
        let res = HashJoinPage::get_history(&page, &pkey, &query_ts4);
        assert!(
            res.is_err(),
            "Expected get_history to fail for query_ts4 ({}), but got {:?}",
            query_ts4,
            res.ok()
        );
    }

    /// Test overlapping history upsert which splits an existing record.
    #[ignore]
    #[test]
    fn test_history_upsert_overlap_split() {
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut expected_used: usize = 0;

        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        let key = b"key-history-overlap".to_vec();
        let pkey = b"pkey-history-overlap".to_vec();

        // Insert original history record: valid [100,150), value "Original"
        let orig_value = b"Original".to_vec();
        let mut orig_entry =
            MvccEntry::new(key.clone(), pkey.clone(), orig_value.clone(), 100, 150);
        let res = HashJoinPage::upsert_history(&mut page, &mut orig_entry);
        assert!(
            res.is_ok(),
            "upsert_history original failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + orig_value.len();
        let free_after_orig = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_orig,
            base_free_space - expected_used,
            "Free space mismatch after original record"
        );

        // Upsert new history record that overlaps:
        // New record has range [80,150) but should be adjusted to [80,100) so that it does not overlap.
        let new_value = b"New".to_vec();
        let mut new_entry = MvccEntry::new(key.clone(), pkey.clone(), new_value.clone(), 80, 150);
        let res = HashJoinPage::upsert_history(&mut page, &mut new_entry);
        assert!(
            res.is_ok(),
            "upsert_history new overlapping failed: {:?}",
            res.err()
        );
        expected_used += SLOT_SIZE + key.len() + pkey.len() + new_value.len();
        let free_after_new = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after_new,
            base_free_space - expected_used,
            "Free space mismatch after new overlapping record"
        );

        // At this point, we expect two history records:
        // Record A: valid [80,100), value "New"
        // Record B: valid [100,150), value "Original"
        let slot0 = HashJoinPage::get_entry_at_slot_id(&page, 0)
            .expect("failed to get history entry at slot 0");
        let slot1 = HashJoinPage::get_entry_at_slot_id(&page, 1)
            .expect("failed to get history entry at slot 1");
        assert!(
            slot0.end_ts() <= slot1.end_ts(),
            "Ordering violation in overlap split: slot0.end_ts {} > slot1.end_ts {}",
            slot0.end_ts(),
            slot1.end_ts()
        );

        // Query a timestamp in the first record's range.
        let query_ts_new: Timestamp = 90;
        let fetched_new = HashJoinPage::get_history(&page, &pkey, &query_ts_new)
            .expect("get_history failed for new record query");
        assert_eq!(
            fetched_new.value(),
            new_value.as_slice(),
            "Expected value {:?} for new record query, got {:?}",
            new_value,
            fetched_new.value()
        );
        assert_eq!(fetched_new.start_ts(), 80);
        assert_eq!(fetched_new.end_ts(), 100);

        // Query a timestamp in the second record's range.
        let query_ts_orig: Timestamp = 120;
        let fetched_orig = HashJoinPage::get_history(&page, &pkey, &query_ts_orig)
            .expect("get_history failed for original record query");
        assert_eq!(
            fetched_orig.value(),
            orig_value.as_slice(),
            "Expected value {:?} for original record query, got {:?}",
            orig_value,
            fetched_orig.value()
        );
        assert_eq!(fetched_orig.start_ts(), 100);
        assert_eq!(fetched_orig.end_ts(), 150);
    }

    /// Test history upsert for multiple primary keys.
    #[test]
    fn test_history_multiple_pkeys() {
        let base_free_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let mut expected_used: usize = 0;

        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Define two different keys.
        let key1 = b"key1".to_vec();
        let pkey1 = b"pkey1".to_vec();
        let key2 = b"key2".to_vec();
        let pkey2 = b"pkey2".to_vec();

        // For key1: record [100,150), value "K1-First"
        let mut entry1 =
            MvccEntry::new(key1.clone(), pkey1.clone(), b"K1-First".to_vec(), 100, 150);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry1);
        assert!(res.is_ok(), "upsert_history for key1 record1 failed");
        expected_used += SLOT_SIZE + key1.len() + pkey1.len() + b"K1-First".len();

        // For key2: record [120,180), value "K2-First"
        let mut entry2 =
            MvccEntry::new(key2.clone(), pkey2.clone(), b"K2-First".to_vec(), 120, 180);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry2);
        assert!(res.is_ok(), "upsert_history for key2 record1 failed");
        expected_used += SLOT_SIZE + key2.len() + pkey2.len() + b"K2-First".len();

        // For key1: record [150,200), value "K1-Second"
        let mut entry3 =
            MvccEntry::new(key1.clone(), pkey1.clone(), b"K1-Second".to_vec(), 150, 200);
        let res = HashJoinPage::upsert_history(&mut page, &mut entry3);
        assert!(res.is_ok(), "upsert_history for key1 record2 failed");
        expected_used += SLOT_SIZE + key1.len() + pkey1.len() + b"K1-Second".len();

        let free_after = HashJoinPage::free_space_after_compaction(&page);
        assert_eq!(
            free_after,
            base_free_space - expected_used,
            "Free space mismatch after multiple pkeys"
        );

        // Verify overall ordering by end_ts.
        let slot_count = page.slot_count();
        for idx in 0..(slot_count - 1) {
            let current = HashJoinPage::get_entry_at_slot_id(&page, idx)
                .expect("failed to get entry during ordering check");
            let next = HashJoinPage::get_entry_at_slot_id(&page, idx + 1)
                .expect("failed to get entry during ordering check");
            assert!(
                current.end_ts() <= next.end_ts(),
                "Ordering violation: slot {} end_ts {} > slot {} end_ts {}",
                idx,
                current.end_ts(),
                idx + 1,
                next.end_ts()
            );
        }

        // Query key1 for its first interval (e.g., timestamp 120 → "K1-First")
        let fetched_k1_first = HashJoinPage::get_history(&page, &pkey1, &120)
            .expect("get_history failed for key1 first interval");
        assert_eq!(fetched_k1_first.value(), b"K1-First");

        // Query key1 for its second interval (e.g., timestamp 160 → "K1-Second")
        let fetched_k1_second = HashJoinPage::get_history(&page, &pkey1, &160)
            .expect("get_history failed for key1 second interval");
        assert_eq!(fetched_k1_second.value(), b"K1-Second");

        // Query key2 for its interval (e.g., timestamp 130 → "K2-First")
        let fetched_k2 = HashJoinPage::get_history(&page, &pkey2, &130)
            .expect("get_history failed for key2 interval");
        assert_eq!(fetched_k2.value(), b"K2-First");
    }

    #[test]
    fn test_history_page_sort_order_and_get() {
        // Create a new empty page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // We use the same key and pkey for history entries.
        let key = b"key-history-test".to_vec();
        let pkey = b"pkey-history-test".to_vec();

        // Create three history entries for the same primary key with non-overlapping intervals:
        // - Entry A: valid from 100 to 150, value "value-A"
        // - Entry B: valid from 150 to 200, value "value-B"
        // - Entry C: valid from 200 to 250, value "value-C"
        //
        // For testing ordering, we insert them in the order: C, A, B.
        let mut entry_a = MvccEntry::new(key.clone(), pkey.clone(), b"value-A".to_vec(), 100, 150);
        let mut entry_b = MvccEntry::new(key.clone(), pkey.clone(), b"value-B".to_vec(), 150, 200);
        let mut entry_c = MvccEntry::new(key.clone(), pkey.clone(), b"value-C".to_vec(), 200, 250);

        // Insert in a non-sorted order.
        // First insert entry C, then entry A, then entry B.
        let res_c = HashJoinPage::upsert_history(&mut page, &mut entry_c);
        assert!(
            res_c.is_ok(),
            "upsert_history for entry C failed: {:?}",
            res_c.err()
        );

        let res_a = HashJoinPage::upsert_history(&mut page, &mut entry_a);
        assert!(
            res_a.is_ok(),
            "upsert_history for entry A failed: {:?}",
            res_a.err()
        );

        let res_b = HashJoinPage::upsert_history(&mut page, &mut entry_b);
        assert!(
            res_b.is_ok(),
            "upsert_history for entry B failed: {:?}",
            res_b.err()
        );

        // Now, check that the slots in the page are sorted by end_ts.
        let slot_count = page.slot_count();
        // It should have 3 slots.
        assert_eq!(
            slot_count, 3,
            "Expected 3 history slots, got {}",
            slot_count
        );
        for idx in 0..(slot_count - 1) {
            let current = HashJoinPage::get_entry_at_slot_id(&page, idx)
                .expect(&format!("Failed to get entry at slot {}", idx));
            let next = HashJoinPage::get_entry_at_slot_id(&page, idx + 1)
                .expect(&format!("Failed to get entry at slot {}", idx + 1));
            assert!(
                current.end_ts() <= next.end_ts(),
                "Ordering violation: slot {} end_ts {} > slot {} end_ts {}",
                idx,
                current.end_ts(),
                idx + 1,
                next.end_ts()
            );
        }

        // --- Test get_history() ---
        // Query with a timestamp that falls within the first entry's interval.
        let query_ts_a: Timestamp = 125; // between 100 and 150
        let fetched_a = HashJoinPage::get_history(&page, &pkey, &query_ts_a)
            .expect("get_history failed for query_ts_a");
        assert_eq!(
            fetched_a.value(),
            b"value-A".as_ref(),
            "For query_ts_a, expected value 'value-A' but got {:?}",
            fetched_a.value()
        );
        assert_eq!(fetched_a.start_ts(), 100);
        assert_eq!(fetched_a.end_ts(), 150);

        // Query with a timestamp that falls within the second entry's interval.
        let query_ts_b: Timestamp = 175; // between 150 and 200
        let fetched_b = HashJoinPage::get_history(&page, &pkey, &query_ts_b)
            .expect("get_history failed for query_ts_b");
        assert_eq!(
            fetched_b.value(),
            b"value-B".as_ref(),
            "For query_ts_b, expected value 'value-B' but got {:?}",
            fetched_b.value()
        );
        assert_eq!(fetched_b.start_ts(), 150);
        assert_eq!(fetched_b.end_ts(), 200);

        // Query with a timestamp that falls within the third entry's interval.
        let query_ts_c: Timestamp = 225; // between 200 and 250
        let fetched_c = HashJoinPage::get_history(&page, &pkey, &query_ts_c)
            .expect("get_history failed for query_ts_c");
        assert_eq!(
            fetched_c.value(),
            b"value-C".as_ref(),
            "For query_ts_c, expected value 'value-C' but got {:?}",
            fetched_c.value()
        );
        assert_eq!(fetched_c.start_ts(), 200);
        assert_eq!(fetched_c.end_ts(), 250);
    }

    #[test]
    fn test_page_garbage_collect() {
        // Create a new, empty page and initialize it.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Use the same key and pkey for all history entries.
        let key = b"key-history-gc".to_vec();
        let pkey = b"pkey-history-gc".to_vec();

        // Insert several history entries with increasing timestamp intervals.
        // For example, insert 5 entries with intervals:
        // Entry 0: valid [100,150)
        // Entry 1: valid [150,200)
        // Entry 2: valid [200,250)
        // Entry 3: valid [250,300)
        // Entry 4: valid [300,350)
        let num_entries = 5;
        for i in 0..num_entries {
            let start_ts = 100 + i * 50;
            let end_ts = start_ts + 50;
            let value = format!("value-history-{}", i).into_bytes();
            let mut entry = MvccEntry::new(key.clone(), pkey.clone(), value, start_ts, end_ts);
            let res = HashJoinPage::upsert_history(&mut page, &mut entry);
            assert!(
                res.is_ok(),
                "upsert_history failed for entry {}: {:?}",
                i,
                res.err()
            );
        }
        // println!("Page statistics before GC:\n{}", page.stat());

        for i in 0..page.slot_count() {
            let slot = HashJoinPage::slot(&page, i);
            println!("slot record offset: {:?}", slot.offset());
            println!("slot record length: {:?}", slot.rec_size());

            // print key, pkey, value, start_ts, end_ts in human readable format
            let entry = HashJoinPage::get_entry_at_slot_id(&page, i)
                .expect(&format!("Failed to get entry at slot {}", i));
            println!(
                "Slot {}: key={}, pkey={}, value={}, start_ts={}, end_ts={}",
                i,
                String::from_utf8_lossy(entry.key()),
                String::from_utf8_lossy(entry.pkey()),
                String::from_utf8_lossy(entry.value()),
                entry.start_ts(),
                entry.end_ts()
            );
        }

        // Define the garbage collection timestamp.
        // We choose gc_ts = 220. This means any history entry with end_ts <= 220 should be collected.
        let gc_ts: Timestamp = 220;
        let gc_res = HashJoinPage::chained_hash_garbage_collect(&mut page, &gc_ts);
        assert!(
            gc_res.is_ok(),
            "garbage_collect failed with ts {}: {:?}",
            gc_ts,
            gc_res.err()
        );

        for i in 0..page.slot_count() {
            // print key, pkey, value, start_ts, end_ts in human readable format
            let entry = HashJoinPage::get_entry_at_slot_id(&page, i)
                .expect(&format!("Failed to get entry at slot {}", i));
            println!(
                "Slot {}: key={}, pkey={}, value={}, start_ts={}, end_ts={}",
                i,
                String::from_utf8_lossy(entry.key()),
                String::from_utf8_lossy(entry.pkey()),
                String::from_utf8_lossy(entry.value()),
                entry.start_ts(),
                entry.end_ts()
            );
        }

        println!("Page statistics after GC:\n{}", page.stat());

        // Now, iterate over all remaining slots in the page.
        // Every remaining entry must have an end_ts greater than gc_ts.
        for idx in 0..page.slot_count() {
            let slot = HashJoinPage::slot(&page, idx);
            println!("slot record offset: {:?}", slot.offset());
            println!("slot record length: {:?}", slot.rec_size());

            let entry = HashJoinPage::get_entry_at_slot_id(&page, idx)
                .expect(&format!("Failed to get entry at slot {}", idx));
            println!("entry: {:?}", entry);
            assert!(
                entry.end_ts() > gc_ts,
                "Entry at slot {} has end_ts {} which is not greater than gc_ts {}",
                idx,
                entry.end_ts(),
                gc_ts
            );
        }
    }

    #[test]
    fn test_slot_encode_decode() {
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // --- Multiple Inserts ---
        let num_entries = 10;
        for i in 1..=num_entries {
            let key = format!("key-large-{:03}-extra", i).into_bytes();
            let pkey = format!("pkey-large-{:03}-extra", i).into_bytes();
            let value = format!("value-large-{:03}-extra", i).into_bytes();
            // For simplicity, set timestamps based on i.
            let start_ts: Timestamp = 100 + i as Timestamp;
            let end_ts: Timestamp = 200 + i as Timestamp;
            let entry = MvccEntry::new(key, pkey.clone(), value, start_ts, end_ts);

            let res = HashJoinPage::insert_recent_history(&mut page, &entry);
            assert!(
                res.is_ok(),
                "Failed to insert entry for pkey {:?}: {:?}",
                pkey,
                res.err()
            );
        }

        // let slot_slice = page.get_slot_slice(0);
        // for slot in slot_slice {
        //     println!("1slot: {:?}", slot);
        // }
        for id in 0..page.slot_count() {
            let slot = HashJoinPage::unsafe_slot(&page, id);
            println!("2slot: {:?}", slot);
        }
    }
}
