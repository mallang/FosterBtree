use crate::{
    access_method::AccessMethodError,
    mvcc_index::{MvccEntry, TxId},
    prelude::{Page, PageId, Timestamp, AVAILABLE_PAGE_SIZE},
};
use std::{cmp::Ordering, result::Result::Ok, sync::atomic::AtomicU64};
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
        min_start_ts: Timestamp,
        max_end_ts: Timestamp,
        recent_entry_count: u32,
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
            let recent_entry_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            current_pos += std::mem::size_of::<u32>();
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
                min_start_ts: min_ts,
                max_end_ts: max_ts,
                recent_entry_count,
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
                .copy_from_slice(&self.min_start_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();
            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()]
                .copy_from_slice(&self.max_end_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();
            bytes[current_pos..current_pos + std::mem::size_of::<u32>()]
                .copy_from_slice(&self.recent_entry_count.to_be_bytes());
            current_pos += std::mem::size_of::<u32>();
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
                min_start_ts: Timestamp::MAX,
                max_end_ts: Timestamp::MIN,
                recent_entry_count: 0,
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
            self.min_start_ts
        }

        pub fn set_min_ts(&mut self, min_ts: &Timestamp) {
            self.min_start_ts = *min_ts;
        }

        pub fn max_ts(&self) -> Timestamp {
            self.max_end_ts
        }

        pub fn set_max_ts(&mut self, max_ts: &Timestamp) {
            self.max_end_ts = *max_ts;
        }

        pub fn recent_entry_count(&self) -> u32 {
            self.recent_entry_count
        }

        pub fn set_recent_entry_count(&mut self, recent_entry_count: usize) {
            self.recent_entry_count = recent_entry_count as u32;
        }
    }
}
use header::*;

pub mod slot {
    use crate::{mvcc_index::TxId, prelude::Timestamp};

    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq)]
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

    impl Slot {
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

        pub fn set_end_ts(&mut self, end_ts: &Timestamp) {
            self.end_ts = *end_ts;
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
    use dashmap::mapref::entry;

    #[derive(Debug)]
    pub struct Record {
        key: Vec<u8>,  // hash key
        pkey: Vec<u8>, // primary key
        val: Vec<u8>,
    }

    impl Record {
        pub fn from_bytes(
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

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes = Vec::with_capacity(self.key.len() + self.pkey.len() + self.val.len());

            bytes.extend_from_slice(&self.key);
            bytes.extend_from_slice(&self.pkey);
            bytes.extend_from_slice(&self.val);

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], val: &[u8]) -> Self {
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

        pub fn update_val(&mut self, new_val: &[u8]) {
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

        pub fn require_size(entry: &crate::mvcc_index::MvccEntry) -> usize {
            entry.key().len() + entry.pkey().len() + entry.value().len()
        }
    }
}
use record::*;

pub trait CommonPageMethods {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8];
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    fn move_bytes(&mut self, src_start_offset: usize, src_end_offset: usize, dest_offset: usize);

    // Header methods
    fn header(&self) -> Header {
        Header::from_bytes(self.read_bytes(0, PAGE_HEADER_SIZE))
    }
    fn set_header(&mut self, header: &Header) {
        self.write_bytes(0, &header.to_bytes());
    }

    // Header method: next_page, next_frame_id
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

    // Header method: total_bytes_used
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
    fn free_space_before_compaction(&self) -> usize {
        self.header().rec_start_offset() - self.slot_end_offset()
    }
    fn free_space_after_compaction(&self) -> usize {
        AVAILABLE_PAGE_SIZE - self.header().total_bytes_used()
    }
    fn require_space(entry: &MvccEntry) -> usize {
        SLOT_SIZE + Record::require_size(entry)
    }

    // Header method: slot_count
    fn slot_count(&self) -> usize {
        self.header().slot_count()
    }
    fn set_slot_count(&mut self, slot_count: usize) {
        let mut header = self.header();
        header.set_slot_count(slot_count);
        self.set_header(&header);
    }
    fn increase_slot_count(&mut self) {
        self.set_slot_count(self.slot_count() + 1);
    }
    fn decrease_slot_count(&mut self) {
        self.set_slot_count(self.slot_count() - 1);
    }
    fn slot_offset(&self, slot_id: usize) -> usize {
        PAGE_HEADER_SIZE + slot_id * SLOT_SIZE
    }
    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE + self.slot_count() * SLOT_SIZE
    }

    // Header method: rec_start_offset
    fn rec_start_offset(&self) -> usize {
        self.header().rec_start_offset()
    }
    fn set_rec_start_offset(&mut self, rec_start_offset: usize) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }

    // Header method: min_ts, max_ts, recent_entry_count
    fn min_ts(&self) -> Timestamp {
        self.header().min_ts()
    }
    fn set_min_ts(&mut self, min_ts: &Timestamp) {
        let mut header = self.header();
        header.set_min_ts(min_ts);
        self.set_header(&header);
    }
    fn max_ts(&self) -> Timestamp {
        self.header().max_ts()
    }
    fn set_max_ts(&mut self, max_ts: &Timestamp) {
        let mut header = self.header();
        header.set_max_ts(max_ts);
        self.set_header(&header);
    }
    fn recent_entry_count(&self) -> usize {
        self.header().recent_entry_count() as usize
    }
    fn set_recent_entry_count(&mut self, recent_entry_count: usize) {
        let mut header = self.header();
        header.set_recent_entry_count(recent_entry_count);
        self.set_header(&header);
    }
    fn increase_recent_entry_count(&mut self) {
        self.set_recent_entry_count(self.recent_entry_count() + 1);
    }
    fn decrease_recent_entry_count(&mut self) {
        self.set_recent_entry_count(self.recent_entry_count() - 1);
    }

    // Slot methods
    fn slot(&self, slot_id: usize) -> Slot {
        Slot::from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
    }
    fn set_slot_at_id(&mut self, slot: &Slot, slot_id: usize) {
        self.write_bytes(self.slot_offset(slot_id), &slot.to_bytes());
    }
    fn insert_slot_at_id(&mut self, slot: &Slot, slot_id: usize) {
        if slot_id > self.slot_count() {
            panic!(
                "Invalid slot_id in insert_slot_at_id: {} > {}",
                slot_id,
                self.slot_count()
            );
        }
        if slot_id < self.slot_count() {
            let start_offset = self.slot_offset(slot_id);
            let end_offset = self.slot_end_offset();
            self.move_bytes(start_offset, end_offset, start_offset + SLOT_SIZE);
        }
        self.set_slot_at_id(slot, slot_id);
        self.increase_slot_count();
        self.increase_total_bytes_used(SLOT_SIZE);
    }
    fn delete_slot_at_id(&mut self, slot_id: usize) {
        if slot_id >= self.slot_count() {
            panic!(
                "Invalid slot_id in delete_slot_at_id: {} >= {}",
                slot_id,
                self.slot_count()
            );
        }
        let start_offset = self.slot_offset(slot_id + 1);
        let end_offset = self.slot_end_offset();
        self.move_bytes(start_offset, end_offset, start_offset - SLOT_SIZE);

        self.decrease_slot_count();
        self.decrease_total_bytes_used(SLOT_SIZE);
    }

    /// Returns `Some(Record)` if the slot's pkey exactly matches `pkey`.
    /// Otherwise returns `None`.
    fn slot_pkey_matches(&self, slot: &Slot, pkey: &[u8]) -> Option<Record> {
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
            let rec_bytes = self.read_bytes(slot.offset(), slot.rec_size());
            let rec = Record::from_bytes(
                rec_bytes,
                slot.key_size(),
                slot.pkey_size(),
                slot.val_size(),
            );
            return Some(rec);
        }

        // 3) pkey is longer than the prefix => compare the remainder.
        let rec = self.record_from_slot(slot);
        if rec.pkey() == pkey {
            Some(rec)
        } else {
            None
        }
    }

    /// Compare the slot’s pkey at slot_id with `search_key`.
    /// Returns Ordering::Less if slot’s pkey < search_key,
    ///         Ordering::Equal if slot’s pkey == search_key,
    ///         Ordering::Greater if slot’s pkey > search_key.
    /// Shorter pkeys are considered less than longer pkeys for simplicity.
    fn slot_cmp_pkey(&self, slot_id: usize, pkey: &[u8]) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        let slot = self.slot(slot_id);

        // 1) First compare lengths
        let slot_pkey_len = slot.pkey_size();
        let input_pkey_len = pkey.len();
        // shorter key < longer key
        if slot_pkey_len != input_pkey_len {
            return slot_pkey_len.cmp(&input_pkey_len);
        }

        // 2) Compare the prefix (up to 8 bytes).
        let prefix_len = std::cmp::min(SLOT_PKEY_PREFIX_SIZE, input_pkey_len);
        let slot_prefix = &slot.pkey_prefix()[..prefix_len];
        let input_prefix = &pkey[..prefix_len];
        match slot_prefix.cmp(input_prefix) {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => {
                if input_pkey_len <= SLOT_PKEY_PREFIX_SIZE {
                    return Ordering::Equal;
                }
            }
        }

        // 3) Need to read the entire pkey from the record area, then compare it to `search_key`.
        let rec = self.record_from_slot(&slot);
        let slot_pkey = rec.pkey();

        slot_pkey.cmp(pkey)
    }

    // Record methods
    fn record(&self, slot_id: usize) -> Record {
        let slot = self.slot(slot_id);
        self.record_from_slot(&slot)
    }
    fn record_from_slot(&self, slot: &Slot) -> Record {
        Record::from_bytes(
            self.read_bytes(slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        )
    }
    fn set_record_at_slot_id(&mut self, rec: &Record, slot_id: usize) {
        let slot = self.slot(slot_id);
        self.write_bytes(slot.offset(), &rec.to_bytes());
    }
    fn set_record_at_offset(&mut self, rec: &Record, offset: usize) {
        self.write_bytes(offset, &rec.to_bytes());
    }
    fn insert_rec_at_offset(&mut self, rec: &Record, offset: usize) {
        self.set_record_at_offset(rec, offset);
        self.increase_total_bytes_used(rec.size());
        if offset < self.rec_start_offset() {
            self.set_rec_start_offset(offset);
        }
    }

    fn insert_at_slot_id(
        &mut self,
        entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<(), AccessMethodError> {
        // Assume that size check has been done before this call.
        let rec = Record::new(entry.key(), entry.pkey(), entry.value());
        let new_rec_start_offset = self.rec_start_offset() - rec.size();
        let slot = Slot::new(
            entry.key(),
            entry.pkey(),
            0, // tx_id is not used now
            entry.start_ts(),
            entry.end_ts(),
            entry.value(),
            new_rec_start_offset,
        );
        self.insert_slot_at_id(&slot, slot_id);
        self.insert_rec_at_offset(&rec, new_rec_start_offset);
        Ok(())
    }

    fn update_at_slot_id(
        &mut self,
        new_entry: &MvccEntry,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError> {
        // Assume that size check has been done before this call.
        let old_slot = self.slot(slot_id);
        // if new_entry.start_ts() < old_slot.start_ts() {
        //     old_slot.set_end_ts(&new_entry.start_ts());
        //     new_entry.set_end_ts(&old_slot.start_ts());
        //     self.set_slot_at_id(&old_slot, slot_id);
        //     return Ok(new_entry.clone());
        // }

        let mut new_slot = Slot::new(
            new_entry.key(),
            new_entry.pkey(),
            0, // tx_id is not used now
            new_entry.start_ts(),
            new_entry.end_ts(),
            new_entry.value(),
            0, // for temporary use
        );
        let new_rec = Record::new(new_entry.key(), new_entry.pkey(), new_entry.value());
        let new_rec_size = new_rec.size();
        let old_rec = self.record_from_slot(&old_slot);
        let old_rec_size = old_rec.size();

        let new_rec_offset;

        // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`
        if new_rec_size <= old_rec_size || old_slot.offset() == self.rec_start_offset() {
            new_rec_offset = old_slot.offset() + old_rec_size - new_rec_size;
            if new_rec_offset < self.slot_end_offset() {
                // TODO: Compact the page
                let old_entry = MvccEntry::new(
                    old_rec.key().to_vec(),
                    old_rec.pkey().to_vec(),
                    old_rec.val().to_vec(),
                    old_slot.start_ts(),
                    new_entry.start_ts(),
                );
                self.delete_slot_at_id(slot_id);
                self.decrease_total_bytes_used(old_rec_size);
                // Reach here means new_rec_size > old_rec_size and offset matches `rec_start_offset`
                self.set_rec_start_offset(self.rec_start_offset() + old_rec_size);
                return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
            }
            self.set_record_at_offset(&new_rec, new_rec_offset);
            if new_rec_size < old_rec_size {
                self.write_bytes(old_slot.offset(), &vec![0; old_rec_size - new_rec_size]);
            }
            if old_slot.offset() == self.rec_start_offset() {
                self.set_rec_start_offset(new_rec_offset);
            }
        }
        // Case 3: New value is larger and offset doesn't match `rec_start_offset`
        else {
            if self.slot_end_offset() + new_rec_size > self.rec_start_offset() {
                // TODO: Compact the page
                let old_entry = MvccEntry::new(
                    old_rec.key().to_vec(),
                    old_rec.pkey().to_vec(),
                    old_rec.val().to_vec(),
                    old_slot.start_ts(),
                    new_entry.start_ts(),
                );
                self.delete_slot_at_id(slot_id);
                self.decrease_total_bytes_used(old_rec_size);
                return Err(AccessMethodError::OutOfSpaceForMvccUpdate(old_entry));
            }
            new_rec_offset = self.rec_start_offset() - new_rec_size;
            self.set_record_at_offset(&new_rec, new_rec_offset);
            self.set_rec_start_offset(new_rec_offset);
        }
        new_slot.set_offset(new_rec_offset);
        self.set_slot_at_id(&new_slot, slot_id);

        self.decrease_total_bytes_used(old_rec_size);
        self.increase_total_bytes_used(new_rec_size);

        let old_entry = MvccEntry::new(
            old_rec.key().to_vec(),
            old_rec.pkey().to_vec(),
            old_rec.val().to_vec(),
            old_slot.start_ts(),
            new_entry.start_ts(),
        );

        Ok(old_entry)
    }

    fn delete_at_slot_id(
        &mut self,
        ts: &Timestamp,
        slot_id: usize,
    ) -> Result<MvccEntry, AccessMethodError> {
        let ts = *ts;
        let old_slot = self.slot(slot_id);
        if old_slot.start_ts() > ts {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        if old_slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(old_slot.offset() + old_slot.rec_size());
        }
        self.delete_slot_at_id(slot_id);
        self.decrease_total_bytes_used(old_slot.rec_size());
        let old_rec = self.record_from_slot(&old_slot);
        let old_entry = MvccEntry::new(
            old_rec.key().to_vec(),
            old_rec.pkey().to_vec(),
            old_rec.val().to_vec(),
            old_slot.start_ts(),
            ts,
        );
        Ok(old_entry)
    }
}

impl CommonPageMethods for Page {
    fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        &self[offset..offset + len]
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn move_bytes(&mut self, src_start_offset: usize, src_end_offset: usize, dest_offset: usize) {
        self.copy_within(src_start_offset..src_end_offset, dest_offset);
    }
}

pub trait HeapPage: CommonPageMethods {
    fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        // Assuming duplication check has been done before this call.
        let rec = Record::new(entry.key(), entry.pkey(), entry.value());
        if SLOT_SIZE + rec.size() > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if SLOT_SIZE + rec.size() > self.free_space_before_compaction() {
            if SLOT_SIZE + rec.size() > self.free_space_after_compaction() {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: (JUN) Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }
        self.insert_at_slot_id(entry, self.slot_count())
    }

    fn update_write_repair(
        &mut self,
        entry: &MvccEntry,
        already_inserted: bool,
        already_repaired: bool,
    ) -> Result<(), AccessMethodError> {
        let mut did_repair = already_repaired;
        let mut did_insert = already_inserted;

        let pkey = entry.pkey();
        let st = entry.start_ts();

        if !already_repaired {
            for i in 0..self.slot_count() {
                let slot = self.slot(i);
                if slot.end_ts() != Timestamp::MAX {
                    continue;
                }
                if let Some(_) = self.slot_pkey_matches(&slot, pkey) {
                    if slot.start_ts() < st {
                        let mut new_slot = slot;
                        new_slot.set_end_ts(&st);
                        self.set_slot_at_id(&new_slot, i);
                        did_repair = true;
                        break;
                    }
                }
            }
        }

        if !already_inserted {
            match self.insert(entry) {
                Ok(_) => {
                    did_insert = true;
                }
                Err(AccessMethodError::OutOfSpace) => { /* skip */ }
                Err(e) => return Err(e),
            }
        }

        match (did_repair, did_insert) {
            (true, true) => Ok(()),
            (true, false) => Err(AccessMethodError::UpdateReapiredButNotInseted),
            (false, true) => Err(AccessMethodError::UpdateInsertedButNotReapired),
            (false, false) => Err(AccessMethodError::NotRepairedAndNotInserted),
        }
    }
}

pub trait RecentPage: CommonPageMethods {}

pub trait HistoryPage: CommonPageMethods {}
