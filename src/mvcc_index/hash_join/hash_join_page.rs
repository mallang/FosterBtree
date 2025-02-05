use crate::{
    access_method::AccessMethodError,
    mvcc_index::{MvccEntry, TxId},
    prelude::{Page, PageId, Timestamp, AVAILABLE_PAGE_SIZE},
};

mod header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    pub struct Header {
        next_page_id: PageId,
        next_frame_id: u32,
        total_bytes_used: u32, // (PAGE_HEADER_SIZE + slots + records)
        slot_count: u32,
        rec_start_offset: u32,
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

            Header {
                next_page_id,
                next_frame_id,
                total_bytes_used,
                slot_count,
                rec_start_offset,
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
            bytes
        }

        pub fn new() -> Self {
            Header {
                next_page_id: PageId::MAX,
                next_frame_id: u32::MAX,
                total_bytes_used: PAGE_HEADER_SIZE as u32,
                slot_count: 0,
                rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
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

        pub fn next_page_id(&self) -> PageId {
            self.next_page_id
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

        pub fn slot_count(&self) -> u32 {
            self.slot_count
        }

        pub fn set_slot_count(&mut self, slot_count: u32) {
            self.slot_count = slot_count;
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
    use super::slot::SLOT_KEY_PREFIX_SIZE;

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

use super::mvcc_hash_join_history_page::MvccHashJoinHistoryPage;

pub trait HashJoinPage {
    fn init(&mut self);

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8];
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);

    fn free_space_before_compaction(&self) -> usize {
        self.header().rec_start_offset() - self.slot_offset(self.header().slot_count())
    }
    fn free_space_after_compaction(&self) -> usize {
        AVAILABLE_PAGE_SIZE - self.header().total_bytes_used()
    }
    fn require_space(entry: &MvccEntry) -> usize {
        SLOT_SIZE + Record::new(entry.key(), entry.pkey(), entry.value()).size()
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
    fn rec_start_offset(&self) -> usize {
        self.header().rec_start_offset()
    }
    fn set_rec_start_offset(&mut self, rec_start_offset: usize) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }
    fn slot_end_offset(&self) -> usize {
        let slot_count = self.header().slot_count();
        PAGE_HEADER_SIZE + slot_count as usize * SLOT_SIZE
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

    fn slot_offset(&self, slot_id: u32) -> usize {
        PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }
    fn slot(&self, slot_id: u32) -> Slot {
        Slot::from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
    }
    fn record(&self, slot_id: u32) -> Record {
        let slot = self.slot(slot_id);
        Record::from_bytes(
            self.read_bytes(slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        )
    }
    fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
        self.write_bytes(self.slot_offset(slot_id), &slot.to_bytes());
    }
    fn set_record_at_slot_id(&mut self, slot_id: u32, rec: &Record) {
        let slot = self.slot(slot_id);
        self.write_bytes(slot.offset(), &rec.to_bytes());
    }
    fn set_record_at_offset(&mut self, offset: usize, rec: &Record) {
        self.write_bytes(offset, &rec.to_bytes());
    }

    fn insert_slot_at_id(&mut self, slot: &Slot, slot_id: u32);
    fn delete_slot_at_id(&mut self, slot_id: u32);

    fn search_slot(&self, sort_key: &[u8]) -> (bool, u32) {
        // self.binary_search(sort_key)
        self.linear_search(sort_key)
    }
    fn binary_search(&self, sort_key: &[u8]) -> (bool, u32); // (found, slot_id)
    fn linear_search(&self, sort_key: &[u8]) -> (bool, u32); // (found, slot_id)

    fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError>;
    fn insert_slot_record(&mut self, slot: &Slot, rec: &Record) -> Result<(), AccessMethodError>;
    fn insert_at_slot_id(
        &mut self,
        slot: &Slot,
        rec: &Record,
        slot_id: u32,
    ) -> Result<(), AccessMethodError>;
    fn get(&self, pkey: &[u8], ts: Timestamp) -> Result<MvccEntry, AccessMethodError>;
    fn get_record(&self, pkey: &[u8]) -> Result<Record, AccessMethodError>;

    /// Update the value of an existing key.
    /// If the key does not exist, it will return an error.
    fn update(
        &mut self,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError>;
    fn update_at_slot_id(
        &mut self,
        slot: &mut Slot,
        rec: &Record,
        slot_id: u32,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError>;

    fn delete(
        &mut self,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError>;
    fn delete_at_slot_id(
        &mut self,
        slot_id: u32,
        ts: &Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError>;

    // /// Upsert a key-value pair into the index.
    // /// If the key already exists, it will update the value.
    // /// If the key does not exist, it will insert a new key-value pair.
    // fn upsert(&mut self, key: &[u8], val: &[u8]) -> Result<(), AccessMethodError>;

    // /// Upsert with a custom merge function.
    // /// If the key already exists, it will update the value with the merge function.
    // /// If the key does not exist, it will insert a new key-value pair.
    // fn upsert_with_merge<F>(
    //     &mut self,
    //     key: &[u8],
    //     value: &[u8],
    //     update_fn: F,
    // ) -> Result<(), AccessMethodError>
    // where
    //     F: Fn(&[u8], &[u8]) -> Vec<u8>;

    // fn compact(&mut self) -> Result<(), AccessMethodError>;
    // // compact with moving record of slot_id to the front of the rec_start_offset
    // fn compact_update(&mut self, slot_id: u32) -> Result<(), AccessMethodError>;

    // fn max_record_size() -> usize {
    //     AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE
    // }
    // fn rec_size(&self, key: &[u8], val: &[u8]) -> usize {
    //     key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE) + val.len()
    // }
    // fn size_require(&self, key: &[u8], val: &[u8]) -> usize {
    //     SLOT_SIZE + self.rec_size(key, val)
    // }

    // fn slot_count(&self) -> u32;
    // fn set_slot_count(&mut self, slot_count: u32);
    // fn increment_slot_count(&mut self);
    // fn decrement_slot_count(&mut self);

    // // Helpers

    // fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    // fn write_record(&mut self, offset: usize, key: &[u8], val: &[u8]);

    // fn insert_slot_at_id(&mut self, slot_id: u32, slot: &Slot);
    // fn delete_slot_at_id(&mut self, slot_id: u32);

    // // Increment the slot count.
    // // The rec_start_offset is also updated.
    // // Only call this function when there is enough space for the slot and record.
    // fn append_slot(&mut self, slot: &Slot);

    // /// Try to append a key value pair to the page.
    // /// If the key value is too large to fit in the page, return false.
    // /// When false is returned, the page is not modified.
    // /// Otherwise, the key value is appended to the page and the page is modified.
    // fn append(&mut self, key: &[u8], value: &[u8]) -> bool;

    // /// Get the record at the slot_id.
    // /// If the slot_id is invalid, panic.
    // fn get_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, &[u8]);

    // /// Get the mutable val at the slot_id.
    // /// If the slot_id is invalid, panic.
    // /// This function is used for updating the val in place.
    // /// Updates of the record should not change the size of the val.
    // // fn get_mut_val_with_slot_id(&mut self, slot_id: u32) -> &mut [u8];

    // // Helpers (Jun)
    // fn get_key_with_slot_id(&self, slot_id: u32) -> Vec<u8>;
    // fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8];
    // fn get_value_with_slot(&self, slot: &Slot) -> &[u8];

    // fn record(&self, slot_id: u32) -> Record;
}

impl HashJoinPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        HashJoinPage::set_header(&mut *self, &header);
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

    fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let rec = Record::new(entry.key(), entry.pkey(), entry.value());
        if SLOT_SIZE + rec.size() > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if SLOT_SIZE + rec.size() > HashJoinPage::free_space_before_compaction(&*self) {
            if SLOT_SIZE + rec.size() > HashJoinPage::free_space_after_compaction(&*self) {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }

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
        HashJoinPage::insert_slot_record(&mut *self, &slot, &rec)
    }

    fn insert_slot_record(&mut self, slot: &Slot, rec: &Record) -> Result<(), AccessMethodError> {
        let (found, slot_id) = self.search_slot(rec.sort_key());
        if found {
            return Err(AccessMethodError::KeyDuplicate);
        }
        HashJoinPage::insert_at_slot_id(&mut *self, slot, rec, slot_id)
    }

    fn insert_at_slot_id(
        &mut self,
        slot: &Slot,
        rec: &Record,
        slot_id: u32,
    ) -> Result<(), AccessMethodError> {
        HashJoinPage::insert_slot_at_id(&mut *self, &slot, slot_id);

        HashJoinPage::write_bytes(&mut *self, slot.offset(), rec.to_bytes().as_ref());
        let mut header = HashJoinPage::header(&*self);
        header.set_rec_start_offset(slot.offset());
        header.inc_total_bytes_used(rec.size());
        HashJoinPage::set_header(&mut *self, &header);

        Ok(())
    }

    fn update(
        &mut self,
        pkey: &[u8],
        entry: &MvccEntry,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            println!(
                "free space before compaction: {}",
                HashJoinPage::free_space_before_compaction(&*self)
            );
            println!(
                "free space after compaction: {}",
                HashJoinPage::free_space_after_compaction(&*self)
            );
            println!(
                "total bytes used: {}",
                HashJoinPage::total_bytes_used(&*self)
            );
            println!("slot count: {}", HashJoinPage::header(&*self).slot_count());
            println!(
                "first slot and record: {:?} / {:?}",
                HashJoinPage::slot(&*self, 0),
                HashJoinPage::record(&*self, 0)
            );
            println!(
                "last slot and record: {:?} / {:?}",
                HashJoinPage::slot(&*self, HashJoinPage::header(&*self).slot_count() - 1),
                HashJoinPage::record(&*self, HashJoinPage::header(&*self).slot_count() - 1)
            );
            // for i in (0..self.slot_count()) {
            //     let slot = HashJoinPage::slot(&*self, i);
            //     let rec = HashJoinPage::record(&*self, i);
            //     println!("Slot: {:?}", slot);
            //     println!("Record: {:?}", rec);
            // }
            return Err(AccessMethodError::KeyNotFound);
        }
        let slot = HashJoinPage::slot(&*self, slot_id);
        if slot.start_ts() > entry.start_ts() {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        let mut new_slot = Slot::new(
            entry.key(),
            entry.pkey(),
            0,
            entry.start_ts(),
            entry.end_ts(),
            entry.value(),
            slot.offset(), // for temporary use
        );
        let new_rec = Record::new(entry.key(), entry.pkey(), entry.value());
        HashJoinPage::update_at_slot_id(&mut *self, &mut new_slot, &new_rec, slot_id)
    }

    fn update_at_slot_id(
        &mut self,
        new_slot: &mut Slot,
        new_rec: &Record,
        slot_id: u32,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let old_slot = HashJoinPage::slot(self, slot_id);
        let old_rec = HashJoinPage::record(self, slot_id);

        let old_rec_size = old_rec.size();
        let new_rec_size = new_rec.size();

        let new_rec_offset;

        if old_rec.key() != new_rec.key() {
            return Err(AccessMethodError::KeyNotFound);
        }

        // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`
        if new_rec_size <= old_rec_size || old_slot.offset() == HashJoinPage::rec_start_offset(self)
        {
            new_rec_offset = old_slot.offset() + old_rec_size - new_rec_size;
            self.set_record_at_offset(new_rec_offset, new_rec);
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
            new_rec_offset = HashJoinPage::rec_start_offset(self) - new_rec_size;
            if HashJoinPage::slot_end_offset(self) > new_rec_offset {
                // TODO: (JUN) Delete here and need to pass the rec to next page
                return Err(AccessMethodError::OutOfSpace);
            }
            self.set_record_at_offset(new_rec_offset, new_rec);
            self.set_rec_start_offset(new_rec_offset);
        }

        new_slot.set_offset(new_rec_offset);
        HashJoinPage::set_slot(self, slot_id, &new_slot);

        HashJoinPage::increase_total_bytes_used(self, new_rec_size);
        HashJoinPage::decrease_total_bytes_used(self, old_rec_size);

        Ok((old_slot.start_ts(), old_rec.val().to_vec()))
    }

    fn delete(
        &mut self,
        pkey: &[u8],
        ts: &Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        HashJoinPage::delete_at_slot_id(&mut *self, slot_id, ts)
    }

    fn delete_at_slot_id(
        &mut self,
        slot_id: u32,
        ts: &Timestamp,
    ) -> Result<(Timestamp, Vec<u8>), AccessMethodError> {
        let slot = self.slot(slot_id);
        if slot.start_ts() > *ts {
            return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
        }
        let rec = self.record(slot_id);
        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(slot.offset() + rec.size());
        }
        HashJoinPage::delete_slot_at_id(self, slot_id);
        HashJoinPage::decrease_total_bytes_used(self, rec.size());
        Ok((slot.start_ts(), rec.val().to_vec()))
    }

    fn insert_slot_at_id(&mut self, slot: &Slot, slot_id: u32) {
        if slot_id < self.slot_count() as u32 {
            let start_offset = HashJoinPage::slot_offset(&*self, slot_id);
            let end_offset = HashJoinPage::slot_offset(&*self, self.slot_count() as u32);
            self.copy_within(start_offset..end_offset, start_offset + SLOT_SIZE);
        }

        HashJoinPage::set_slot(&mut *self, slot_id, slot);
        let mut header = HashJoinPage::header(&*self);
        header.inc_slot_count();
        header.inc_total_bytes_used(SLOT_SIZE);
        HashJoinPage::set_header(&mut *self, &header);
    }

    fn delete_slot_at_id(&mut self, slot_id: u32) {
        if slot_id < self.slot_count() as u32 {
            let start_offset = HashJoinPage::slot_offset(&*self, slot_id + 1);
            let end_offset = HashJoinPage::slot_offset(&*self, self.slot_count() as u32);
            self.copy_within(start_offset..end_offset, start_offset - SLOT_SIZE);
        }

        let mut header = HashJoinPage::header(&*self);
        header.dec_slot_count();
        header.dec_total_bytes_used(SLOT_SIZE);
        HashJoinPage::set_header(&mut *self, &header);
    }

    fn get(&self, pkey: &[u8], ts: Timestamp) -> Result<MvccEntry, AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        let slot = HashJoinPage::slot(&*self, slot_id);
        let rec = Record::from_bytes(
            HashJoinPage::read_bytes(&*self, slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        );
        Ok(MvccEntry::new(
            rec.key().to_vec(),
            rec.pkey().to_vec(),
            rec.val().to_vec(),
            slot.start_ts(),
            slot.end_ts(),
        ))
    }

    fn get_record(&self, pkey: &[u8]) -> Result<Record, AccessMethodError> {
        let (found, slot_id) = self.search_slot(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        let slot = HashJoinPage::slot(&*self, slot_id);
        let rec = Record::from_bytes(
            HashJoinPage::read_bytes(&*self, slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        );
        Ok(rec)
    }

    fn binary_search(&self, sort_key: &[u8]) -> (bool, u32) {
        let mut high = self.slot_count() as u32;
        if high == 0 {
            return (false, 0);
        }
        high -= 1;

        let high_rec = HashJoinPage::record(&*self, high);
        let high_sort_key = high_rec.sort_key();
        if sort_key > high_sort_key {
            return (false, self.slot_count() as u32);
        } else if sort_key == high_sort_key {
            return (true, high);
        } else if self.slot_count() == 1 {
            return (false, 0);
        }

        let mut low = 0;
        let low_rec = HashJoinPage::record(&*self, low);
        let low_sort_key = low_rec.sort_key();
        if sort_key < low_sort_key {
            return (false, 0);
        } else if sort_key == low_sort_key {
            return (true, low);
        }

        while low < high {
            let mid = low + (high - low) / 2;
            let mid_rec = HashJoinPage::record(&*self, mid);
            let mid_sort_key = mid_rec.sort_key();
            if mid_sort_key == sort_key {
                return (true, mid);
            } else if mid_sort_key < sort_key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        (false, low)
    }

    fn linear_search(&self, sort_key: &[u8]) -> (bool, u32) {
        for i in 0..self.slot_count() {
            let rec = HashJoinPage::record(&*self, i);
            if rec.sort_key() == sort_key {
                return (true, i);
            }
        }
        (false, self.slot_count() as u32)
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
    fn test_insert_entry() {
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        let entry = MvccEntry::new(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![100, 101, 102],
            100,
            200,
        );

        assert!(
            HashJoinPage::insert(&mut page, &entry).is_ok(),
            "Insert should succeed"
        );

        let retrieved = HashJoinPage::get(&page, &entry.pkey(), entry.start_ts())
            .expect("Failed to retrieve inserted entry");

        assert_eq!(retrieved, entry);
    }

    #[test]
    fn test_update_entry() {
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        let entry = MvccEntry::new(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![100, 101, 102],
            100,
            200,
        );
        assert!(
            HashJoinPage::insert(&mut page, &entry).is_ok(),
            "Insert should succeed"
        );

        let updated_entry = MvccEntry::new(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![200, 201, 202],
            100,
            300,
        );

        assert!(
            HashJoinPage::update(&mut page, &entry.pkey(), &updated_entry).is_ok(),
            "Update should succeed"
        );

        let retrieved = HashJoinPage::get(&page, &entry.pkey(), updated_entry.start_ts())
            .expect("Failed to retrieve updated entry");

        assert_eq!(retrieved, updated_entry);
    }

    #[test]
    fn test_multiple_insert_update_get_with_large_keys() {
        // Create and initialize a new page.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Create multiple entries with keys and pkeys larger than the configured prefix sizes.
        let mut entries = Vec::new();
        for i in 0..100 {
            // Generate keys and pkeys that are definitely longer than 8 bytes.
            let key = format!("key-large-{:03}-extra", i).into_bytes();
            let pkey = format!("pkey-large-{:03}-extra", i).into_bytes();
            let value = format!("value-{:03}", i).into_bytes();

            // Assert that the key and pkey lengths exceed the prefix sizes.
            assert!(
                key.len() > SLOT_KEY_PREFIX_SIZE,
                "Key length {} is not greater than {}",
                key.len(),
                SLOT_KEY_PREFIX_SIZE
            );
            assert!(
                pkey.len() > SLOT_PKEY_PREFIX_SIZE,
                "Pkey length {} is not greater than {}",
                pkey.len(),
                SLOT_PKEY_PREFIX_SIZE
            );

            // Create an MVCC entry.
            let entry = MvccEntry::new(key, pkey, value, 100 + i, 200 + i);
            entries.push(entry);
        }

        // Insert all entries into the page.
        for entry in &entries {
            assert!(
                HashJoinPage::insert(&mut page, entry).is_ok(),
                "Insert should succeed for entry with pkey: {:?}",
                entry.pkey()
            );
        }

        // Retrieve and verify each inserted entry.
        for entry in &entries {
            let retrieved = HashJoinPage::get(&page, &entry.pkey(), entry.start_ts())
                .expect("Failed to retrieve inserted entry");
            assert_eq!(
                retrieved,
                *entry,
                "Retrieved entry does not match the inserted entry for pkey: {:?}",
                entry.pkey()
            );
        }

        // Update each entry with a new value and an updated end timestamp.
        let updated_entries: Vec<MvccEntry> = entries
            .iter()
            .map(|entry| {
                let mut new_value = entry.value.clone();
                new_value.extend_from_slice(b"-updated");
                MvccEntry::new(
                    entry.key.clone(),
                    entry.pkey.clone(),
                    new_value,
                    entry.start_ts(),
                    entry.end_ts() + 50,
                )
            })
            .collect();

        // Perform the updates.
        for (old_entry, updated_entry) in entries.iter().zip(updated_entries.iter()) {
            assert!(
                HashJoinPage::update(&mut page, &old_entry.pkey(), updated_entry).is_ok(),
                "Update should succeed for entry with pkey: {:?}",
                old_entry.pkey()
            );
        }

        // Retrieve and verify each updated entry.
        for updated_entry in &updated_entries {
            let retrieved =
                HashJoinPage::get(&page, &updated_entry.pkey(), updated_entry.start_ts())
                    .expect("Failed to retrieve updated entry");
            assert_eq!(
                retrieved,
                *updated_entry,
                "Retrieved updated entry does not match the expected updated entry for pkey: {:?}",
                updated_entry.pkey()
            );
        }
    }

    // #[test]
    // fn test_pkeys_sorted() {
    //     let mut page = Page::new_empty();
    //     HashJoinPage::init(&mut page);

    //     // Create entries with pkeys deliberately out-of-order.
    //     let entries = vec![
    //         MvccEntry::new(
    //             b"key-large-001-extra".to_vec(),
    //             b"pkey-large-005-extra".to_vec(),
    //             b"value-001".to_vec(),
    //             110,
    //             210,
    //         ),
    //         MvccEntry::new(
    //             b"key-large-002-extra".to_vec(),
    //             b"pkey-large-002-extra".to_vec(),
    //             b"value-002".to_vec(),
    //             111,
    //             211,
    //         ),
    //         MvccEntry::new(
    //             b"key-large-003-extra".to_vec(),
    //             b"pkey-large-009-extra".to_vec(),
    //             b"value-003".to_vec(),
    //             112,
    //             212,
    //         ),
    //         MvccEntry::new(
    //             b"key-large-004-extra".to_vec(),
    //             b"pkey-large-001-extra".to_vec(),
    //             b"value-004".to_vec(),
    //             113,
    //             213,
    //         ),
    //         MvccEntry::new(
    //             b"key-large-005-extra".to_vec(),
    //             b"pkey-large-007-extra".to_vec(),
    //             b"value-005".to_vec(),
    //             114,
    //             214,
    //         ),
    //     ];

    //     // Insert the entries in the given (unsorted) order.
    //     for entry in &entries {
    //         assert!(
    //             HashJoinPage::insert(&mut page, entry).is_ok(),
    //             "Insert failed for pkey: {:?}",
    //             entry.pkey()
    //         );
    //     }

    //     // Now verify that the internal slot order is sorted by pkey.
    //     // Assume that the header contains the number of slots (records) inserted.
    //     let slot_count = HashJoinPage::header(&page).slot_count();
    //     let mut prev_pkey: Option<Vec<u8>> = None;

    //     for slot_id in 0..slot_count {
    //         // Retrieve the record stored at this slot.
    //         let record = HashJoinPage::record(&page, slot_id);
    //         // Assume that `record.pkey()` returns the primary key as a &[u8].
    //         let current_pkey = record.pkey();

    //         if let Some(prev) = prev_pkey {
    //             // Check that the previous pkey is lexicographically not greater than the current one.
    //             assert!(
    //                 prev.as_slice() <= current_pkey,
    //                 "pkeys are not sorted: {:?} > {:?}",
    //                 prev,
    //                 current_pkey
    //             );
    //         }
    //         prev_pkey = Some(current_pkey.to_vec());
    //     }
    // }

    #[test]
    fn test_delete_entry() {
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        let entry = MvccEntry::new(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![100, 101, 102],
            100,
            200,
        );

        assert!(
            HashJoinPage::insert(&mut page, &entry).is_ok(),
            "Insert should succeed"
        );

        assert!(
            HashJoinPage::delete(&mut page, &entry.pkey(), &150).is_ok(),
            "Delete should succeed"
        );

        assert!(
            HashJoinPage::get(&page, &entry.pkey(), entry.start_ts()).is_err(),
            "Deleted entry should not be retrievable"
        );
    }

    #[test]
    fn test_multiple_insert_update_delete_with_large_keys_sorted() {
        // Initialize a new page.
        let mut page = Page::new_empty();
        HashJoinPage::init(&mut page);

        // Create multiple entries with keys and pkeys longer than the defined prefix sizes.
        let mut entries = Vec::new();
        for i in 0..100 {
            let key = format!("key-large-{:03}-extra", i).into_bytes();
            let pkey = format!("pkey-large-{:03}-extra", i).into_bytes();
            let value = format!("value-{:03}", i).into_bytes();

            // Ensure keys and pkeys exceed the required prefix size.
            assert!(
                key.len() > SLOT_KEY_PREFIX_SIZE,
                "Key length {} is not greater than {}",
                key.len(),
                SLOT_KEY_PREFIX_SIZE
            );
            assert!(
                pkey.len() > SLOT_PKEY_PREFIX_SIZE,
                "Pkey length {} is not greater than {}",
                pkey.len(),
                SLOT_PKEY_PREFIX_SIZE
            );

            let entry = MvccEntry::new(key, pkey, value, 100 + i, 200 + i);
            entries.push(entry);
        }

        // Insert all entries into the page.
        for entry in &entries {
            assert!(
                HashJoinPage::insert(&mut page, entry).is_ok(),
                "Insert failed for entry with pkey: {:?}",
                entry.pkey()
            );
        }

        // Update a couple of entries to change only the value (pkey remains unchanged).
        let mut updated_entries = entries.clone();
        // For example, update entry at index 2.
        updated_entries[2] = MvccEntry::new(
            updated_entries[2].key.clone(),
            updated_entries[2].pkey.clone(), // pkey is unchanged.
            {
                let mut new_value = updated_entries[2].value.clone();
                new_value.extend_from_slice(b"-updated");
                new_value
            },
            updated_entries[2].start_ts,
            updated_entries[2].end_ts + 50,
        );
        // And update entry at index 7.
        updated_entries[7] = MvccEntry::new(
            updated_entries[7].key.clone(),
            updated_entries[7].pkey.clone(), // pkey is unchanged.
            {
                let mut new_value = updated_entries[7].value.clone();
                new_value.extend_from_slice(b"-updated");
                new_value
            },
            updated_entries[7].start_ts,
            updated_entries[7].end_ts + 50,
        );

        // Apply updates by using the original entry's pkey to locate the record.
        for (old_entry, updated_entry) in entries.iter().zip(updated_entries.iter()) {
            assert!(
                HashJoinPage::update(&mut page, old_entry.pkey(), updated_entry).is_ok(),
                "Update failed for entry with pkey: {:?}",
                old_entry.pkey()
            );
        }

        // Delete a subset of entries (for example, indices 1, 4, and 8).
        let delete_indices = vec![1, 4, 8];
        for &i in &delete_indices {
            let entry = &updated_entries[i];
            assert!(
                HashJoinPage::delete(&mut page, entry.pkey(), &(100 + i as u64)).is_ok(),
                "Delete failed for entry with pkey: {:?}",
                entry.pkey()
            );
        }

        // Verify that deleted entries are no longer retrievable.
        for &i in &delete_indices {
            let entry = &updated_entries[i];
            assert!(
                HashJoinPage::get(&page, entry.pkey(), entry.start_ts()).is_err(),
                "Deleted entry with pkey {:?} should not be retrievable",
                entry.pkey()
            );
        }

        // Verify that remaining entries are still retrievable and updated.
        let remaining_entries: Vec<MvccEntry> = updated_entries
            .into_iter()
            .enumerate()
            .filter_map(|(i, entry)| {
                if delete_indices.contains(&i) {
                    None
                } else {
                    Some(entry)
                }
            })
            .collect();

        for entry in &remaining_entries {
            let retrieved = HashJoinPage::get(&page, entry.pkey(), entry.start_ts())
                .expect("Remaining entry should be retrievable");
            assert_eq!(
                retrieved,
                *entry,
                "Mismatch for entry with pkey {:?}",
                entry.pkey()
            );
        }

        // Finally, check that the internal slot order is sorted by primary key.
        let slot_count = HashJoinPage::header(&page).slot_count();
        let mut prev_pkey: Option<Vec<u8>> = None;
        for slot_id in 0..slot_count {
            let record = HashJoinPage::record(&page, slot_id);
            let current_pkey = record.pkey();
            if let Some(prev) = prev_pkey {
                assert!(
                    prev.as_slice() <= current_pkey,
                    "Slots not sorted: previous pkey {:?} > current pkey {:?}",
                    prev,
                    current_pkey
                );
            }
            prev_pkey = Some(current_pkey.to_vec());
        }
    }
}
