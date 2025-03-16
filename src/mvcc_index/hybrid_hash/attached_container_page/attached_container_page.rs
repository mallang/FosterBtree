mod header {
    use crate::page::{AVAILABLE_PAGE_SIZE, PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();
    const BASE_HEADER_SIZE: usize = PAGE_SIZE - AVAILABLE_PAGE_SIZE;
    const TOTAL_HEADER_SIZE: usize = PAGE_HEADER_SIZE + BASE_HEADER_SIZE;

    #[derive(Debug)]
    pub struct Header {
        total_bytes_used: u32,
        slot_meta_end_offset: u32,
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

            let slot_meta_end_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse slot_meta_end_offset")?,
            );
            current_pos += 4;

            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse rec_start_offset")?,
            );

            Ok(Self {
                total_bytes_used,
                slot_meta_end_offset,
                rec_start_offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.slot_meta_end_offset.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4]
                .copy_from_slice(&self.rec_start_offset.to_be_bytes());

            bytes
        }

        pub fn new() -> Self {
            Self {
                total_bytes_used: PAGE_HEADER_SIZE as u32,
                slot_meta_end_offset: PAGE_HEADER_SIZE as u32,
                rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
            }
        }
        pub fn total_bytes_used(&self) -> u32 {
            self.total_bytes_used
        }
        pub fn set_total_bytes_used(&mut self, total_bytes_used: u32) {
            self.total_bytes_used = total_bytes_used;
        }

        pub fn slot_meta_end_offset(&self) -> u32 {
            self.slot_meta_end_offset
        }
        pub fn set_slot_meta_end_offset(&mut self, slot_meta_end_offset: u32) {
            self.slot_meta_end_offset = slot_meta_end_offset;
        }

        pub fn rec_start_offset(&self) -> u32 {
            self.rec_start_offset
        }
        pub fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
            self.rec_start_offset = rec_start_offset;
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

        pub fn slot_end_offset(&self) -> u32 {
            self.slot_meta_end_offset
        }
    }
}

use header::*;

pub mod slot {
    use std::u32;

    use crate::log_warn;

    pub const DELETE_MARKER_IN_VAL_SIZE: u32 = u32::MAX;
    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq, Default, Copy, Clone)]
    pub struct InterPageLoc {
        pub page_id: u32,
        pub b_offset: u32,
    }

    impl InterPageLoc {
        pub fn new_end() -> Self {
            Self {
                page_id: u32::MAX,
                b_offset: u32::MAX,
            }
        }
    }

    #[derive(Debug, PartialEq, Default)]
    pub struct SlotMeta<'a> {
        pub latest_version_loc: InterPageLoc,
        pub prev_meta_loc: InterPageLoc,

        // pub key_size: u32,
        // pub pkey_size: u32,
        pub remain_key: &'a [u8],
        pub remain_pkey: &'a [u8],
    }

    impl<'a> SlotMeta<'a> {
        pub fn from_bytes(slot: &Slot, bytes: &'a [u8]) -> Self {
            let mut current_pos = 0;

            let latest_version_page_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse latest_version_page_id")
                    .unwrap(),
            );
            current_pos += 4;

            let latest_version_b_off = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse latest_version_b_off")
                    .unwrap(),
            );
            current_pos += 4;

            let prev_meta_page_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse prev_meta_page_id")
                    .unwrap(),
            );
            current_pos += 4;

            let prev_meta_b_off = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse prev_meta_b_off")
                    .unwrap(),
            );
            current_pos += 4;

            let remain_key_size = (slot.key_size as usize).saturating_sub(SLOT_KEY_PREFIX_SIZE);
            let remain_pkey_size = (slot.pkey_size as usize).saturating_sub(SLOT_PKEY_PREFIX_SIZE);

            Self {
                prev_meta_loc: InterPageLoc {
                    page_id: prev_meta_page_id,
                    b_offset: prev_meta_b_off,
                },
                latest_version_loc: InterPageLoc {
                    page_id: latest_version_page_id,
                    b_offset: latest_version_b_off,
                },
                remain_key: &bytes[current_pos..current_pos + remain_key_size],
                remain_pkey: &bytes[current_pos + remain_key_size
                    ..current_pos + remain_key_size + remain_pkey_size],
            }
        }

        pub fn space_need_from_slot(slot: &Slot) -> u32 {
            let key_size = slot.key_size();
            let pkey_size = slot.pkey_size();

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
            remain_key_size + remain_pkey_size + (std::mem::size_of::<InterPageLoc>() * 2) as u32
        }

        pub fn space_need(&self) -> u32 {
            let remain_key_size = self.remain_key.len() as u32;
            let remain_pkey_size = self.remain_pkey.len() as u32;
            remain_key_size + remain_pkey_size + (std::mem::size_of::<InterPageLoc>() * 2) as u32
        }

        pub fn space_need_from_kpk(key: &[u8], pkey: &[u8]) -> u32 {
            let key_size = key.len() as u32;
            let pkey_size = pkey.len() as u32;

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
            remain_key_size + remain_pkey_size + (std::mem::size_of::<InterPageLoc>() * 2) as u32
        }

        pub fn check_match_remain_key_pkey(&self, slot: &Slot, key: &[u8], pkey: &[u8]) -> bool {
            let slot_key_size = slot.key_size();
            let slot_pkey_size = slot.pkey_size();

            let slot_remain_key_size = slot_key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let slot_remain_pkey_size = slot_pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);

            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

            let remain_key = &key[key_prefix_len..];
            let remain_pkey = &pkey[pkey_prefix_len..];

            remain_key.len() as u32 == slot_remain_key_size
                && remain_pkey.len() as u32 == slot_remain_pkey_size
                && self.remain_key == remain_key
                && self.remain_pkey == remain_pkey
        }

        pub fn check_match_remain_key(&self, slot: &Slot, key: &[u8]) -> bool {
            let slot_key_size = slot.key_size();

            let slot_remain_key_size = slot_key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);

            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());

            let remain_key = &key[key_prefix_len..];

            remain_key.len() as u32 == slot_remain_key_size && self.remain_key == remain_key
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut latest_bytes = Vec::<u8>::new();
            latest_bytes.extend(u32::to_be_bytes(self.latest_version_loc.b_offset));
            latest_bytes.extend(u32::to_be_bytes(self.latest_version_loc.page_id));
            latest_bytes.extend(u32::to_be_bytes(self.prev_meta_loc.b_offset));
            latest_bytes.extend(u32::to_be_bytes(self.prev_meta_loc.page_id));
            latest_bytes.extend_from_slice(&self.remain_key);
            latest_bytes.extend_from_slice(&self.remain_pkey);
            latest_bytes
        }

        pub fn print_meta_of_slot(self, slot: &Slot) {
            let space_need = Self::space_need_from_slot(slot);
            log_warn!("space: {space_need}, slot_meta: {:?}", self);
        }
    }

    const SLOT_HASH_SEED: u32 = 23333;
    pub fn get_slot_hash(bytes: &[u8]) -> u32 {
        farmhash::hash32_with_seed(bytes, SLOT_HASH_SEED)
    }

    pub fn get_remain_key(key: &[u8]) -> &[u8] {
        if key.len() > SLOT_KEY_PREFIX_SIZE {
            &key[SLOT_KEY_PREFIX_SIZE..]
        } else {
            &[]
        }
    }

    pub fn get_remain_pkey(pkey: &[u8]) -> &[u8] {
        if pkey.len() > SLOT_PKEY_PREFIX_SIZE {
            &pkey[SLOT_PKEY_PREFIX_SIZE..]
        } else {
            &[]
        }
    }

    #[derive(Debug, PartialEq, Default, Copy, Clone)]
    pub struct Slot {
        key_hash: u32,
        pkey_hash: u32,
        meta_loc: InterPageLoc,
        key_size: u32,
        pkey_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        pkey_prefix: [u8; SLOT_PKEY_PREFIX_SIZE],
    }

    impl Slot {
        pub fn new(key: &[u8], pkey: &[u8], meta_loc: InterPageLoc) -> Self {
            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

            Self {
                key_hash: get_slot_hash(key),
                pkey_hash: get_slot_hash(pkey),
                meta_loc,
                key_size: key.len() as u32,
                pkey_size: pkey.len() as u32,
                key_prefix: {
                    let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
                    key_prefix[..key_prefix_len].copy_from_slice(&key[..key_prefix_len]);
                    key_prefix
                },
                pkey_prefix: {
                    let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
                    pkey_prefix[..pkey_prefix_len].copy_from_slice(&pkey[..pkey_prefix_len]);
                    pkey_prefix
                },
            }
        }

        pub fn key_hash(&self) -> u32 {
            self.key_hash
        }

        pub fn pkey_hash(&self) -> u32 {
            self.pkey_hash
        }

        pub fn space_need() -> u32 {
            std::mem::size_of::<Slot>() as u32
        }

        pub fn match_k_pk_prefix(&self, key: &[u8], pkey: &[u8]) -> bool {
            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

            self.key_size as usize == key.len()
                && self.pkey_size as usize == pkey.len()
                && self.key_prefix[..key_prefix_len] == key[..key_prefix_len]
                && self.pkey_prefix[..pkey_prefix_len] == pkey[..pkey_prefix_len]
        }

        pub fn match_k_prefix(&self, key: &[u8]) -> bool {
            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());

            self.key_size as usize == key.len()
                && self.key_prefix[..key_prefix_len] == key[..key_prefix_len]
        }

        // pub fn to_bytes(&self) -> Vec<u8> {
        //     let mut bytes = Vec::<u8>::new();
        //     let mut current_pos = 0;

        //     bytes.extend(&self.key_hash.to_be_bytes());
        //     current_pos += 4;

        //     bytes.extend(&self.pkey_hash.to_be_bytes());
        //     current_pos += 4;

        //     bytes.extend(&self.meta_loc.page_id.to_be_bytes());
        //     current_pos += 4;

        //     bytes.extend(&self.meta_loc.b_offset.to_be_bytes());
        //     current_pos += 4;

        //     bytes.extend(&self.key_size.to_be_bytes());
        //     current_pos += 4;

        //     bytes.extend(&self.pkey_size.to_be_bytes());
        //     current_pos += 4;

        //     bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE].copy_from_slice(&self.key_prefix);
        //     current_pos += SLOT_KEY_PREFIX_SIZE;

        //     bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE].copy_from_slice(&self.pkey_prefix);

        //     bytes
        // }

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

        pub fn meta_loc(&self) -> InterPageLoc {
            self.meta_loc
        }

        pub fn remain_key_size(&self) -> u32 {
            let key_size = self.key_size();
            key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
        }

        pub fn remain_pkey_size(&self) -> u32 {
            let pkey_size = self.pkey_size();
            pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
        }
    }

    impl std::cmp::Eq for Slot {}

    impl PartialOrd for Slot {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Slot {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            if self.key_hash == other.key_hash {
                self.pkey_hash.cmp(&other.pkey_hash)
            } else {
                self.key_hash.cmp(&other.key_hash)
            }
        }
    }
}
use slot::*;

mod record {
    use crate::prelude::Timestamp;

    use super::{slot::InterPageLoc, DELETE_MARKER_IN_VAL_SIZE};

    #[derive(Debug, Clone)]
    pub struct CommittedRecord<'a> {
        pub prev_offset: InterPageLoc,
        pub start_ts: u64,
        pub val_size: u32,
        pub value: &'a [u8],
    }

    #[derive(Debug, Clone)]
    pub struct CommittedRecordOwned {
        pub prev_offset: InterPageLoc,
        pub start_ts: u64,
        pub val_size: u32,
        pub value: Vec<u8>,
    }

    impl CommittedRecordOwned {
        pub fn is_deleted(&self) -> bool {
            self.val_size == DELETE_MARKER_IN_VAL_SIZE
        }
    }

    impl<'a> CommittedRecord<'a> {
        pub fn to_owned(&self) -> CommittedRecordOwned {
            CommittedRecordOwned {
                prev_offset: self.prev_offset,
                start_ts: self.start_ts,
                val_size: self.val_size,
                value: self.value.to_vec(),
            }
        }
        pub fn from_bytes(bytes: &'a [u8]) -> Self {
            let mut current_pos = 0;

            let prev_page_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "parse failed")
                    .unwrap(),
            );
            current_pos += 4;

            let prev_b_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "parse failed")
                    .unwrap(),
            );
            current_pos += 4;

            let start_ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + 8]
                    .try_into()
                    .map_err(|_| "parse failed")
                    .unwrap(),
            );
            current_pos += 8;

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "parse failed")
                    .unwrap(),
            );
            current_pos += 4;

            let actual_val_size = if val_size == DELETE_MARKER_IN_VAL_SIZE {
                0
            } else {
                val_size
            };
            let value = &bytes[current_pos..current_pos + actual_val_size as usize];

            Self {
                prev_offset: InterPageLoc {
                    page_id: prev_page_id,
                    b_offset: prev_b_offset,
                },
                start_ts,
                val_size,
                value,
            }
        }

        pub fn is_deleted(&self) -> bool {
            self.val_size == DELETE_MARKER_IN_VAL_SIZE
        }
        pub fn get_prev_and_ts(bytes: &[u8]) -> (u32, u64) {
            let next_off = Self::prev_offset_from_bytes(bytes);

            let tx_id = Self::ts_from_bytes(bytes);

            (next_off, tx_id)
        }

        pub fn to_bytes(
            prev_loc: InterPageLoc,
            ts: Timestamp,
            value: &[u8],
            is_delete: bool,
        ) -> Vec<u8> {
            let mut latest_bytes = Vec::<u8>::new();
            latest_bytes.extend(u32::to_be_bytes(prev_loc.page_id));
            latest_bytes.extend(u32::to_be_bytes(prev_loc.b_offset));
            latest_bytes.extend(u64::to_be_bytes(ts));
            if is_delete {
                latest_bytes.extend(u32::to_be_bytes(DELETE_MARKER_IN_VAL_SIZE));
            } else {
                latest_bytes.extend(u32::to_be_bytes(value.len() as u32));
                latest_bytes.extend_from_slice(value);
            }
            latest_bytes
        }

        pub fn space_need_from_value(value: &[u8], is_deleted: bool) -> u32 {
            let base_space_need = (std::mem::size_of::<InterPageLoc>()
                + std::mem::size_of::<u64>()
                + std::mem::size_of::<u32>()) as u32;
            if is_deleted {
                base_space_need
            } else {
                base_space_need + value.len() as u32
            }
        }

        pub fn val_size_from_bytes(bytes: &[u8]) -> Option<u32> {
            let res = u32::from_be_bytes(
                bytes[12..16]
                    .try_into()
                    .map_err(|_| "parse failed")
                    .unwrap(),
            );
            if res == DELETE_MARKER_IN_VAL_SIZE {
                None
            } else {
                Some(res)
            }
        }

        pub fn prev_offset_from_bytes(bytes: &[u8]) -> u32 {
            u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| "parse failed").unwrap())
        }

        pub fn ts_from_bytes(bytes: &[u8]) -> u64 {
            u64::from_be_bytes(bytes[4..12].try_into().map_err(|_| "parse failed").unwrap())
        }
    }
}
pub use record::*;

use crate::{
    bp::{FrameWriteGuard, MemPool},
    log_warn,
    mvcc_index::hybrid_hash::hash_join_table_common::HashTableAccessMethodError,
    page::Page,
    prelude::Timestamp,
};

type Result<T> = core::result::Result<T, HashTableAccessMethodError>;

pub trait BytesOpPage {
    fn read_bytes(&self, offset: u32, len: u32) -> &[u8];
    fn write_bytes(&mut self, offset: u32, bytes: &[u8]);
}

impl BytesOpPage for Page {
    fn read_bytes(&self, offset: u32, len: u32) -> &[u8] {
        &self[offset as usize..(offset + len) as usize]
    }
    fn write_bytes(&mut self, offset: u32, bytes: &[u8]) {
        self[offset as usize..(offset + bytes.len() as u32) as usize].copy_from_slice(bytes);
    }
}

#[derive(Debug)]
pub struct WritePageLocAgent {
    pub offset: u32,
    pub page_id: u32,
}

impl WritePageLocAgent {
    pub fn new(offset: u32, pid: u32) -> Self {
        Self {
            offset,
            page_id: pid,
        }
    }
}

pub trait AttachedPage: BytesOpPage {
    fn init(&mut self);
    fn get_header(&self) -> Header;
    fn set_header(&mut self, header: Header);

    fn get_slot_meta(&self, slot: &Slot) -> SlotMeta;
    fn set_slot_meta(&mut self, slot_meta: &SlotMeta) -> Result<u32>;
    fn get_record(&self, offset: u32) -> CommittedRecord;

    fn insert_first_version(&mut self, value: &[u8], ts: Timestamp, is_delete: bool)
        -> Result<u32>;
    fn get_rec_value(&self, loc_offset: u32) -> Option<Vec<u8>>;

    // fn find_pivot_rec_to_write<T: MemPool + 'static>(&self, mem_pool: T, slot: &Slot, meta_off: u32) -> WriteU32Agent;
}

impl AttachedPage for Page {
    fn get_header(&self) -> Header {
        let header_bytes = self.read_bytes(0, PAGE_HEADER_SIZE as u32);
        Header::from_bytes(header_bytes).unwrap()
    }

    fn get_rec_value(&self, loc_offset: u32) -> Option<Vec<u8>> {
        let rec = CommittedRecord::from_bytes(&self[loc_offset as usize..]);
        match rec.is_deleted() {
            true => None,
            false => Some(rec.value.to_vec()),
        }
    }

    fn get_record(&self, offset: u32) -> CommittedRecord {
        CommittedRecord::from_bytes(&self[offset as usize..])
    }

    fn get_slot_meta(&self, slot: &Slot) -> SlotMeta {
        let meta_bytes = self.read_bytes(
            slot.meta_loc().b_offset,
            SlotMeta::space_need_from_slot(slot),
        );
        SlotMeta::from_bytes(slot, meta_bytes)
    }

    fn init(&mut self) {
        let header = Header::new();
        self.write_bytes(0, &header.to_bytes());
    }

    fn insert_first_version(
        &mut self,
        value: &[u8],
        ts: Timestamp,
        is_delete: bool,
    ) -> Result<u32> {
        let mut header = self.get_header();
        let unused_space = header.rec_start_offset() - header.slot_end_offset();
        let rec_space = CommittedRecord::space_need_from_value(value, is_delete);

        if unused_space < rec_space {
            return Err(HashTableAccessMethodError::OutOfSpace);
        }
        let new_rec_start_off = header.rec_start_offset() - rec_space;

        self.write_bytes(
            new_rec_start_off,
            &CommittedRecord::to_bytes(InterPageLoc::new_end(), ts, value, is_delete),
        );
        header.set_rec_start_offset(new_rec_start_off);
        header.increase_total_bytes_used(rec_space);
        self.set_header(header);
        Ok(new_rec_start_off)
    }

    fn set_header(&mut self, header: Header) {
        let header_bytes = header.to_bytes();
        self.write_bytes(0, &header_bytes);
    }

    fn set_slot_meta(&mut self, slot_meta: &SlotMeta) -> Result<u32> {
        let mut header = self.get_header();
        let slot_meta_space = slot_meta.space_need();
        let new_slot_start_off = header.slot_end_offset();
        let unused_space = header.rec_start_offset() - new_slot_start_off;

        log_warn!("{:?}", header);
        if unused_space < slot_meta_space {
            return Err(HashTableAccessMethodError::OutOfSpace);
        }

        self.write_bytes(new_slot_start_off, &slot_meta.to_bytes());
        header.set_slot_meta_end_offset(new_slot_start_off + slot_meta_space);
        header.increase_total_bytes_used(slot_meta_space);
        self.set_header(header);
        Ok(new_slot_start_off)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bp::{ContainerKey, InMemPool, MemPool, PageFrameKey},
        log_warn,
        mvcc_index::hybrid_hash::{
            attached_container_page::attached_container_page::{AttachedPage, WritePageLocAgent},
            hash_join_table_common::HashTableAccessMethodError,
            hybrid_hash_table::hybrid_hash_slot_page::TableSlotsPage,
        },
        page::Page,
        prelude::Timestamp,
    };

    use super::slot::InterPageLoc;

    #[test]
    fn test_insert_meta() {
        let page_id = 0u32;

        let mut page = Page::new(page_id);

        let page: &mut dyn super::AttachedPage = &mut page;
        page.init();

        let slot_meta = super::SlotMeta {
            prev_meta_loc: super::InterPageLoc {
                page_id: 0,
                b_offset: 0,
            },
            latest_version_loc: super::InterPageLoc {
                page_id: 0,
                b_offset: 0,
            },
            remain_key: b"key",
            remain_pkey: b"pkey",
        };

        let meta_off = page.set_slot_meta(&slot_meta).unwrap();

        let version_off = page.insert_first_version(b"value", 1, false).unwrap();

        let mut latest_version_bytes: [u8; 8] = [0; 8];
        latest_version_bytes[..4].copy_from_slice(&u32::to_be_bytes(0));
        latest_version_bytes[4..].copy_from_slice(&u32::to_be_bytes(version_off));
        page.write_bytes(
            meta_off + std::mem::size_of::<InterPageLoc>() as u32,
            &latest_version_bytes,
        );

        let slot = super::Slot::new(
            b"key",
            b"pkey",
            InterPageLoc {
                page_id,
                b_offset: meta_off,
            },
        );
        let meta = page.get_slot_meta(&slot);
        meta.print_meta_of_slot(&slot);

        let record = page.get_record(version_off);
        assert_eq!(record.start_ts, 1);
        assert_eq!(record.val_size, 5);
        assert_eq!(record.value, b"value");
    }
    const C_KEY: ContainerKey = ContainerKey { db_id: 0, c_id: 0 };

    fn find_pivot_rec_to_write<T: MemPool + 'static>(
        mem_pool: &T,
        slot: &super::Slot,
        ts: Timestamp,
    ) -> (WritePageLocAgent, InterPageLoc) {
        let (cur_page_id, _b_off) = (slot.meta_loc().page_id, slot.meta_loc().b_offset);
        let pfk = PageFrameKey::new(C_KEY, cur_page_id);
        let page = mem_pool.get_page_for_read(pfk).unwrap();
        let meta = (*page).get_slot_meta(slot);
        log_warn!("meta in find_func: {:?}", meta);
        let mut next_version_loc = meta.latest_version_loc;

        drop(page);
        let mut write_agent =
            WritePageLocAgent::new(slot.meta_loc().b_offset, slot.meta_loc().page_id);
        let mut next_start_ts = Timestamp::MAX;
        let mut cur_version_loc = InterPageLoc {
            page_id: cur_page_id,
            b_offset: _b_off,
        };
        while next_start_ts > ts {
            if next_version_loc == InterPageLoc::new_end() {
                write_agent =
                    WritePageLocAgent::new(cur_version_loc.b_offset, cur_version_loc.page_id);
                return (write_agent, next_version_loc);
            }
            let pfk = PageFrameKey::new(C_KEY, next_version_loc.page_id);
            let page = mem_pool.get_page_for_read(pfk).unwrap();
            let cur_version = (*page).get_record(next_version_loc.b_offset);
            log_warn!("record: {:?}", cur_version);

            if next_start_ts != Timestamp::MAX {
                write_agent =
                    WritePageLocAgent::new(cur_version_loc.b_offset, cur_version_loc.page_id);
            }
            next_start_ts = cur_version.start_ts;
            cur_version_loc = next_version_loc;
            next_version_loc = cur_version.prev_offset;
        }

        return (write_agent, cur_version_loc);
    }

    #[test]
    fn get_latest_version_loc() {
        let mem_pool = InMemPool::new();

        let mut page = mem_pool.create_new_page_for_write(C_KEY).unwrap();
        let page_id = page.get_id();
        let write_page: &mut dyn super::AttachedPage = &mut *page;
        write_page.init();

        let slot_meta = super::SlotMeta {
            prev_meta_loc: super::InterPageLoc::new_end(),
            latest_version_loc: super::InterPageLoc::new_end(),
            remain_key: b"key",
            remain_pkey: b"pkey",
        };

        let meta_off = write_page.set_slot_meta(&slot_meta).unwrap();

        let version_off = write_page.insert_first_version(b"value", 1, false).unwrap();

        let mut latest_version_bytes: [u8; 8] = [0; 8];
        latest_version_bytes[..4].copy_from_slice(&u32::to_be_bytes(page_id));
        latest_version_bytes[4..].copy_from_slice(&u32::to_be_bytes(version_off));
        write_page.write_bytes(meta_off, &latest_version_bytes);

        let slot = super::Slot::new(
            b"key",
            b"pkey",
            InterPageLoc {
                page_id,
                b_offset: meta_off,
            },
        );
        let meta = write_page.get_slot_meta(&slot);
        log_warn!("meta: {:?}", meta);

        drop(page);

        let write_agent0 = find_pivot_rec_to_write(&mem_pool, &slot, 0);
        let write_agent1 = find_pivot_rec_to_write(&mem_pool, &slot, 1);
        let write_agent2 = find_pivot_rec_to_write(&mem_pool, &slot, 2);

        log_warn!("{:?}", write_agent0);
        log_warn!("{:?}", write_agent1);
        log_warn!("{:?}", write_agent2);
    }
    use super::Slot;
    fn add_version<T: MemPool + 'static>(
        cur_page: &mut impl TableSlotsPage,
        slot: &Slot,
        mem_pool: &T,
        value: &[u8],
        ts: Timestamp,
        is_delete: bool,
    ) {
        let (prev_node_update_agent, next_loc) = find_pivot_rec_to_write(mem_pool, slot, ts);
        let attach_page_id = cur_page.get_attached_page_id();
        let pfk = PageFrameKey::new(C_KEY, attach_page_id);
        let mut page = mem_pool.get_page_for_write(pfk).unwrap();
        let new_rec_write_page: &mut dyn super::AttachedPage = &mut *page;
        let insert_res = new_rec_write_page.insert_first_version(value, ts, is_delete);
        let next_version_loc_bytes = {
            let mut b = [0_u8; 8];
            b[..4].copy_from_slice(&u32::to_be_bytes(next_loc.page_id));
            b[4..].copy_from_slice(&u32::to_be_bytes(next_loc.b_offset));
            b
        };
        let update_loc_bytes = match insert_res {
            Ok(version_off) => {
                new_rec_write_page.write_bytes(version_off, &next_version_loc_bytes);
                let mut update_loc_bytes: [u8; 8] = [0; 8];
                update_loc_bytes[..4].copy_from_slice(&u32::to_be_bytes(attach_page_id));
                update_loc_bytes[4..].copy_from_slice(&u32::to_be_bytes(version_off));
                update_loc_bytes
            }
            // out of space
            Err(HashTableAccessMethodError::OutOfSpace) => {
                let mut new_page = mem_pool.create_new_page_for_write(C_KEY).unwrap();
                let new_page_id = new_page.get_id();
                cur_page.set_attached_page_id(new_page_id);
                let new_write_page: &mut dyn super::AttachedPage = &mut *new_page;
                new_write_page.init();
                let new_version_off = new_write_page
                    .insert_first_version(value, ts, is_delete)
                    .unwrap();
                new_write_page.write_bytes(new_version_off, &next_version_loc_bytes);
                let mut update_loc_bytes: [u8; 8] = [0; 8];
                update_loc_bytes[..4].copy_from_slice(&u32::to_be_bytes(new_page_id));
                update_loc_bytes[4..].copy_from_slice(&u32::to_be_bytes(new_version_off));
                update_loc_bytes
            }
            Err(_) => {
                panic!("unknown err");
            }
        };

        let mut prev_page = mem_pool
            .get_page_for_write(PageFrameKey::new(C_KEY, prev_node_update_agent.page_id))
            .unwrap();
        let prev_write_page: &mut dyn super::AttachedPage = &mut *prev_page;
        prev_write_page.write_bytes(prev_node_update_agent.offset, &update_loc_bytes);
    }

    fn add_version_test<T: MemPool + 'static>(
        attach_page_id: u32,
        slot: &Slot,
        mem_pool: &T,
        value: &[u8],
        ts: Timestamp,
        is_delete: bool,
    ) {
        let (prev_node_update_agent, next_loc) = find_pivot_rec_to_write(mem_pool, slot, ts);
        let pfk = PageFrameKey::new(C_KEY, attach_page_id);
        let mut page: crate::prelude::FrameWriteGuard<'_> =
            mem_pool.get_page_for_write(pfk).unwrap();
        let new_rec_write_page: &mut dyn super::AttachedPage = &mut *page;
        let insert_res = new_rec_write_page.insert_first_version(value, ts, is_delete);
        let next_version_loc_bytes = {
            let mut b = [0_u8; 8];
            b[..4].copy_from_slice(&u32::to_be_bytes(next_loc.page_id));
            b[4..].copy_from_slice(&u32::to_be_bytes(next_loc.b_offset));
            b
        };
        let update_loc_bytes = match insert_res {
            Ok(version_off) => {
                new_rec_write_page.write_bytes(version_off, &next_version_loc_bytes);
                let mut update_loc_bytes: [u8; 8] = [0; 8];
                update_loc_bytes[..4].copy_from_slice(&u32::to_be_bytes(attach_page_id));
                update_loc_bytes[4..].copy_from_slice(&u32::to_be_bytes(version_off));
                update_loc_bytes
            }
            // out of space
            Err(HashTableAccessMethodError::OutOfSpace) => {
                let mut new_page = mem_pool.create_new_page_for_write(C_KEY).unwrap();
                let new_page_id = new_page.get_id();
                log_warn!("out of space, new page id : {:?}", new_page_id);
                let new_write_page: &mut dyn super::AttachedPage = &mut *new_page;
                new_write_page.init();
                let new_version_off = new_write_page
                    .insert_first_version(value, ts, is_delete)
                    .unwrap();
                new_write_page.write_bytes(new_version_off, &next_version_loc_bytes);
                let mut update_loc_bytes: [u8; 8] = [0; 8];
                update_loc_bytes[..4].copy_from_slice(&u32::to_be_bytes(new_page_id));
                update_loc_bytes[4..].copy_from_slice(&u32::to_be_bytes(new_version_off));
                update_loc_bytes
            }
            Err(_) => {
                panic!("unknown err");
            }
        };
        drop(page);

        let mut prev_page = mem_pool
            .get_page_for_write(PageFrameKey::new(C_KEY, prev_node_update_agent.page_id))
            .unwrap();
        let prev_write_page: &mut dyn super::AttachedPage = &mut *prev_page;
        prev_write_page.write_bytes(prev_node_update_agent.offset, &update_loc_bytes);
    }

    #[test]
    fn test_multiple_versions() {
        let mem_pool = InMemPool::new();

        let mut page = mem_pool.create_new_page_for_write(C_KEY).unwrap();
        let page_id = page.get_id();
        let attach_page: &mut dyn super::AttachedPage = &mut *page;
        attach_page.init();

        let slot_meta = super::SlotMeta {
            prev_meta_loc: super::InterPageLoc::new_end(),
            latest_version_loc: super::InterPageLoc::new_end(),
            remain_key: b"key",
            remain_pkey: b"pkey",
        };

        let meta_off = attach_page.set_slot_meta(&slot_meta).unwrap();
        // let version_off = attach_page.insert_version(b"value", 1, false).unwrap();

        // let mut latest_version_bytes: [u8; 8] = [0; 8];
        // latest_version_bytes[..4].copy_from_slice(&u32::to_be_bytes(page_id));
        // latest_version_bytes[4..].copy_from_slice(&u32::to_be_bytes(version_off));
        // attach_page.write_bytes(meta_off, &latest_version_bytes);
        drop(page);

        let slot = super::Slot::new(
            b"key",
            b"pkey",
            InterPageLoc {
                page_id,
                b_offset: meta_off,
            },
        );
        add_version_test(page_id, &slot, &mem_pool, b"value", 1, false);
        add_version_test(page_id, &slot, &mem_pool, b"value2", 2, false);
        add_version_test(page_id, &slot, &mem_pool, b"value3", 3, false);
        add_version_test(page_id, &slot, &mem_pool, b"value4", 4, false);

        let record1 = find_pivot_rec_to_write(&mem_pool, &slot, 1);
        log_warn!("record1: {:?}", record1);
        let record2 = find_pivot_rec_to_write(&mem_pool, &slot, 2);
        log_warn!("record2: {:?}", record2);
        let record3 = find_pivot_rec_to_write(&mem_pool, &slot, 3);
        log_warn!("record3: {:?}", record3);
        let record4 = find_pivot_rec_to_write(&mem_pool, &slot, 4);
        log_warn!("record4: {:?}", record4);

        let page = mem_pool
            .get_page_for_read(PageFrameKey::new(C_KEY, page_id))
            .unwrap();
        let page = &*page as &dyn super::AttachedPage;
        let val1 = page.get_rec_value(record1.1.b_offset).unwrap();
        let val2 = page.get_rec_value(record2.1.b_offset).unwrap();
        let val3 = page.get_rec_value(record3.1.b_offset).unwrap();
        let val4 = page.get_rec_value(record4.1.b_offset).unwrap();
        assert_eq!(val1, b"value");
        assert_eq!(val2, b"value2");
        assert_eq!(val3, b"value3");
        assert_eq!(val4, b"value4");

        let header = page.get_header();
        log_warn!("header: {:?}", header);
    }

    #[test]
    fn test_large_versions() {
        let mem_pool = InMemPool::new();

        let mut page = mem_pool.create_new_page_for_write(C_KEY).unwrap();
        let page_id = page.get_id();
        let attach_page: &mut dyn super::AttachedPage = &mut *page;
        attach_page.init();

        let slot_meta = super::SlotMeta {
            prev_meta_loc: super::InterPageLoc::new_end(),
            latest_version_loc: super::InterPageLoc::new_end(),
            remain_key: b"key",
            remain_pkey: b"pkey",
        };

        let meta_off = attach_page.set_slot_meta(&slot_meta).unwrap();

        drop(page);

        let value1 = &[1_u8; 100];
        let value2 = &[2_u8; 100];
        let value3 = &[3_u8; 100];
        let value4 = &[4_u8; 100];
        let value6 = &[6_u8; 100];

        let slot = super::Slot::new(
            b"key",
            b"pkey",
            InterPageLoc {
                page_id,
                b_offset: meta_off,
            },
        );
        add_version_test(page_id, &slot, &mem_pool, value1, 1, false);
        add_version_test(page_id, &slot, &mem_pool, value2, 2, false);
        add_version_test(page_id, &slot, &mem_pool, value3, 3, false);
        add_version_test(page_id, &slot, &mem_pool, value4, 4, false);
        add_version_test(page_id, &slot, &mem_pool, &[], 5, true);
        add_version_test(page_id, &slot, &mem_pool, value6, 6, false);

        let record1 = find_pivot_rec_to_write(&mem_pool, &slot, 1);
        log_warn!("record1: {:?}", record1);
        let record2 = find_pivot_rec_to_write(&mem_pool, &slot, 2);
        log_warn!("record2: {:?}", record2);
        let record3 = find_pivot_rec_to_write(&mem_pool, &slot, 3);
        log_warn!("record3: {:?}", record3);
        let record4 = find_pivot_rec_to_write(&mem_pool, &slot, 4);
        log_warn!("record4: {:?}", record4);
        let record6 = find_pivot_rec_to_write(&mem_pool, &slot, 6);
        log_warn!("record6: {:?}", record6);

        for rec in [record1, record2, record3, record4, record6]
            .iter()
            .enumerate()
        {
            let page = mem_pool
                .get_page_for_read(PageFrameKey::new(C_KEY, rec.1 .1.page_id))
                .unwrap();
            let page = &*page as &dyn super::AttachedPage;
            let header = page.get_header();
            log_warn!("header: {:?}", header);
            let val = page.get_rec_value(rec.1 .1.b_offset).unwrap();
            assert_eq!(
                val,
                match rec.0 {
                    0 => value1,
                    1 => value2,
                    2 => value3,
                    3 => value4,
                    4 => value6,
                    _ => panic!("unknown"),
                }
            );
        }
        let record5 = find_pivot_rec_to_write(&mem_pool, &slot, 5);
        let page = mem_pool
            .get_page_for_read(PageFrameKey::new(C_KEY, record5.1.page_id))
            .unwrap();
        let page = &*page as &dyn super::AttachedPage;
        let header = page.get_header();
        log_warn!("header: {:?}", header);
        let val = page.get_rec_value(record5.1.b_offset);
        assert_eq!(val, None);
    }
}
