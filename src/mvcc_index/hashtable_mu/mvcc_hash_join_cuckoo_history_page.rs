
mod header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

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
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse total_bytes_used")?
            );
            current_pos += 4;

            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse slot_count")?
            );
            current_pos += 4;

            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse rec_start_offset")?
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

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.rec_start_offset.to_be_bytes());

            bytes
        }

        pub fn new() -> Self {
            Self {
                total_bytes_used: PAGE_HEADER_SIZE as u32,
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
    }
}

use header::*;

mod slot {
    use super::super::Timestamp;
    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq)]
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
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < SLOT_SIZE {
                return Err("Insufficient bytes to form Slot".into());
            }

            let mut current_pos = 0;

            let key_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse key_size")?,
            );
            current_pos += 4;

            let mut key_prefix = [0u8; SLOT_KEY_PREFIX_SIZE];
            key_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE]);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            let pkey_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse pkey_size")?,
            );
            current_pos += 4;

            let mut pkey_prefix = [0u8; SLOT_PKEY_PREFIX_SIZE];
            pkey_prefix.copy_from_slice(&bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE]);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            let start_ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()].try_into().map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let end_ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()].try_into().map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += std::mem::size_of::<Timestamp>();

            let val_size = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse val_size")?,
            );
            current_pos += 4;

            let offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse offset")?,
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

            bytes[current_pos..current_pos + SLOT_KEY_PREFIX_SIZE].copy_from_slice(&self.key_prefix);
            current_pos += SLOT_KEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.pkey_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + SLOT_PKEY_PREFIX_SIZE].copy_from_slice(&self.pkey_prefix);
            current_pos += SLOT_PKEY_PREFIX_SIZE;

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()].copy_from_slice(&self.start_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + std::mem::size_of::<Timestamp>()].copy_from_slice(&self.end_ts.to_be_bytes());
            current_pos += std::mem::size_of::<Timestamp>();

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], start_ts: Timestamp, end_ts: Timestamp, val: &[u8], offset: usize) -> Self {
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
            self.val_size
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
            let mut bytes = Vec::with_capacity(self.remain_key.len() + self.remain_pkey.len() + self.val.len());

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

use crate::{log_debug, mvcc_index::Timestamp, page::{Page, AVAILABLE_PAGE_SIZE}};

use super::mvcc_hash_join_cuckoo_common::CuckooAccessMethodError;

pub trait MvccHashJoinCuckooHistoryPage {
    fn init(&mut self);
    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError>;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, CuckooAccessMethodError>;
   
    // helper function
    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32;
    fn free_space_without_compaction(&self) -> u32;
    fn free_space_with_compaction(&self) -> u32;

    fn compact(
        &mut self,
    ) -> u32;

    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }

    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE as u32 + slot_id as u32 * SLOT_SIZE as u32
    }

    fn set_slot(&mut self, slot_id: u32, slot: &Slot);

    fn write_record(&mut self, offset: u32, record: &Record) {
        let bytes = record.to_bytes();
        self.write_bytes(offset as usize, &bytes);
    }
 
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);

    fn check_larger_record(
        &self,
        space_need_size: u32,
        start_idx: u32,
    ) -> Option<(u32, Vec<u8>, Vec<u8>, Vec<u8>)>;

    fn slot(&self, slot_id: u32) -> Option<Slot>;

    fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8];

    fn get_value_with_slot(&self, slot: &Slot) -> &[u8];

    fn get_key_pkey_val_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, Vec<u8>, Vec<u8>);

    fn get_key_pkey_val_with_slot(&self, slot: &Slot) -> (Vec<u8>, Vec<u8>, Vec<u8>);

    fn get_key_pkey_val_ts_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp);

    fn get_key_pkey_val_ts_with_slot(&self, slot: &Slot) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp);

    fn check_slot_key_and_space_of_id(
        &self,
        slot_id: u32,
        key: &[u8],
        space_want: u32,
    ) -> bool;

    fn swap_record_at_slot_id(
        &mut self,
        slot_id: u32,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp), CuckooAccessMethodError>;

    fn rec_start_offset(&self) -> u32;

    fn set_rec_start_offset(&mut self, rec_start_offset: u32);

    fn delete_slot_at_id(&mut self, slot_id: u32)
        -> Result<(), CuckooAccessMethodError>;

    fn increase_total_bytes_used(&mut self, bytes: u32);

    fn decrement_slot_count(&mut self);

    fn decrease_total_bytes_used(&mut self, num_bytes: u32);
    
}

impl MvccHashJoinCuckooHistoryPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn header(&self) -> Header {
        let header_bytes = &self[0..PAGE_HEADER_SIZE];
        Header::from_bytes(&header_bytes).unwrap()
    }

    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset as usize..slot_offset as usize + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    fn compact(
        &mut self,
    ) -> u32 {
        // decreasing order 3, 2, 1
        let slot_offsets_sorted_decreased = {
            // (slot, slot_rec_offset, slot_idx)
            let mut rec_offsets = vec![];

            let mut slot_offset = PAGE_HEADER_SIZE;
            for i in 0..self.slot_count() {
                let slot_bytes = &self[slot_offset..slot_offset + SLOT_SIZE];
                let slot = Slot::from_bytes(slot_bytes).unwrap();
                let rec_offset = slot.offset();
                rec_offsets.push((slot, rec_offset, i));
                slot_offset += SLOT_SIZE as usize;
            }
            
            rec_offsets.sort_by(|a, b| {
                b.1.cmp(&a.1)
            });

            rec_offsets
        };

        let mut new_rec_offset = AVAILABLE_PAGE_SIZE;

        assert_eq!(slot_offsets_sorted_decreased.len() as u32, self.slot_count());

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

        self.free_space_without_compaction()
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    /// insert into a Page without checking duplicate \
    /// if full -> OutOfSpace \
    /// else -> return Ok
    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        val: &[u8],
    ) -> Result<(), CuckooAccessMethodError> {
        let space_need = <Page as MvccHashJoinCuckooHistoryPage>::space_need(key, pkey, val);
        if space_need > self.free_space_with_compaction() {
            log_debug!("should not happen, detect before calling cuckoopage::insert");
            return Err(CuckooAccessMethodError::OutOfSpace);
        }

        if space_need > self.free_space_without_compaction() {
            // DO COMPACT 
            let want_free_with_compaction_space = self.free_space_with_compaction();
            let actual_free_with_compaction_space = self.compact();
            assert_eq!(want_free_with_compaction_space, actual_free_with_compaction_space);
        }


        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;
        let slot_id = self.slot_count();
        let slot_offset = self.slot_offset(self.slot_count());

        if rec_offset < slot_offset + SLOT_SIZE as u32 {
            log_debug!("should not happen, detect before calling cuckoopage::insert");
            return Err(CuckooAccessMethodError::OutOfSpace);
        }

        let slot = Slot::new(key, pkey, start_ts, end_ts, val, rec_offset as usize);
        let record = Record::new(key, pkey, val);

        self.set_slot(slot_id, &slot);
        self.write_record(rec_offset, &record);

        header.increment_slot_count();
        header.increase_total_bytes_used(space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);

        Ok(())
    }

    fn get(
            &self,
            key: &[u8],
            pkey: &[u8],
            ts: Timestamp,
        ) -> Result<Vec<u8>, CuckooAccessMethodError> {
            let header = self.header();
            let slot_count = header.slot_count();
            let mut slot_offset = PAGE_HEADER_SIZE;
    
            for _ in 0..slot_count {
                let slot_bytes = &self[slot_offset..slot_offset + SLOT_SIZE];
                let slot = Slot::from_bytes(slot_bytes).unwrap();
                if slot.key_size() == key.len() as u32 
                    && slot.pkey_size() == pkey.len() as u32
                    && slot.key_prefix() == &key[..SLOT_KEY_PREFIX_SIZE.min(key.len())]
                    && slot.pkey_prefix() == &pkey[..SLOT_PKEY_PREFIX_SIZE.min(pkey.len())] 
                {
                    let rec_offset = slot.offset() as usize;
                    let rec_size = slot.val_size() 
                        + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32) 
                        + slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
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
                        if slot.start_ts() <= ts && ts <= slot.end_ts() {
                            return Ok(record.val().to_vec());
                        } else {
                            // return Err(CuckooAccessMethodError::KeyFoundButInvalidTimestamp);
                        }
                    }
                }
                slot_offset += SLOT_SIZE;
            }
        Err(CuckooAccessMethodError::KeyNotFound)
    }
    fn free_space_without_compaction(&self) -> u32 {
        let header = self.header();
        header.rec_start_offset() - header.slot_count() * SLOT_SIZE as u32
    }
    fn free_space_with_compaction(&self) -> u32 {
        let header = self.header();
        header.total_bytes_used()
    }
    
    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }

    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE as u32 + slot_id as u32 * SLOT_SIZE as u32
    }

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        SLOT_SIZE as u32 + remain_key_size as u32 + remain_pkey_size as u32 + val.len() as u32
    }

    fn write_record(&mut self, offset: u32, record: &Record) {
        let bytes = record.to_bytes();
        self.write_bytes(offset as usize, &bytes);
    }


    /*
        u32::MAX: reach end and not found
     */
    fn check_larger_record(
        &self,
        space_need_size: u32,
        start_idx: u32,
    ) -> Option<(u32, Vec<u8>, Vec<u8>, Vec<u8>)> {
        let slot_count = self.slot_count();
        let mut slot_idx = start_idx;
        loop {
            if slot_idx >= slot_count {
                return Some((u32::MAX, vec![], vec![], vec![]));
            }
            let slot = self.slot(slot_idx).unwrap();

            let record_size = slot.get_record_size_of_slot();
            let (key, pkey, val) = self.get_key_pkey_val_with_slot(&slot);
            if record_size >= (space_need_size - SLOT_SIZE as u32) {
                return Some((slot_idx, key, pkey, val));
            }
            slot_idx += 1;
        }
    }

    fn slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id < self.slot_count() {
            let offset = self.slot_offset(slot_id) as usize;
            let slot_bytes = &self[offset..offset + SLOT_SIZE];
            Some(Slot::from_bytes(slot_bytes).unwrap())
        } else {
            None
        }
    }

    fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8] {
        let slot = self.slot(slot_id).expect("Invalid slot_id");
        self.get_value_with_slot(&slot)
    }

    fn get_value_with_slot(&self, slot: &Slot) -> &[u8] {
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let pkey_size = slot.pkey_size() as usize;
        let val_size = slot.val_size() as usize;

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        let val_offset = offset + remain_key_size + remain_pkey_size;
        &self[val_offset..val_offset + val_size]
    }

    fn get_key_pkey_val_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let slot = self.slot(slot_id).expect("Invalid slot_id");
        self.get_key_pkey_val_with_slot(&slot)
    }

    fn get_key_pkey_val_ts_with_slot_id(&self, slot_id: u32) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp) {
        let slot = self.slot(slot_id).expect("Invalid slot_id");
        self.get_key_pkey_val_ts_with_slot(&slot)
    }

    fn get_key_pkey_val_ts_with_slot(&self, slot: &Slot) -> (Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp) {
        let rec_offset = slot.offset();
        let key_size = slot.key_size();
        let pkey_size = slot.pkey_size();
        let val_size = slot.val_size();

        let rec_size = val_size
            + key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let rec_bytes = &self[rec_offset as usize..rec_offset as usize + rec_size as usize];
        let record = Record::from_bytes(
            rec_bytes, key_size, pkey_size, val_size);

        let mut full_key = slot.key_prefix().to_vec();
        full_key.extend_from_slice(record.remain_key());

        let mut full_pkey = slot.pkey_prefix().to_vec();
        full_pkey.extend_from_slice(record.remain_pkey());

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
        let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let val_offset = (rec_offset + remain_key_size + remain_pkey_size) as usize;
        let val =  (&self[val_offset..val_offset + val_size as usize]).to_vec();
        (full_key, full_pkey, val, slot.start_ts(), slot.end_ts())
    }

    fn get_key_pkey_val_with_slot(&self, slot: &Slot) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let rec_offset = slot.offset();
        let key_size = slot.key_size();
        let pkey_size = slot.pkey_size();
        let val_size = slot.val_size();

        let rec_size = val_size
            + key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
            + pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let rec_bytes = &self[rec_offset as usize..rec_offset as usize + rec_size as usize];
        let record = Record::from_bytes(
            rec_bytes, key_size, pkey_size, val_size);

        let mut full_key = slot.key_prefix().to_vec();
        full_key.extend_from_slice(record.remain_key());

        let mut full_pkey = slot.pkey_prefix().to_vec();
        full_pkey.extend_from_slice(record.remain_pkey());

        let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
        let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
        let val_offset = (rec_offset + remain_key_size + remain_pkey_size) as usize;
        let val =  (&self[val_offset..val_offset + val_size as usize]).to_vec();
        (full_key, full_pkey, val)
    }

    fn check_slot_key_and_space_of_id(
        &self,
        slot_id: u32,
        key: &[u8],
        space_want: u32,
    ) -> bool {
        let slot = self.slot(slot_id).unwrap();

        let (slot_key, slot_pkey, slot_val) = self.get_key_pkey_val_with_slot(&slot);
        let slot_space_need = Self::space_need(&slot_key, &slot_pkey, &slot_val);

        (key == &slot_key) && (slot_space_need == space_want)
    }


    fn swap_record_at_slot_id(
        &mut self,
        slot_id: u32,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        start_ts: Timestamp,
        end_ts: Timestamp,
        ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Timestamp, Timestamp), CuckooAccessMethodError> {
        let old_slot = self.slot(slot_id).unwrap();

        let new_space_need = Self::space_need(key, pkey, val);
        assert!(old_slot.get_record_size_of_slot() + SLOT_SIZE as u32 >= new_space_need);
        let diff_bytes = (old_slot.get_record_size_of_slot() + SLOT_SIZE as u32) - new_space_need;

        // do swap
        let (old_key, old_pkey, old_val) = self.get_key_pkey_val_with_slot(&old_slot);
        let old_start_ts = old_slot.start_ts();
        let old_end_ts = old_slot.end_ts();

        let new_slot = Slot::new(key, pkey, start_ts, end_ts, val, old_slot.offset() as usize);
        let new_record = Record::new(key, pkey, val);
        self.set_slot(slot_id, &new_slot);
        self.write_record(new_slot.offset(), &new_record);
        
        let mut header = self.header();
        header.decrease_total_bytes_used(diff_bytes);
        self.set_header(&header);

        Ok((old_key, old_pkey, old_val, old_start_ts, old_end_ts))
        
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

    fn decrease_total_bytes_used(&mut self, num_bytes: u32) {
        let mut header = self.header();
        header.set_total_bytes_used(header.total_bytes_used() - num_bytes);
        self.set_header(&header);
    }

    fn decrement_slot_count(&mut self) {
        let mut header = self.header();
        header.decrement_slot_count();
        self.set_header(&header);
    }


    /// delete slot at specific id \
    /// used in delete() and re-hash.
    fn delete_slot_at_id(&mut self, slot_id: u32)
        -> Result<(), CuckooAccessMethodError> {
        // check if rec_start_offset should change 
        let slot = self.slot(slot_id).expect("Invalid slot_id");

        let old_val = self.get_value_with_slot(&slot).to_vec();

        if slot.offset() == self.rec_start_offset() {
            self.set_rec_start_offset(
                slot.offset()
                    + slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                    + slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
                    + slot.val_size(),
            );
        }
        self.decrease_total_bytes_used(
            slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
                + slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
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
        Ok(())
    }

}

