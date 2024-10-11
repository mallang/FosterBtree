use crate::{
    access_method::AccessMethodError, log_debug, prelude::{Page, PageId, AVAILABLE_PAGE_SIZE}
};
use super::Timestamp;

mod header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE};
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    #[derive(Debug)]
    pub struct Header {
        next_page_id: PageId,
        next_frame_id: u32,
        total_bytes_used: u32, // only valid slots and records.
        slot_count: u32,
        rec_start_offset: u32,
    }

    impl Header {
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
            if bytes.len() < PAGE_HEADER_SIZE {
                return Err("Insufficient bytes to form Header".into());
            }

            let mut current_pos = 0;

            let next_page_id = PageId::from_be_bytes(
                bytes[current_pos..current_pos + std::mem::size_of::<PageId>()].try_into().map_err(|_| "Failed to parse next_page_id")?,
            );
            current_pos += std::mem::size_of::<PageId>();

            let next_frame_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse next_frame_id")?,
            );
            current_pos += 4;

            let total_bytes_used = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse total_bytes_used")?,
            );
            current_pos += 4;

            let slot_count = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse slot_count")?,
            );
            current_pos += 4;

            let rec_start_offset = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4].try_into().map_err(|_| "Failed to parse rec_start_offset")?,
            );

            Ok(Header {
                next_page_id,
                next_frame_id,
                total_bytes_used,
                slot_count,
                rec_start_offset,
            })
        }

        pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
            let mut bytes = [0; PAGE_HEADER_SIZE];
            let mut current_pos = 0;

            bytes[current_pos..current_pos + std::mem::size_of::<PageId>()].copy_from_slice(&self.next_page_id.to_be_bytes());
            current_pos += std::mem::size_of::<PageId>();

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.next_frame_id.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.total_bytes_used.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.slot_count.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.rec_start_offset.to_be_bytes());

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

        pub fn increment_slot_count(&mut self) {
            self.slot_count += 1;
        }

        pub fn decrement_slot_count(&mut self) {
            self.slot_count -= 1;
        }

        pub fn rec_start_offset(&self) -> u32 {
            self.rec_start_offset
        }

        pub fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
            self.rec_start_offset = rec_start_offset;
        }
    }
}
use header::*;

mod slot {
    use super::Timestamp;
    use std::convert::TryInto;

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
        ts: Timestamp,
        // value
        val_size: u32,
        offset: u32,
    }

    impl Slot {
        // create a dummy Slot
        pub fn new_dummy() -> Self {
            return Self::new("".as_bytes(), "".as_bytes(), 0, "".as_bytes(), 0);
        }

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

            let ts = u64::from_be_bytes(
                bytes[current_pos..current_pos + 8].try_into().map_err(|_| "Failed to parse timestamp")?,
            );
            current_pos += 8;

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
                ts,
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

            bytes[current_pos..current_pos + 8].copy_from_slice(&self.ts.to_be_bytes());
            current_pos += 8;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.val_size.to_be_bytes());
            current_pos += 4;

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.offset.to_be_bytes());

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], ts: Timestamp, val: &[u8], offset: usize) -> Self {
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
                ts,
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

        pub fn ts(&self) -> Timestamp {
            self.ts
        }

        pub fn set_ts(&mut self, ts: Timestamp) {
            self.ts = ts;
        }

        pub fn val_size(&self) -> u32 {
            self.val_size
        }

        pub fn set_val_size(&mut self, val_size: usize) {
            self.val_size = val_size as u32;
        }

        /*
            record offset
         */
        pub fn offset(&self) -> u32 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u32) {
            self.offset = offset;
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

pub trait MvccHashJoinRecentPage {
    fn init(&mut self);
    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError>;
    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError>;
    fn update(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError>;
    fn delete(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError>;
    
    fn next_page(&self) -> Option<(PageId, u32)>;
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32);
    fn delete_slot_at_id(&mut self, slot_id: u32);

    // helper function
    fn header(&self) -> Header;
    fn set_header(&mut self, header: &Header);

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32;
    fn free_space_without_compaction(&self) -> u32;
    fn free_space_with_compaction(&self) -> u32;

    fn slot_offset(&self, slot_id: u32) -> u32;

    fn slot_count(&self) -> u32;

    // not set header version
    fn decrease_total_bytes_used(&mut self, num_bytes: u32);

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);

    fn update_at_slot_id(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        ts: Timestamp,
        slot_id: u32,
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError>;

    fn slot(&self, slot_id: u32) -> Option<Slot>;

    fn get_value_with_slot_id(&self, slot_id: u32) -> &[u8];

    fn get_value_with_slot(&self, slot: &Slot) -> &[u8];

    fn rec_start_offset(&self) -> u32;

    fn write_record(&mut self, offset: u32, record: Record);

    fn set_rec_start_offset(&mut self, rec_start_offset: u32);

    fn set_slot(&mut self, slot_id: u32, slot: &Slot);

    fn increase_total_bytes_used(&mut self, bytes: u32);

    fn decrement_slot_count(&mut self);
}

impl MvccHashJoinRecentPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn insert(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(), AccessMethodError> {
        // maybe need to check duplicate with columns(key, pkey, ts) before calling it.
        let space_need = Self::space_need(key, pkey, val);
        if space_need > self.free_space_without_compaction() {
            log_debug!("should not happen, detect before calling insert");
            return Err(AccessMethodError::OutOfSpace);
        }

        let mut header = self.header();
        let record_size = space_need - SLOT_SIZE as u32;
        let rec_offset = header.rec_start_offset() - record_size;
        let slot_offset = self.slot_offset(header.slot_count());
        if rec_offset < slot_offset + SLOT_SIZE as u32 {
            log_debug!("should not happen, detect before calling insert");
            return Err(AccessMethodError::OutOfSpace);
        }

        let slot = Slot::new(key, pkey, ts, val, rec_offset as usize);
        let slot_bytes = slot.to_bytes();
        
        let record = Record::new(key, pkey, val);
        let record_bytes = record.to_bytes();
        
        self[slot_offset as usize..slot_offset as usize + SLOT_SIZE].copy_from_slice(&slot_bytes);
        self[rec_offset as usize..rec_offset as usize + record_size as usize].copy_from_slice(&record_bytes);

        // update metadata
        header.increment_slot_count();
        header.set_total_bytes_used(header.total_bytes_used() + space_need);
        header.set_rec_start_offset(rec_offset);
        self.set_header(&header);

        Ok(())
    }

    fn get(
        &self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<Vec<u8>, AccessMethodError> {
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
                    if slot.ts() <= ts {
                        return Ok(record.val().to_vec());
                    } else {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }
            slot_offset += SLOT_SIZE;
        }
        Err(AccessMethodError::KeyNotFound)
    }

/*  
    update a specific key with ts
    if not found -> key not found
    if record too large for a single page -> RecordTooLarge
    if space is not enough in current page -> space not enough [AccessMethodError::OutOfSpace]
    (if record ts > ts -> return [AccessMethodError::KeyFoundButInvalidTimestamp])
*/
    fn update(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        val: &[u8],
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count() /* default value: slot_count */;
        let mut slot_id_to_update = slot_count;
        for slot_idx in 0..slot_count {
            let slot_offset = self.slot_offset(slot_idx) as usize;
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
                    // find record we want to update
                    if slot.ts() <= ts {
                        slot_id_to_update = slot_idx;
                        break;
                    } else {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }
        }

        if slot_id_to_update == slot_count{
            // not find record to be update
            return Err(AccessMethodError::KeyNotFound)
        }

        /* 
            we find the record to be update
            1. insert new record at new offset
            2. update slot
            3. update header(metadata)
        */        

        // only check RecordTooLarge
        let space_need = <Page as MvccHashJoinRecentPage>::space_need(key, pkey, val);
        if space_need > AVAILABLE_PAGE_SIZE as u32 {
            return Err(AccessMethodError::RecordTooLarge);
        }

        // we can now update record (record size check is inside)
        self.update_at_slot_id(key, pkey, val, ts, slot_id_to_update)
    }

    /*  
        delete a specific key with ts
        if not found -> key not found
        (if record ts > ts -> return [AccessMethodError::KeyFoundButInvalidTimestamp])
     */
    fn delete(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError> {
        let header = self.header();
        let slot_count = header.slot_count() /* default value: slot_count */;
        let mut slot_id_to_delete = slot_count;
        let mut old_val: Vec<u8> = Vec::new();
        let mut old_ts: Timestamp = 0;
        for slot_idx in (0..slot_count).rev() {
            let slot_offset = self.slot_offset(slot_idx) as usize;
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
                let record_bytes: &[u8] = &self[rec_offset..rec_offset + rec_size as usize];
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
                    // find record we want to delete
                    if slot.ts() <= ts {
                        slot_id_to_delete = slot_idx;
                        old_val.extend_from_slice(record.val());
                        old_ts = slot.ts();
                        break;
                    } else {
                        return Err(AccessMethodError::KeyFoundButInvalidTimestamp);
                    }
                }
            }
        }

        if slot_id_to_delete == slot_count{
            // not find record to be delete
            return Err(AccessMethodError::KeyNotFound)
        }

        /* 
            we find the record to be delete
            1. move afterwards slots forward by one slot
            2. update header(metadata)  (update offset if necessary?)
         */
        self.delete_slot_at_id(slot_id_to_delete);

        return Ok((old_val, old_ts));
    }

    fn next_page(&self) -> Option<(PageId, u32)> {
        let header = self.header();
        header.next_page()
    }

    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        let mut header = self.header();
        header.set_next_page(next_page_id, frame_id);
        self.set_header(&header);
    }

    // helper function
    fn header(&self) -> Header {
        let header_bytes = &self[0..PAGE_HEADER_SIZE];
        Header::from_bytes(&header_bytes).unwrap()
    }

    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    fn space_need(key: &[u8], pkey: &[u8], val: &[u8]) -> u32 {
        let remain_key_size = key.len().saturating_sub(SLOT_KEY_PREFIX_SIZE);
        let remain_pkey_size = pkey.len().saturating_sub(SLOT_PKEY_PREFIX_SIZE);
        SLOT_SIZE as u32 + remain_key_size as u32 + remain_pkey_size as u32 + val.len() as u32
    }

    fn free_space_without_compaction(&self) -> u32 {
        let header = self.header();
        header.rec_start_offset() - (PAGE_HEADER_SIZE as u32 + header.slot_count() * SLOT_SIZE as u32)
    }

    fn free_space_with_compaction(&self) -> u32 {
        let header = self.header();
        AVAILABLE_PAGE_SIZE as u32 - header.total_bytes_used() as u32
    }

    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE as u32 + slot_id as u32 * SLOT_SIZE as u32
    }

    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }

    fn decrease_total_bytes_used(&mut self, num_bytes: u32) {
        let mut header = self.header();
        header.set_total_bytes_used(header.total_bytes_used() - num_bytes);
        self.set_header(&header);
    }

    fn delete_slot_at_id(&mut self, slot_id: u32) {
        // check if rec_start_offset should change 
        let slot = self.slot(slot_id).expect("Invalid slot_id");

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
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    /*  
        calculate to find whether the record size is okay or not
        if compaction is not enough -> OutOfSpace
     */
    fn update_at_slot_id(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        val: &[u8],
        ts: Timestamp,
        slot_id: u32,
    ) -> Result<(Vec<u8>, Timestamp), AccessMethodError> {
        let new_rec_size = Self::space_need(key, pkey, val) - SLOT_SIZE as u32;

        let mut slot = self.slot(slot_id).expect("Invalid slot_id");
        let old_val = self.get_value_with_slot(&slot).to_vec();
        let old_ts = slot.ts();
        let old_record_offset = slot.offset();
        let old_val_size = slot.val_size();
        let old_rec_size = slot.key_size().saturating_sub(SLOT_KEY_PREFIX_SIZE as u32) + 
            slot.pkey_size().saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32) + old_val_size;

        if self.free_space_without_compaction() < new_rec_size {
            if (self.free_space_with_compaction() as i32) < (new_rec_size as i32) - (old_rec_size as i32) {
                return Err(AccessMethodError::OutOfSpace);
            }
            if new_rec_size > old_rec_size && self.free_space_with_compaction() != self.free_space_without_compaction() {
                // compaction
                todo!()
            }
        }

        let new_record = Record::new(key, pkey, val);
        let new_rec_offset = {
            if new_rec_size <= old_rec_size || old_record_offset == self.rec_start_offset() {
                // Case 1: New value size is smaller or equal (or) Case 2: Offset matches `rec_start_offset`
                let new_rec_offset = old_record_offset + old_rec_size - new_rec_size;
                self.write_record(new_rec_offset, new_record);
                if new_rec_size < old_rec_size {
                    self.write_bytes(old_record_offset as usize, &vec![0; old_rec_size as usize - new_rec_size as usize]);
                }
                if old_record_offset == self.rec_start_offset() {
                    self.set_rec_start_offset(new_rec_offset as u32);
                }
                new_rec_offset
            } else {
                // Case 3: New value is larger and offset doesn't match `rec_start_offset`
                let new_rec_offset = self.rec_start_offset() - new_rec_size;
                self.write_record(new_rec_offset, new_record);
                self.set_rec_start_offset(new_rec_offset as u32);
                new_rec_offset
            }
        };
        
        slot.set_ts(ts);
        slot.set_val_size(val.len());
        slot.set_offset(new_rec_offset as u32);

        self.set_slot(slot_id, &slot);

        self.increase_total_bytes_used(new_rec_size as u32);
        self.decrease_total_bytes_used(old_rec_size as u32);

        Ok((old_val, old_ts))
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

    fn rec_start_offset(&self) -> u32 {
        self.header().rec_start_offset()
    }

    fn write_record(&mut self, offset: u32, record: Record) {
        let bytes = record.to_bytes();
        self.write_bytes(offset as usize, &bytes);
    }

    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }

    fn set_slot(&mut self, slot_id: u32, slot: &Slot) {
        let slot_offset = self.slot_offset(slot_id);
        self[slot_offset as usize..slot_offset as usize + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Retrieve the entry
        let retrieved_val = page.get(key, pkey, ts).unwrap();
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
        page.init();
    
        // Insert entries
        for (key, pkey, ts, val) in &entries {
            page.insert(key, pkey, *ts, val).unwrap();
        }
    
        // Retrieve and verify entries
        for (key, pkey, ts, val) in &entries {
            let retrieved_val = page.get(key, pkey, *ts).unwrap();
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val).unwrap();

        // Attempt to retrieve with earlier timestamp
        let result = page.get(key, pkey, ts_query);
        assert!(matches!(result, Err(AccessMethodError::KeyFoundButInvalidTimestamp)));

        // Retrieve with correct timestamp
        let retrieved_val = page.get(key, pkey, ts_insert).unwrap();
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
        page.init();

        // Insert first entry
        page.insert(key, pkey1, ts1, val1).unwrap();

        // Insert second entry with a newer timestamp
        page.insert(key, pkey2, ts2, val2).unwrap();

        // Retrieve with ts1
        let retrieved_val = page.get(key, pkey1, ts1).unwrap();
        assert_eq!(retrieved_val, val1);

        // Retrieve with ts2
        let retrieved_val = page.get(key, pkey2, ts2).unwrap();
        assert_eq!(retrieved_val, val2);
    }

    #[test]
    fn test_insert_when_page_full() {
        // Fill the page to capacity and attempt to insert another entry
        let mut page = Page::new_empty();
        page.init();

        // Use fixed-length keys and pkeys
        let key = b"key_full_full";         // Length 12 bytes
        let pkey = b"pkey_full_full";       // Length 13 bytes
        let val = b"value_full";            // Value size
        let ts: Timestamp = 1;

        let space_per_entry = Page::space_need(key, pkey, val) as usize;
        let available_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let page_capacity = available_space / space_per_entry;

        // Insert entries until the page is full
        for _ in 0..page_capacity {
            page.insert(key, pkey, ts, val).unwrap();
        }

        // Attempt to insert one more entry
        let result = page.insert(key, pkey, ts, val);
        assert!(matches!(result, Err(AccessMethodError::OutOfSpace)));
    }

    #[test]
    fn test_delete_and_get_key_pkey_len_less_than_prefix() {
        // Key and PKey lengths less than prefix sizes
        let key = b"key1"; // Length 4
        let pkey = b"pk1"; // Length 3
        let ts: Timestamp = 1;
        let val = b"value1";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Delete the entry
        let del_result = page.delete(key, pkey, ts + 1);

        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().0, val);
        assert_eq!(del_result.as_ref().unwrap().1, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts);
        assert!(retrieved_res.is_err());
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_insert_and_del_key_len_less_than_prefix_pkey_len_greater_than_prefix() {
        // Key length less than prefix size, PKey length greater than prefix size
        let key = b"key2"; // Length 4
        let pkey = b"primarykey_longer"; // Length > 8
        let ts: Timestamp = 2;
        let val = b"value2";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Delete the entry
        let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().0, val);
        assert_eq!(del_result.as_ref().unwrap().1, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts);
        assert!(retrieved_res.is_err());
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_insert_and_delete_key_len_greater_than_prefix_pkey_len_less_than_prefix() {
        // Key length greater than prefix size, PKey length less than prefix size
        let key = b"key_longer_than_prefix"; // Length > 8
        let pkey = b"pk2"; // Length 3
        let ts: Timestamp = 3;
        let val = b"value3";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Delete the entry
        let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().0, val);
        assert_eq!(del_result.as_ref().unwrap().1, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts);
        assert!(retrieved_res.is_err());
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_insert_and_delete_key_pkey_len_greater_than_prefix() {
        // Key and PKey lengths greater than prefix sizes
        let key = b"key_longer_than_prefix_size"; // Length > 8
        let pkey = b"primarykey_longer_than_prefix"; // Length > 8
        let ts: Timestamp = 4;
        let val = b"value4";

        let mut page = Page::new_empty();
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts, val).unwrap();

        // Delete the entry
        let del_result = page.delete(key, pkey, ts + 1);
        assert!(del_result.is_ok());
        assert_eq!(del_result.as_ref().unwrap().0, val);
        assert_eq!(del_result.as_ref().unwrap().1, ts);
        assert_eq!(page.header().slot_count(), 0);

        // Retrieve the key after del
        let retrieved_res = page.get(key, pkey, ts);
        assert!(retrieved_res.is_err());
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
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
        page.init();
    
        // Insert entries
        for (key, pkey, ts, val) in &entries {
            page.insert(key, pkey, *ts, val).unwrap();
        }

        // Delete entries
        for (key, pkey, ts, val) in &entries {
            assert_eq!(page.delete(key, pkey, *ts).unwrap().0, *val);
        }
    
        // Retrieve and verify entries
        for (key, pkey, ts, _val) in &entries {
            let retrieved_res = page.get(key, pkey, *ts);
            assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
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
        page.init();

        // Insert the entry
        page.insert(key, pkey, ts_insert, val).unwrap();

        // Delete the entry
        let delete_result = page.delete(key, pkey, ts_query);
        assert_eq!(delete_result.err(), Some(AccessMethodError::KeyFoundButInvalidTimestamp));
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
        page.init();

        // Insert first entry
        page.insert(key, pkey1, ts1, val1).unwrap();

        // Insert second entry with a newer timestamp
        page.insert(key, pkey2, ts2, val2).unwrap();

        // Delete the 2
        page.delete(key, pkey1, ts1).unwrap();
        page.delete(key, pkey2, ts2).unwrap();


        // Retrieve with ts1
        let retrieved_res = page.get(key, pkey1, ts1);
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));

        // Retrieve with ts2
        let retrieved_res = page.get(key, pkey2, ts2);
        assert_eq!(retrieved_res.err(), Some(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_insert_and_delete_when_page_full() {
        // Fill the page to capacity and attempt to insert another entry
        let mut page = Page::new_empty();
        page.init();

        // Use fixed-length keys and pkeys
        let key = b"key_full_full";         // Length 12 bytes
        let pkey = b"pkey_full_full";       // Length 13 bytes
        let val = b"value_full";            // Value size
        let ts: Timestamp = 1;

        let space_per_entry = Page::space_need(key, pkey, val) as usize;
        let available_space = AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE;
        let page_capacity = available_space / space_per_entry;

        // Insert entries until the page is full
        for _ in 0..page_capacity {
            page.insert(key, pkey, ts, val).unwrap();
        }

        let before_delete_pg_sz = page.header().total_bytes_used();
        // Attempt to insert one more entry
        let _result = page.delete(key, pkey, ts);

        assert_eq!(space_per_entry as u32, before_delete_pg_sz - page.header().total_bytes_used());
        assert!(page.insert(key, pkey, ts, val).is_ok());
    }

    // -------------UPDATE---------------
    #[test]
    fn test_update_with_smaller_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20, 30, 40];
        page.insert(&key, &pkey, ts, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a smaller value
        let new_value = vec![50, 60];
        page.update(&key,&pkey, ts, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key, &pkey, ts).expect("Failed to get updated value");

        assert_eq!(updated_value, &[50, 60]);
    }

    #[test]
    fn test_update_with_larger_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20];
        page.insert(&key, &pkey, ts, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![30, 40, 50, 60];
        page.update(&key, &pkey, ts, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key, &pkey, ts).expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[30, 40, 50, 60]);
    }

    #[test]
    fn test_update_with_equal_size_value() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20, 30];
        page.insert(&key, &pkey, ts, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a value of equal size
        let new_value = vec![40, 50, 60];
        page.update(&key, &pkey, ts, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let updated_value = page.get(&key, &pkey, ts).expect("Failed to get updated value");

        // Assert that the value has been updated correctly
        assert_eq!(updated_value, &[40, 50, 60]);
    }

    #[test]
    fn test_update_non_existent_key() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let value = vec![10, 20, 30];
        page.insert(&key, &pkey, ts, &value)
            .expect("Failed to insert key-value pair");

        // Attempt to update a non-existent key
        let non_existent_key = vec![4, 5, 6];
        let new_value = vec![40, 50, 60];
        let result = page.update(&non_existent_key, &pkey, ts, &new_value);

        // Assert that the update operation fails with KeyNotFound
        assert_eq!(result, Err(AccessMethodError::KeyNotFound));
    }

    #[test]
    fn test_update_with_large_value_out_of_space() {
        let mut page = Page::new_empty();
        page.init();

        // Insert a key-value pair
        let key = vec![1, 2, 3];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let initial_value = vec![10, 20];
        page.insert(&key, &pkey, ts, &initial_value)
            .expect("Failed to insert key-value pair");

        // Attempt to update with a value that is too large
        let large_value = vec![0; AVAILABLE_PAGE_SIZE]; // Value larger than the remaining space
        let result = page.update(&key, &pkey, ts, &large_value);

        // Assert that the update operation fails with RecordTooLarge
        assert_eq!(result, Err(AccessMethodError::RecordTooLarge));
    }

    #[test]
    fn test_insert_multiple_and_update_multiple() {
        let mut page = Page::new_empty();
        page.init();

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
            page.insert(key, &pkey, ts,  value)
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
            page.update(key, &pkey, ts, new_value).expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        for (key, expected_value) in keys.iter().zip(new_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts).expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_multiple_update_mixed_sizes_and_get() {
        let mut page = Page::new_empty();
        page.init();

        // Insert multiple key-value pairs
        let keys = vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]];
        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        let values = vec![vec![10, 20, 30], vec![40, 50, 60], vec![70, 80, 90]];

        for (key, value) in keys.iter().zip(values.iter()) {
            page.insert(key, &pkey, ts, value)
                .expect("Failed to insert key-value pair");
        }

        // Update the values with mixed sizes
        let new_values = vec![
            vec![13, 14],             // Smaller
            vec![16, 17, 18],         // Same size
            vec![19, 20, 21, 22, 23], // Larger
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            page.update(key, &pkey, ts, new_value).expect("Failed to update value");
        }

        // Retrieve the updated values and verify correctness
        let expected_values = vec![
            vec![13, 14],             // Padded with zeros
            vec![16, 17, 18],         // Same size
            vec![19, 20, 21, 22, 23], // Larger, no padding needed
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts).expect("Failed to get updated value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

    #[test]
    fn test_insert_update_and_get_mixed() {
        let mut page = Page::new_empty();
        page.init();

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert initial key-value pairs
        let initial_keys = vec![vec![1, 1, 1], vec![2, 2, 2]];
        let initial_values = vec![vec![11, 12, 13], vec![21, 22, 23]];

        for (key, value) in initial_keys.iter().zip(initial_values.iter()) {
            page.insert(key, &pkey, ts, value)
                .expect("Failed to insert key-value pair");
        }

        // Update existing and insert new key-value pairs
        let keys_to_update = vec![vec![1, 1, 1], vec![2, 2, 2]];
        let new_values = vec![
            vec![14, 15, 16],
            vec![24, 25], // Smaller, expect padding
        ];

        for (key, new_value) in keys_to_update.iter().zip(new_values.iter()) {
            page.update(key, &pkey, ts, new_value).expect("Failed to update value");
        }

        // Insert a new key-value pair
        let new_key = vec![3, 3, 3];
        let new_value = vec![31, 32, 33];
        page.insert(&new_key, &pkey, ts, &new_value)
            .expect("Failed to insert new key-value pair");

        // Verify updates and new insertions
        let retrieved_values = vec![
            vec![14, 15, 16], // Updated
            vec![24, 25],     // Updated with padding
            vec![31, 32, 33], // Newly inserted
        ];

        let all_keys = vec![vec![1, 1, 1], vec![2, 2, 2], vec![3, 3, 3]];

        for (key, expected_value) in all_keys.iter().zip(retrieved_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }

   
    #[test]
    fn test_update_large_keys() {
        let mut page = Page::new_empty();
        page.init();

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Key sizes larger than SLOT_KEY_PREFIX_SIZE (8 bytes)
        let key = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]; // 10 bytes
        let initial_value = vec![101, 102, 103];

        // Insert a key-value pair
        page.insert(&key, &pkey, ts, &initial_value)
            .expect("Failed to insert key-value pair");

        // Update the value with a larger value
        let new_value = vec![104, 105, 106, 107, 108];
        page.update(&key, &pkey, ts, &new_value)
            .expect("Failed to update value");

        // Retrieve the updated value and verify correctness
        let retrieved_value = page.get(&key, &pkey, ts).expect("Failed to get updated value");
        assert_eq!(retrieved_value, new_value.as_slice());

        // Update the value with a smaller value
        let smaller_value = vec![109, 110];
        page.update(&key, &pkey, ts, &smaller_value)
            .expect("Failed to update value");

        // Retrieve the updated value
        let retrieved_value = page.get(&key, &pkey, ts).expect("Failed to get updated value");
        assert_eq!(retrieved_value, smaller_value.as_slice());
    }

    #[test]
    fn test_insert_update_and_get_with_large_keys() {
        let mut page = Page::new_empty();
        page.init();

        let pkey = vec![1, 2, 3];
        let ts: Timestamp = 2;
        // Insert multiple key-value pairs with keys larger than 8 bytes
        let keys = vec![
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],                      // 9 bytes
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],         // 10 bytes
            vec![20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31], // 12 bytes
        ];
        let initial_values = vec![
            vec![101, 102],
            vec![201, 202, 203],
            vec![251, 252, 253, 254],
        ];

        for (key, value) in keys.iter().zip(initial_values.iter()) {
            page.insert(key, &pkey, ts, value)
                .expect("Failed to insert key-value pair");
        }

        // Update some of the values with new data
        let new_values = vec![
            vec![103, 104, 105], // Update with a larger value
            vec![206],           // Update with a smaller value
            vec![208, 209],      // Update with a smaller value
        ];

        for (key, new_value) in keys.iter().zip(new_values.iter()) {
            page.update(key, &pkey, ts, new_value).expect("Failed to update value");
        }

        // Retrieve and verify all values
        let expected_values = vec![
            vec![103, 104, 105], // Updated with a larger value
            vec![206],           // Updated with a smaller value, no padding
            vec![208, 209],      // Updated with a smaller value, no padding
        ];

        for (key, expected_value) in keys.iter().zip(expected_values.iter()) {
            let retrieved_value = page.get(key, &pkey, ts).expect("Failed to get value");
            assert_eq!(retrieved_value, expected_value.as_slice());
        }
    }
}