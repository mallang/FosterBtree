use crate::{
    access_method::AccessMethodError,
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

// FosterBtreePage-derived leaf page for paged hash buckets.
//
// Page layout:
// 1 byte: flags
// 1 byte: level/reserved, kept to match the FosterBtreePage shape
// 4 byte: slot count, including low/high fence slots
// 4 byte: total bytes used
// 4 byte: record start offset
// 4 byte: next overflow page id
// 4 byte: next overflow frame id
pub const HASH_LEAF_PAGE_HEADER_SIZE: usize = 1 + 1 + 4 + 4 + 4 + 4 + 4;

const SLOT_FLAG_GHOST: u8 = 1;
const PAGE_FLAG_VALID: u8 = 0b1000_0000;
const NO_NEXT_PAGE_ID: PageId = PageId::MAX;
const NO_NEXT_FRAME_ID: u32 = u32::MAX;

mod slot {
    pub const SLOT_SIZE: usize = std::mem::size_of::<u8>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<u32>();

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Slot {
        ghost: u8,
        offset: u32,
        key_size: u32,
        value_size: u32,
    }

    impl Slot {
        pub fn from_bytes(bytes: [u8; SLOT_SIZE]) -> Self {
            let ghost = bytes[0];
            let offset = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
            let key_size = u32::from_be_bytes(bytes[5..9].try_into().unwrap());
            let value_size = u32::from_be_bytes(bytes[9..13].try_into().unwrap());
            Self {
                ghost,
                offset,
                key_size,
                value_size,
            }
        }

        pub fn to_bytes(self) -> [u8; SLOT_SIZE] {
            let mut bytes = [0_u8; SLOT_SIZE];
            bytes[0] = self.ghost;
            bytes[1..5].copy_from_slice(&self.offset.to_be_bytes());
            bytes[5..9].copy_from_slice(&self.key_size.to_be_bytes());
            bytes[9..13].copy_from_slice(&self.value_size.to_be_bytes());
            bytes
        }

        pub fn new(is_ghost: bool, offset: u32, key_size: u32, value_size: u32) -> Self {
            Self {
                ghost: is_ghost as u8,
                offset,
                key_size,
                value_size,
            }
        }

        pub fn is_ghost(self) -> bool {
            self.ghost == super::SLOT_FLAG_GHOST
        }

        pub fn set_ghost(&mut self, is_ghost: bool) {
            self.ghost = is_ghost as u8;
        }

        pub fn offset(self) -> u32 {
            self.offset
        }

        pub fn set_offset(&mut self, offset: u32) {
            self.offset = offset;
        }

        pub fn key_size(self) -> u32 {
            self.key_size
        }

        pub fn value_size(self) -> u32 {
            self.value_size
        }

        pub fn total_size(self) -> u32 {
            SLOT_SIZE as u32 + self.key_size + self.value_size
        }
    }
}

use slot::{Slot, SLOT_SIZE};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HashLeafHeader {
    flags: u8,
    level: u8,
    slot_count: u32,
    total_bytes_used: u32,
    rec_start_offset: u32,
    next_page_id: PageId,
    next_frame_id: u32,
}

impl HashLeafHeader {
    fn new() -> Self {
        Self {
            flags: PAGE_FLAG_VALID,
            level: 0,
            slot_count: 0,
            total_bytes_used: HASH_LEAF_PAGE_HEADER_SIZE as u32,
            rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
            next_page_id: NO_NEXT_PAGE_ID,
            next_frame_id: NO_NEXT_FRAME_ID,
        }
    }

    fn from_page(page: &Page) -> Self {
        Self {
            flags: page[0],
            level: page[1],
            slot_count: u32::from_be_bytes(page[2..6].try_into().unwrap()),
            total_bytes_used: u32::from_be_bytes(page[6..10].try_into().unwrap()),
            rec_start_offset: u32::from_be_bytes(page[10..14].try_into().unwrap()),
            next_page_id: PageId::from_be_bytes(page[14..18].try_into().unwrap()),
            next_frame_id: u32::from_be_bytes(page[18..22].try_into().unwrap()),
        }
    }

    fn write_to_page(self, page: &mut Page) {
        page[0] = self.flags;
        page[1] = self.level;
        page[2..6].copy_from_slice(&self.slot_count.to_be_bytes());
        page[6..10].copy_from_slice(&self.total_bytes_used.to_be_bytes());
        page[10..14].copy_from_slice(&self.rec_start_offset.to_be_bytes());
        page[14..18].copy_from_slice(&self.next_page_id.to_be_bytes());
        page[18..22].copy_from_slice(&self.next_frame_id.to_be_bytes());
    }
}

pub trait HashLeafPage {
    fn init_hash_leaf(&mut self);
    fn hash_leaf_is_valid(&self) -> bool;
    fn hash_leaf_slot_count(&self) -> u32;
    fn hash_leaf_record_count(&self) -> u32;
    fn hash_leaf_total_bytes_used(&self) -> u32;
    fn hash_leaf_total_free_space(&self) -> u32;
    fn hash_leaf_contiguous_free_space(&self) -> u32;
    fn hash_leaf_next_page(&self) -> Option<(PageId, u32)>;
    fn hash_leaf_set_next_page(&mut self, next_page_id: PageId, next_frame_id: u32);
    fn hash_leaf_clear_next_page(&mut self);
    fn hash_leaf_get_raw_key(&self, slot_id: u32) -> &[u8];
    fn hash_leaf_get_val(&self, slot_id: u32) -> &[u8];
    fn hash_leaf_lower_bound_slot_id(&self, key: &[u8]) -> u32;
    fn hash_leaf_find_slot_id(&self, key: &[u8]) -> Option<u32>;
    fn hash_leaf_get(&self, key: &[u8]) -> Result<&[u8], AccessMethodError>;
    fn hash_leaf_insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError>;
    fn hash_leaf_insert_ghost(&mut self, key: &[u8], value: &[u8])
        -> Result<(), AccessMethodError>;
    fn hash_leaf_is_ghost(&self, slot_id: u32) -> bool;
    fn hash_leaf_set_ghost(&mut self, slot_id: u32, is_ghost: bool);
    fn hash_leaf_remove_at(&mut self, slot_id: u32);
    fn hash_leaf_compact(&mut self);
}

impl HashLeafPage for Page {
    fn init_hash_leaf(&mut self) {
        HashLeafHeader::new().write_to_page(self);
        self.hash_leaf_insert_at(0, &[], &[], false)
            .expect("low fence must fit");
        self.hash_leaf_insert_at(1, &[], &[], false)
            .expect("high fence must fit");
    }

    fn hash_leaf_is_valid(&self) -> bool {
        HashLeafHeader::from_page(self).flags & PAGE_FLAG_VALID != 0
    }

    fn hash_leaf_slot_count(&self) -> u32 {
        HashLeafHeader::from_page(self).slot_count
    }

    fn hash_leaf_record_count(&self) -> u32 {
        if self.hash_leaf_slot_count() < 2 {
            return 0;
        }
        (self.hash_leaf_low_fence_slot_id() + 1..self.hash_leaf_high_fence_slot_id())
            .filter(|&slot_id| !self.hash_leaf_is_ghost(slot_id))
            .count() as u32
    }

    fn hash_leaf_total_bytes_used(&self) -> u32 {
        HashLeafHeader::from_page(self).total_bytes_used
    }

    fn hash_leaf_total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.hash_leaf_total_bytes_used()
    }

    fn hash_leaf_contiguous_free_space(&self) -> u32 {
        HashLeafHeader::from_page(self).rec_start_offset
            - self.hash_leaf_slot_offset(self.hash_leaf_slot_count()) as u32
    }

    fn hash_leaf_next_page(&self) -> Option<(PageId, u32)> {
        let header = HashLeafHeader::from_page(self);
        if header.next_page_id == NO_NEXT_PAGE_ID {
            None
        } else {
            Some((header.next_page_id, header.next_frame_id))
        }
    }

    fn hash_leaf_set_next_page(&mut self, next_page_id: PageId, next_frame_id: u32) {
        let mut header = HashLeafHeader::from_page(self);
        header.next_page_id = next_page_id;
        header.next_frame_id = next_frame_id;
        header.write_to_page(self);
    }

    fn hash_leaf_clear_next_page(&mut self) {
        let mut header = HashLeafHeader::from_page(self);
        header.next_page_id = NO_NEXT_PAGE_ID;
        header.next_frame_id = NO_NEXT_FRAME_ID;
        header.write_to_page(self);
    }

    fn hash_leaf_get_raw_key(&self, slot_id: u32) -> &[u8] {
        assert!(slot_id < self.hash_leaf_slot_count());
        let slot = self.hash_leaf_slot(slot_id).expect("invalid slot id");
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        &self[offset..offset + key_size]
    }

    fn hash_leaf_get_val(&self, slot_id: u32) -> &[u8] {
        assert!(slot_id < self.hash_leaf_slot_count());
        let slot = self.hash_leaf_slot(slot_id).expect("invalid slot id");
        let offset = slot.offset() as usize;
        let key_size = slot.key_size() as usize;
        let value_size = slot.value_size() as usize;
        &self[offset + key_size..offset + key_size + value_size]
    }

    fn hash_leaf_lower_bound_slot_id(&self, key: &[u8]) -> u32 {
        let mut low = self.hash_leaf_low_fence_slot_id() + 1;
        let mut high = self.hash_leaf_high_fence_slot_id();

        while low < high {
            let mid = low + (high - low) / 2;
            if self.hash_leaf_get_raw_key(mid) < key {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        low
    }

    fn hash_leaf_find_slot_id(&self, key: &[u8]) -> Option<u32> {
        let slot_id = self.hash_leaf_lower_bound_slot_id(key);
        if slot_id == self.hash_leaf_high_fence_slot_id() {
            return None;
        }
        if self.hash_leaf_get_raw_key(slot_id) == key {
            Some(slot_id)
        } else {
            None
        }
    }

    fn hash_leaf_get(&self, key: &[u8]) -> Result<&[u8], AccessMethodError> {
        match self.hash_leaf_find_slot_id(key) {
            Some(slot_id) if !self.hash_leaf_is_ghost(slot_id) => {
                Ok(self.hash_leaf_get_val(slot_id))
            }
            _ => Err(AccessMethodError::KeyNotFound),
        }
    }

    fn hash_leaf_insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), AccessMethodError> {
        self.hash_leaf_insert_with_ghost(key, value, false)
    }

    fn hash_leaf_insert_ghost(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError> {
        self.hash_leaf_insert_with_ghost(key, value, true)
    }

    fn hash_leaf_is_ghost(&self, slot_id: u32) -> bool {
        self.hash_leaf_slot(slot_id)
            .expect("invalid slot id")
            .is_ghost()
    }

    fn hash_leaf_set_ghost(&mut self, slot_id: u32, is_ghost: bool) {
        let mut slot = self.hash_leaf_slot(slot_id).expect("invalid slot id");
        slot.set_ghost(is_ghost);
        self.hash_leaf_set_slot(slot_id, slot);
    }

    fn hash_leaf_remove_at(&mut self, slot_id: u32) {
        if slot_id <= self.hash_leaf_low_fence_slot_id()
            || slot_id >= self.hash_leaf_high_fence_slot_id()
        {
            panic!("cannot remove hash leaf fence slot");
        }

        let removed = self.hash_leaf_slot(slot_id).expect("invalid slot id");
        let start = self.hash_leaf_slot_offset(slot_id + 1);
        let end = self.hash_leaf_slot_offset(self.hash_leaf_slot_count());
        if start < end {
            self.copy_within(start..end, start - SLOT_SIZE);
        }

        let mut header = HashLeafHeader::from_page(self);
        header.slot_count -= 1;
        header.total_bytes_used -= removed.total_size();
        header.write_to_page(self);
    }

    fn hash_leaf_compact(&mut self) {
        self.hash_leaf_compact_records();
    }
}

trait HashLeafPagePrivate {
    fn hash_leaf_low_fence_slot_id(&self) -> u32;
    fn hash_leaf_high_fence_slot_id(&self) -> u32;
    fn hash_leaf_slot_offset(&self, slot_id: u32) -> usize;
    fn hash_leaf_slot(&self, slot_id: u32) -> Option<Slot>;
    fn hash_leaf_set_slot(&mut self, slot_id: u32, slot: Slot);
    fn hash_leaf_insert_with_ghost(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError>;
    fn hash_leaf_insert_at(
        &mut self,
        slot_id: u32,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError>;
    fn hash_leaf_compact_records(&mut self);
}

impl HashLeafPagePrivate for Page {
    fn hash_leaf_low_fence_slot_id(&self) -> u32 {
        0
    }

    fn hash_leaf_high_fence_slot_id(&self) -> u32 {
        self.hash_leaf_slot_count() - 1
    }

    fn hash_leaf_slot_offset(&self, slot_id: u32) -> usize {
        HASH_LEAF_PAGE_HEADER_SIZE + slot_id as usize * SLOT_SIZE
    }

    fn hash_leaf_slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id >= self.hash_leaf_slot_count() {
            return None;
        }
        let offset = self.hash_leaf_slot_offset(slot_id);
        Some(Slot::from_bytes(
            self[offset..offset + SLOT_SIZE].try_into().unwrap(),
        ))
    }

    fn hash_leaf_set_slot(&mut self, slot_id: u32, slot: Slot) {
        assert!(slot_id < self.hash_leaf_slot_count());
        let offset = self.hash_leaf_slot_offset(slot_id);
        self[offset..offset + SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    fn hash_leaf_insert_with_ghost(
        &mut self,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError> {
        let slot_id = self.hash_leaf_lower_bound_slot_id(key);
        if slot_id != self.hash_leaf_high_fence_slot_id()
            && self.hash_leaf_get_raw_key(slot_id) == key
        {
            return Err(AccessMethodError::KeyDuplicate);
        }
        self.hash_leaf_insert_at(slot_id, key, value, is_ghost)
    }

    fn hash_leaf_insert_at(
        &mut self,
        slot_id: u32,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError> {
        let record_size = key.len() + value.len();
        let bytes_needed = SLOT_SIZE + record_size;
        if bytes_needed > AVAILABLE_PAGE_SIZE - HASH_LEAF_PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        }

        if bytes_needed > self.hash_leaf_contiguous_free_space() as usize {
            if bytes_needed > self.hash_leaf_total_free_space() as usize {
                return Err(AccessMethodError::OutOfSpace);
            }
            self.hash_leaf_compact_records();
        }

        let mut header = HashLeafHeader::from_page(self);
        if slot_id > header.slot_count {
            panic!("invalid hash leaf insertion slot");
        }

        let record_offset = header.rec_start_offset as usize - record_size;
        self[record_offset..record_offset + key.len()].copy_from_slice(key);
        self[record_offset + key.len()..record_offset + record_size].copy_from_slice(value);

        let start = self.hash_leaf_slot_offset(slot_id);
        let end = self.hash_leaf_slot_offset(header.slot_count);
        if start < end {
            self.copy_within(start..end, start + SLOT_SIZE);
        }

        header.slot_count += 1;
        header.rec_start_offset = record_offset as u32;
        header.total_bytes_used += bytes_needed as u32;
        header.write_to_page(self);

        let slot = Slot::new(
            is_ghost,
            record_offset as u32,
            key.len() as u32,
            value.len() as u32,
        );
        self.hash_leaf_set_slot(slot_id, slot);
        Ok(())
    }

    fn hash_leaf_compact_records(&mut self) {
        let slot_count = self.hash_leaf_slot_count();
        let mut records = Vec::with_capacity(slot_count as usize);
        let mut total_record_bytes = 0usize;

        for slot_id in 0..slot_count {
            let slot = self.hash_leaf_slot(slot_id).expect("invalid slot id");
            let record_size = slot.key_size() as usize + slot.value_size() as usize;
            let record =
                self[slot.offset() as usize..slot.offset() as usize + record_size].to_vec();
            total_record_bytes += record_size;
            records.push((slot_id, slot, record));
        }

        let mut offset = AVAILABLE_PAGE_SIZE;
        for (slot_id, mut slot, record) in records {
            offset -= record.len();
            self[offset..offset + record.len()].copy_from_slice(&record);
            slot.set_offset(offset as u32);
            self.hash_leaf_set_slot(slot_id, slot);
        }

        let mut header = HashLeafHeader::from_page(self);
        header.rec_start_offset = (AVAILABLE_PAGE_SIZE - total_record_bytes) as u32;
        header.write_to_page(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_hash_leaf() -> Page {
        let mut page = Page::new(0);
        page.init_hash_leaf();
        page
    }

    fn encoded(hash: u64, key: &[u8]) -> Vec<u8> {
        let mut encoded = hash.to_be_bytes().to_vec();
        encoded.extend_from_slice(key);
        encoded
    }

    #[test]
    fn initializes_fence_slots_and_next_page() {
        let page = new_hash_leaf();

        assert!(page.hash_leaf_is_valid());
        assert_eq!(page.hash_leaf_slot_count(), 2);
        assert_eq!(page.hash_leaf_record_count(), 0);
        assert_eq!(page.hash_leaf_next_page(), None);
    }

    #[test]
    fn links_to_next_overflow_page() {
        let mut page = new_hash_leaf();

        page.hash_leaf_set_next_page(12, 34);
        assert_eq!(page.hash_leaf_next_page(), Some((12, 34)));

        page.hash_leaf_clear_next_page();
        assert_eq!(page.hash_leaf_next_page(), None);
    }

    #[test]
    fn inserts_and_orders_encoded_keys() {
        let mut page = new_hash_leaf();
        let key_b = encoded(7, b"b");
        let key_a = encoded(7, b"a");
        let key_z = encoded(3, b"z");

        page.hash_leaf_insert(&key_b, b"v-b").unwrap();
        page.hash_leaf_insert(&key_a, b"v-a").unwrap();
        page.hash_leaf_insert(&key_z, b"v-z").unwrap();

        assert_eq!(page.hash_leaf_record_count(), 3);
        assert_eq!(page.hash_leaf_get_raw_key(1), key_z.as_slice());
        assert_eq!(page.hash_leaf_get_raw_key(2), key_a.as_slice());
        assert_eq!(page.hash_leaf_get_raw_key(3), key_b.as_slice());
        assert_eq!(page.hash_leaf_get(&key_z).unwrap(), b"v-z");
        assert_eq!(page.hash_leaf_get(&key_a).unwrap(), b"v-a");
        assert_eq!(page.hash_leaf_get(&key_b).unwrap(), b"v-b");
    }

    #[test]
    fn rejects_duplicate_key() {
        let mut page = new_hash_leaf();
        let key = encoded(11, b"k");

        page.hash_leaf_insert(&key, b"v1").unwrap();
        let err = page.hash_leaf_insert(&key, b"v2").unwrap_err();

        assert_eq!(err, AccessMethodError::KeyDuplicate);
        assert_eq!(page.hash_leaf_get(&key).unwrap(), b"v1");
    }

    #[test]
    fn hides_ghost_record_from_get() {
        let mut page = new_hash_leaf();
        let key = encoded(11, b"k");

        page.hash_leaf_insert(&key, b"v").unwrap();
        let slot_id = page.hash_leaf_find_slot_id(&key).unwrap();
        page.hash_leaf_set_ghost(slot_id, true);

        assert_eq!(
            page.hash_leaf_get(&key),
            Err(AccessMethodError::KeyNotFound)
        );
        assert!(page.hash_leaf_is_ghost(slot_id));
    }
}
