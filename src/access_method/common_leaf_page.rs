use std::cmp::Ordering;

use crate::{
    access_method::AccessMethodError,
    page::{Page, AVAILABLE_PAGE_SIZE},
};

pub const COMMON_LEAF_KEY_PREFIX_SIZE: usize = 8;
pub const COMMON_LEAF_PAGE_HEADER_SIZE: usize = 12;
pub const COMMON_LEAF_SLOT_SIZE: usize = 32;

const SLOT_FLAG_GHOST: u8 = 0b0000_0001;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CommonLeafHeader {
    total_bytes_used: u32,
    slot_count: u32,
    rec_start_offset: u32,
}

impl CommonLeafHeader {
    fn new() -> Self {
        Self {
            total_bytes_used: COMMON_LEAF_PAGE_HEADER_SIZE as u32,
            slot_count: 0,
            rec_start_offset: AVAILABLE_PAGE_SIZE as u32,
        }
    }

    fn from_bytes(bytes: &[u8; COMMON_LEAF_PAGE_HEADER_SIZE]) -> Self {
        let total_bytes_used = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let slot_count = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
        let rec_start_offset = u32::from_be_bytes(bytes[8..12].try_into().unwrap());
        Self {
            total_bytes_used,
            slot_count,
            rec_start_offset,
        }
    }

    fn to_bytes(self) -> [u8; COMMON_LEAF_PAGE_HEADER_SIZE] {
        let mut bytes = [0_u8; COMMON_LEAF_PAGE_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.total_bytes_used.to_be_bytes());
        bytes[4..8].copy_from_slice(&self.slot_count.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.rec_start_offset.to_be_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommonLeafSlot {
    flags: u8,
    hash: u64,
    key_prefix: [u8; COMMON_LEAF_KEY_PREFIX_SIZE],
    key_size: u32,
    value_size: u32,
    offset: u32,
}

impl CommonLeafSlot {
    pub fn new(hash: u64, key: &[u8], value: &[u8], offset: u32, is_ghost: bool) -> Self {
        let mut key_prefix = [0_u8; COMMON_LEAF_KEY_PREFIX_SIZE];
        let copy_len = COMMON_LEAF_KEY_PREFIX_SIZE.min(key.len());
        key_prefix[..copy_len].copy_from_slice(&key[..copy_len]);

        Self {
            flags: if is_ghost { SLOT_FLAG_GHOST } else { 0 },
            hash,
            key_prefix,
            key_size: key.len() as u32,
            value_size: value.len() as u32,
            offset,
        }
    }

    pub fn from_bytes(bytes: &[u8; COMMON_LEAF_SLOT_SIZE]) -> Self {
        let flags = bytes[0];
        let hash = u64::from_be_bytes(bytes[4..12].try_into().unwrap());

        let mut key_prefix = [0_u8; COMMON_LEAF_KEY_PREFIX_SIZE];
        key_prefix.copy_from_slice(&bytes[12..20]);

        let key_size = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        let value_size = u32::from_be_bytes(bytes[24..28].try_into().unwrap());
        let offset = u32::from_be_bytes(bytes[28..32].try_into().unwrap());

        Self {
            flags,
            hash,
            key_prefix,
            key_size,
            value_size,
            offset,
        }
    }

    pub fn to_bytes(self) -> [u8; COMMON_LEAF_SLOT_SIZE] {
        let mut bytes = [0_u8; COMMON_LEAF_SLOT_SIZE];
        bytes[0] = self.flags;
        bytes[4..12].copy_from_slice(&self.hash.to_be_bytes());
        bytes[12..20].copy_from_slice(&self.key_prefix);
        bytes[20..24].copy_from_slice(&self.key_size.to_be_bytes());
        bytes[24..28].copy_from_slice(&self.value_size.to_be_bytes());
        bytes[28..32].copy_from_slice(&self.offset.to_be_bytes());
        bytes
    }

    pub fn hash(self) -> u64 {
        self.hash
    }

    pub fn key_size(self) -> usize {
        self.key_size as usize
    }

    pub fn value_size(self) -> usize {
        self.value_size as usize
    }

    pub fn offset(self) -> usize {
        self.offset as usize
    }

    pub fn is_ghost(self) -> bool {
        self.flags & SLOT_FLAG_GHOST != 0
    }

    pub fn set_ghost(&mut self, is_ghost: bool) {
        if is_ghost {
            self.flags |= SLOT_FLAG_GHOST;
        } else {
            self.flags &= !SLOT_FLAG_GHOST;
        }
    }

    fn key_suffix_size(self) -> usize {
        self.key_size().saturating_sub(COMMON_LEAF_KEY_PREFIX_SIZE)
    }

    fn record_size(self) -> usize {
        self.key_suffix_size() + self.value_size()
    }
}

pub trait CommonLeafPage {
    fn init_common_leaf(&mut self);
    fn common_leaf_slot_count(&self) -> u32;
    fn common_leaf_total_bytes_used(&self) -> u32;
    fn common_leaf_total_free_space(&self) -> u32;
    fn common_leaf_contiguous_free_space(&self) -> u32;
    fn common_leaf_slot(&self, slot_id: u32) -> Option<CommonLeafSlot>;
    fn common_leaf_key_at(&self, slot_id: u32) -> Vec<u8>;
    fn common_leaf_value_at(&self, slot_id: u32) -> &[u8];
    fn common_leaf_lower_bound(&self, hash: u64, key: &[u8]) -> u32;
    fn common_leaf_find_slot(&self, hash: u64, key: &[u8]) -> Option<u32>;
    fn common_leaf_get(&self, hash: u64, key: &[u8]) -> Result<&[u8], AccessMethodError>;
    fn common_leaf_insert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError>;
    fn common_leaf_upsert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError>;
    fn common_leaf_set_ghost(&mut self, slot_id: u32, is_ghost: bool);
}

impl CommonLeafPage for Page {
    fn init_common_leaf(&mut self) {
        self.set_common_leaf_header(CommonLeafHeader::new());
    }

    fn common_leaf_slot_count(&self) -> u32 {
        self.common_leaf_header().slot_count
    }

    fn common_leaf_total_bytes_used(&self) -> u32 {
        self.common_leaf_header().total_bytes_used
    }

    fn common_leaf_total_free_space(&self) -> u32 {
        AVAILABLE_PAGE_SIZE as u32 - self.common_leaf_total_bytes_used()
    }

    fn common_leaf_contiguous_free_space(&self) -> u32 {
        self.common_leaf_header().rec_start_offset
            - self.common_leaf_slot_offset(self.common_leaf_slot_count()) as u32
    }

    fn common_leaf_slot(&self, slot_id: u32) -> Option<CommonLeafSlot> {
        if slot_id >= self.common_leaf_slot_count() {
            return None;
        }
        let offset = self.common_leaf_slot_offset(slot_id);
        let bytes = self[offset..offset + COMMON_LEAF_SLOT_SIZE]
            .try_into()
            .unwrap();
        Some(CommonLeafSlot::from_bytes(bytes))
    }

    fn common_leaf_key_at(&self, slot_id: u32) -> Vec<u8> {
        let slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");
        self.common_leaf_key_from_slot(slot)
    }

    fn common_leaf_value_at(&self, slot_id: u32) -> &[u8] {
        let slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");
        self.common_leaf_value_from_slot(slot)
    }

    fn common_leaf_lower_bound(&self, hash: u64, key: &[u8]) -> u32 {
        let mut low = 0;
        let mut high = self.common_leaf_slot_count();
        while low < high {
            let mid = low + (high - low) / 2;
            match self.common_leaf_compare_slot_key(mid, hash, key) {
                Ordering::Less => low = mid + 1,
                Ordering::Equal | Ordering::Greater => high = mid,
            }
        }
        low
    }

    fn common_leaf_find_slot(&self, hash: u64, key: &[u8]) -> Option<u32> {
        let slot_id = self.common_leaf_lower_bound(hash, key);
        if slot_id < self.common_leaf_slot_count()
            && self.common_leaf_compare_slot_key(slot_id, hash, key) == Ordering::Equal
        {
            Some(slot_id)
        } else {
            None
        }
    }

    fn common_leaf_get(&self, hash: u64, key: &[u8]) -> Result<&[u8], AccessMethodError> {
        self.common_leaf_find_slot(hash, key)
            .map(|slot_id| self.common_leaf_value_at(slot_id))
            .ok_or(AccessMethodError::KeyNotFound)
    }

    fn common_leaf_insert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError> {
        let slot_id = self.common_leaf_lower_bound(hash, key);
        if slot_id < self.common_leaf_slot_count()
            && self.common_leaf_compare_slot_key(slot_id, hash, key) == Ordering::Equal
        {
            return Err(AccessMethodError::KeyDuplicate);
        }
        self.common_leaf_insert_at(slot_id, hash, key, value, false)
    }

    fn common_leaf_upsert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError> {
        let slot_id = self.common_leaf_lower_bound(hash, key);
        if slot_id < self.common_leaf_slot_count()
            && self.common_leaf_compare_slot_key(slot_id, hash, key) == Ordering::Equal
        {
            return self.common_leaf_update_at(slot_id, key, value);
        }
        self.common_leaf_insert_at(slot_id, hash, key, value, false)
    }

    fn common_leaf_set_ghost(&mut self, slot_id: u32, is_ghost: bool) {
        let mut slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");
        slot.set_ghost(is_ghost);
        self.set_common_leaf_slot(slot_id, slot);
    }
}

trait CommonLeafPagePrivate {
    fn common_leaf_header(&self) -> CommonLeafHeader;
    fn set_common_leaf_header(&mut self, header: CommonLeafHeader);
    fn common_leaf_slot_offset(&self, slot_id: u32) -> usize;
    fn set_common_leaf_slot(&mut self, slot_id: u32, slot: CommonLeafSlot);
    fn common_leaf_compare_slot_key(&self, slot_id: u32, hash: u64, key: &[u8]) -> Ordering;
    fn common_leaf_key_from_slot(&self, slot: CommonLeafSlot) -> Vec<u8>;
    fn common_leaf_value_from_slot(&self, slot: CommonLeafSlot) -> &[u8];
    fn common_leaf_write_record(&mut self, offset: usize, key: &[u8], value: &[u8]);
    fn common_leaf_insert_at(
        &mut self,
        slot_id: u32,
        hash: u64,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError>;
    fn common_leaf_update_at(
        &mut self,
        slot_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError>;
    fn compact_common_leaf(&mut self);
}

impl CommonLeafPagePrivate for Page {
    fn common_leaf_header(&self) -> CommonLeafHeader {
        let bytes = self[0..COMMON_LEAF_PAGE_HEADER_SIZE].try_into().unwrap();
        CommonLeafHeader::from_bytes(bytes)
    }

    fn set_common_leaf_header(&mut self, header: CommonLeafHeader) {
        self[0..COMMON_LEAF_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    fn common_leaf_slot_offset(&self, slot_id: u32) -> usize {
        COMMON_LEAF_PAGE_HEADER_SIZE + slot_id as usize * COMMON_LEAF_SLOT_SIZE
    }

    fn set_common_leaf_slot(&mut self, slot_id: u32, slot: CommonLeafSlot) {
        let offset = self.common_leaf_slot_offset(slot_id);
        self[offset..offset + COMMON_LEAF_SLOT_SIZE].copy_from_slice(&slot.to_bytes());
    }

    fn common_leaf_compare_slot_key(&self, slot_id: u32, hash: u64, key: &[u8]) -> Ordering {
        let slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");

        match slot.hash().cmp(&hash) {
            Ordering::Equal => {}
            other => return other,
        }

        let slot_key_size = slot.key_size();
        let target_key_size = key.len();
        let slot_prefix_len = COMMON_LEAF_KEY_PREFIX_SIZE.min(slot_key_size);
        let target_prefix_len = COMMON_LEAF_KEY_PREFIX_SIZE.min(target_key_size);
        let common_prefix_len = slot_prefix_len.min(target_prefix_len);

        match slot.key_prefix[..common_prefix_len].cmp(&key[..common_prefix_len]) {
            Ordering::Equal => {}
            other => return other,
        }

        if slot_key_size <= common_prefix_len || target_key_size <= common_prefix_len {
            return slot_key_size.cmp(&target_key_size);
        }

        let slot_suffix = &self[slot.offset()..slot.offset() + slot.key_suffix_size()];
        let target_suffix = &key[COMMON_LEAF_KEY_PREFIX_SIZE..];
        slot_suffix.cmp(target_suffix)
    }

    fn common_leaf_key_from_slot(&self, slot: CommonLeafSlot) -> Vec<u8> {
        let key_size = slot.key_size();
        let prefix_size = COMMON_LEAF_KEY_PREFIX_SIZE.min(key_size);
        let mut key = Vec::with_capacity(key_size);
        key.extend_from_slice(&slot.key_prefix[..prefix_size]);
        if key_size > COMMON_LEAF_KEY_PREFIX_SIZE {
            let suffix_size = key_size - COMMON_LEAF_KEY_PREFIX_SIZE;
            key.extend_from_slice(&self[slot.offset()..slot.offset() + suffix_size]);
        }
        key
    }

    fn common_leaf_value_from_slot(&self, slot: CommonLeafSlot) -> &[u8] {
        let value_offset = slot.offset() + slot.key_suffix_size();
        &self[value_offset..value_offset + slot.value_size()]
    }

    fn common_leaf_write_record(&mut self, offset: usize, key: &[u8], value: &[u8]) {
        let suffix_size = key.len().saturating_sub(COMMON_LEAF_KEY_PREFIX_SIZE);
        if suffix_size > 0 {
            self[offset..offset + suffix_size].copy_from_slice(&key[COMMON_LEAF_KEY_PREFIX_SIZE..]);
        }
        self[offset + suffix_size..offset + suffix_size + value.len()].copy_from_slice(value);
    }

    fn common_leaf_insert_at(
        &mut self,
        slot_id: u32,
        hash: u64,
        key: &[u8],
        value: &[u8],
        is_ghost: bool,
    ) -> Result<(), AccessMethodError> {
        let record_size = key.len().saturating_sub(COMMON_LEAF_KEY_PREFIX_SIZE) + value.len();
        if record_size > AVAILABLE_PAGE_SIZE - COMMON_LEAF_PAGE_HEADER_SIZE - COMMON_LEAF_SLOT_SIZE
        {
            return Err(AccessMethodError::RecordTooLarge);
        }

        let bytes_needed = COMMON_LEAF_SLOT_SIZE + record_size;
        if bytes_needed > self.common_leaf_contiguous_free_space() as usize {
            if bytes_needed > self.common_leaf_total_free_space() as usize {
                return Err(AccessMethodError::OutOfSpace);
            }
            self.compact_common_leaf();
        }

        let mut header = self.common_leaf_header();
        let record_offset = header.rec_start_offset as usize - record_size;
        self.common_leaf_write_record(record_offset, key, value);

        let old_slot_count = header.slot_count;
        if slot_id > old_slot_count {
            panic!("invalid common leaf insertion slot");
        }

        let start = self.common_leaf_slot_offset(slot_id);
        let end = self.common_leaf_slot_offset(old_slot_count);
        if start < end {
            self.copy_within(start..end, start + COMMON_LEAF_SLOT_SIZE);
        }

        header.slot_count += 1;
        header.rec_start_offset = record_offset as u32;
        header.total_bytes_used += bytes_needed as u32;
        self.set_common_leaf_header(header);

        let slot = CommonLeafSlot::new(hash, key, value, record_offset as u32, is_ghost);
        self.set_common_leaf_slot(slot_id, slot);
        Ok(())
    }

    fn common_leaf_update_at(
        &mut self,
        slot_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), AccessMethodError> {
        let old_slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");
        let old_record_size = old_slot.record_size();
        let new_record_size = key.len().saturating_sub(COMMON_LEAF_KEY_PREFIX_SIZE) + value.len();

        if new_record_size
            > AVAILABLE_PAGE_SIZE - COMMON_LEAF_PAGE_HEADER_SIZE - COMMON_LEAF_SLOT_SIZE
        {
            return Err(AccessMethodError::RecordTooLarge);
        }

        if new_record_size <= old_record_size {
            let new_offset = old_slot.offset() + old_record_size - new_record_size;
            self.common_leaf_write_record(new_offset, key, value);
            let mut new_slot = old_slot;
            new_slot.value_size = value.len() as u32;
            new_slot.offset = new_offset as u32;
            self.set_common_leaf_slot(slot_id, new_slot);

            let mut header = self.common_leaf_header();
            if old_slot.offset() as u32 == header.rec_start_offset {
                header.rec_start_offset = new_offset as u32;
            }
            header.total_bytes_used -= (old_record_size - new_record_size) as u32;
            self.set_common_leaf_header(header);
            return Ok(());
        }

        let extra_bytes_needed = new_record_size - old_record_size;
        if extra_bytes_needed > self.common_leaf_total_free_space() as usize {
            return Err(AccessMethodError::OutOfSpaceForUpdate(
                self.common_leaf_value_from_slot(old_slot).to_vec(),
            ));
        }
        if new_record_size > self.common_leaf_contiguous_free_space() as usize {
            self.compact_common_leaf();
        }

        let mut header = self.common_leaf_header();
        let new_offset = header.rec_start_offset as usize - new_record_size;
        self.common_leaf_write_record(new_offset, key, value);

        let mut new_slot = self
            .common_leaf_slot(slot_id)
            .expect("invalid common leaf slot id");
        new_slot.value_size = value.len() as u32;
        new_slot.offset = new_offset as u32;
        self.set_common_leaf_slot(slot_id, new_slot);

        header.rec_start_offset = new_offset as u32;
        header.total_bytes_used += extra_bytes_needed as u32;
        self.set_common_leaf_header(header);
        Ok(())
    }

    fn compact_common_leaf(&mut self) {
        let slot_count = self.common_leaf_slot_count();
        let mut records = Vec::with_capacity(slot_count as usize);
        let mut total_record_bytes = 0usize;

        for slot_id in 0..slot_count {
            let slot = self
                .common_leaf_slot(slot_id)
                .expect("invalid common leaf slot id");
            let record_size = slot.record_size();
            let record = self[slot.offset()..slot.offset() + record_size].to_vec();
            total_record_bytes += record_size;
            records.push((slot_id, slot, record));
        }

        let mut current_offset = AVAILABLE_PAGE_SIZE;
        for (slot_id, mut slot, record) in records {
            current_offset -= record.len();
            self[current_offset..current_offset + record.len()].copy_from_slice(&record);
            slot.offset = current_offset as u32;
            self.set_common_leaf_slot(slot_id, slot);
        }

        let mut header = self.common_leaf_header();
        header.rec_start_offset = (AVAILABLE_PAGE_SIZE - total_record_bytes) as u32;
        self.set_common_leaf_header(header);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_page() -> Page {
        let mut page = Page::new(0);
        page.init_common_leaf();
        page
    }

    #[test]
    fn orders_slots_by_hash_then_key() {
        let mut page = new_page();

        page.common_leaf_insert(7, b"b", b"v-b").unwrap();
        page.common_leaf_insert(7, b"a", b"v-a").unwrap();
        page.common_leaf_insert(3, b"z", b"v-z").unwrap();

        assert_eq!(page.common_leaf_slot_count(), 3);
        assert_eq!(page.common_leaf_key_at(0), b"z");
        assert_eq!(page.common_leaf_key_at(1), b"a");
        assert_eq!(page.common_leaf_key_at(2), b"b");
        assert_eq!(page.common_leaf_get(3, b"z").unwrap(), b"v-z");
        assert_eq!(page.common_leaf_get(7, b"a").unwrap(), b"v-a");
        assert_eq!(page.common_leaf_get(7, b"b").unwrap(), b"v-b");
    }

    #[test]
    fn supports_key_prefix_and_suffix_layout() {
        let mut page = new_page();
        let key = b"abcdefgh-suffix";
        let value = b"value";

        page.common_leaf_insert(11, key, value).unwrap();

        let slot = page.common_leaf_slot(0).unwrap();
        assert_eq!(slot.key_prefix, *b"abcdefgh");
        assert_eq!(page.common_leaf_key_at(0), key);
        assert_eq!(page.common_leaf_value_at(0), value);
        assert_eq!(page.common_leaf_get(11, key).unwrap(), value);
    }

    #[test]
    fn rejects_duplicate_hash_key_pair() {
        let mut page = new_page();

        page.common_leaf_insert(11, b"k", b"v1").unwrap();
        let err = page.common_leaf_insert(11, b"k", b"v2").unwrap_err();

        assert_eq!(err, AccessMethodError::KeyDuplicate);
        assert_eq!(page.common_leaf_get(11, b"k").unwrap(), b"v1");
    }

    #[test]
    fn upsert_replaces_value_without_changing_sort_order() {
        let mut page = new_page();

        page.common_leaf_insert(10, b"a", b"short").unwrap();
        page.common_leaf_insert(10, b"c", b"value-c").unwrap();
        page.common_leaf_upsert(10, b"a", b"larger-value").unwrap();

        assert_eq!(page.common_leaf_key_at(0), b"a");
        assert_eq!(page.common_leaf_key_at(1), b"c");
        assert_eq!(page.common_leaf_get(10, b"a").unwrap(), b"larger-value");
    }

    #[test]
    fn tracks_ghost_flag_in_slot() {
        let mut page = new_page();

        page.common_leaf_insert(1, b"k", b"v").unwrap();
        assert!(!page.common_leaf_slot(0).unwrap().is_ghost());

        page.common_leaf_set_ghost(0, true);
        assert!(page.common_leaf_slot(0).unwrap().is_ghost());

        page.common_leaf_set_ghost(0, false);
        assert!(!page.common_leaf_slot(0).unwrap().is_ghost());
    }
}
