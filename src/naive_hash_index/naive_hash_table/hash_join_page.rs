mod header {
    use crate::define_header_with_common;
    define_header_with_common!(Header {});

    use crate::{
        page::{PageId, AVAILABLE_PAGE_SIZE},
        prelude::Timestamp,
    };
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Header>();

    impl Header {
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

        pub fn next_page_id(&self) -> Option<PageId> {
            if self.next_page_id == PageId::MAX {
                None
            } else {
                Some(self.next_page_id)
            }
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
    }
}
use std::collections::HashMap;

use header::*;

pub mod slot {
    use std::fmt::Debug;

    use crate::{mvcc_index::TxId, prelude::Timestamp};

    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    use crate::define_slot_with_common;

    define_slot_with_common!(Slot {});

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

        pub fn new(key: &[u8], pkey: &[u8], tx_id: TxId, val: &[u8], offset: usize) -> Self {
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

use crate::{
    mvcc_index::{
        hash_common::RowDelta,
        hash_join_page::record::{Record, RecordRef},
        MvccEntry,
    },
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
    prelude::{AccessMethodError, Timestamp},
};

pub trait NaiveHashPage {
    fn init(&mut self);

    fn insert_heap_no_repair(
        &mut self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError>;
    fn insert_recent_history(
        &mut self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError>;

    fn insert_entry_at_slot_id(
        &mut self,
        slot_id: usize,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError>;

    fn heap_get(&self, pkey: &[u8]) -> Result<MvccEntry, AccessMethodError>;

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8];
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    // write vector of bytes into offset, skip using vector to collect bytes
    fn write_bytes_slice(&mut self, offset: usize, bytes: &[&[u8]]);

    fn free_space_before_compaction(&self) -> usize {
        self.unsafe_header().rec_start_offset() - self.slot_offset(self.slot_count())
    }
    fn free_space_after_compaction(&self) -> usize {
        AVAILABLE_PAGE_SIZE - self.unsafe_header().total_bytes_used()
    }
    fn require_space(entry: &MvccEntry) -> usize {
        SLOT_SIZE + RecordRef::new(entry.key(), entry.pkey(), entry.value()).size()
    }
    fn require_space_rec(rec: &RecordRef) -> usize {
        SLOT_SIZE + rec.size()
    }
    fn unsafe_header(&self) -> &Header;
    fn unsafe_header_mut(&self) -> &mut Header;
    fn set_header(&mut self, header: &Header);
    fn next_page(&self) -> Option<(PageId, u32)> {
        self.unsafe_header().next_page()
    }
    fn set_next_page(&mut self, next_page_id: PageId, frame_id: u32) {
        let header = self.unsafe_header_mut();
        header.set_next_page(next_page_id, frame_id);
    }
    fn set_next_page_frame(&mut self, page_frame: (PageId, u32)) {
        let header = self.unsafe_header_mut();
        header.set_next_page(page_frame.0, page_frame.1);
    }
    fn rec_start_offset(&self) -> usize {
        self.unsafe_header().rec_start_offset()
    }
    fn set_rec_start_offset(&mut self, rec_start_offset: usize) {
        let header = self.unsafe_header_mut();
        header.set_rec_start_offset(rec_start_offset);
    }
    fn slot_count(&self) -> usize {
        self.unsafe_header().slot_count()
    }
    fn set_slot_count(&mut self, slot_count: usize) {
        let header = self.unsafe_header_mut();
        header.set_slot_count(slot_count);
    }
    fn slot_end_offset(&self) -> usize {
        PAGE_HEADER_SIZE + self.slot_count() * SLOT_SIZE
    }
    fn total_bytes_used(&self) -> usize {
        self.unsafe_header().total_bytes_used()
    }
    fn set_total_bytes_used(&mut self, total_bytes_used: usize) {
        let header = self.unsafe_header_mut();
        header.set_total_bytes_used(total_bytes_used);
    }
    fn increase_total_bytes_used(&mut self, bytes: usize) {
        self.set_total_bytes_used(self.total_bytes_used() + bytes);
    }
    fn decrease_total_bytes_used(&mut self, bytes: usize) {
        self.set_total_bytes_used(self.total_bytes_used() - bytes);
    }
    fn increase_slot_count(&mut self) {
        let header = self.unsafe_header_mut();
        header.inc_slot_count();
    }
    fn decrease_slot_count(&mut self) {
        let header = self.unsafe_header_mut();
        header.dec_slot_count();
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

    fn record_ref_from_slotid(&self, slot_id: usize) -> RecordRef {
        let slot = self.unsafe_slot(slot_id);
        self.record_ref_from_slot(&slot)
    }

    fn record_ref_from_slot(&self, slot: &Slot) -> RecordRef {
        RecordRef::from_bytes(
            self.read_bytes(slot.offset(), slot.rec_size()),
            slot.key_size(),
            slot.pkey_size(),
            slot.val_size(),
        )
    }
    fn set_slot(&mut self, slot_id: usize, slot: Slot) {
        // self.write_bytes(self.slot_offset(slot_id), &slot.to_bytes());
        let slot_mut_ref = unsafe {
            Slot::unsafe_mut_from_bytes(&self.read_bytes(self.slot_offset(slot_id), SLOT_SIZE))
        };
        *slot_mut_ref = slot;
    }

    fn set_record_ref_at_offset(&mut self, offset: usize, rec: &RecordRef) {
        // self.write_bytes(offset, &rec.to_bytes());
        let bytes_slice = [rec.key(), rec.pkey(), rec.val()];
        self.write_bytes_slice(offset, &bytes_slice);
    }

    fn insert_slot_at_id(&mut self, slot: Slot, slot_id: usize);
    fn delete_slot_at_id(&mut self, slot_id: usize);

    fn insert_rec_ref_at_offset(&mut self, rec: &RecordRef, offset: usize);
    /// Returns `Some(Record)` if the slot's pkey exactly matches `pkey`.
    /// Otherwise returns `None`.
    fn slot_pkey_matches(&self, slot: &Slot, pkey: &[u8]) -> Option<RecordRef>;
    fn slot_pkey_matches_new(&self, slot: &Slot, pkey: &[u8]) -> bool;
    fn get_pkey_from_slot(&self, slot: &Slot) -> &[u8];

    /// Compare the slot’s pkey at slot_id with `search_key`.
    /// Returns Ordering::Less if slot’s pkey < search_key,
    ///         Ordering::Equal if slot’s pkey == search_key,
    ///         Ordering::Greater if slot’s pkey > search_key.
    fn slot_cmp_key(&self, slot_id: usize, search_key: &[u8]) -> std::cmp::Ordering;

    fn chain_scan_into_vec(&self, results: &mut Vec<MvccEntry>);
    fn scan_delta_as_from(&self, delta_map: &mut HashMap<Vec<u8>, RowDelta>);
    fn scan_delta_as_to(&self, delta_map: &mut HashMap<Vec<u8>, RowDelta>);
}

impl NaiveHashPage for Page {
    fn init(&mut self) {
        let header = Header::new();
        NaiveHashPage::set_header(&mut *self, &header);
    }

    fn insert_recent_history(
        &mut self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let needed_space = SLOT_SIZE + rec.size();
        if needed_space > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if needed_space > self.free_space_before_compaction() {
            if needed_space > self.free_space_after_compaction() {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }

        let slot_id = self.slot_count();
        NaiveHashPage::insert_entry_at_slot_id(&mut *self, slot_id, rec, start_ts, end_ts)
    }
    fn insert_entry_at_slot_id(
        &mut self,
        slot_id: usize,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        // let rec = RecordRef::new(entry.key(), entry.pkey(), entry.value());

        let new_rec_start_offset =
            NaiveHashPage::unsafe_header(&*self).rec_start_offset() - rec.size();
        let slot = Slot::new(
            rec.key(),
            rec.pkey(),
            0, // tx_id not used now
            rec.val(),
            new_rec_start_offset,
        );

        NaiveHashPage::insert_slot_at_id(&mut *self, slot, slot_id);
        NaiveHashPage::insert_rec_ref_at_offset(&mut *self, &rec, new_rec_start_offset);

        Ok(())
    }

    fn insert_heap_no_repair(
        &mut self,
        rec: &RecordRef,
        start_ts: Timestamp,
        end_ts: Timestamp,
    ) -> Result<(), AccessMethodError> {
        let needed_space = SLOT_SIZE + rec.size();
        if needed_space > AVAILABLE_PAGE_SIZE - PAGE_HEADER_SIZE {
            return Err(AccessMethodError::RecordTooLarge);
        } else if needed_space > NaiveHashPage::free_space_before_compaction(&*self) {
            if needed_space > NaiveHashPage::free_space_after_compaction(&*self) {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: (JUN) Need to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }

        self.insert_entry_at_slot_id(self.slot_count(), &rec, start_ts, end_ts)?;
        Ok(())
    }

    fn heap_get(&self, pkey: &[u8]) -> Result<MvccEntry, AccessMethodError> {
        for i in 0..self.slot_count() {
            let slot = self.unsafe_slot(i);

            // Attempt a cheap pkey check first; if no match, skip it.
            if let Some(rec) = self.slot_pkey_matches(slot, pkey) {
                return Ok(MvccEntry::new(
                    rec.key().to_vec(),
                    rec.pkey().to_vec(),
                    rec.val().to_vec(),
                    0,
                    0,
                ));
            }
        }

        Err(AccessMethodError::KeyNotFound)
    }

    fn read_bytes(&self, offset: usize, len: usize) -> &[u8] {
        &self[offset..offset + len]
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn unsafe_header(&self) -> &Header {
        unsafe { &*((&self.read_bytes(0, PAGE_HEADER_SIZE)).as_ptr() as *const Header) }
    }

    fn unsafe_header_mut(&self) -> &mut Header {
        unsafe { &mut *((&self.read_bytes(0, PAGE_HEADER_SIZE)).as_ptr() as *mut Header) }
    }

    fn set_header(&mut self, header: &Header) {
        let myheader = self.unsafe_header_mut();
        *myheader = *header;
    }

    fn insert_slot_at_id(&mut self, slot: Slot, slot_id: usize) {
        if slot_id < self.slot_count() {
            let start_offset = NaiveHashPage::slot_offset(&*self, slot_id);
            let end_offset = NaiveHashPage::slot_offset(&*self, self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset + SLOT_SIZE);
        }

        NaiveHashPage::set_slot(&mut *self, slot_id, slot);

        self.increase_slot_count();
        self.increase_total_bytes_used(SLOT_SIZE);
    }

    fn delete_slot_at_id(&mut self, slot_id: usize) {
        if slot_id < self.slot_count() {
            let start_offset = NaiveHashPage::slot_offset(&*self, slot_id + 1);
            let end_offset = NaiveHashPage::slot_offset(&*self, self.slot_count());
            self.copy_within(start_offset..end_offset, start_offset - SLOT_SIZE);
        }

        self.decrease_slot_count();
        self.decrease_total_bytes_used(SLOT_SIZE);
    }

    fn insert_rec_ref_at_offset(&mut self, rec: &RecordRef, offset: usize) {
        let rec_bytes_slice = [rec.key(), rec.pkey(), rec.val()];
        NaiveHashPage::write_bytes_slice(&mut *self, offset, &rec_bytes_slice);
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

    fn slot_pkey_matches_new(&self, slot: &Slot, pkey: &[u8]) -> bool {
        // 1) Check pkey length first
        if pkey.len() != slot.pkey_size() {
            return false;
        }

        // 2) Compare prefix
        let prefix_len = std::cmp::min(SLOT_PKEY_PREFIX_SIZE, pkey.len());
        let slot_prefix = &slot.pkey_prefix()[..prefix_len];
        let input_prefix = &pkey[..prefix_len];
        if slot_prefix != input_prefix {
            return false;
        }

        // If the entire pkey fits within the prefix, we've already confirmed equality:
        if pkey.len() <= SLOT_PKEY_PREFIX_SIZE {
            return true;
        }

        // 3) pkey is longer than the prefix => compare the remainder.
        let rec = self.record_ref_from_slot(slot);
        if rec.pkey() == pkey {
            true
        } else {
            false
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

    fn chain_scan_into_vec(&self, results: &mut Vec<MvccEntry>) {
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // 2) Read the entire record to confirm pkey equality.
            let rec = self.record_ref_from_slot(&slot);
            // 4) Finally, build an MvccEntry
            let entry = MvccEntry::new(
                rec.key().to_vec(),
                rec.pkey().to_vec(),
                rec.val().to_vec(),
                0,
                0,
            );
            results.push(entry);
        }
    }

    fn write_bytes_slice(&mut self, mut offset: usize, bytes_vec: &[&[u8]]) {
        for bytes in bytes_vec {
            self[offset..offset + bytes.len()].copy_from_slice(bytes);
            offset += bytes.len();
        }
    }

    fn scan_delta_as_from(&self, delta_map: &mut HashMap<Vec<u8>, RowDelta>) {
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // from in the slot
            let rec = self.record_ref_from_slot(&slot);

            let delta_entry = delta_map.get_mut(rec.pkey());
            if let Some(entry_v) = delta_entry {
                entry_v.from().set(0, rec.key(), rec.val());
            } else {
                let mut row_delta = RowDelta::new();
                row_delta.from().set(0, rec.key(), rec.val());
                delta_map.insert(rec.pkey().to_vec(), row_delta);
            }
        }
    }

    fn scan_delta_as_to(&self, delta_map: &mut HashMap<Vec<u8>, RowDelta>) {
        let slot_count = self.slot_count();

        for i in 0..slot_count {
            let slot = self.unsafe_slot(i);

            // to in the slot
            let rec = self.record_ref_from_slot(&slot);

            let delta_entry = delta_map.get_mut(rec.pkey());
            if let Some(entry_v) = delta_entry {
                entry_v.to().set(0, rec.key(), rec.val());
            } else {
                let mut row_delta = RowDelta::new();
                row_delta.to().set(0, rec.key(), rec.val());
                delta_map.insert(rec.pkey().to_vec(), row_delta);
            }
        }
    }
}
