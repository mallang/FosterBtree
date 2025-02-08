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
            (self.slot_count * SLOT_SIZE as u32 + PAGE_HEADER_SIZE_ALIGNED as u32) as usize
        }
    }
}

use core::slice;
use header::*;

mod slot {
    use crate::log_warn;

    pub const DELETE_MARKER_IN_VAL_SIZE: u32 = u32::MAX;
    pub const SLOT_SIZE: usize = std::mem::size_of::<Slot>();
    pub const SLOT_KEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();
    pub const SLOT_PKEY_PREFIX_SIZE: usize = std::mem::size_of::<[u8; 8]>();

    #[derive(Debug, PartialEq, Default)]
    pub struct SlotMeta {
        committed_records_offset: u32,
        remain_key: Vec<u8>,
        remain_pkey: Vec<u8>,
    }

    impl SlotMeta {
        pub fn space_need_from_slot(slot: &Slot) -> u32 {
            let key_size = slot.key_size();
            let pkey_size = slot.pkey_size();

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);
            remain_key_size + remain_pkey_size + std::mem::size_of::<u32>() as u32
        }

        pub fn space_need_from_kpk(key: &[u8], pkey: &[u8]) -> u32 {
            let key_size = key.len();
            let pkey_size = pkey.len();

            let remain_key_size = key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE);
            let remain_pkey_size = pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE);
            (remain_key_size + remain_pkey_size + std::mem::size_of::<u32>()) as u32
        }

        pub fn check_match_remain_key_pkey(
            slot: &Slot,
            meta_bytes: &[u8],
            key: &[u8],
            pkey: &[u8],
        ) -> bool {
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
                && &meta_bytes[4..4 + slot_remain_key_size as usize] == remain_key
                && &meta_bytes[4 + slot_remain_key_size as usize
                    ..4 + slot_remain_key_size as usize + slot_remain_pkey_size as usize]
                    == remain_pkey
        }

        pub fn check_match_remain_key(slot: &Slot, meta_bytes: &[u8], key: &[u8]) -> bool {
            let slot_key_size = slot.key_size();

            let slot_remain_key_size = slot_key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);

            let key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());

            let remain_key = &key[key_prefix_len..];

            remain_key.len() as u32 == slot_remain_key_size
                && &meta_bytes[4..4 + slot_remain_key_size as usize] == remain_key
        }

        pub fn get_remain_key<'a>(slot: &Slot, meta_bytes: &'a [u8]) -> &'a [u8] {
            let slot_key_size = slot.key_size();

            let slot_remain_key_size = slot_key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);

            &meta_bytes[4..4 + slot_remain_key_size as usize]
        }

        pub fn get_remain_pkey<'a>(slot: &Slot, meta_bytes: &'a [u8]) -> &'a [u8] {
            let slot_key_size = slot.key_size();
            let slot_pkey_size = slot.pkey_size();

            let slot_remain_key_size = slot_key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32);
            let slot_remain_pkey_size = slot_pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32);

            &meta_bytes[4 + slot_remain_key_size as usize
                ..(4 + slot_remain_key_size + slot_remain_pkey_size) as usize]
        }

        pub fn get_committed_offset_from_bytes(bytes: &[u8]) -> u32 {
            let offset = u32::from_be_bytes(
                bytes[..4]
                    .try_into()
                    .map_err(|_| "failed to parse")
                    .unwrap(),
            );
            offset
        }
        pub fn to_bytes(
            committed_records_offset: u32,
            remain_key: &[u8],
            remain_pkey: &[u8],
        ) -> Vec<u8> {
            let mut latest_bytes = Vec::<u8>::new();
            latest_bytes.extend(u32::to_be_bytes(committed_records_offset));
            latest_bytes.extend_from_slice(remain_key);
            latest_bytes.extend_from_slice(remain_pkey);
            latest_bytes
        }

        pub fn print_meta_of_slot(slot: &Slot, bytes: &[u8]) {
            let space_need = Self::space_need_from_slot(slot);
            log_warn!("[SlotMeta] space need: {space_need}, first_record_offset: {}, remain_key: {:?} remain_pkey: {:?}", SlotMeta::get_committed_offset_from_bytes(bytes), Self::get_remain_key(slot, bytes), Self::get_remain_pkey(slot, bytes));
        }
    }

    #[derive(Debug, PartialEq, Default, Copy, Clone)]
    pub struct Slot {
        // hash key for join
        key_size: u32,
        key_prefix: [u8; SLOT_KEY_PREFIX_SIZE],
        // primary key for row
        pkey_size: u32,
        pkey_prefix: [u8; SLOT_PKEY_PREFIX_SIZE],
        meta_offset: u32,
    }

    impl Slot {
        // Slot + SlotMeta
        pub fn space_need(key: &[u8], pkey: &[u8]) -> u32 {
            std::mem::size_of::<Slot>() as u32 + SlotMeta::space_need_from_kpk(key, pkey)
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

            bytes[current_pos..current_pos + 4].copy_from_slice(&self.meta_offset.to_be_bytes());

            bytes
        }

        pub fn new(key: &[u8], pkey: &[u8], meta_offset: u32) -> Self {
            let key_size = key.len() as u32;
            let pkey_size = pkey.len() as u32;

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
                meta_offset,
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

        pub fn meta_offset(&self) -> u32 {
            self.meta_offset
        }

        pub fn set_meta_offset(&mut self, offset: u32) {
            self.meta_offset = offset;
        }

        pub fn remain_key_size(&self) -> u32 {
            let key_size = self.key_size();
            key_size.saturating_sub(SLOT_KEY_PREFIX_SIZE as u32)
        }

        pub fn remain_pkey_size(&self) -> u32 {
            let pkey_size = self.pkey_size();
            pkey_size.saturating_sub(SLOT_PKEY_PREFIX_SIZE as u32)
        }

        pub fn meta_offset_offset_in_slot() -> u32 {
            24
        }
    }
}

use slot::*;

mod record {
    use crate::mvcc_index::Timestamp;

    use super::DELETE_MARKER_IN_VAL_SIZE;

    #[derive(Debug, Clone)]
    pub struct CommittedRecord<'a> {
        pub next_offset: u32,
        pub start_ts: u64,
        pub val_size: u32,
        pub value: &'a [u8],
    }

    impl<'a> CommittedRecord<'a> {
        pub fn is_deleted(&self) -> bool {
            self.val_size == DELETE_MARKER_IN_VAL_SIZE
        }
        pub fn get_next_and_ts(bytes: &[u8]) -> (u32, u64) {
            let next_off = Self::next_offset_from_bytes(bytes);

            let tx_id = Self::ts_from_bytes(bytes);

            (next_off, tx_id)
        }

        pub fn to_bytes(next_offset: u32, ts: Timestamp, value: &[u8], is_delete: bool) -> Vec<u8> {
            let mut latest_bytes = Vec::<u8>::new();
            latest_bytes.extend(u32::to_be_bytes(next_offset));
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
            if is_deleted {
                8 + 4 + 4
            } else {
                8 + 4 + 4 + value.len() as u32
            }
        }

        // pub fn space_need(&self) -> u32 {
        //     if self.is_deleted() {
        //         8 + 4 + 4
        //     } else {
        //         8 + 4 + 4 + self.value.len() as u32
        //     }
        // }

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

        pub fn next_offset_from_bytes(bytes: &[u8]) -> u32 {
            u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| "parse failed").unwrap())
        }

        pub fn ts_from_bytes(bytes: &[u8]) -> u64 {
            u64::from_be_bytes(bytes[4..12].try_into().map_err(|_| "parse failed").unwrap())
        }
    }
}
use record::*;

use crate::{
    log_warn,
    mvcc_index::{
        hashtable_mu::hash_join_table_common::HashTableAccessMethodError, Delta, DeltaEntry,
        MvccEntry, Timestamp,
    },
    page::{Page, AVAILABLE_PAGE_SIZE},
};

pub trait TableDataPageBase {
    fn find_latest_record_before_ts(
        &self,
        search_upper_ts: Timestamp,
        start_rec_offset: u32,
        prev_next_offset: u32,
        prev_ts: Timestamp,
    ) -> LatestRecordReturnInfo {
        let mut latest_record_offset = start_rec_offset;
        let mut prev_next_offset = prev_next_offset;
        let mut prev_ts = prev_ts;

        while latest_record_offset != 0 {
            let bytes = self.read_bytes(
                latest_record_offset as usize,
                std::mem::size_of::<u32>()
                    + std::mem::size_of::<u64>()
                    + std::mem::size_of::<u32>(),
            );
            let (next_offset, record_ts) = CommittedRecord::get_next_and_ts(bytes);
            let val_size = CommittedRecord::val_size_from_bytes(bytes);
            if record_ts <= search_upper_ts {
                return LatestRecordReturnInfo {
                    record_offset: Some(latest_record_offset),
                    prev_next_offset,
                    val_size,
                    end_ts: prev_ts,
                };
            }

            prev_next_offset = latest_record_offset + 0;
            latest_record_offset = next_offset;
            prev_ts = CommittedRecord::ts_from_bytes(bytes);
        }

        return LatestRecordReturnInfo {
            record_offset: None, // 0
            prev_next_offset,
            val_size: None,
            end_ts: prev_ts,
        }; // no record before ts
    }

    fn rehash_collect_used_space(
        &self,
        left_ids: LeftSlotIDs,
    ) -> (SpaceVersionVec, MapSlotId2VersionsSpace) {
        let mut version_space: SpaceVersionVec = SpaceVersionVec::new();
        let mut page_slots_adrs = MapSlotId2VersionsSpace::new();

        let mut push_new_space_fn =
            |space: (u32, u32), slot_id: u32, slot_adrs: &mut SlotAddresses| {
                slot_adrs.push(space);
                version_space.push((space, (slot_id, slot_adrs.len() as u32 - 1)));
            };

        let get_first_record_offset_fn = |slot: &Slot| {
            let meta_offset = slot.meta_offset();
            u32::from_be_bytes(
                self.read_bytes(meta_offset as usize, 4)
                    .try_into()
                    .map_err(|_| "failed parse")
                    .unwrap(),
            )
        };

        for slot_id in left_ids {
            let mut slot_adrs: SlotAddresses = vec![];
            let slot = self.get_slot_ref(slot_id as usize);

            // add meta space to free space
            let meta_offset = slot.meta_offset();
            let meta_size = SlotMeta::space_need_from_slot(slot);
            push_new_space_fn((meta_offset, meta_size), slot_id, &mut slot_adrs);

            // insert versions to new page and collect free_space info
            let mut offset = get_first_record_offset_fn(slot);
            let mut record = self.next_version(offset);
            while let Some(commit_record) = record {
                push_new_space_fn(
                    (
                        offset,
                        CommittedRecord::space_need_from_value(
                            &commit_record.value,
                            commit_record.is_deleted(),
                        ),
                    ),
                    slot_id,
                    &mut slot_adrs,
                );

                record = self.next_version(commit_record.next_offset);
                offset = commit_record.next_offset;
            }
            page_slots_adrs.insert(slot_id, slot_adrs);
        }

        (version_space, page_slots_adrs)
    }

    fn rehash_truncate_separate_space(
        &mut self,
        mut free_spaces: FreeSpaceVec,
        mut space_version: SpaceVersionVec,
        mut page_slotids_2_adrs: MapSlotId2VersionsSpace,
    ) -> u32 {
        let copy = (
            free_spaces.clone(),
            space_version.clone(),
            page_slotids_2_adrs.clone(),
        );
        assert!(!free_spaces.is_empty());
        let mut new_rec_start_offset = AVAILABLE_PAGE_SIZE as u32;
        while let Some(mut largest_free_space) = free_spaces.pop() {
            while let Some(cur_top) = free_spaces.peek() {
                if cur_top.0 + cur_top.1 == largest_free_space.0 {
                    largest_free_space = (cur_top.0, cur_top.1 + largest_free_space.1);
                    free_spaces.pop();
                } else {
                    break;
                }
            }
            // get largest continuous free space

            while let Some(largest_used_space) = space_version.pop() {
                new_rec_start_offset = largest_used_space.0 .0;
                if largest_used_space.0 .0 > largest_free_space.0 {
                    continue;
                } else {
                    assert!(
                        largest_used_space.0 .0 + largest_used_space.0 .1 == largest_free_space.0,
                        "{:?}, {:?}, free: {:?}, version: {:?}, slot_2_adr: {:?}",
                        largest_used_space.0,
                        largest_free_space,
                        &copy.0,
                        &copy.1,
                        &copy.2
                    );
                    let new_space = (
                        largest_free_space.0 + largest_free_space.1 - largest_used_space.0 .1,
                        largest_used_space.0 .1,
                    );
                    let (slot_id, version_idx) = largest_used_space.1;
                    let versions = page_slotids_2_adrs.get_mut(&slot_id).unwrap();
                    versions[version_idx as usize] = new_space;
                    free_spaces.push((largest_used_space.0 .0, largest_free_space.1));

                    let old_space = largest_used_space.0;
                    self.write_bytes_overlapping(
                        old_space.0 as usize..(old_space.0 + old_space.1) as usize,
                        new_space.0 as usize,
                    );

                    if version_idx == 0 {
                        self.get_slot_slice_mutable(slot_id)[0].set_meta_offset(new_space.0);
                    } else {
                        let update_value_offset = versions[version_idx as usize - 1].0;
                        self.write_bytes(
                            update_value_offset as usize,
                            &u32::to_be_bytes(new_space.0),
                        );
                    };

                    new_rec_start_offset = new_space.0;
                    break;
                }
            }
        }

        // truncate slots
        let slot_count = self.slot_count() as usize;
        let slot_slice_mut = self.get_slot_slice_mutable(0);
        let new_slot_count = {
            let mut i = 0;
            for j in 0..slot_count {
                if page_slotids_2_adrs.get(&(j as u32)).is_some() {
                    // exist slot
                    slot_slice_mut.copy_within(j..j + 1, i);
                    i += 1;
                }
            }
            i as u32
        };
        let mut header = self.header();
        header.set_slot_count(new_slot_count);
        header.set_rec_start_offset(new_rec_start_offset);
        header.set_total_bytes_used(
            page_slotids_2_adrs
                .iter()
                .map(|entry| entry.1.iter().map(|space| space.1).sum::<u32>())
                .sum::<u32>()
                + header.slot_end_offset() as u32,
        );
        self.set_header(&header);
        return header.rec_start_offset() - header.slot_end_offset() as u32;
    }

    fn gc_get_free_spaces(&mut self, safe_ts: Timestamp) -> (FreeSpaceVec, LeftSlotIDs) {
        let slot_sli = self.get_slot_slice(0);
        let mut free_spaces: FreeSpaceVec = FreeSpaceVec::new();
        free_spaces.reserve(self.slot_count() as usize);
        let mut left_ids: LeftSlotIDs = vec![];

        let get_first_record_offset_fn = |slot: &Slot| {
            let meta_offset = slot.meta_offset();
            u32::from_be_bytes(
                self.read_bytes(meta_offset as usize, 4)
                    .try_into()
                    .map_err(|_| "failed parse")
                    .unwrap(),
            )
        };

        let is_slot_removable_fn = |first_record: &Option<CommittedRecord>| {
            if let Some(first_record) = first_record.as_ref() {
                first_record.start_ts < safe_ts && first_record.is_deleted()
            } else {
                false
            }
        };

        let mut write_4b_zero_offset: Vec<u32> = vec![]; // fk rust mutable borrow check

        for (idx, slot) in slot_sli.iter().enumerate() {
            let mut offset = get_first_record_offset_fn(slot);
            let mut record = self.next_version(offset);

            if is_slot_removable_fn(&record) {
                // slot can be removed
                let meta_offset = slot.meta_offset();
                let meta_size = SlotMeta::space_need_from_slot(slot);
                free_spaces.push((meta_offset, meta_size));

                while let Some(commit_record) = record {
                    free_spaces.push((
                        offset,
                        CommittedRecord::space_need_from_value(
                            &commit_record.value,
                            commit_record.is_deleted(),
                        ),
                    ));
                    record = self.next_version(commit_record.next_offset);
                    offset = commit_record.next_offset;
                }
            } else {
                left_ids.push(idx as u32);

                let last_safe_record_info = self
                    .find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| {
                        record_ts <= safe_ts
                    });
                let (first_delete_rcd_off, prev_next_offset) = {
                    let last_safe_record_offset = last_safe_record_info.record_offset.unwrap_or(0);
                    let last_safe_rcd = self.next_version(last_safe_record_offset);
                    if let Some(last_safe_rcd) = last_safe_rcd {
                        (last_safe_rcd.next_offset, last_safe_record_offset + 0)
                    } else {
                        continue;
                    }
                };

                // must can be deleted if exist
                let mut record = self.next_version(first_delete_rcd_off);
                if record.is_none() {
                    continue;
                }
                write_4b_zero_offset.push(prev_next_offset);
                let mut offset = first_delete_rcd_off;

                while let Some(commit_record) = record {
                    free_spaces.push((
                        offset,
                        CommittedRecord::space_need_from_value(
                            &commit_record.value,
                            commit_record.is_deleted(),
                        ),
                    ));
                    record = self.next_version(commit_record.next_offset);
                    offset = commit_record.next_offset;
                }
            }
        }

        for off in write_4b_zero_offset {
            self.write_bytes(off as usize, &[0; 4]);
        }

        (free_spaces, left_ids)
    }

    fn get_value_after_ts_by_slot(&self, slot: &Slot, ts: Timestamp) -> Option<Vec<u8>> {
        let latest_record_res =
            self.find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| record_ts <= ts);
        let (latest_record_offset, val_size) =
            (latest_record_res.record_offset, latest_record_res.val_size);
        if val_size.is_some() && latest_record_offset.is_some() {
            Some(
                self.read_bytes(
                    latest_record_offset.unwrap() as usize + 8 + 4 + 4,
                    val_size.unwrap() as usize,
                )
                .to_vec(),
            )
        } else {
            None
        }
    }

    fn check_slot_match_key(&self, slot: &Slot, key: &[u8]) -> bool {
        let find_slot_fn = |slot: &Slot, key: &[u8], page: &Self| -> bool {
            if slot.match_k_prefix(key) {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    page.read_bytes(meta_offset as usize, meta_size as usize)
                };
                SlotMeta::check_match_remain_key(slot, meta_bytes, key)
            } else {
                false
            }
        };

        find_slot_fn(slot, key, self)
    }

    fn get_key_by_slot(&self, slot: &Slot) -> Vec<u8> {
        let key_prefix = slot.key_prefix();
        let key_remain = {
            let meta_bytes = {
                let meta_size = SlotMeta::space_need_from_slot(slot);
                let meta_offset = slot.meta_offset();
                self.read_bytes(meta_offset as usize, meta_size as usize)
            };
            SlotMeta::get_remain_key(slot, meta_bytes)
        };
        let mut ve = vec![];
        ve.extend_from_slice(key_prefix);
        ve.extend_from_slice(key_remain);
        ve
    }

    fn get_pkey_by_slot(&self, slot: &Slot) -> Vec<u8> {
        let pkey_prefix = slot.pkey_prefix();
        let pkey_remain = {
            let meta_bytes = {
                let meta_size = SlotMeta::space_need_from_slot(slot);
                let meta_offset = slot.meta_offset();
                self.read_bytes(meta_offset as usize, meta_size as usize)
            };
            SlotMeta::get_remain_pkey(slot, meta_bytes)
        };
        let mut ve = vec![];
        ve.extend_from_slice(pkey_prefix);
        ve.extend_from_slice(pkey_remain);
        ve
    }

    fn next_version(&self, start_offset: u32) -> Option<CommittedRecord> {
        if start_offset == 0 {
            return None;
        }

        let get_record_fn = |off: u32| -> CommittedRecord {
            let bytes = self.read_bytes(off as usize, 4 + 8 + 4);
            let ts = CommittedRecord::ts_from_bytes(bytes);
            let next_offset = CommittedRecord::next_offset_from_bytes(bytes);
            let val_size = CommittedRecord::val_size_from_bytes(bytes);
            let val = {
                self.read_bytes(off as usize + 8 + 4 + 4, val_size.unwrap_or(0) as usize)
            };

            let val_size = val_size.unwrap_or(DELETE_MARKER_IN_VAL_SIZE);
            CommittedRecord {
                start_ts: ts,
                next_offset,
                val_size,
                value: val,
            }
        };
        Some(get_record_fn(start_offset))
    }

    fn find_latest_record_before_ts_by_slot(
        &self,
        slot: &Slot,
        ts_cmp_fn: impl Fn(Timestamp) -> bool,
    ) -> LatestRecordReturnInfo {
        let meta_offset = slot.meta_offset();
        let meta_size = SlotMeta::space_need_from_slot(&slot);
        let meta_bytes = self.read_bytes(meta_offset as usize, meta_size as usize);

        let mut latest_record_offset = SlotMeta::get_committed_offset_from_bytes(meta_bytes);
        let mut prev_next_offset = meta_offset + 0;
        let mut prev_ts = Timestamp::MAX;

        while latest_record_offset != 0 {
            let bytes = self.read_bytes(
                latest_record_offset as usize,
                std::mem::size_of::<u32>()
                    + std::mem::size_of::<u64>()
                    + std::mem::size_of::<u32>(),
            );
            let (next_offset, record_ts) = CommittedRecord::get_next_and_ts(bytes);
            let val_size = CommittedRecord::val_size_from_bytes(bytes);
            if ts_cmp_fn(record_ts) {
                return LatestRecordReturnInfo {
                    record_offset: Some(latest_record_offset),
                    prev_next_offset,
                    val_size,
                    end_ts: prev_ts,
                };
            }

            prev_next_offset = latest_record_offset + 0;
            latest_record_offset = next_offset;
            prev_ts = CommittedRecord::ts_from_bytes(bytes);
        }

        return LatestRecordReturnInfo {
            record_offset: None, // 0
            prev_next_offset,
            val_size: None,
            end_ts: prev_ts,
        }; // no record before ts
    }
    fn insert_slot_and_meta(&mut self, key: &[u8], pkey: &[u8], new_slot_idx: usize) {
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

        let slot_meta_bytes = SlotMeta::to_bytes(0, remain_key, remain_pkey);

        // insert meta
        let meta_size = slot_meta_bytes.len() as u32;
        let meta_offset = self.rec_start_offset() - meta_size;
        self.write_bytes(meta_offset as usize, &slot_meta_bytes);

        // move slot to provide inserting space
        let slot_sli_mut_append_1 = unsafe {
            let slot_sli_mut = self.get_slot_slice_mutable(0);
            let len = slot_sli_mut.len();
            std::slice::from_raw_parts_mut(slot_sli_mut.as_mut_ptr(), len + 1)
        };

        slot_sli_mut_append_1
            .copy_within(new_slot_idx..self.slot_count() as usize, new_slot_idx + 1);
        // insert slot
        let new_slot = Slot::new(key, pkey, meta_offset);
        self.set_slot(new_slot_idx as u32, &new_slot);

        // log_warn!("[insert], current_slot_offset: {:?}, current_rec_offset: {:?}, slot_id: {:?}", slot_offset, rec_offset, slot_id);
        let mut header = self.header();
        header.increment_slot_count();
        header.increase_total_bytes_used(meta_size + SLOT_SIZE as u32);
        header.set_rec_start_offset(meta_offset);
        self.set_header(&header);
    }
    fn set_header(&mut self, header: &Header) {
        let header_bytes = header.to_bytes();
        self.write_bytes(0, &header_bytes);
    }
    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }
    fn header(&self) -> Header {
        let header_bytes = self.read_bytes(0, PAGE_HEADER_SIZE);
        Header::from_bytes(header_bytes).unwrap()
    }
    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE_ALIGNED as u32 + slot_id * SLOT_SIZE as u32
    }
    fn rec_start_offset(&self) -> u32 {
        self.header().rec_start_offset()
    }
    fn set_rec_start_offset(&mut self, rec_start_offset: u32) {
        let mut header = self.header();
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);
    }
    fn get_slot_ref(&self, slot_id: usize) -> &Slot {
        let sli = self.get_slot_slice(0);
        &sli[slot_id]
    }
    fn free_space_without_compaction(&self) -> u32 {
        let header = self.header();
        header.rec_start_offset() - header.slot_end_offset() as u32
    }
    fn slot_end_offset(&self) -> u32 {
        self.header().slot_end_offset() as u32
    }

    fn write_bytes(&mut self, offset: usize, bytes: &[u8]);
    fn read_bytes(&self, offset: usize, length: usize) -> &[u8];
    fn get_slot_slice(&self, slot_id: u32) -> &[Slot];
    fn get_slot_slice_mutable(&mut self, slot_id: u32) -> &mut [Slot];
    fn set_slot(&mut self, slot_id: u32, slot: &Slot);
    fn get_slot(&self, slot_id: u32) -> Option<Slot>;
    fn write_bytes_overlapping<R: std::ops::RangeBounds<usize>>(&mut self, range: R, dest: usize);
}

impl TableDataPageBase for Page {
    fn write_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    fn write_bytes_overlapping<R: std::ops::RangeBounds<usize>>(&mut self, range: R, dest: usize) {
        self.copy_within(range, dest);
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

    fn get_slot_slice_mutable(&mut self, slot_id: u32) -> &mut [Slot] {
        let slots_start_offset_in_page = self.slot_offset(slot_id) as usize;
        let slots_start_ptr = &mut self[slots_start_offset_in_page] as *mut u8 as *mut Slot;
        // assert!(slots_start_ptr.is_aligned());
        let len = self.slot_count();
        assert!(slot_id <= len, "sid {:?}, len{:?}", slot_id, len);
        unsafe { slice::from_raw_parts_mut(slots_start_ptr, (len - slot_id) as usize) }
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

type Result<T> = core::result::Result<T, HashTableAccessMethodError>;

mod rehash_common {
    use std::collections::BinaryHeap;
    use std::collections::HashMap;

    use crate::mvcc_index::Timestamp;

    /// start, len
    type Space = (u32, u32);
    /// SlotID, VersionID
    type VersionIdx = (u32, u32);
    pub type FreeSpaceVec = BinaryHeap<Space>;
    pub type LeftSlotIDs = Vec<u32>;
    pub type SpaceVersionVec = BinaryHeap<(Space, VersionIdx)>;

    pub type SlotAddresses = Vec<Space>;
    /// slot_id -> versions
    pub type MapSlotId2VersionsSpace = HashMap<u32, SlotAddresses>;

    /// value, is_delete, ts
    type VersionRef<'a> = (&'a [u8], bool, Timestamp);  
    pub type VersionRefVec<'a> = Vec<VersionRef<'a>>;
}
use rehash_common::*;

pub struct LatestRecordReturnInfo {
    pub record_offset: Option<u32>,
    pub prev_next_offset: u32,
    pub val_size: Option<u32>,
    pub end_ts: u64,
}

#[cfg(feature = "unsorted_page")]
pub trait TableDataPageInterface: TableDataPageBase {
    fn find_slot_by_kpk(&self, key: &[u8], pkey: &[u8]) -> (Option<&Slot>, usize) {
        let slot_sli = <Self as TableDataPageBase>::get_slot_slice(&self, 0);

        let find_slot_fn = |slot: &Slot, key: &[u8], pkey: &[u8], page: &Self| -> bool {
            if slot.match_k_pk_prefix(key, pkey) {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    page.read_bytes(meta_offset as usize, meta_size as usize)
                };
                SlotMeta::check_match_remain_key_pkey(slot, meta_bytes, key, pkey)
            } else {
                false
            }
        };

        for (idx, slot) in slot_sli.iter().enumerate() {
            if find_slot_fn(slot, key, pkey, self) {
                return (Some(slot), idx);
            }
        }
        (None, self.slot_count() as usize)
    }

    fn find_slot_idx_to_insert(&self, key: &[u8], pkey: &[u8]) -> (Option<&Slot>, usize) {
        (None, self.slot_count() as usize)
    }

    fn get_slot_range_by_key(&self, _key: Option<&[u8]>) -> std::ops::Range<usize> {
        0..self.slot_count() as usize
    }
}

#[cfg(not(feature = "unsorted_page"))]
pub trait TableDataPageInterface: TableDataPageBase {

    fn find_slot_idx_to_insert(&self, key: &[u8], pkey: &[u8]) -> (Option<&Slot>, usize) {
        let slot_sli = <Self as TableDataPageBase>::get_slot_slice(&self, 0);

        let probe_slot_fn = |slot: &Slot| -> std::cmp::Ordering {
            let probe_key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let probe_pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

            let probe = (
                &key[0..probe_key_prefix_len],
                &key[probe_key_prefix_len..],
                &pkey[0..probe_pkey_prefix_len],
                &pkey[probe_pkey_prefix_len..],
            );
            let slot_cmped = {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    self.read_bytes(meta_offset as usize, meta_size as usize)
                };
                (
                    slot.key_prefix(),
                    SlotMeta::get_remain_key(slot, meta_bytes),
                    slot.pkey_prefix(),
                    SlotMeta::get_remain_pkey(slot, meta_bytes),
                )
            };

            probe.cmp(&slot_cmped)
        };

        let find_res = slot_sli.binary_search_by(probe_slot_fn);
        let smallest_ge_idx = *find_res.as_ref().unwrap_or_else(|x| x);
        (
            find_res.ok().map(|idx| self.get_slot_ref(idx)),
            smallest_ge_idx,
        )
    }

    fn find_slot_by_kpk(&self, key: &[u8], pkey: &[u8]) -> (Option<&Slot>, usize) {
        let slot_sli = <Self as TableDataPageBase>::get_slot_slice(&self, 0);

        let probe_slot_fn = |slot: &Slot| -> std::cmp::Ordering {
            let probe_key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let probe_pkey_prefix_len = SLOT_PKEY_PREFIX_SIZE.min(pkey.len());

            let probe = (
                &key[0..probe_key_prefix_len],
                &key[probe_key_prefix_len..],
                &pkey[0..probe_pkey_prefix_len],
                &pkey[probe_pkey_prefix_len..],
            );
            let slot_cmped = {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    self.read_bytes(meta_offset as usize, meta_size as usize)
                };
                (
                    slot.key_prefix(),
                    SlotMeta::get_remain_key(slot, meta_bytes),
                    slot.pkey_prefix(),
                    SlotMeta::get_remain_pkey(slot, meta_bytes),
                )
            };

            probe.cmp(&slot_cmped)
        };

        let find_res = slot_sli.binary_search_by(probe_slot_fn);
        let smallest_ge_idx = *find_res.as_ref().unwrap_or_else(|x| x);
        (
            find_res.ok().map(|idx| self.get_slot_ref(idx)),
            smallest_ge_idx,
        )
    }

    fn get_slot_range_by_key(&self, key: Option<&[u8]>) -> std::ops::Range<usize> {
        let key = if let Some(_key) = key {
            _key
        } else {
            return 0..self.slot_count() as usize;
        };
        let less_than_fn = |slot: &Slot| -> bool {
            let probe_key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let probe = (&key[0..probe_key_prefix_len], &key[probe_key_prefix_len..]);
            let slot_cmped = {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    self.read_bytes(meta_offset as usize, meta_size as usize)
                };
                (
                    slot.key_prefix(),
                    SlotMeta::get_remain_key(slot, meta_bytes),
                )
            };

            probe < slot_cmped
        };

        let le_fn = |slot: &Slot| -> bool {
            let probe_key_prefix_len = SLOT_KEY_PREFIX_SIZE.min(key.len());
            let probe = (&key[0..probe_key_prefix_len], &key[probe_key_prefix_len..]);
            let slot_cmped = {
                let meta_bytes = {
                    let meta_size = SlotMeta::space_need_from_slot(slot);
                    let meta_offset = slot.meta_offset();
                    self.read_bytes(meta_offset as usize, meta_size as usize)
                };
                (
                    slot.key_prefix(),
                    SlotMeta::get_remain_key(slot, meta_bytes),
                )
            };

            probe <= slot_cmped
        };

        let slot_sli = self.get_slot_slice(0);
        let start = slot_sli.partition_point(less_than_fn);
        let end = slot_sli.partition_point(le_fn);

        start..end
    }
}

impl TableDataPageInterface for Page {}

pub trait TableDataPageTools: TableDataPageInterface {
    fn rehash_relocate_slot(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        versions: VersionRefVec,
    ) -> Result<()> {
        // let (_find_slot_res, new_slot_id_optional) = self.find_slot_idx_to_insert(key, pkey);
        let new_slot_id = self.slot_count() as usize;
        let slot = {
            self.insert_slot_and_meta(key, pkey, new_slot_id);
            self.get_slot_ref(new_slot_id)
        };

        let (mut _latest_record_offset, mut prev_next_offset) = (
            0 as u32,
            slot.meta_offset() + 0,
        );
        let mut rec_start_offset = self.rec_start_offset();
        let mut increase_bytes_delta = 0_u32;
        for version in versions {
            let new_record_bytes = 
                { CommittedRecord::to_bytes(0, version.2, version.0, version.1) };
            let record_size = new_record_bytes.len() as u32;
            let record_offset = rec_start_offset - record_size;
            self.write_bytes(record_offset as usize, &new_record_bytes);
            rec_start_offset = record_offset;
            increase_bytes_delta += record_size;

            let update_bytes: [u8; 4] = u32::to_be_bytes(record_offset);
            self.write_bytes(prev_next_offset as usize, &update_bytes);
            
            prev_next_offset = record_offset + 0;
        }

        let mut header = self.header();
        header.increase_total_bytes_used(increase_bytes_delta);
        header.set_rec_start_offset(rec_start_offset);
        self.set_header(&header);

        Ok(())
    }


    fn add_commit_version_to_exist_slot(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        value: &[u8],
        is_delete: bool,
    ) -> Result<()> {
        let (find_slot_res, new_slot_id_optional) = self.find_slot_by_kpk(key, pkey);

        let slot = if let Some(sl) = find_slot_res {
            // slot exist => check only new record size
            let need_space = CommittedRecord::space_need_from_value(value, is_delete);
            let free_space = self.free_space_without_compaction();
            if need_space > free_space {
                return Err(HashTableAccessMethodError::OutOfSpace);
            }
            sl
        } else {
            let need_space =
                Slot::space_need(key, pkey) + CommittedRecord::space_need_from_value(value, is_delete);
            let free_space = self.free_space_without_compaction();
            if need_space > free_space {
                return Err(HashTableAccessMethodError::OutOfSpace);
            }
            self.insert_slot_and_meta(key, pkey, new_slot_id_optional);
            self.get_slot_ref(new_slot_id_optional)
        };

        let latest_record_res =
            self.find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| record_ts <= ts);
        let (latest_record_offset, prev_next_offset) = (
            latest_record_res.record_offset,
            latest_record_res.prev_next_offset,
        );
        let new_record_bytes =
            { CommittedRecord::to_bytes(latest_record_offset.unwrap_or(0), ts, &value, is_delete) };

        // insert latest record
        let record_size = new_record_bytes.len() as u32;
        let record_offset = self.rec_start_offset() - record_size;
        self.write_bytes(record_offset as usize, &new_record_bytes);

        let mut header = self.header();
        header.increase_total_bytes_used(record_size);
        header.set_rec_start_offset(record_offset);
        self.set_header(&header);

        // update prev next
        let update_bytes: [u8; 4] = u32::to_be_bytes(record_offset);
        self.write_bytes(prev_next_offset as usize, &update_bytes);
        Ok(())
    }

    fn add_commit_version_to_new_slot(
        &mut self,
        key: &[u8],
        pkey: &[u8],
        ts: Timestamp,
        value: &[u8],
        is_delete: bool,
    ) -> Result<()> {
        let (find_slot_res, new_slot_id_optional) = self.find_slot_idx_to_insert(key, pkey);

        let slot = if let Some(sl) = find_slot_res {
            unreachable!()
        } else {
            let need_space =
                Slot::space_need(key, pkey) + CommittedRecord::space_need_from_value(value, is_delete);
            let free_space = self.free_space_without_compaction();
            if need_space > free_space {
                return Err(HashTableAccessMethodError::OutOfSpace);
            }
            self.insert_slot_and_meta(key, pkey, new_slot_id_optional);
            self.get_slot_ref(new_slot_id_optional)
        };

        let latest_record_res =
            self.find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| record_ts <= ts);
        let (latest_record_offset, prev_next_offset) = (
            latest_record_res.record_offset,
            latest_record_res.prev_next_offset,
        );
        let new_record_bytes =
            { CommittedRecord::to_bytes(latest_record_offset.unwrap_or(0), ts, &value, is_delete) };

        // insert latest record
        let record_size = new_record_bytes.len() as u32;
        let record_offset = self.rec_start_offset() - record_size;
        self.write_bytes(record_offset as usize, &new_record_bytes);

        let mut header = self.header();
        header.increase_total_bytes_used(record_size);
        header.set_rec_start_offset(record_offset);
        self.set_header(&header);

        // update prev next
        let update_bytes: [u8; 4] = u32::to_be_bytes(record_offset);
        self.write_bytes(prev_next_offset as usize, &update_bytes);
        Ok(())
    }

    // fn add_delete_version(&mut self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<()> {
    //     let (find_slot_res, new_slot_id_optional) = self.find_slot_by_kpk(key, pkey);

    //     let slot = if let Some(sl) = find_slot_res {
    //         // slot exist => check only new record size
    //         let need_space = CommittedRecord::space_need_from_value(&[], true);
    //         let free_space = self.free_space_without_compaction();
    //         if need_space > free_space {
    //             return Err(HashTableAccessMethodError::OutOfSpace);
    //         }
    //         sl
    //     } else {
    //         let need_space =
    //             Slot::space_need(key, pkey) + CommittedRecord::space_need_from_value(&[], true);
    //         let free_space = self.free_space_without_compaction();
    //         if need_space > free_space {
    //             return Err(HashTableAccessMethodError::OutOfSpace);
    //         }
    //         self.insert_slot_and_meta(key, pkey, new_slot_id_optional);
    //         self.get_slot_ref(new_slot_id_optional)
    //     };

    //     let latest_record_res =
    //         self.find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| record_ts <= ts);
    //     let (latest_record_offset, prev_next_offset) = (
    //         latest_record_res.record_offset,
    //         latest_record_res.prev_next_offset,
    //     );

    //     let new_record_bytes =
    //         { CommittedRecord::to_bytes(latest_record_offset.unwrap_or(0), ts, &[], true) };

    //     // insert latest record
    //     let record_size = new_record_bytes.len() as u32;
    //     let record_offset = self.rec_start_offset() - record_size;
    //     self.write_bytes(record_offset as usize, &new_record_bytes);

    //     let mut header = self.header();
    //     header.increase_total_bytes_used(record_size);
    //     header.set_rec_start_offset(record_offset);
    //     self.set_header(&header);

    //     // update prev next
    //     let update_bytes: [u8; 4] = u32::to_be_bytes(record_offset);
    //     self.write_bytes(prev_next_offset as usize, &update_bytes);
    //     Ok(())
    // }

    fn rehash_export_to_new_page(
        &self,
        new_page: &mut Self,
        is_new_page_fn: impl Fn(&[u8]) -> bool,
    ) -> (FreeSpaceVec, LeftSlotIDs) {
        let get_first_record_offset_fn = |slot: &Slot| {
            let meta_offset = slot.meta_offset();
            u32::from_be_bytes(
                self.read_bytes(meta_offset as usize, 4)
                    .try_into()
                    .map_err(|_| "failed parse")
                    .unwrap(),
            )
        };

        let slot_sli = self.get_slot_slice(0);
        let mut free_space: FreeSpaceVec = FreeSpaceVec::new();
        free_space.reserve(self.slot_count() as usize);
        let mut left_ids: LeftSlotIDs = vec![];
        for (idx, slot) in slot_sli.iter().enumerate() {
            let key = self.get_key_by_slot(slot);
            if is_new_page_fn(&key) {
                
                // new_page.insert_slot_and_meta(&key, &pkey, new_page.slot_count() as usize);

                // add meta space to free space
                let meta_offset = slot.meta_offset();
                let meta_size = SlotMeta::space_need_from_slot(slot);
                free_space.push((meta_offset, meta_size));

                // insert versions to new page and collect free_space info
                let mut offset = get_first_record_offset_fn(slot);
                let mut record = self.next_version(offset);
                let mut versions = VersionRefVec::new();
                while let Some(commit_record) = record {
                    if commit_record.is_deleted() {
                        free_space.push((
                            offset,
                            CommittedRecord::space_need_from_value(&commit_record.value, true),
                        ));
                        // new_page
                        //     .add_commit_version_to_exist_slot(&key, &pkey, commit_record.start_ts, &[], true)
                        //     .unwrap();
                        versions.push((&[], true, commit_record.start_ts));
                    } else {
                        free_space.push((
                            offset,
                            CommittedRecord::space_need_from_value(&commit_record.value, false),
                        ));
                        // new_page
                        //     .add_commit_version_to_exist_slot(
                        //         &key,
                        //         &pkey,
                        //         commit_record.start_ts,
                        //         &commit_record.value,
                        //         false,
                        //     )
                        //     .unwrap();
                        versions.push((&commit_record.value, false, commit_record.start_ts));
                    }

                    record = self.next_version(commit_record.next_offset);
                    offset = commit_record.next_offset;
                }
                let pkey = self.get_pkey_by_slot(slot);
                new_page.rehash_relocate_slot(&key, &pkey, versions).unwrap();
            } else {
                // left out
                left_ids.push(idx as u32);
            }
        }

        (free_space, left_ids)
    }
}

pub trait TableDataPage: TableDataPageTools {
    fn init(&mut self) {
        let header = Header::new();
        self.set_header(&header);
    }

    fn upsert(&mut self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) -> Result<()> {
        self.add_commit_version_to_exist_slot(key, pkey, ts, value, false)
    }

    fn update(&mut self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) -> Result<()> {
        self.add_commit_version_to_exist_slot(key, pkey, ts, value, false)
    }

    fn insert(&mut self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) -> Result<()> {
        self.add_commit_version_to_new_slot(key, pkey, ts, value, false)
    }

    fn get(&self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<Vec<u8>> {
        let (slot, _) = self.find_slot_by_kpk(key, pkey);
        slot.map(|slot| self.get_value_after_ts_by_slot(slot, ts))
            .unwrap_or(None)
            .map(|vec| Ok(vec))
            .unwrap_or(Err(HashTableAccessMethodError::KeyNotFound))
    }

    fn get_keys(&self, key: &[u8], ts: Timestamp) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let slot_sli = {
            let slot_sli = self.get_slot_slice(0);
            let sli_range = self.get_slot_range_by_key(Some(key));
            &slot_sli[sli_range]
        };
        Ok(slot_sli
            .iter()
            .filter(|slot| self.check_slot_match_key(*slot, key))
            .map(|slot| {
                self.get_value_after_ts_by_slot(slot, ts)
                    .map(|val| (self.get_pkey_by_slot(slot), val))
            })
            .filter_map(|res| res)
            .collect::<Vec<_>>())
    }

    fn delete(&mut self, key: &[u8], pkey: &[u8], ts: Timestamp) -> Result<()> {
        self.add_commit_version_to_exist_slot(key, pkey, ts,&[],  true)
    }

    /// return free space
    fn rehash(&mut self, new_page: &mut Self, hash_fn: impl Fn(&[u8]) -> bool) -> u32 {
        let (free_spaces, left_ids) = self.rehash_export_to_new_page(new_page, hash_fn);
        if free_spaces.is_empty() {
            return self.header().rec_start_offset() - self.header().slot_end_offset() as u32;
        }
        // log_warn!("free_space & left_ids{:?}", (free_spaces.clone().into_sorted_vec(), &left_ids));

        let (space_version_vec, slotids_2_versions) = self.rehash_collect_used_space(left_ids);
        // log_warn!("left_space_index_vec & map: {:?}", (space_version_vec.clone().into_sorted_vec(), &slotids_2_versions));

        // log_warn!("free_space:{}, slot_offset:{}, rec_start_offset: {}", self.free_space_without_compaction(), self.slot_end_offset(), self.rec_start_offset());

        self.rehash_truncate_separate_space(free_spaces, space_version_vec, slotids_2_versions)
    }

    fn dbg_print_slots(&self) -> usize {
        let get_first_record_offset_fn = |slot: &Slot| {
            let meta_offset = slot.meta_offset();
            u32::from_be_bytes(
                self.read_bytes(meta_offset as usize, 4)
                    .try_into()
                    .map_err(|_| "failed parse")
                    .unwrap(),
            )
        };

        let slot_sli = self.get_slot_slice(0);
        for slot in slot_sli {
            log_warn!("{:?}", slot);
            SlotMeta::print_meta_of_slot(
                slot,
                self.read_bytes(
                    slot.meta_offset() as usize,
                    SlotMeta::space_need_from_slot(slot) as usize,
                ),
            );

            // insert versions to new page and collect free_space info
            let mut offset = get_first_record_offset_fn(slot);
            let mut record = self.next_version(offset);
            while let Some(commit_record) = record {
                log_warn!(
                    "[versionRecord] start offset: {offset}, space need: {}, {:?}",
                    commit_record.space_need(),
                    commit_record
                );

                record = self.next_version(commit_record.next_offset);
                offset = commit_record.next_offset;
            }
        }

        let header = self.header();
        log_warn!("header: {:?}", header);
        header.slot_count() as usize
    }

    fn garbage_collect(&mut self, safe_ts: Timestamp) {
        let (free_spaces, left_ids) = self.gc_get_free_spaces(safe_ts);
        if free_spaces.is_empty() {
            return;
        }
        log_warn!(
            "[GC] free_space & left_ids{:?}",
            (free_spaces.clone().into_sorted_vec(), &left_ids)
        );

        let (space_version_vec, slotids_2_versions) = self.rehash_collect_used_space(left_ids);
        log_warn!(
            "[GC] left_space_index_vec & map: {:?}",
            (
                space_version_vec.clone().into_sorted_vec(),
                &slotids_2_versions
            )
        );

        log_warn!(
            "[GC] before gc free_space:{}, slot_offset:{}, rec_start_offset: {}",
            self.free_space_without_compaction(),
            self.slot_end_offset(),
            self.rec_start_offset()
        );

        self.rehash_truncate_separate_space(free_spaces, space_version_vec, slotids_2_versions);
    }

    fn scan_one_version(&self, ts: Timestamp, key: Option<&[u8]>) -> Vec<MvccEntry> {
        let mut res = vec![];

        let slot_sli = {
            let slot_sli = self.get_slot_slice(0);
            let sli_range = self.get_slot_range_by_key(key);
            &slot_sli[sli_range]
        };

        for slot in slot_sli {
            let latest_record_res = self
                .find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| record_ts <= ts);
            let (rec_off, rec_end_ts) = (latest_record_res.record_offset, latest_record_res.end_ts);
            if let Some(rec_off) = rec_off {
                // TODO: fn next_version can be [optimized]
                let commit_record = self.next_version(rec_off).unwrap();
                if !commit_record.is_deleted() {
                    let (record_key, record_pkey) =
                        { (self.get_key_by_slot(slot), self.get_pkey_by_slot(slot)) };

                    if let Some(want_key) = key {
                        if record_key == want_key {
<<<<<<< Updated upstream
                            res.push(MvccEntry::new(
                                record_key,
                                record_pkey,
                                commit_record.value,
                                commit_record.start_ts,
                                rec_end_ts,
                            ));
                            // res.push(MvccEntry {
                            //     key: record_key,
                            //     pkey: record_pkey,
                            //     value: commit_record.value,
                            //     start_ts: commit_record.start_ts,
                            //     end_ts: rec_end_ts,
                            // });
                        }
                    } else {
                        res.push(MvccEntry::new(
                            record_key,
                            record_pkey,
                            commit_record.value,
                            commit_record.start_ts,
                            rec_end_ts,
                        ));
                        // res.push(MvccEntry {
                        //     key: record_key,
                        //     pkey: record_pkey,
                        //     value: commit_record.value,
                        //     start_ts: commit_record.start_ts,
                        //     end_ts: rec_end_ts,
                        // });
=======
                            res.push(MvccEntry {
                                key: record_key,
                                pkey: record_pkey,
                                value: commit_record.value.to_vec(),
                                start_ts: commit_record.start_ts,
                                end_ts: rec_end_ts,
                            });
                        }
                    } else {
                        res.push(MvccEntry {
                            key: record_key,
                            pkey: record_pkey,
                            value: commit_record.value.to_vec(),
                            start_ts: commit_record.start_ts,
                            end_ts: rec_end_ts,
                        });
>>>>>>> Stashed changes
                    }
                }
            }
        }
        res
    }

    fn scan_all_versions_all_keys(&self) -> Vec<MvccEntry> {
        let mut res = vec![];

        let get_first_record_offset_fn = |slot: &Slot| {
            let meta_offset = slot.meta_offset();
            u32::from_be_bytes(
                self.read_bytes(meta_offset as usize, 4)
                    .try_into()
                    .map_err(|_| "failed parse")
                    .unwrap(),
            )
        };

        let slot_sli = self.get_slot_slice(0);
        for slot in slot_sli {
            let (key, pkey) = { (self.get_key_by_slot(slot), self.get_pkey_by_slot(slot)) };
            let mut prev_ts = Timestamp::MAX;
            // insert versions to new page and collect free_space info
            let offset = get_first_record_offset_fn(slot);
            let mut record = self.next_version(offset);
            while let Some(commit_record) = record {
                if !commit_record.is_deleted() {
<<<<<<< Updated upstream
                    res.push(MvccEntry::new(
                        key.clone(),
                        pkey.clone(),
                        commit_record.value,
                        commit_record.start_ts,
                        prev_ts,
                    ));
                    // res.push(MvccEntry {
                    //     key: key.clone(),
                    //     pkey: pkey.clone(),
                    //     value: commit_record.value,
                    //     start_ts: commit_record.start_ts,
                    //     end_ts: prev_ts,
                    // });
=======
                    res.push(MvccEntry {
                        key: key.clone(),
                        pkey: pkey.clone(),
                        value: commit_record.value.to_vec(),
                        start_ts: commit_record.start_ts,
                        end_ts: prev_ts,
                    });
>>>>>>> Stashed changes
                }
                record = self.next_version(commit_record.next_offset);
                prev_ts = commit_record.start_ts;
            }
        }
        res
    }

    fn scan_delta_of_btw_ts(
        &self,
        small_ts: Timestamp,
        large_ts: Timestamp,
    ) -> Vec<DeltaEntry<Vec<u8>>> {
        let mut res = vec![];

        let slot_sli = self.get_slot_slice(0);
        for slot in slot_sli {
            let large_ts_record_res = self
                .find_latest_record_before_ts_by_slot(slot, |record_ts: Timestamp| {
                    record_ts <= large_ts
                });
            let (large_ts_rec_off, _large_ts_rec_end_ts) = (
                large_ts_record_res.record_offset,
                large_ts_record_res.end_ts,
            );
            let large_ts_commit_record = if let Some(rec_off) = &large_ts_rec_off {
                // find a rec whose start_ts <= large_ts
                self.next_version(*rec_off).unwrap()
            } else {
                continue;
            };
            let small_ts_record_res = if large_ts_commit_record.start_ts <= small_ts {
                Some(large_ts_commit_record.clone())
            } else {
                let small_ts_record_res = self.find_latest_record_before_ts(
                    small_ts,
                    large_ts_commit_record.next_offset,
                    large_ts_rec_off.unwrap(),
                    large_ts_commit_record.start_ts,
                );

                let small_ts_rec_off = small_ts_record_res.record_offset;
                if let Some(rec_off) = &small_ts_rec_off {
                    Some(self.next_version(*rec_off).unwrap())
                } else {
                    None
                }
            };

            let is_small_ts_not_exist = (small_ts_record_res.is_none()
                || small_ts_record_res.as_ref().unwrap().is_deleted());

            // no delta
            if large_ts_commit_record.is_deleted() && is_small_ts_not_exist {
                continue;
            }

            let (key, pkey) = { (self.get_key_by_slot(slot), self.get_pkey_by_slot(slot)) };

            if !is_small_ts_not_exist {
                // insert
                res.push(DeltaEntry {
                    key,
                    pkey,
                    value_delta: Delta::Inserted(large_ts_commit_record.value.to_vec()),
                });
            } else if large_ts_commit_record.is_deleted() {
                // delete
                let _small_ts_value = small_ts_record_res.unwrap().value;
                res.push(DeltaEntry {
                    key,
                    pkey,
                    value_delta: Delta::Deleted,
                });
            } else {
                // update
                // TODO: value may still the same after update
                res.push(DeltaEntry {
                    key,
                    pkey,
                    value_delta: Delta::Updated(large_ts_commit_record.value.to_vec()),
                });
            }
        }
        res
    }
}

impl TableDataPageTools for Page {}

impl TableDataPage for Page {}

#[cfg(test)]
mod test_vec {
    use crate::{log_warn, page::Page};

    use super::{TableDataPage, TableDataPageBase, TableDataPageInterface};

    #[test]
    fn test_small_key() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);
        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[4, 4, 4], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[1, 1, 1], 1).unwrap();
        <Page as TableDataPage>::delete(&mut page, &[4, 4, 4], &[4, 4, 4], 3).unwrap();
        log_warn!(
            "get with ts = 5: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 5)
        );
        log_warn!(
            "get with ts = 4: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 4)
        );
        log_warn!(
            "get with ts = 3: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 3)
        );
        log_warn!(
            "get with ts = 2: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 2)
        );
        log_warn!(
            "get with ts = 1: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 1)
        );
        log_warn!(
            "get with ts = 0: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4, 4, 4], &[4, 4, 4], 0)
        );
        page.dbg_print_slots();
    }

    #[test]
    fn test_large_key() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);
        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[4; 10], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[1, 1, 1], 1).unwrap();
        <Page as TableDataPage>::delete(&mut page, &[4; 10], &[4; 10], 3).unwrap();
        log_warn!(
            "get with ts = 5: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 5)
        );
        log_warn!(
            "get with ts = 4: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 4)
        );
        log_warn!(
            "get with ts = 3: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 3)
        );
        log_warn!(
            "get with ts = 2: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 2)
        );
        log_warn!(
            "get with ts = 1: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 1)
        );
        log_warn!(
            "get with ts = 0: {:?}",
            <Page as TableDataPage>::get(&mut page, &[4; 10], &[4; 10], 0)
        );
        page.dbg_print_slots();
    }

    #[test]
    fn test_rehash() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);

        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[4; 10], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[1; 10], 1).unwrap();
        <Page as TableDataPage>::delete(&mut page, &[4; 10], &[4; 10], 3).unwrap();

        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[4, 4, 4], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[1, 1, 1], 1).unwrap();
        <Page as TableDataPage>::delete(&mut page, &[4, 4, 4], &[4, 4, 4], 3).unwrap();

        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[6; 10], 6).unwrap();

        let mut new_page = Page::new(1);
        <Page as TableDataPage>::init(&mut new_page);

        let fn1 = |a: &[u8]| -> bool { a.len() < 5 };

        page.dbg_print_slots();

        page.rehash(&mut new_page, fn1);

        page.dbg_print_slots();
        new_page.dbg_print_slots();
    }

    #[test]
    fn test_gc() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);

        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[4; 10], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4; 10], &[4; 10], &[1; 10], 1).unwrap();

        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[4, 4, 4], 4).unwrap();
        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[1, 1, 1], 1).unwrap();

        <Page as TableDataPage>::delete(&mut page, &[6; 6], &[6; 6], 2).unwrap();

        page.dbg_print_slots();

        page.garbage_collect(3);

        page.dbg_print_slots();
    }

    #[cfg(not(feature = "unsorted_page"))]
    #[test]
    fn test_sorted1() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);

        <Page as TableDataPage>::upsert(&mut page, &[4, 4, 4], &[4, 4, 4], &[4, 4, 4], 4).unwrap();

        <Page as TableDataPage>::delete(&mut page, &[6; 6], &[6; 6], 2).unwrap();

        <Page as TableDataPage>::upsert(&mut page, &[2; 3], &[3; 6], &[2, 3, 5], 6).unwrap();
        <Page as TableDataPage>::delete(&mut page, &[2; 3], &[4; 6], 2).unwrap();

        page.dbg_print_slots();
    }

    #[cfg(not(feature = "unsorted_page"))]
    #[test]
    fn test_sorted2() {
        let mut page = Page::new(0);
        <Page as TableDataPage>::init(&mut page);

        <Page as TableDataPage>::upsert(
            &mut page,
            &[4, 4, 4, 4, 4, 4, 4, 4, 5],
            &[4, 4, 4, 4, 4, 4, 4, 4, 1],
            &[4],
            4,
        )
        .unwrap();
        <Page as TableDataPage>::upsert(
            &mut page,
            &[4, 4, 4, 4, 4, 4, 4, 4, 5],
            &[4, 4, 4, 4, 4, 4, 4, 4, 7],
            &[4],
            4,
        )
        .unwrap();
        <Page as TableDataPage>::upsert(
            &mut page,
            &[4, 4, 4, 4, 4, 4, 4, 4, 3],
            &[4, 4, 4],
            &[4],
            4,
        )
        .unwrap();
        <Page as TableDataPage>::upsert(
            &mut page,
            &[4, 4, 4, 4, 4, 4, 4, 4, 1],
            &[4, 4, 4],
            &[4],
            4,
        )
        .unwrap();
        page.dbg_print_slots();
    }
}
