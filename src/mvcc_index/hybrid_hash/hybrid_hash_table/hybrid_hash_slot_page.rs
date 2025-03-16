mod slots_page_header {
    use crate::page::{PageId, AVAILABLE_PAGE_SIZE, PAGE_SIZE};

    use super::SLOT_SIZE;
    pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<SlotsPageHeader>();
    const BASE_HEADER_SIZE: usize = PAGE_SIZE - AVAILABLE_PAGE_SIZE;
    const _: () = assert!(SLOT_SIZE >= PAGE_HEADER_SIZE + BASE_HEADER_SIZE);
    pub const PAGE_HEADER_SIZE_ALIGNED: usize = SLOT_SIZE - BASE_HEADER_SIZE;
    #[derive(Debug)]
    pub struct SlotsPageHeader {
        total_bytes_used: u32,
        slot_count: u32,
        attached_page_id: u32,
    }
    impl SlotsPageHeader {
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

            let attached_page_id = u32::from_be_bytes(
                bytes[current_pos..current_pos + 4]
                    .try_into()
                    .map_err(|_| "Failed to parse")?,
            );

            Ok(Self {
                total_bytes_used,
                slot_count,
                attached_page_id,
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
                .copy_from_slice(&self.attached_page_id.to_be_bytes());

            bytes
        }

        pub fn new(attach_id: PageId) -> Self {
            Self {
                total_bytes_used: PAGE_HEADER_SIZE_ALIGNED as u32,
                slot_count: 0,
                attached_page_id: attach_id,
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

        pub fn attached_page_id(&self) -> u32 {
            self.attached_page_id
        }
        pub fn set_attached_page_id(&mut self, attached_page_id: u32) {
            self.attached_page_id = attached_page_id;
        }

        pub fn free_space(&self) -> u32 {
            AVAILABLE_PAGE_SIZE as u32 - self.total_bytes_used
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

        pub fn slot_end_offset(&self) -> usize {
            (self.slot_count * SLOT_SIZE as u32 + PAGE_HEADER_SIZE_ALIGNED as u32) as usize
        }
    }
}
use core::slice;
use slots_page_header::*;

use super::super::attached_container_page::attached_container_page::slot::*;

use crate::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    log_warn,
    mvcc_index::{
        hybrid_hash::{
            attached_container_page::{
                attached_container_common::{get_all_versions, get_delta, get_le_ts_version},
                attached_container_page::AttachedPage,
            },
            hash_join_table_common::HashTableAccessMethodError,
            hybrid_hash_table::hybrid_hash_common::read_page,
        },
        Delta, DeltaEntry, MvccEntry, Timestamp,
    },
    page::{Page, PageId, AVAILABLE_PAGE_SIZE},
};

pub trait TableDataPageBase {
    fn set_header(&mut self, header: &SlotsPageHeader) {
        let header_bytes = header.to_bytes();
        self.write_bytes(0, &header_bytes);
    }
    fn slot_count(&self) -> u32 {
        let header = self.header();
        header.slot_count()
    }
    fn header(&self) -> SlotsPageHeader {
        let header_bytes = self.read_bytes(0, PAGE_HEADER_SIZE);
        SlotsPageHeader::from_bytes(header_bytes).unwrap()
    }
    fn slot_offset(&self, slot_id: u32) -> u32 {
        PAGE_HEADER_SIZE_ALIGNED as u32 + slot_id * SLOT_SIZE as u32
    }

    fn get_slot_ref(&self, slot_id: usize) -> &Slot {
        let sli = self.get_slot_slice(0);
        &sli[slot_id]
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

pub trait TableDataPageTools: TableDataPageBase {
    fn truncate_left_out_slots(&mut self, hashed_out_indexes: Vec<u32>) {
        let slot_count = self.slot_count() as usize;
        let slot_slice_mut = self.get_slot_slice_mutable(0);
        let new_slot_count = {
            let mut i = 0;
            for j in 0..slot_count {
                if !hashed_out_indexes.contains(&(j as u32)) {
                    // Slot remains in the current page
                    slot_slice_mut.copy_within(j..j + 1, i);
                    i += 1;
                }
            }
            i as u32
        };
        let mut header = self.header();
        header.set_slot_count(new_slot_count);
        header.set_total_bytes_used(header.slot_end_offset() as u32);
        self.set_header(&header);
    }
}

pub trait TableSlotsPage: TableDataPageTools {
    fn get_attached_page_id(&self) -> PageId {
        self.header().attached_page_id()
    }

    fn set_attached_page_id(&mut self, id: PageId) {
        let mut header = self.header();
        header.set_attached_page_id(id);
        self.set_header(&header);
    }

    fn init(&mut self, attach_page_id: PageId) {
        let header = SlotsPageHeader::new(attach_page_id);
        self.set_header(&header);
    }

    fn free_space(&self) -> u32 {
        let header = self.header();
        header.free_space()
    }

    fn insert_slot(&mut self, slot: Slot) -> Result<()> {
        let slot_id = self.slot_count();
        let header = self.header();
        let free_space = header.free_space();

        if free_space < SLOT_SIZE as u32 {
            return Err(HashTableAccessMethodError::OutOfSpace);
        }
        self.set_slot(slot_id, &slot);

        let mut header = self.header();
        header.increment_slot_count();
        header.increase_total_bytes_used(SLOT_SIZE as u32);
        self.set_header(&header);
        Ok(())
    }

    /// return free space
    fn rehash(&mut self, new_page: &mut Self, hash_fn: impl Fn(u32) -> bool) -> u32 {
        let mut hashed_out_indexes = Vec::new();
        // Iterate through the slots
        let mut new_page_slot_id = 0_u32;

        {
            let mut header = new_page.header();
            header.set_slot_count(self.slot_count());
            new_page.set_header(&header);
        }

        for (idx, slot) in self.get_slot_slice(0).iter().enumerate() {
            if hash_fn(slot.key_hash()) {
                new_page.set_slot(new_page_slot_id, &slot);
                new_page_slot_id += 1;

                hashed_out_indexes.push(idx as u32);
            }
        }
        let mut header = new_page.header();
        header.set_slot_count(new_page_slot_id);
        header.set_total_bytes_used(header.slot_end_offset() as u32);
        new_page.set_header(&header);

        // Truncate left out slots through the indexes
        self.truncate_left_out_slots(hashed_out_indexes);
        self.free_space()
    }

    fn dbg_print_slots(&self) -> usize {
        let slot_sli = self.get_slot_slice(0);
        for slot in slot_sli {
            log_warn!("{:?}", slot);
            todo!("print versions");
        }

        let header = self.header();
        log_warn!("header: {:?}", header);
        header.slot_count() as usize
    }

    fn garbage_collect(&mut self, safe_ts: Timestamp) {
        todo!()
    }

    fn scan_one_version_all_keys<T: MemPool + 'static>(
        &self,
        mem_pool: &T,
        c_key: ContainerKey,
        ts: Timestamp,
    ) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let get_kpk_fn = |slot: &Slot, meta: &SlotMeta| -> (Vec<u8>, Vec<u8>) {
            let (mut k, mut pk) = (slot.key_prefix().to_vec(), slot.pkey_prefix().to_vec());
            k.extend_from_slice(&meta.remain_key);
            pk.extend_from_slice(&meta.remain_pkey);
            (k, pk)
        };

        let slots = self.get_slot_slice(0);
        slots
            .iter()
            .filter_map(|slot| {
                let meta_loc = slot.meta_loc();
                let pfk = PageFrameKey::new(c_key, meta_loc.page_id);
                let meta_attached_guard = read_page(mem_pool, pfk);
                let meta_attached_page = &*meta_attached_guard as &dyn AttachedPage;

                let slot_meta = meta_attached_page.get_slot_meta(slot);
                let first_version_loc = slot_meta.latest_version_loc;

                let kpk = get_kpk_fn(slot, &slot_meta);
                drop(meta_attached_guard);
                let version = get_le_ts_version(first_version_loc, mem_pool, c_key, ts);
                version.map(|v| (kpk.0, kpk.1, v.value.to_vec()))
            })
            .collect()
    }

    fn scan_one_version_one_key<T: MemPool + 'static>(
        &self,
        mem_pool: &T,
        c_key: ContainerKey,
        ts: Timestamp,
        key: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let check_key_match_fn = |slot: &Slot, meta: &SlotMeta| -> Option<Vec<u8>> {
            let prefix_len = key.len().min(SLOT_KEY_PREFIX_SIZE);
            let key_prefix = &key[..prefix_len];
            let key_remain = &key[prefix_len..];
            if key_prefix == slot.key_prefix() && key_remain == meta.remain_key {
                let mut pk = slot.pkey_prefix().to_vec();
                pk.extend_from_slice(&meta.remain_pkey);
                return Some(pk);
            } else {
                return None;
            }
        };

        let slots = self.get_slot_slice(0);
        slots
            .iter()
            .filter_map(|slot| {
                let meta_loc = slot.meta_loc();
                let pfk = PageFrameKey::new(c_key, meta_loc.page_id);
                let meta_attached_guard = read_page(mem_pool, pfk);
                let meta_attached_page = &*meta_attached_guard as &dyn AttachedPage;

                let slot_meta = meta_attached_page.get_slot_meta(slot);
                let first_version_loc = slot_meta.latest_version_loc;

                if let Some(pk) = check_key_match_fn(slot, &slot_meta) {
                    drop(meta_attached_guard);
                    let version = get_le_ts_version(first_version_loc, mem_pool, c_key, ts);
                    return version.map(|v| (pk, v.value.to_vec()));
                } else {
                    None
                }
            })
            .collect()
    }

    fn scan_all_versions_all_keys<T: MemPool + 'static>(
        &self,
        mem_pool: &T,
        c_key: ContainerKey,
    ) -> Vec<MvccEntry> {
        let get_full_key_pk_fn = |slot: &Slot, meta: &SlotMeta| -> (Vec<u8>, Vec<u8>) {
            let (mut k, mut pk) = (slot.key_prefix().to_vec(), slot.pkey_prefix().to_vec());
            k.extend_from_slice(&meta.remain_key);
            pk.extend_from_slice(&meta.remain_pkey);
            (k, pk)
        };

        let slots = self.get_slot_slice(0);
        slots
            .iter()
            .flat_map(|slot| {
                let meta_loc = slot.meta_loc();
                let pfk = PageFrameKey::new(c_key, meta_loc.page_id);
                let meta_attached_guard = read_page(mem_pool, pfk);
                let meta_attached_page = &*meta_attached_guard as &dyn AttachedPage;

                let slot_meta = meta_attached_page.get_slot_meta(slot);
                let first_version_loc = slot_meta.latest_version_loc;
                let kpk = get_full_key_pk_fn(slot, &slot_meta);
                drop(meta_attached_guard);
                get_all_versions(mem_pool, c_key, first_version_loc, &kpk.0, &kpk.1)
            })
            .collect()
    }

    fn scan_delta_btw_ts<T: MemPool + 'static>(
        &self,
        small_ts: Timestamp,
        large_ts: Timestamp,
        mem_pool: &T,
        c_key: ContainerKey,
    ) -> Vec<DeltaEntry<Vec<u8>>> {
        let get_full_key_pk_fn = |slot: &Slot, meta: &SlotMeta| -> (Vec<u8>, Vec<u8>) {
            let (mut k, mut pk) = (slot.key_prefix().to_vec(), slot.pkey_prefix().to_vec());
            k.extend_from_slice(&meta.remain_key);
            pk.extend_from_slice(&meta.remain_pkey);
            (k, pk)
        };

        let slots = self.get_slot_slice(0);
        slots
            .iter()
            .filter_map(|slot| {
                let meta_loc = slot.meta_loc();
                let pfk = PageFrameKey::new(c_key, meta_loc.page_id);
                let meta_attached_guard = read_page(mem_pool, pfk);
                let meta_attached_page = &*meta_attached_guard as &dyn AttachedPage;

                let slot_meta = meta_attached_page.get_slot_meta(slot);
                let first_version_loc = slot_meta.latest_version_loc;
                let kpk = get_full_key_pk_fn(slot, &slot_meta);
                drop(meta_attached_guard);
                let delta = get_delta(mem_pool, c_key, first_version_loc, small_ts, large_ts);

                delta.map(|d| (kpk, d))
            })
            .map(|(kpk, delta)| DeltaEntry::new(kpk.0, kpk.1, delta))
            .collect()
    }
}

impl TableDataPageTools for Page {}
impl TableSlotsPage for Page {}
