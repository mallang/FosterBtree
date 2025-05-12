use crate::{
    access_method::AccessMethodError,
    mvcc_index::{MvccEntry, TxId},
    prelude::{Page, PageId, Timestamp, AVAILABLE_PAGE_SIZE},
};
use core::ptr::{read_unaligned, write_unaligned};
use memoffset::offset_of;
use std::mem::size_of;
use std::{cmp::Ordering, result::Result::Ok, sync::atomic::AtomicU64};

/* ========================================================================== */
/*                              Layout constants                              */
/* ========================================================================== */

#[repr(C)]
struct HEADER {
    next_page_id: PageId,  // 0  (u32)
    next_frame_id: u32,    // 4
    total_bytes_used: u32, // 8
    slot_count: u32,       // 12
    rec_start_offset: u32, // 16
    recent_entry_cnt: u32, // 20
    min_start_ts: u64,     // 24
    max_end_ts: u64,       // 32
    is_full: u8,           // 40
}

#[repr(C)]
pub struct Slot {
    hash_key_size: u32,       // 0
    pkey_size: u32,           // 4
    hash_key_prefix: [u8; 8], // 8
    pkey_prefix: [u8; 8],     // 16
    start_ts: Timestamp,      // 24 (u64)
    end_ts: Timestamp,        // 32 (u64)
    val_size: u32,            // 40
    offset: u32,              // 44
    tx_id: TxId,              // 48 (u64)
}

pub const HEADER_SIZE: usize = size_of::<HEADER>();
pub const SLOT_SIZE: usize = size_of::<Slot>();

#[inline]
const fn slot_base(idx: usize) -> usize {
    HEADER_SIZE + idx * SLOT_SIZE
}

/* ---------------- field offsets (compile‑time) ---------------- */
macro_rules! off {
    ($t:ty, $f:ident) => {
        offset_of!($t, $f)
    };
}

// HEADER offsets
pub const HDR_NEXT_PAGE_ID_OFF: usize = off!(HEADER, next_page_id);
pub const HDR_NEXT_FRAME_ID_OFF: usize = off!(HEADER, next_frame_id);
pub const HDR_TOTAL_BYTES_USED_OFF: usize = off!(HEADER, total_bytes_used);
pub const HDR_SLOT_COUNT_OFF: usize = off!(HEADER, slot_count);
pub const HDR_REC_START_OFF: usize = off!(HEADER, rec_start_offset);
pub const HDR_RECENT_CNT_OFF: usize = off!(HEADER, recent_entry_cnt);
pub const HDR_MIN_START_TS_OFF: usize = off!(HEADER, min_start_ts);
pub const HDR_MAX_END_TS_OFF: usize = off!(HEADER, max_end_ts);
pub const HDR_IS_FULL_OFF: usize = off!(HEADER, is_full);

// Slot offsets
pub const SLOT_HASH_KEY_SIZE_OFF: usize = off!(Slot, hash_key_size);
pub const SLOT_PKEY_SIZE_OFF: usize = off!(Slot, pkey_size);
pub const SLOT_HASH_KEY_PREFIX_OFF: usize = off!(Slot, hash_key_prefix);
pub const SLOT_PKEY_PREFIX_OFF: usize = off!(Slot, pkey_prefix);
pub const SLOT_START_TS_OFF: usize = off!(Slot, start_ts);
pub const SLOT_END_TS_OFF: usize = off!(Slot, end_ts);
pub const SLOT_VAL_SIZE_OFF: usize = off!(Slot, val_size);
pub const SLOT_OFFSET_OFF: usize = off!(Slot, offset);
pub const SLOT_TX_ID_OFF: usize = off!(Slot, tx_id);

/* ========================================================================== */
/*                            Unsafe zero‑copy IO                              */
/* ========================================================================== */

#[inline]
unsafe fn rd_u32(buf: &[u8], off: usize) -> u32 {
    read_unaligned(buf.as_ptr().add(off) as *const u32)
}
#[inline]
unsafe fn wr_u32(buf: &mut [u8], off: usize, v: u32) {
    write_unaligned(buf.as_mut_ptr().add(off) as *mut u32, v);
}
#[inline]
unsafe fn rd_u64(buf: &[u8], off: usize) -> u64 {
    read_unaligned(buf.as_ptr().add(off) as *const u64)
}
#[inline]
unsafe fn wr_u64(buf: &mut [u8], off: usize, v: u64) {
    write_unaligned(buf.as_mut_ptr().add(off) as *mut u64, v);
}
#[inline]
fn rd_u8(buf: &[u8], off: usize) -> u8 {
    buf[off]
}
#[inline]
fn wr_u8(buf: &mut [u8], off: usize, v: u8) {
    buf[off] = v
}

/* ========================================================================== */
/*                                PageOps API                                 */
/* ========================================================================== */

pub trait PageOps {
    /* HEADER getters */
    fn hdr_next_page_id(&self) -> u32;
    fn hdr_next_frame_id(&self) -> u32;
    fn hdr_total_bytes_used(&self) -> usize;
    fn hdr_slot_count(&self) -> usize;
    fn hdr_rec_start_off(&self) -> usize;
    fn hdr_recent_entry_cnt(&self) -> usize;
    fn hdr_min_start_ts(&self) -> Timestamp;
    fn hdr_max_end_ts(&self) -> Timestamp;
    fn hdr_is_full(&self) -> bool;

    /* HEADER setters */
    fn set_hdr_next_page_id(&mut self, v: u32);
    fn set_hdr_next_frame_id(&mut self, v: u32);
    fn set_hdr_total_bytes_used(&mut self, v: usize);
    fn set_hdr_slot_count(&mut self, v: usize);
    fn set_hdr_rec_start_off(&mut self, v: usize);
    fn set_hdr_recent_entry_cnt(&mut self, v: usize);
    fn set_hdr_min_start_ts(&mut self, v: Timestamp);
    fn set_hdr_max_end_ts(&mut self, v: Timestamp);
    fn set_hdr_is_full(&mut self, is_full: bool);

    /* Slot getters */
    fn slot_hash_key_size(&self, idx: usize) -> usize;
    fn slot_hash_key_size_ref<'a>(&'a self, idx: usize) -> &'a [u8];
    fn slot_pkey_size(&self, idx: usize) -> usize;
    fn slot_pkey_size_ref<'a>(&'a self, idx: usize) -> &'a [u8];
    fn slot_hash_key_prefix(&self, idx: usize) -> &[u8; 8];
    fn slot_pkey_prefix(&self, idx: usize) -> &[u8; 8];
    fn slot_start_ts(&self, idx: usize) -> Timestamp;
    fn slot_end_ts(&self, idx: usize) -> Timestamp;
    fn slot_val_size(&self, idx: usize) -> usize;
    fn slot_offset(&self, idx: usize) -> usize;
    fn slot_tx_id(&self, idx: usize) -> TxId;

    /* Slot setters */
    fn set_slot_hash_key_size(&mut self, idx: usize, v: usize);
    fn set_slot_pkey_size(&mut self, idx: usize, v: usize);
    fn set_slot_start_ts(&mut self, idx: usize, ts: Timestamp);
    fn set_slot_end_ts(&mut self, idx: usize, ts: Timestamp);
    fn set_slot_val_size(&mut self, idx: usize, v: usize);
    fn set_slot_offset(&mut self, idx: usize, v: usize);
    fn set_slot_tx_id(&mut self, idx: usize, tx: TxId);
    fn set_slot_hash_key_prefix(&mut self, idx: usize, p: &[u8; 8]);
    fn set_slot_pkey_prefix(&mut self, idx: usize, p: &[u8; 8]);

    /* data‑region raw slice getters (zero‑copy) */
    fn slot_hash_key<'a>(&'a self, idx: usize) -> &'a [u8];
    fn slot_pkey<'a>(&'a self, idx: usize) -> &'a [u8];
    fn slot_value<'a>(&'a self, idx: usize) -> &'a [u8];

    /* data‑region setters */
    fn set_slot_hash_key(&mut self, idx: usize, key: &[u8]);
    fn set_slot_pkey(&mut self, idx: usize, pkey: &[u8]);
    fn set_slot_value(&mut self, idx: usize, val: &[u8]);
}

impl PageOps for Page {
    /* ---------- HEADER getters ---------- */
    #[inline]
    fn hdr_next_page_id(&self) -> u32 {
        unsafe { rd_u32(self, HDR_NEXT_PAGE_ID_OFF) }
    }
    #[inline]
    fn hdr_next_frame_id(&self) -> u32 {
        unsafe { rd_u32(self, HDR_NEXT_FRAME_ID_OFF) }
    }
    #[inline]
    fn hdr_total_bytes_used(&self) -> usize {
        unsafe { rd_u32(self, HDR_TOTAL_BYTES_USED_OFF) as usize }
    }
    #[inline]
    fn hdr_slot_count(&self) -> usize {
        unsafe { rd_u32(self, HDR_SLOT_COUNT_OFF) as usize }
    }
    #[inline]
    fn hdr_rec_start_off(&self) -> usize {
        unsafe { rd_u32(self, HDR_REC_START_OFF) as usize }
    }
    #[inline]
    fn hdr_recent_entry_cnt(&self) -> usize {
        unsafe { rd_u32(self, HDR_RECENT_CNT_OFF) as usize }
    }
    #[inline]
    fn hdr_min_start_ts(&self) -> Timestamp {
        unsafe { rd_u64(self, HDR_MIN_START_TS_OFF) }
    }
    #[inline]
    fn hdr_max_end_ts(&self) -> Timestamp {
        unsafe { rd_u64(self, HDR_MAX_END_TS_OFF) }
    }
    #[inline]
    fn hdr_is_full(&self) -> bool {
        rd_u8(self, HDR_IS_FULL_OFF) != 0
    }

    /* ---------- HEADER setters ---------- */
    #[inline]
    fn set_hdr_next_page_id(&mut self, v: u32) {
        unsafe { wr_u32(self, HDR_NEXT_PAGE_ID_OFF, v) }
    }
    #[inline]
    fn set_hdr_next_frame_id(&mut self, v: u32) {
        unsafe { wr_u32(self, HDR_NEXT_FRAME_ID_OFF, v) }
    }
    #[inline]
    fn set_hdr_total_bytes_used(&mut self, v: usize) {
        unsafe { wr_u32(self, HDR_TOTAL_BYTES_USED_OFF, v as u32) }
    }
    #[inline]
    fn set_hdr_slot_count(&mut self, v: usize) {
        unsafe { wr_u32(self, HDR_SLOT_COUNT_OFF, v as u32) }
    }
    #[inline]
    fn set_hdr_rec_start_off(&mut self, v: usize) {
        unsafe { wr_u32(self, HDR_REC_START_OFF, v as u32) }
    }
    #[inline]
    fn set_hdr_recent_entry_cnt(&mut self, v: usize) {
        unsafe { wr_u32(self, HDR_RECENT_CNT_OFF, v as u32) }
    }
    #[inline]
    fn set_hdr_min_start_ts(&mut self, ts: Timestamp) {
        unsafe { wr_u64(self, HDR_MIN_START_TS_OFF, ts) }
    }
    #[inline]
    fn set_hdr_max_end_ts(&mut self, ts: Timestamp) {
        unsafe { wr_u64(self, HDR_MAX_END_TS_OFF, ts) }
    }
    #[inline]
    fn set_hdr_is_full(&mut self, is_full: bool) {
        wr_u8(self, HDR_IS_FULL_OFF, if is_full { 1 } else { 0 });
    }

    /* ---------- Slot getters ---------- */
    #[inline]
    fn slot_hash_key_size(&self, idx: usize) -> usize {
        unsafe { rd_u32(self, slot_base(idx) + SLOT_HASH_KEY_SIZE_OFF) as usize }
    }
    #[inline]
    fn slot_hash_key_size_ref<'a>(&'a self, idx: usize) -> &'a [u8] {
        let off = slot_base(idx) + SLOT_HASH_KEY_SIZE_OFF;
        unsafe { &*(self.as_ptr().add(off) as *const [u8; 4]) }
    }
    #[inline]
    fn slot_pkey_size(&self, idx: usize) -> usize {
        unsafe { rd_u32(self, slot_base(idx) + SLOT_PKEY_SIZE_OFF) as usize }
    }
    #[inline]
    fn slot_pkey_size_ref<'a>(&'a self, idx: usize) -> &'a [u8] {
        let off = slot_base(idx) + SLOT_PKEY_SIZE_OFF;
        unsafe { &*(self.as_ptr().add(off) as *const [u8; 4]) }
    }
    #[inline]
    fn slot_hash_key_prefix(&self, idx: usize) -> &[u8; 8] {
        let off = slot_base(idx) + SLOT_HASH_KEY_PREFIX_OFF;
        unsafe { &*(self.as_ptr().add(off) as *const [u8; 8]) }
    }
    #[inline]
    fn slot_pkey_prefix(&self, idx: usize) -> &[u8; 8] {
        let off = slot_base(idx) + SLOT_PKEY_PREFIX_OFF;
        unsafe { &*(self.as_ptr().add(off) as *const [u8; 8]) }
    }
    #[inline]
    fn slot_start_ts(&self, idx: usize) -> Timestamp {
        unsafe { rd_u64(self, slot_base(idx) + SLOT_START_TS_OFF) }
    }
    #[inline]
    fn slot_end_ts(&self, idx: usize) -> Timestamp {
        unsafe { rd_u64(self, slot_base(idx) + SLOT_END_TS_OFF) }
    }
    #[inline]
    fn slot_val_size(&self, idx: usize) -> usize {
        unsafe { rd_u32(self, slot_base(idx) + SLOT_VAL_SIZE_OFF) as usize }
    }
    #[inline]
    fn slot_offset(&self, idx: usize) -> usize {
        unsafe { rd_u32(self, slot_base(idx) + SLOT_OFFSET_OFF) as usize }
    }
    #[inline]
    fn slot_tx_id(&self, idx: usize) -> TxId {
        unsafe { rd_u64(self, slot_base(idx) + SLOT_TX_ID_OFF) }
    }

    /* ---------- Slot setters ---------- */
    #[inline]
    fn set_slot_hash_key_size(&mut self, idx: usize, v: usize) {
        unsafe { wr_u32(self, slot_base(idx) + SLOT_HASH_KEY_SIZE_OFF, v as u32) }
    }
    #[inline]
    fn set_slot_pkey_size(&mut self, idx: usize, v: usize) {
        unsafe { wr_u32(self, slot_base(idx) + SLOT_PKEY_SIZE_OFF, v as u32) }
    }
    #[inline]
    fn set_slot_hash_key_prefix(&mut self, idx: usize, p: &[u8; 8]) {
        self[slot_base(idx) + SLOT_HASH_KEY_PREFIX_OFF
            ..slot_base(idx) + SLOT_HASH_KEY_PREFIX_OFF + 8]
            .copy_from_slice(p);
    }
    #[inline]
    fn set_slot_pkey_prefix(&mut self, idx: usize, p: &[u8; 8]) {
        self[slot_base(idx) + SLOT_PKEY_PREFIX_OFF..slot_base(idx) + SLOT_PKEY_PREFIX_OFF + 8]
            .copy_from_slice(p);
    }
    #[inline]
    fn set_slot_start_ts(&mut self, idx: usize, ts: Timestamp) {
        unsafe { wr_u64(self, slot_base(idx) + SLOT_START_TS_OFF, ts) }
    }
    #[inline]
    fn set_slot_end_ts(&mut self, idx: usize, ts: Timestamp) {
        unsafe { wr_u64(self, slot_base(idx) + SLOT_END_TS_OFF, ts) }
    }
    #[inline]
    fn set_slot_val_size(&mut self, idx: usize, v: usize) {
        unsafe { wr_u32(self, slot_base(idx) + SLOT_VAL_SIZE_OFF, v as u32) }
    }
    #[inline]
    fn set_slot_offset(&mut self, idx: usize, v: usize) {
        unsafe { wr_u32(self, slot_base(idx) + SLOT_OFFSET_OFF, v as u32) }
    }
    #[inline]
    fn set_slot_tx_id(&mut self, idx: usize, tx: TxId) {
        unsafe { wr_u64(self, slot_base(idx) + SLOT_TX_ID_OFF, tx) }
    }

    /* ----------- zero‑copy data slice getters ----------- */
    #[inline]
    fn slot_hash_key<'a>(&'a self, idx: usize) -> &'a [u8] {
        let base = self.slot_offset(idx) as usize;
        let len = self.slot_hash_key_size(idx) as usize;
        &self[base..base + len]
    }
    #[inline]
    fn slot_pkey<'a>(&'a self, idx: usize) -> &'a [u8] {
        let base = self.slot_offset(idx) as usize + self.slot_hash_key_size(idx) as usize;
        let len = self.slot_pkey_size(idx) as usize;
        &self[base..base + len]
    }
    #[inline]
    fn slot_value<'a>(&'a self, idx: usize) -> &'a [u8] {
        let base = self.slot_offset(idx) as usize
            + self.slot_hash_key_size(idx) as usize
            + self.slot_pkey_size(idx) as usize;
        let len = self.slot_val_size(idx) as usize;
        &self[base..base + len]
    }

    /* ---------- data‑region setters ---------- */
    #[inline]
    fn set_slot_hash_key(&mut self, idx: usize, key: &[u8]) {
        let base = self.slot_offset(idx) as usize;
        self[base..base + key.len()].copy_from_slice(key);
        self.set_slot_hash_key_size(idx, key.len());
    }
    #[inline]
    fn set_slot_pkey(&mut self, idx: usize, pkey: &[u8]) {
        let base = self.slot_offset(idx) as usize + self.slot_hash_key_size(idx) as usize;
        self[base..base + pkey.len()].copy_from_slice(pkey);
        self.set_slot_pkey_size(idx, pkey.len());
    }
    #[inline]
    fn set_slot_value(&mut self, idx: usize, val: &[u8]) {
        let base = self.slot_offset(idx) as usize
            + self.slot_hash_key_size(idx) as usize
            + self.slot_pkey_size(idx) as usize;
        self[base..base + val.len()].copy_from_slice(val);
        self.set_slot_val_size(idx, val.len());
    }
}

pub trait HashCommonPage: PageOps {
    fn init(&mut self);

    fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError>;
    fn insert_entry_at_idx(
        &mut self,
        entry: &MvccEntry,
        idx: usize,
    ) -> Result<(), AccessMethodError>;

    fn update(&mut self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError>;
    fn update_entry_at_idx(
        &mut self,
        entry: &MvccEntry,
        idx: usize,
    ) -> Result<MvccEntry, AccessMethodError>;

    fn search_pkey(&self, pkey: &[u8]) -> (bool, usize); // (found, idx)
}

impl HashCommonPage for Page {
    fn init(&mut self) {
        self.set_hdr_next_page_id(PageId::MAX);
        self.set_hdr_next_frame_id(u32::MAX);
        self.set_hdr_total_bytes_used(HEADER_SIZE);
        self.set_hdr_slot_count(0);
        self.set_hdr_rec_start_off(AVAILABLE_PAGE_SIZE);
        self.set_hdr_recent_entry_cnt(0);
        self.set_hdr_min_start_ts(u64::MAX);
        self.set_hdr_max_end_ts(0);
        self.set_hdr_is_full(false);
    }

    fn insert(&mut self, entry: &MvccEntry) -> Result<(), AccessMethodError> {
        let payload_len = entry.key().len() + entry.pkey().len() + entry.value().len() + SLOT_SIZE;
        if AVAILABLE_PAGE_SIZE < payload_len {
            return Err(AccessMethodError::RecordTooLarge);
        } else if self.hdr_rec_start_off() - slot_base(self.hdr_slot_count()) < payload_len {
            if AVAILABLE_PAGE_SIZE - self.hdr_total_bytes_used() < payload_len {
                return Err(AccessMethodError::OutOfSpace);
            }
            // TODO: neeed to compact the page
            return Err(AccessMethodError::OutOfSpace);
        }
        self.insert_entry_at_idx(entry, self.hdr_slot_count())
    }
    fn insert_entry_at_idx(
        &mut self,
        entry: &MvccEntry,
        idx: usize,
    ) -> Result<(), AccessMethodError> {
        let slot_cnt = self.hdr_slot_count();

        let payload_len = entry.key().len() + entry.pkey().len() + entry.value().len();
        let new_rec_start = self.hdr_rec_start_off() - payload_len;

        if idx < slot_cnt {
            let from = slot_base(idx);
            let to = slot_base(idx + 1);
            let bytes = (slot_cnt - idx) * SLOT_SIZE;
            self.copy_within(from..from + bytes, to);
        }

        let mut cur = new_rec_start;
        self[cur..cur + entry.key().len()].copy_from_slice(entry.key());
        cur += entry.key().len();
        self[cur..cur + entry.pkey.len()].copy_from_slice(entry.pkey());
        cur += entry.pkey().len();
        self[cur..cur + entry.value.len()].copy_from_slice(entry.value());

        self.set_slot_offset(idx, new_rec_start);
        self.set_slot_hash_key_size(idx, entry.key().len());
        self.set_slot_pkey_size(idx, entry.pkey().len());
        self.set_slot_val_size(idx, entry.value().len());
        self.set_slot_start_ts(idx, entry.start_ts());
        self.set_slot_end_ts(idx, entry.end_ts());
        self.set_slot_tx_id(idx, entry.tx_id());

        let mut hk_pref = [0u8; 8];
        hk_pref[..entry.key().len().min(8)]
            .copy_from_slice(&entry.key()[..entry.key().len().min(8)]);
        self.set_slot_hash_key_prefix(idx, &hk_pref);

        let mut pk_pref = [0u8; 8];
        pk_pref[..entry.pkey().len().min(8)]
            .copy_from_slice(&entry.pkey()[..entry.pkey().len().min(8)]);
        self.set_slot_pkey_prefix(idx, &pk_pref);

        self.set_hdr_slot_count(slot_cnt + 1);
        self.set_hdr_rec_start_off(new_rec_start);
        self.set_hdr_total_bytes_used(self.hdr_total_bytes_used() + SLOT_SIZE + payload_len);

        if entry.start_ts() < self.hdr_min_start_ts() {
            self.set_hdr_min_start_ts(entry.start_ts());
        }
        if entry.end_ts() == Timestamp::MAX {
            self.set_hdr_recent_entry_cnt(self.hdr_recent_entry_cnt() + 1);
        } else if entry.end_ts() > self.hdr_max_end_ts() {
            self.set_hdr_max_end_ts(entry.end_ts());
        }

        Ok(())
    }

    fn update(&mut self, pkey: &[u8], entry: &MvccEntry) -> Result<MvccEntry, AccessMethodError> {
        let (found, idx) = self.search_pkey(pkey);
        if !found {
            return Err(AccessMethodError::KeyNotFound);
        }
        self.update_entry_at_idx(entry, idx)
    }

    fn update_entry_at_idx(
        &mut self,
        entry: &MvccEntry,
        idx: usize,
    ) -> Result<MvccEntry, AccessMethodError> {
        // only vlaue and end_ts are updated now
        let old_val = self.slot_value(idx).to_vec();
        let old_start_ts = self.slot_start_ts(idx);

        self.set_slot_value(idx, entry.value());
        self.set_slot_start_ts(idx, entry.start_ts());

        Ok(MvccEntry::new(
            entry.key().to_vec(),
            entry.pkey().to_vec(),
            old_val,
            old_start_ts,
            entry.end_ts,
        ))
    }

    fn search_pkey(&self, pkey: &[u8]) -> (bool, usize) {
        let slot_cnt = self.hdr_slot_count();
        for idx in 0..slot_cnt {
            if self.slot_pkey_size(idx) == pkey.len()
                && self.slot_pkey_prefix(idx)[..pkey.len().min(8)] == pkey[..pkey.len().min(8)]
                && (pkey.len() <= 8 || self.slot_pkey(idx) == pkey)
            {
                return (true, idx);
            }
        }
        (false, slot_cnt)
    }
}

pub trait HeapPage: HashCommonPage {
    fn update_write_repair(
        &mut self,
        entry: &MvccEntry,
        inserted: bool,
        repaired: bool,
    ) -> Result<(), AccessMethodError>;
}
impl HeapPage for Page {
    fn update_write_repair(
        &mut self,
        entry: &MvccEntry,
        inserted: bool,
        repaired: bool,
    ) -> Result<(), AccessMethodError> {
        let mut did_repair = repaired;
        let mut did_insert = inserted;

        if !repaired {
            for idx in 0..self.hdr_slot_count() {
                if self.slot_end_ts(idx) == Timestamp::MAX
                    && self.slot_pkey_size(idx) == entry.pkey().len()
                    && self.slot_pkey_prefix(idx)[..entry.pkey().len().min(8)]
                        == entry.pkey()[..entry.pkey().len().min(8)]
                    && (entry.pkey().len() <= 8 || self.slot_pkey(idx) == entry.pkey())
                {
                    did_repair = true;
                    self.set_slot_end_ts(idx, entry.start_ts());
                    if self.hdr_max_end_ts() < entry.start_ts() {
                        self.set_hdr_max_end_ts(entry.start_ts());
                    }
                    self.set_hdr_recent_entry_cnt(self.hdr_recent_entry_cnt() - 1);
                    break;
                }
            }
        }

        if !inserted {
            match self.insert(entry) {
                Ok(_) => did_insert = true,
                Err(AccessMethodError::OutOfSpace) => {}
                Err(e) => {
                    return Err(e);
                }
            }
        }

        match (did_repair, did_insert) {
            (true, true) => Ok(()),
            (true, false) => Err(AccessMethodError::UpdateReapiredButNotInseted),
            (false, true) => Err(AccessMethodError::UpdateInsertedButNotReapired),
            (false, false) => Err(AccessMethodError::KeyNotFound),
        }
    }
}
pub trait RecentPage: HashCommonPage {}
pub trait HistoryPage: HashCommonPage {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        let mut page = Page::new_empty();
        page.init();
        assert_eq!(page.hdr_next_page_id(), PageId::MAX);
        assert_eq!(page.hdr_next_frame_id(), u32::MAX);
        assert_eq!(page.hdr_total_bytes_used(), HEADER_SIZE);
        assert_eq!(page.hdr_slot_count(), 0);
        assert_eq!(page.hdr_rec_start_off(), AVAILABLE_PAGE_SIZE);
        assert_eq!(page.hdr_recent_entry_cnt(), 0);
        assert_eq!(page.hdr_min_start_ts(), u64::MAX);
        assert_eq!(page.hdr_max_end_ts(), 0);
        assert_eq!(page.hdr_is_full(), false);
    }

    #[test]
    fn test_slot_methods() {
        let mut page = Page::new_empty();
        // write a slot at idx 0
        page.set_slot_hash_key_size(0, 11);
        page.set_slot_pkey_size(0, 22);
        page.set_slot_hash_key_prefix(0, b"12345678");
        page.set_slot_pkey_prefix(0, b"ABCDEFGH");
        page.set_slot_start_ts(0, 33);
        page.set_slot_end_ts(0, 44);
        page.set_slot_val_size(0, 55);
        page.set_slot_offset(0, 66);
        page.set_slot_tx_id(0, 77);

        assert_eq!(page.slot_hash_key_size(0), 11);
        assert_eq!(page.slot_pkey_size(0), 22);
        assert_eq!(page.slot_hash_key_prefix(0), b"12345678");
        assert_eq!(page.slot_pkey_prefix(0), b"ABCDEFGH");
        assert_eq!(page.slot_start_ts(0), 33);
        assert_eq!(page.slot_end_ts(0), 44);
        assert_eq!(page.slot_val_size(0), 55);
        assert_eq!(page.slot_offset(0), 66);
        assert_eq!(page.slot_tx_id(0), 77);
    }
}
