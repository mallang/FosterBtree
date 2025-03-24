use crate::{
    bp::{ContainerKey, FrameReadGuard, MemPool, PageFrameKey},
    log_warn,
    mvcc_index::{
        hash_common::{read_page, write_page}, hybrid_hash::{
            attached_container_page::attached_container_page::{
                slot::{InterPageLoc, Slot},
                CommittedRecord, WritePageLocAgent,
            },
            hash_join_table_common::HashTableAccessMethodError,
            hybrid_hash_table::hybrid_hash_slot_page::TableSlotsPage,
        }, Delta, MvccEntry
    },
};

use crate::prelude::Timestamp;

use super::attached_container_page::{slot::SlotMeta, AttachedPage, CommittedRecordOwned};

///  None:: not match,
///  Some(_, _ None) : NotExistAtTimestamp or Deleted
///  Some(_, _, Some(_)) : not deleted
pub fn find_pivot_rec_to_write<T: MemPool + 'static>(
    mem_pool: &T,
    slot: &Slot,
    ts: Timestamp,
    c_key: ContainerKey,
    key: &[u8],
    pkey: &[u8],
    is_only_key: bool,
) -> Option<(WritePageLocAgent, InterPageLoc, Option<(Vec<u8>, Vec<u8>)>)> {
    let (cur_page_id, _b_off) = (slot.meta_loc().page_id, slot.meta_loc().b_offset);
    let pfk = PageFrameKey::new(c_key, cur_page_id);
    let page = read_page(mem_pool, pfk);
    let meta = (*page).get_slot_meta(slot);
    let is_match = if is_only_key {
        meta.check_match_remain_key(slot, key)
    } else {
        meta.check_match_remain_key_pkey(slot, key, pkey)
    };
    if !is_match {
        return None;
    }
    log_warn!("meta in find_func: {:?}", meta);
    let mut next_version_loc = meta.latest_version_loc;
    let remain_pkey = meta.remain_pkey.to_vec();
    drop(page);

    let mut write_agent = WritePageLocAgent::new(slot.meta_loc().b_offset, slot.meta_loc().page_id);
    let mut cur_start_ts = Timestamp::MAX;
    let mut cur_version_loc = InterPageLoc {
        page_id: cur_page_id,
        b_offset: _b_off,
    };
    let val = loop {
        if next_version_loc == InterPageLoc::new_end() {
            write_agent = WritePageLocAgent::new(cur_version_loc.b_offset, cur_version_loc.page_id);
            return Some((write_agent, next_version_loc, None));
        }
        let pfk = PageFrameKey::new(c_key, next_version_loc.page_id);
        let page = read_page(mem_pool, pfk);
        let cur_version = (*page).get_record(next_version_loc.b_offset);
        log_warn!("record: {:?}", cur_version);

        if cur_start_ts != Timestamp::MAX {
            write_agent = WritePageLocAgent::new(cur_version_loc.b_offset, cur_version_loc.page_id);
        }
        cur_start_ts = cur_version.start_ts;
        cur_version_loc = next_version_loc;
        next_version_loc = cur_version.prev_offset;

        if cur_start_ts <= ts {
            break if cur_version.is_deleted() {
                None
            } else {
                Some(cur_version.value.to_vec())
            };
        }
    };

    return Some((write_agent, cur_version_loc, val.map(|v| (remain_pkey, v))));
}

pub fn add_version<T: MemPool + 'static>(
    cur_page: &mut impl TableSlotsPage,
    c_key: ContainerKey,
    slot: &Slot,
    mem_pool: &T,
    value: &[u8],
    ts: Timestamp,
    is_delete: bool,
    key: &[u8],
    pkey: &[u8],
) -> bool {
    let find_res = find_pivot_rec_to_write(mem_pool, slot, ts, c_key, key, pkey, false);
    if find_res.is_none() {
        return false;
    }
    let (prev_node_update_agent, next_loc, _) = find_res.unwrap();
    let attach_page_id = cur_page.get_attached_page_id();
    let pfk = PageFrameKey::new(c_key, attach_page_id);
    let mut attached_page = write_page(mem_pool, pfk);
    let new_rec_write_page: &mut dyn AttachedPage = &mut *attached_page;
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
            let mut new_page = mem_pool.create_new_page_for_write(c_key).unwrap();
            let new_page_id = new_page.get_id();
            cur_page.set_attached_page_id(new_page_id);
            let new_write_page: &mut dyn AttachedPage = &mut *new_page;
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
    drop(attached_page);

    let mut prev_page = write_page(
        mem_pool,
        PageFrameKey::new(c_key, prev_node_update_agent.page_id),
    );
    let prev_write_page: &mut dyn AttachedPage = &mut *prev_page;
    prev_write_page.write_bytes(prev_node_update_agent.offset, &update_loc_bytes);
    true
}

pub fn get_le_ts_version<T: MemPool + 'static>(
    mut loc: InterPageLoc,
    mem_pool: &T,
    c_key: ContainerKey,
    ts: Timestamp,
) -> Option<CommittedRecordOwned> {
    loop {
        if loc == InterPageLoc::new_end() {
            return None;
        }
        let pfk = PageFrameKey::new(c_key, loc.page_id);
        let page = read_page(mem_pool, pfk);
        let record = (*page).get_record(loc.b_offset);
        if record.start_ts <= ts {
            return Some(record.to_owned());
        }
        loc = record.prev_offset;
    }
}

pub fn get_delta<T: MemPool + 'static>(
    mem_pool: &T,
    c_key: ContainerKey,
    loc: InterPageLoc,
    from_ts: Timestamp,
    to_ts: Timestamp,
) -> Option<Delta<Vec<u8>>> {
    let vto = get_le_ts_version(loc, mem_pool, c_key, to_ts);
    if vto.is_none() {
        // to_ts is too small, oldest version > to_ts
        return None;
    }
    let vto = vto.unwrap();
    if vto.start_ts <= from_ts {
        // not changed btw from_ts and to_ts
        return None;
    }
    let vfrom = get_le_ts_version(vto.prev_offset, mem_pool, c_key, from_ts);
    if vfrom.is_none() {
        // from_ts is too small, vfrom not exist

        if vto.is_deleted() {
            // vto is deleted
            return None;
        }

        // vto is existed
        return Some(Delta::Inserted(vto.value));
    }

    // vfrom is existed
    let vfrom = vfrom.unwrap();
    if vto.is_deleted() {
        if vfrom.is_deleted() {
            // vfrom and vto are deleted
            return None;
        }
        // vfrom is existed, vto is deleted
        return Some(Delta::Deleted);
    }
    // vfrom is existed, vto is existed
    if vfrom.value == vto.value {
        // vfrom and vto are not changed
        return None;
    }
    Some(Delta::Updated(vto.value))
}

pub fn get_all_versions<T: MemPool + 'static>(
    mem_pool: &T,
    c_key: ContainerKey,
    loc: InterPageLoc,
    key: &[u8],
    pkey: &[u8],
) -> Vec<MvccEntry> {
    let mut res = Vec::new();
    let mut cur_loc = loc;
    let mut end_ts = Timestamp::MAX;
    loop {
        if cur_loc == InterPageLoc::new_end() {
            break;
        }
        let pfk = PageFrameKey::new(c_key, cur_loc.page_id);
        let page = read_page(mem_pool, pfk);
        let record = (*page).get_record(cur_loc.b_offset);
        res.push(MvccEntry {
            start_ts: record.start_ts,
            end_ts,
            key: key.to_vec(),
            pkey: pkey.to_vec(),
            value: record.value.to_vec(),
            tx_id: 0,
        });

        end_ts = record.start_ts;
        cur_loc = record.prev_offset;
    }
    res
}
