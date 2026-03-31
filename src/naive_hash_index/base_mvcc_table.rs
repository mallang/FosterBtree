use std::{cell::RefCell, collections::BTreeMap};

use crate::prelude::Timestamp;

#[derive(Clone)]
struct BaseVersion {
    ts: Timestamp,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// A simple base-table MVCC model for the benchmark baselines.
///
/// The representation intentionally avoids a hash table: records are grouped by
/// primary key in a BTreeMap, and each key stores a time-ordered version vector.
pub struct BaseMvccTable {
    versions_by_pkey: RefCell<BTreeMap<Vec<u8>, Vec<BaseVersion>>>,
}

impl BaseMvccTable {
    pub fn new() -> Self {
        Self {
            versions_by_pkey: RefCell::new(BTreeMap::new()),
        }
    }

    pub fn insert_at_ts(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        self.upsert_at_ts(key, pkey, value, ts);
    }

    pub fn update_at_ts(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        self.upsert_at_ts(key, pkey, value, ts);
    }

    fn upsert_at_ts(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        let version = BaseVersion {
            ts,
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let mut versions_by_pkey = self.versions_by_pkey.borrow_mut();
        let versions = versions_by_pkey.entry(pkey.to_vec()).or_default();
        match versions.binary_search_by(|probe| probe.ts.cmp(&ts)) {
            Ok(idx) => versions[idx] = version,
            Err(idx) => versions.insert(idx, version),
        }
    }

    pub fn scan_as_of(&self, ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let versions_by_pkey = self.versions_by_pkey.borrow();
        versions_by_pkey
            .iter()
            .filter_map(|(pkey, versions)| {
                versions
                    .iter()
                    .rev()
                    .find(|version| version.ts <= ts)
                    .map(|version| (version.key.clone(), pkey.clone(), version.value.clone()))
            })
            .collect()
    }

    pub fn get_as_of(
        &self,
        pkey: &[u8],
        ts: Timestamp,
    ) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let versions_by_pkey = self.versions_by_pkey.borrow();
        versions_by_pkey.get(pkey).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|version| version.ts <= ts)
                .map(|version| (version.key.clone(), pkey.to_vec(), version.value.clone()))
        })
    }
}
