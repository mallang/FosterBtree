use std::collections::HashMap;

use parking_lot::RwLock;

use crate::prelude::Timestamp;

const HEAP_PAGE_CAPACITY: usize = 256;

#[derive(Clone)]
struct HeapBaseVersion {
    begin_ts: Timestamp,
    key: Vec<u8>,
    pkey: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Default)]
struct HeapBasePage {
    versions: Vec<HeapBaseVersion>,
}

/// A page-oriented heap base-table model for benchmark baselines.
///
/// The representation keeps no persistent primary-key index. Inserts and
/// updates append new versions to the tail page, and `scan_as_of()` performs a
/// full heap scan with visibility checks to reconstruct the snapshot.
pub struct HeapBaseMvccTable {
    pages: RwLock<Vec<HeapBasePage>>,
}

impl HeapBaseMvccTable {
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(Vec::new()),
        }
    }

    pub fn insert_at_ts(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        self.append_version(key, pkey, value, ts);
    }

    pub fn update_at_ts(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        self.append_version(key, pkey, value, ts);
    }

    fn append_version(&self, key: &[u8], pkey: &[u8], value: &[u8], ts: Timestamp) {
        let mut pages = self.pages.write();
        let needs_new_page = pages
            .last()
            .map_or(true, |page| page.versions.len() >= HEAP_PAGE_CAPACITY);
        if needs_new_page {
            pages.push(HeapBasePage::default());
        }

        let page = pages.last_mut().unwrap();
        page.versions.push(HeapBaseVersion {
            begin_ts: ts,
            key: key.to_vec(),
            pkey: pkey.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn scan_as_of(&self, ts: Timestamp) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let pages = self.pages.read();
        let mut visible = HashMap::<Vec<u8>, (Timestamp, Vec<u8>, Vec<u8>)>::new();

        for page in pages.iter() {
            for version in &page.versions {
                if version.begin_ts > ts {
                    continue;
                }

                match visible.get_mut(&version.pkey) {
                    Some((best_ts, best_key, best_value)) if *best_ts <= version.begin_ts => {
                        *best_ts = version.begin_ts;
                        *best_key = version.key.clone();
                        *best_value = version.value.clone();
                    }
                    None => {
                        visible.insert(
                            version.pkey.clone(),
                            (version.begin_ts, version.key.clone(), version.value.clone()),
                        );
                    }
                    _ => {}
                }
            }
        }

        visible
            .into_iter()
            .map(|(pkey, (_, key, value))| (key, pkey, value))
            .collect()
    }

    pub fn get_as_of(&self, pkey: &[u8], ts: Timestamp) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let pages = self.pages.read();
        let mut best: Option<(Timestamp, Vec<u8>, Vec<u8>)> = None;

        for page in pages.iter() {
            for version in &page.versions {
                if version.pkey != pkey || version.begin_ts > ts {
                    continue;
                }
                match &mut best {
                    Some((best_ts, best_key, best_value)) if *best_ts <= version.begin_ts => {
                        *best_ts = version.begin_ts;
                        *best_key = version.key.clone();
                        *best_value = version.value.clone();
                    }
                    None => {
                        best = Some((
                            version.begin_ts,
                            version.key.clone(),
                            version.value.clone(),
                        ));
                    }
                    _ => {}
                }
            }
        }

        best.map(|(_, key, value)| (key, pkey.to_vec(), value))
    }
}
