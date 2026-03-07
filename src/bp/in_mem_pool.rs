use std::{
    cell::UnsafeCell,
    collections::{hash_map::Entry, HashMap},
};

use crate::{page::Page, rwlatch::RwLatch};

use super::{
    buffer_frame::BufferFrame,
    mem_pool_trait::{MemPool, MemoryStats, PageKey},
    prelude::{ContainerKey, FrameReadGuard, FrameWriteGuard, MemPoolStatus, PageFrameKey},
};

/// A simple in-memory page pool.
/// All the pages are stored in a vector in memory.
/// A latch is used to synchronize access to the pool.
/// An exclusive latch is required to create a new page and append it to the pool.
/// Getting a page for read or write requires a shared latch.

#[derive(Clone, Copy)]
struct FrameLocator {
    slab_index: usize,
    frame_index: usize,
}

struct FrameSlab {
    frames: Box<[BufferFrame]>,
}

struct FrameRegistry {
    flat_index: Vec<FrameLocator>,
    slabs: Vec<FrameSlab>,
}

impl FrameRegistry {
    fn new() -> Self {
        Self {
            flat_index: Vec::new(),
            slabs: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.flat_index.len()
    }

    fn reserve_flat(&mut self, additional: usize) {
        self.flat_index.reserve(additional);
        self.slabs.reserve(1);
    }

    fn push_frame(&mut self, frame: BufferFrame) -> usize {
        self.push_slab(vec![frame]).start
    }

    fn push_slab(&mut self, frames: Vec<BufferFrame>) -> std::ops::Range<usize> {
        let start = self.flat_index.len();
        let slab_index = self.slabs.len();
        let frames = frames.into_boxed_slice();
        let frame_count = frames.len();
        self.slabs.push(FrameSlab { frames });
        self.flat_index.reserve(frame_count);
        for frame_index in 0..frame_count {
            self.flat_index.push(FrameLocator {
                slab_index,
                frame_index,
            });
        }
        start..start + frame_count
    }

    fn get(&self, flat_index: usize) -> &BufferFrame {
        let locator = self.flat_index[flat_index];
        &self.slabs[locator.slab_index].frames[locator.frame_index]
    }

    fn iter(&self) -> impl Iterator<Item = &BufferFrame> {
        self.flat_index.iter().map(move |locator| {
            &self.slabs[locator.slab_index].frames[locator.frame_index]
        })
    }
}

pub struct InMemPool {
    latch: RwLatch,
    frames: UnsafeCell<FrameRegistry>,
    id_to_index: UnsafeCell<HashMap<PageKey, usize>>,
    container_page_count: UnsafeCell<HashMap<ContainerKey, u32>>,
}

impl Default for InMemPool {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemPool {
    pub fn new() -> Self {
        InMemPool {
            latch: RwLatch::default(),
            frames: UnsafeCell::new(FrameRegistry::new()),
            id_to_index: UnsafeCell::new(HashMap::new()),
            container_page_count: UnsafeCell::new(HashMap::new()),
        }
    }

    fn shared(&self) {
        self.latch.shared();
    }

    fn exclusive(&self) {
        self.latch.exclusive();
    }

    fn release_shared(&self) {
        self.latch.release_shared();
    }

    fn release_exclusive(&self) {
        self.latch.release_exclusive();
    }
}

impl MemPool for InMemPool {
    fn create_new_page_for_write(
        &self,
        c_key: ContainerKey,
    ) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.exclusive();
        let frames = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_page_count = unsafe { &mut *self.container_page_count.get() };

        let page_id = match container_page_count.entry(c_key) {
            Entry::Occupied(mut entry) => {
                let page_id = *entry.get();
                *entry.get_mut() += 1;
                page_id
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
                0
            }
        };
        let page_key = PageKey::new(c_key, page_id);
        let frame_index = frames.len();
        let frame = BufferFrame::new(frame_index as u32);
        frames.push_frame(frame);
        id_to_index.insert(page_key, frame_index);
        let mut guard = frames.get(frame_index).write(true);
        guard.set_id(page_id);
        guard.set_lsn(crate::write_ahead_log::prelude::Lsn::new(0, 0));
        *guard.page_key_mut() = Some(page_key);
        self.release_exclusive();
        Ok(guard)
    }

    fn create_new_pages_for_write(
        &self,
        c_key: ContainerKey,
        count: usize,
    ) -> Result<Vec<FrameWriteGuard>, MemPoolStatus> {
        if count == 0 {
            return Ok(Vec::new());
        }

        self.exclusive();
        let frames = unsafe { &mut *self.frames.get() };
        let id_to_index = unsafe { &mut *self.id_to_index.get() };
        let container_page_count = unsafe { &mut *self.container_page_count.get() };

        // Reserve capacity once
        frames.reserve_flat(count);
        id_to_index.reserve(count);

        let start_page_id = match container_page_count.entry(c_key) {
            Entry::Occupied(mut entry) => {
                let start = *entry.get();
                *entry.get_mut() += count as u32;
                start
            }
            Entry::Vacant(entry) => {
                entry.insert(count as u32);
                0
            }
        };

        let base_frame_index = frames.len();
        #[cfg(feature = "heap_allocated_page")]
        let mut bulk_pages = Page::new_empty_pages_in_slab(count).into_iter();

        let mut to_init = Vec::with_capacity(count);
        let mut new_frames = Vec::with_capacity(count);
        let mut guards = Vec::with_capacity(count);
        for i in 0..count {
            let page_id = start_page_id + i as u32;
            let page_key = PageKey::new(c_key, page_id);
            let frame_index = base_frame_index + i;
            #[cfg(feature = "heap_allocated_page")]
            let frame = BufferFrame::with_page(frame_index as u32, bulk_pages.next().unwrap());
            #[cfg(not(feature = "heap_allocated_page"))]
            let frame = BufferFrame::new(frame_index as u32);
            new_frames.push(frame);
            id_to_index.insert(page_key, frame_index);
            to_init.push((frame_index, page_id, page_key));
        }
        frames.push_slab(new_frames);

        for (frame_index, page_id, page_key) in to_init {
            let mut guard = frames.get(frame_index).write(true);
            guard.set_id(page_id);
            guard.set_lsn(crate::write_ahead_log::prelude::Lsn::new(0, 0));
            *guard.page_key_mut() = Some(page_key);
            guards.push(guard);
        }
        self.release_exclusive();
        Ok(guards)
    }

    fn get_page_for_write(&self, key: PageFrameKey) -> Result<FrameWriteGuard, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let frame_index = match id_to_index.get(&key.p_key()) {
            Some(index) => *index,
            None => {
                self.release_shared();
                return Err(MemPoolStatus::PageNotFound);
            }
        };

        let frame = frames.get(frame_index).try_write(true);
        self.release_shared();
        if let Some(frame) = frame {
            Ok(frame)
        } else {
            Err(MemPoolStatus::FrameWriteLatchGrantFailed)
        }
    }

    fn get_page_for_read(&self, key: PageFrameKey) -> Result<FrameReadGuard, MemPoolStatus> {
        self.shared();
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        let frame_index = match id_to_index.get(&key.p_key()) {
            Some(index) => *index,
            None => {
                self.release_shared();
                return Err(MemPoolStatus::PageNotFound);
            }
        };

        let frame = frames.get(frame_index).try_read();
        self.release_shared();
        if let Some(frame) = frame {
            Ok(frame)
        } else {
            Err(MemPoolStatus::FrameReadLatchGrantFailed)
        }
    }

    fn prefetch_page(&self, _key: PageFrameKey) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn stats(&self) -> MemoryStats {
        let num_frames = unsafe { &*self.frames.get() }.len();
        MemoryStats {
            num_frames_in_mem: num_frames,
            new_page_created: num_frames,
            read_page_from_disk: num_frames,
            write_page_to_disk: num_frames,
        }
    }

    fn reset_stats(&self) {
        // Do nothing
    }

    fn flush_all(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn flush_all_and_reset(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn clear_dirty_flags(&self) -> Result<(), MemPoolStatus> {
        Ok(())
    }

    fn fast_evict(&self, _frame_id: u32) -> Result<(), MemPoolStatus> {
        Ok(())
    }
}

#[cfg(test)]
impl InMemPool {
    pub fn check_all_frames_unlatched(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            frame.try_write(false).unwrap();
        }
    }

    // Invariant: id_to_index contains all pages in frames
    pub fn check_id_to_index(&self) {
        let frames = unsafe { &*self.frames.get() };
        let id_to_index = unsafe { &*self.id_to_index.get() };
        for (key, index) in id_to_index.iter() {
            let frame = frames.get(*index);
            let frame = frame.read();
            assert_eq!(*frame.page_key(), Some(*key));
        }
    }

    pub fn check_frame_id_and_page_id_match(&self) {
        let frames = unsafe { &*self.frames.get() };
        for frame in frames.iter() {
            let frame = frame.read();
            let key = frame.page_key().unwrap();
            let page_id = frame.get_id();
            assert_eq!(key.page_id, page_id);
        }
    }
}

unsafe impl Sync for InMemPool {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_mp_and_frame_latch() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        let frame = mp.create_new_page_for_write(c_key).unwrap();
        let page_key = frame.page_frame_key().unwrap();
        drop(frame);

        let num_threads = 3;
        let num_iterations = 80;
        thread::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|| {
                    for _ in 0..num_iterations {
                        loop {
                            if let Ok(mut guard) = mp.get_page_for_write(page_key) {
                                guard[0] += 1;
                                break;
                            } else {
                                // spin
                                println!("spin: {:?}", thread::current().id());
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            }
        });

        mp.check_all_frames_unlatched();
        mp.check_id_to_index();
        mp.check_frame_id_and_page_id_match();
        let guard = mp.get_page_for_read(page_key).unwrap();
        assert_eq!(guard[0], num_threads * num_iterations);
    }

    #[test]
    fn test_create_new_page() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        for i in 0..20 {
            let frame = mp.create_new_page_for_write(c_key).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
            drop(frame);
        }

        for i in 0..20 {
            let frame = mp.get_page_for_read(PageFrameKey::new(c_key, i)).unwrap();
            assert_eq!(frame.page_key().unwrap(), PageKey::new(c_key, i));
        }

        mp.check_all_frames_unlatched();
        mp.check_id_to_index();
        mp.check_frame_id_and_page_id_match();
    }

    #[test]
    fn test_concurrent_create_new_page() {
        let mp = InMemPool::new();
        let c_key = ContainerKey { db_id: 0, c_id: 0 };

        let mut frame1 = mp.create_new_page_for_write(c_key).unwrap();
        frame1[0] = 1;
        let mut frame2 = mp.create_new_page_for_write(c_key).unwrap();
        frame2[0] = 2;
        assert_eq!(frame1.page_key().unwrap(), PageKey::new(c_key, 0));
        assert_eq!(frame2.page_key().unwrap(), PageKey::new(c_key, 1));
        drop(frame1);
        drop(frame2);

        let frame1 = mp.get_page_for_read(PageFrameKey::new(c_key, 0)).unwrap();
        let frame2 = mp.get_page_for_read(PageFrameKey::new(c_key, 1)).unwrap();
        assert_eq!(frame1[0], 1);
        assert_eq!(frame2[0], 2);
    }
}
