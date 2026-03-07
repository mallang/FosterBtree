use std::ops::{Deref, DerefMut};
#[cfg(feature = "heap_allocated_page")]
use std::{ptr::NonNull, sync::Arc};

use crate::write_ahead_log::prelude::{Lsn, LSN_SIZE};

#[cfg(feature = "test_page")]
pub const PAGE_SIZE: usize = 256;

// A slot offset is u32, so the maximum page size is 2^32 bytes
#[cfg(feature = "4k_page")]
pub const PAGE_SIZE: usize = 4096;
#[cfg(feature = "8k_page")]
pub const PAGE_SIZE: usize = 8192;
#[cfg(feature = "16k_page")]
pub const PAGE_SIZE: usize = 16384;
#[cfg(feature = "32k_page")]
pub const PAGE_SIZE: usize = 32768;
#[cfg(feature = "64k_page")]
pub const PAGE_SIZE: usize = 65536;
#[cfg(feature = "128k_page")]
pub const PAGE_SIZE: usize = 131072;
#[cfg(feature = "256k_page")]
pub const PAGE_SIZE: usize = 262144;
#[cfg(feature = "512k_page")]
pub const PAGE_SIZE: usize = 524288;
#[cfg(feature = "1m_page")]
pub const PAGE_SIZE: usize = 1048576;
// If nothing is specified, use 16K page
#[cfg(not(any(
    feature = "test_page",
    feature = "4k_page",
    feature = "8k_page",
    feature = "16k_page",
    feature = "32k_page",
    feature = "64k_page",
    feature = "128k_page",
    feature = "256k_page",
    feature = "512k_page",
    feature = "1m_page"
)))]
pub const PAGE_SIZE: usize = 16384;

pub type PageId = u32;
const BASE_PAGE_HEADER_SIZE: usize = ((4 + LSN_SIZE) + 7) & (!7);
pub const AVAILABLE_PAGE_SIZE: usize = PAGE_SIZE - BASE_PAGE_HEADER_SIZE;

#[cfg(all(feature = "heap_allocated_page", not(unix)))]
compile_error!("heap_allocated_page requires unix mmap-backed storage");

#[cfg(feature = "heap_allocated_page")]
struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
}

#[cfg(feature = "heap_allocated_page")]
impl MmapRegion {
    fn new(len: usize) -> Self {
        use std::{ffi::c_void, io};

        #[cfg(any(target_os = "macos", target_os = "ios"))]
        const MAP_ANON_FLAG: i32 = 0x1000;
        #[cfg(not(any(target_os = "macos", target_os = "ios")))]
        const MAP_ANON_FLAG: i32 = 0x20;

        const PROT_READ: i32 = 0x1;
        const PROT_WRITE: i32 = 0x2;
        const MAP_PRIVATE: i32 = 0x2;

        unsafe extern "C" {
            fn mmap(
                addr: *mut c_void,
                len: usize,
                prot: i32,
                flags: i32,
                fd: i32,
                offset: i64,
            ) -> *mut c_void;
        }

        let raw = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANON_FLAG,
                -1,
                0,
            )
        };
        if raw == (-1isize as *mut c_void) {
            panic!("mmap failed for {} bytes: {}", len, io::Error::last_os_error());
        }
        let ptr = NonNull::new(raw.cast::<u8>()).unwrap();
        Self { ptr, len }
    }

    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }
}

#[cfg(feature = "heap_allocated_page")]
impl Drop for MmapRegion {
    fn drop(&mut self) {
        use std::{ffi::c_void, io};

        unsafe extern "C" {
            fn munmap(addr: *mut c_void, len: usize) -> i32;
        }

        let rc = unsafe { munmap(self.ptr.as_ptr().cast::<c_void>(), self.len) };
        debug_assert_eq!(rc, 0, "munmap failed: {}", io::Error::last_os_error());
    }
}

#[cfg(feature = "heap_allocated_page")]
unsafe impl Send for MmapRegion {}
#[cfg(feature = "heap_allocated_page")]
unsafe impl Sync for MmapRegion {}

#[cfg(feature = "heap_allocated_page")]
pub(crate) struct PageSlab {
    region: MmapRegion,
    page_count: usize,
}

#[cfg(feature = "heap_allocated_page")]
impl PageSlab {
    fn new(page_count: usize) -> Self {
        let len = PAGE_SIZE
            .checked_mul(page_count)
            .expect("page slab size overflow");
        Self {
            region: MmapRegion::new(len),
            page_count,
        }
    }

    fn page_ptr(&self, page_index: usize) -> NonNull<u8> {
        assert!(page_index < self.page_count);
        unsafe {
            NonNull::new_unchecked(self.region.ptr().as_ptr().add(page_index * PAGE_SIZE))
        }
    }
}

#[cfg(feature = "heap_allocated_page")]
unsafe impl Send for PageSlab {}
#[cfg(feature = "heap_allocated_page")]
unsafe impl Sync for PageSlab {}

#[cfg(feature = "heap_allocated_page")]
enum PageOwner {
    Single(MmapRegion),
    Slab(Arc<PageSlab>),
}

#[cfg(feature = "heap_allocated_page")]
pub struct Page {
    ptr: NonNull<u8>,
    _owner: PageOwner,
} // A page with large size must be heap allocated. Otherwise, it will cause stack overflow during the test.
#[cfg(not(feature = "heap_allocated_page"))]
pub struct Page([u8; PAGE_SIZE]);

impl Page {
    #[cfg(feature = "heap_allocated_page")]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    #[cfg(feature = "heap_allocated_page")]
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), PAGE_SIZE) }
    }

    #[cfg(not(feature = "heap_allocated_page"))]
    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    #[cfg(not(feature = "heap_allocated_page"))]
    fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }

    pub fn new(page_id: PageId) -> Self {
        let mut page = Self::new_empty();
        page.set_id(page_id);
        page.set_lsn(Lsn::new(0, 0));
        page
    }

    pub fn new_empty() -> Self {
        #[cfg(feature = "heap_allocated_page")]
        {
            let region = MmapRegion::new(PAGE_SIZE);
            let ptr = region.ptr();
            return Page {
                ptr,
                _owner: PageOwner::Single(region),
            };
        }
        #[cfg(not(feature = "heap_allocated_page"))]
        return Page([0; PAGE_SIZE]);
    }

    #[cfg(feature = "heap_allocated_page")]
    pub(crate) fn new_empty_pages_in_slab(count: usize) -> Vec<Self> {
        if count == 0 {
            return Vec::new();
        }
        let slab = Arc::new(PageSlab::new(count));
        let mut pages = Vec::with_capacity(count);
        for page_index in 0..count {
            pages.push(Page {
                ptr: slab.page_ptr(page_index),
                _owner: PageOwner::Slab(Arc::clone(&slab)),
            });
        }
        pages
    }

    pub fn copy(&mut self, other: &Page) {
        self.as_slice_mut().copy_from_slice(other.as_slice());
    }

    pub fn copy_data_only(&mut self, other: &Page) {
        self.as_slice_mut()[BASE_PAGE_HEADER_SIZE..]
            .copy_from_slice(&other.as_slice()[BASE_PAGE_HEADER_SIZE..]);
    }

    fn base_header(&self) -> BasePageHeader {
        BasePageHeader::from_bytes(&self.as_slice()[0..BASE_PAGE_HEADER_SIZE].try_into().unwrap())
    }

    pub fn get_id(&self) -> PageId {
        self.base_header().id
    }

    pub fn set_id(&mut self, id: PageId) {
        let mut header = self.base_header();
        header.id = id;
        self.as_slice_mut()[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn get_lsn(&self) -> Lsn {
        self.base_header().lsn
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        let mut header = self.base_header();
        header.lsn = lsn;
        self.as_slice_mut()[0..BASE_PAGE_HEADER_SIZE].copy_from_slice(&header.to_bytes());
    }

    pub fn get_raw_bytes(&self) -> &[u8] {
        self.as_slice()
    }

    pub fn get_raw_bytes_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

#[cfg(feature = "heap_allocated_page")]
unsafe impl Send for Page {}
#[cfg(feature = "heap_allocated_page")]
unsafe impl Sync for Page {}

struct BasePageHeader {
    id: u32,
    lsn: Lsn,
}

impl BasePageHeader {
    fn from_bytes(bytes: &[u8; BASE_PAGE_HEADER_SIZE]) -> Self {
        let id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let lsn = Lsn::from_bytes(&bytes[4..4 + LSN_SIZE].try_into().unwrap());
        BasePageHeader { id, lsn }
    }

    fn to_bytes(&self) -> [u8; BASE_PAGE_HEADER_SIZE] {
        let id_bytes = self.id.to_be_bytes();
        let lsn_bytes = self.lsn.to_bytes();
        let mut bytes = [0; BASE_PAGE_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&id_bytes);
        bytes[4..4 + LSN_SIZE].copy_from_slice(&lsn_bytes);
        bytes
    }
}

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.as_slice()[BASE_PAGE_HEADER_SIZE..]
    }
}

impl DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.as_slice_mut()[BASE_PAGE_HEADER_SIZE..]
    }
}
