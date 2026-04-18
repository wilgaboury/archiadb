use core::slice;
use std::{
    array,
    cell::UnsafeCell,
    collections::HashSet,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU64, Ordering},
};

use crate::fio::Fio;

const CACHE_LINE_SIZE: usize = 64; // standard size on pretty much every system that matters
const SEGMENTS_LEN: usize = 2048;
const SEGMENT_LEN: usize = (65536 /* largest supported page size */ * 128 /* just some num */) / 8 /* bytes in 64 bit int */;
const DIRTY_MAP_SIZE: usize = 1024;
const OPEN_SLOT_VALUE: u64 = 1u64 << 63;
const NUM_HEADER_PAGES: u64 = 3;

type Segment = [AtomicU64; SEGMENT_LEN]; // ~8mb of memory, represents ~274 GB of disk assuming 4kb page

struct PageAllocator {
    // dirty set is basically a lock-free hashset with infinite linear probing (hence lock-free)
    // making the assumption here that the high bit will never be used for a page index in practice,
    // which is a pretty safe assumption because it would require the database to  be at least
    // 37 zettabytes (aka 37 trillion gigabytes, assuming a 4kb page size).
    dirty: [AtomicU64; DIRTY_MAP_SIZE],
    segments: [AtomicPtr<Segment>; SEGMENTS_LEN], // maximum of ~17 GB memory, represents ~562 terabytes of disk assuming 4kb page
    capaticy: AtomicU64,
    allocated: AtomicU64,
    fio: Fio,
}

thread_local! {
    static LOC: UnsafeCell<usize> = UnsafeCell::new(0);
}

impl PageAllocator {
    pub async fn new(fio: Fio) -> Self {
        // TODO: scan file and initalize values from disk
        Self {
            dirty: array::from_fn(|_| AtomicU64::new(OPEN_SLOT_VALUE)),
            segments: array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            capaticy: AtomicU64::new(0),
            allocated: AtomicU64::new(0),
            fio,
        }
    }

    pub fn alloc(&self) -> u64 {
        loop {
            let loc = LOC.with(|loc| unsafe { *loc.get() });
            let seg_idx = loc / SEGMENT_LEN;
            let in_seg_idx = loc % SEGMENT_LEN;
            if seg_idx >= self.capaticy.load(Ordering::Relaxed) as usize {
                LOC.with(|loc| unsafe { *loc.get() = 0 });
                continue;
            }
            let seg = &self.segments[seg_idx];
            let seg = unsafe { &*seg.load(Ordering::Acquire) };
            let word = seg[in_seg_idx].load(Ordering::Relaxed);
            let free_idx = word.leading_ones();
            if free_idx == 64 {
                continue;
            }
            let mask = 1u64 << free_idx;
            let old = seg[in_seg_idx].fetch_or(mask, Ordering::Relaxed);
            if old & mask == 0 {
                let old = self.allocated.fetch_add(1, Ordering::Relaxed);
                // TODO: .len is not accurate, need to adjust for header and bitmap blocks
                if (old + 1) as f32 / self.fio.len() as f32 > 0.75 {
                    // TODO: allocate new segment
                }
                let pg_idx_no_head = free_idx as u64 + (loc as u64 * 64);
                let chunk_idx = pg_idx_no_head / self.pages_per_chunk();
                self.set_dirty(chunk_idx);
                let pg_idx = self.add_headers_to_page_index(pg_idx_no_head);
                return pg_idx;
            } else {
                // there was conflict move forward one cache line
                LOC.with(|location| unsafe { *location.get() = loc + CACHE_LINE_SIZE });
            }
        }
    }

    pub fn free(&self, pg_idx: u64) {
        let pg_idx = self.remove_headers_from_page_index(pg_idx);
        let loc = (pg_idx / 64) as usize;
        let seg_idx = loc / SEGMENT_LEN;
        let in_seg_idx = loc % SEGMENT_LEN;
        let seg = &self.segments[seg_idx];
        let seg = unsafe { &*seg.load(Ordering::Acquire) };
        let mask = !(1u64 << (pg_idx % 64));
        seg[in_seg_idx].fetch_and(mask, Ordering::Relaxed);
    }

    fn set_dirty(&self, chunk_idx: u64) {
        let mut loc = (chunk_idx % DIRTY_MAP_SIZE as u64) as usize;
        let mut attempts = 0;
        loop {
            match self.dirty[loc].compare_exchange_weak(
                OPEN_SLOT_VALUE,
                chunk_idx,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(cur) => {
                    if cur == chunk_idx {
                        return;
                    }
                }
            }
            loc = (loc + 1) % DIRTY_MAP_SIZE;
            attempts += 1;

            if attempts >= DIRTY_MAP_SIZE {
                // flush the map when full to prevent a live lock
                self.flush()
            }
        }
    }

    pub fn flush(&self) {
        let mut chunk_idxs = HashSet::new();
        for i in 0..DIRTY_MAP_SIZE {
            let chunk_idx = self.dirty[i].load(Ordering::Acquire);
            self.dirty[i].store(OPEN_SLOT_VALUE, Ordering::Release);
            chunk_idxs.insert(chunk_idx);
        }

        for chunk_idx in chunk_idxs.iter() {
            let loc = chunk_idx * self.pages_per_chunk() / 8;
            let seg_idx = (loc / SEGMENT_LEN as u64) as usize;
            let in_seg_idx = (8 * (loc % SEGMENT_LEN as u64)) as usize;
            let seg = unsafe {
                // we stan doing evil things in this code base :D
                let seg = &self.segments[seg_idx];
                let ptr = seg.load(Ordering::Acquire) as *const u8;
                let len = std::mem::size_of_val(&seg);
                slice::from_raw_parts(ptr, len)
            };
            let mut buf = self.fio.get_buf();
            buf.get_mut()
                .copy_from_slice(&seg[in_seg_idx..in_seg_idx + self.fio.page_size()]);

            let pg_idx = self.chunk_index_to_page_index(*chunk_idx);
            self.fio.write(pg_idx, buf);
        }
    }

    fn pages_per_chunk(&self) -> u64 {
        self.fio.page_size() as u64 * 8
    }

    fn chunk_index_to_page_index(&self, chunk_idx: u64) -> u64 {
        self.add_headers_to_page_index(chunk_idx * self.pages_per_chunk()) - 1
    }

    fn add_headers_to_page_index(&self, pg_idx: u64) -> u64 {
        NUM_HEADER_PAGES + 1 + pg_idx + (pg_idx / self.pages_per_chunk())
    }

    fn remove_headers_from_page_index(&self, pg_idx: u64) -> u64 {
        let ret = pg_idx - (NUM_HEADER_PAGES + 1);
        ret - (ret / self.pages_per_chunk())
    }
}
