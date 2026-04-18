use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicPtr, AtomicU64, Ordering},
};

use crate::fio::Fio;

const CACHE_LINE_SIZE: usize = 64; // standard size on pretty much every system that matters
const SEGMENTS_LEN: usize = 2048;
const SEGMENT_LEN: usize = (65536 /* largest supported page size */ * 128 /* just some num */) / 8 /* bytes in 64 bit int */;
const DIRTY_MAP_SIZE: usize = 1024;
const OPEN_MASK: u64 = 1u64 << 63;

type Segment = [AtomicU64; SEGMENT_LEN]; // ~8mb of memory, represents ~274 GB of disk assuming 4kb page

struct PageAllocator {
    // dirty set is basically a lock-free hashset with infinite linear probing (hence lock-free)
    // making the assumption here that the high bit will never be used which is a pretty safe bet,
    // since a database would have to be at least 37 zettabytes (aka 37 trillion gigabytes, assuming 4kb page size)
    // TODO: init with OPEN_MASK
    dirty: [AtomicU64; 1024],
    segments: [AtomicPtr<Segment>; SEGMENTS_LEN], // maximum of ~17 GB memory, represents ~562 terabytes of disk assuming 4kb page
    capaticy: AtomicU64,
    allocated: AtomicU64,
    fio: Fio,
}

thread_local! {
    static LOC: UnsafeCell<usize> = UnsafeCell::new(0);
}

impl PageAllocator {
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
                let ret = free_idx as u64 + (loc as u64 * 64);
                self.set_dirty(ret);
                return ret;
            } else {
                // there was conflict move forward one cache line
                LOC.with(|location| unsafe { *location.get() = loc + CACHE_LINE_SIZE });
            }
        }
    }

    pub fn free(&self, idx: u64) {
        let loc = (idx / 64) as usize;
        let seg_idx = loc / SEGMENT_LEN;
        let in_seg_idx = loc % SEGMENT_LEN;
        let seg = &self.segments[seg_idx];
        let seg = unsafe { &*seg.load(Ordering::Acquire) };
        let mask = !(1u64 << (idx % 64));
        seg[in_seg_idx].fetch_and(mask, Ordering::Relaxed);
    }

    fn set_dirty(&self, idx: u64) {
        let mut loc = (idx % DIRTY_MAP_SIZE as u64) as usize;
        loop {
            if let Ok(_) = self.dirty[loc].compare_exchange_weak(
                OPEN_MASK,
                idx,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                return;
            }
            loc = (loc + 1) % DIRTY_MAP_SIZE;
        }
    }

    fn flush(&self) {
        for i in 0..DIRTY_MAP_SIZE {
            let idx = self.dirty[i].load(Ordering::Acquire);
            self.dirty[i].store(OPEN_MASK, Ordering::Release);
            // TODO: write block to fio
        }
    }
}
