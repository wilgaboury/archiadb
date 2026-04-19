use core::slice;
use std::{
    array,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    ptr,
    sync::atomic::{AtomicPtr, AtomicU64, Ordering, fence},
};

use anyhow::Result;
use tokio::sync::Mutex;

use crate::fio::{Fio, MIN_PAGE_SIZE};

const CACHE_LINE_SIZE: usize = 64; // standard size on pretty much every system that matters
const SEGMENTS_LEN: usize = 2048;
const SEGMENT_LEN: usize = (65536 /* largest supported page size */ * 128 /* just some num */) / 8 /* bytes in 64 bit int */;
const FLUSH_SERIALIZER_LEN: usize = 1024;
const OPEN_SLOT_VALUE: u64 = 1u64 << 63;
const NUM_HEADER_PAGES: u64 = 3;

struct Segment {
    bitfield: [AtomicU64; SEGMENT_LEN], // ~8mb of memory, represents ~274 GB of disk assuming 4kb page
    chunks: [Chunk; SEGMENT_LEN * 8 / MIN_PAGE_SIZE],
}

struct Chunk {
    // I think this version scheme will help dedup concurrent writes, but I need to think
    // through the edge cases more. I'm not fully convinced that writes will not be missed.
    version: AtomicU64,
    written: AtomicU64,
    write: Mutex<()>,
}

struct PageAllocator {
    segments: [AtomicPtr<Segment>; SEGMENTS_LEN], // maximum of ~17 GB memory, represents ~562 terabytes of disk assuming 4kb page
    capaticy: AtomicU64,
    allocated: AtomicU64,
    fio: Fio,
}

pub struct AllocationSet<'a> {
    alloc: &'a PageAllocator,
    allocations: HashSet<u64>, // page index
    dirty: HashMap<u64, u64>,  // chunk index
}

impl<'a> Drop for AllocationSet<'a> {
    fn drop(&mut self) {
        for allocation in &self.allocations {
            self.alloc.free(*allocation);
        }
    }
}

thread_local! {
    static LOC: UnsafeCell<usize> = UnsafeCell::new(0);
}

impl Segment {
    fn nwe() -> Self {
        Self {
            bitfield: array::from_fn(|_| AtomicU64::new(0)),
            chunks: array::from_fn(|_| Chunk::new()),
        }
    }
}

impl Chunk {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            written: AtomicU64::new(0),
            write: Mutex::new(()),
        }
    }
}

impl<'a> AllocationSet<'a> {
    pub async fn flush(&mut self) -> Result<()> {
        for (chunk_idx, version) in &self.dirty {
            // just a note, short circuit from error may temporarily leak blocks in already written chunk headers
            self.alloc.flush(*chunk_idx, *version).await?;
        }

        self.allocations.clear();
        self.dirty.clear();

        Ok(())
    }
}

impl PageAllocator {
    pub async fn new(fio: Fio) -> Self {
        // TODO: scan file and initalize values from disk
        Self {
            segments: array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            capaticy: AtomicU64::new(0),
            allocated: AtomicU64::new(0),
            fio,
        }
    }

    pub fn alloc_set(&self) -> AllocationSet<'_> {
        AllocationSet {
            alloc: &self,
            allocations: HashSet::new(),
            dirty: HashMap::new(),
        }
    }

    pub fn alloc(&self, allocs: &mut AllocationSet) -> u64 {
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
            let word = seg.bitfield[in_seg_idx].load(Ordering::Relaxed);
            let free_idx = word.leading_ones();
            if free_idx == 64 {
                continue;
            }
            let mask = 1u64 << free_idx;
            let old = seg.bitfield[in_seg_idx].fetch_or(mask, Ordering::AcqRel);
            if old & mask == 0 {
                let old = self.allocated.fetch_add(1, Ordering::Relaxed);
                // TODO: .len is not accurate, need to adjust for header and bitmap blocks
                if (old + 1) as f32 / self.fio.len() as f32 > 0.75 {
                    // TODO: allocate new segment
                }
                let pg_idx_no_head = free_idx as u64 + (loc as u64 * 64);
                let chunk_idx = pg_idx_no_head / self.pages_per_chunk();
                let pg_idx = self.add_headers_to_page_index(pg_idx_no_head);
                allocs.allocations.insert(pg_idx);
                let version = seg.chunks[chunk_idx as usize]
                    .version
                    .fetch_add(1, Ordering::AcqRel);
                allocs.dirty.insert(chunk_idx, version + 1);
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
        seg.bitfield[in_seg_idx].fetch_and(mask, Ordering::Relaxed);
    }

    async fn flush(&self, chunk_idx: u64, version: u64) -> Result<()> {
        let loc = chunk_idx * self.pages_per_chunk() / 8;
        let seg_idx = (loc / SEGMENT_LEN as u64) as usize;
        let in_seg_idx = (8 * (loc % SEGMENT_LEN as u64)) as usize;
        let seg = unsafe { &*self.segments[seg_idx].load(Ordering::Acquire) };
        let chunk = &seg.chunks[chunk_idx as usize];
        let pg_idx = self.chunk_index_to_page_index(chunk_idx);

        if chunk.written.load(Ordering::Acquire) >= version {
            // some other thread wrote this version, we all good
            return Ok(());
        }

        let mut buf = self.fio.get_buf();
        {
            let _gaurd = chunk.write.lock().await;
            let version = chunk.version.load(Ordering::Acquire);
            let len = std::mem::size_of_val(&seg.bitfield);

            // we stan doing evil things in this code base :D
            fence(Ordering::Acquire);
            let bitfield =
                unsafe { slice::from_raw_parts((&raw const seg.bitfield) as *const u8, len) };

            buf.get_mut()
                .copy_from_slice(&bitfield[in_seg_idx..in_seg_idx + self.fio.page_size()]);

            // It's important that the write is awaited. The kernel gives no submission queue
            // ordering garuntees (without special flags), so we must wait to make sure writes
            // are not submitted concurrently and get clobbered.
            self.fio.write(pg_idx, buf).await?;

            chunk.written.store(version, Ordering::Release);
        }

        Ok(())
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
