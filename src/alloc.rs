use core::slice;
use std::{
    array,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    io::Write,
    mem::MaybeUninit,
    ptr,
    sync::atomic::{AtomicPtr, AtomicU64, Ordering, fence},
};

use anyhow::{Result, anyhow};
use tokio::sync::Mutex;

use crate::fio::{Fio, MIN_PAGE_SIZE};

const CACHE_LINE_SIZE: u64 = 64; // standard size on pretty much every system that matters
const SEGMENTS_LEN: u64 = 2048;
// TODO: test with small segment length
const SEGMENT_LEN: u64 = (65536 /* largest supported page size */ * 128 /* just some num */) / 8 /* bytes in 64 bit int */;
const FLUSH_SERIALIZER_LEN: u64 = 1024;
const OPEN_SLOT_VALUE: u64 = 1u64 << 63;
const NUM_HEADER_PAGES: u64 = 3;
const BITS_PER_BYTE: u64 = 8;
const BYTES_PER_UNIT: u64 = size_of::<BitfieldUnit>() as u64;
const BITS_PER_UNIT: u64 = BYTES_PER_UNIT * BITS_PER_BYTE;
const EXPAND_FRACTION: f32 = 0.7;
const CHUNKS_LEN: usize = (SEGMENT_LEN * BYTES_PER_UNIT / MIN_PAGE_SIZE) as usize;

type BitfieldUnit = AtomicU64;
struct Segment {
    bitfield: [BitfieldUnit; SEGMENT_LEN as usize], // ~8mb of memory, represents ~274 GB of disk assuming 4kb page
    chunks: [Chunk; CHUNKS_LEN],
}

struct Chunk {
    // I think this version scheme will help dedup concurrent writes, but I need to think
    // through the edge cases more. I'm not fully convinced that writes will not be missed.
    version: AtomicU64,
    written: AtomicU64,
    write: Mutex<()>,
}

struct PageAllocator {
    segments: [AtomicPtr<Segment>; SEGMENTS_LEN as usize], // maximum of ~17 GB memory, represents ~562 terrabytes of disk assuming 4kb page
    num_segs: AtomicU64,
    num_allocs: AtomicU64,
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
    static CROSS_SEG_IDX: UnsafeCell<u64> = UnsafeCell::new(0);
}

impl Segment {
    fn new() -> Box<Self> {
        let mut boxed = Box::<Segment>::new_zeroed();
        // unsafe { boxed.assume_init() }

        unsafe {
            // Get raw pointer to the box's contents
            let ptr = boxed.as_mut_ptr();

            let chunks_ptr = std::ptr::addr_of_mut!((*ptr).chunks) as *mut Chunk;
            for i in 0..CHUNKS_LEN {
                std::ptr::write(chunks_ptr.add(i), Chunk::new());
            }

            boxed.assume_init()
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
    pub async fn new(fio: Fio) -> Result<Self> {
        let ret = Self {
            segments: array::from_fn(|_| AtomicPtr::new(ptr::null_mut())),
            num_segs: AtomicU64::new(0),
            num_allocs: AtomicU64::new(0),
            fio,
        };

        println!("getting here 3?");

        if ret.fio.len() <= NUM_HEADER_PAGES + 1 {
            return Ok(ret);
        }

        let len_pages = ret.remove_headers_from_page_index(ret.fio.len());
        println!("raw len: {}, len: {}", ret.fio.len(), len_pages);
        let mut x_seg_idx = 0;
        while x_seg_idx < (len_pages / BITS_PER_BYTE) {
            println!("x_seg_idx: {}", x_seg_idx);
            std::io::stdout().flush().unwrap();
            let seg_idx = x_seg_idx / SEGMENT_LEN;
            ret.check_num_segments(seg_idx)?;
            let in_seg_idx = x_seg_idx % SEGMENT_LEN;

            if in_seg_idx == 0 {
                let new_seg = Box::into_raw(Segment::new());
                let seg_ptr = &ret.segments[seg_idx as usize];
                seg_ptr.store(new_seg, Ordering::Release);
                ret.num_segs.store(seg_idx + 1, Ordering::Release);
            }

            let pg_idx = ret.add_headers_to_page_index(x_seg_idx * BITS_PER_UNIT);
            println!("attempting to read: {}, fio len, {}", pg_idx, ret.fio.len());
            let buf = ret.fio.read(pg_idx).await?;
            let seg = &ret.segments[seg_idx as usize];
            let seg = unsafe { &*seg.load(Ordering::Acquire) };
            let len = std::mem::size_of_val(&seg.bitfield);
            let bitfield = unsafe {
                slice::from_raw_parts_mut((&raw const seg.bitfield) as *const u8 as *mut u8, len)
            };
            {
                let start = (in_seg_idx * BYTES_PER_UNIT) as usize;
                let end = start + ret.fio.page_size();
                bitfield[start..end].copy_from_slice(buf.get());
            }

            x_seg_idx += (ret.fio.page_size() as u64 * BITS_PER_BYTE) / (BITS_PER_UNIT);
        }

        Ok(ret)
    }

    fn check_num_segments(&self, segs: u64) -> Result<()> {
        if segs >= SEGMENTS_LEN {
            Err(anyhow!(
                "How in the world did you create a 562 terrabyte file and not run into any other issues!"
            ))
        } else {
            Ok(())
        }
    }

    async fn extend(&self) -> Result<()> {
        // lock free routine for allocation of new segment

        loop {
            let num_segs = self.num_segs.load(Ordering::Acquire);
            self.check_num_segments(num_segs)?;

            let num_allocs = self.num_allocs.load(Ordering::Acquire);

            if (num_allocs as f32 / (num_segs * BITS_PER_UNIT) as f32) < EXPAND_FRACTION {
                return Ok(());
            } else {
                let new_seg = Box::into_raw(Segment::new());
                let seg_ptr = &self.segments[num_segs as usize];
                if let Ok(_) = seg_ptr.compare_exchange(
                    ptr::null_mut(),
                    new_seg,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    // let result = self
                    //     .fio
                    //     .alloc(
                    //         self.fio.len()
                    //             + (SEGMENT_LEN * BITS_PER_UNIT * self.fio.page_size() as u64),
                    //     )
                    //     .await;
                    // if let Err(e) = result {
                    //     // we don't want to leak memory or prevent other threads from making progress
                    //     seg_ptr.swap(ptr::null_mut(), Ordering::AcqRel);
                    //     unsafe {
                    //         drop(Box::from_raw(new_seg));
                    //     }
                    //     Err(e)?
                    // }
                    self.num_segs.fetch_add(1, Ordering::AcqRel);
                    return Ok(());
                } else {
                    // TODO: could just reuse this allocation at the expense of making the logic more complex
                    unsafe {
                        drop(Box::from_raw(new_seg));
                    }
                }
            }
        }
    }

    pub fn create_set(&self) -> AllocationSet<'_> {
        AllocationSet {
            alloc: &self,
            allocations: HashSet::new(),
            dirty: HashMap::new(),
        }
    }

    pub async fn alloc(&self, allocs: &mut AllocationSet<'_>) -> Result<u64> {
        let mut attempts = 0;

        loop {
            if attempts > SEGMENT_LEN || self.fio.len() == 0 {
                let len = self.fio.len();
                let new_len = len + 1 + self.fio.page_size() as u64 * BITS_PER_BYTE;
                println!("len: {}; new len: {}, attempts: {}", len, new_len, attempts);
                self.fio.alloc(new_len).await?;
                if (len / BITS_PER_UNIT) / SEGMENT_LEN >= self.num_segs.load(Ordering::Acquire) {
                    self.extend().await?;
                }
                attempts = 0;
            }
            attempts += 1;

            let x_seg_idx = CROSS_SEG_IDX.with(|idx| unsafe { *idx.get() });
            let seg_idx = (x_seg_idx / SEGMENT_LEN) as usize;
            let in_seg_idx = (x_seg_idx % SEGMENT_LEN) as usize;
            if seg_idx >= self.num_segs.load(Ordering::Relaxed) as usize {
                CROSS_SEG_IDX.with(|idx| unsafe { *idx.get() = 0 });
                continue;
            }
            let seg = &self.segments[seg_idx as usize];
            let seg = unsafe { &*seg.load(Ordering::Acquire) };
            let word = seg.bitfield[in_seg_idx].load(Ordering::Relaxed);
            let free_idx = word.trailing_ones();
            if free_idx == 64 {
                CROSS_SEG_IDX.with(|idx| unsafe {
                    *idx.get() = (x_seg_idx + 1) % (self.fio.len() / BITS_PER_UNIT)
                });
                continue;
            }
            let mask = 1u64 << free_idx;
            let old = seg.bitfield[in_seg_idx].fetch_or(mask, Ordering::AcqRel);
            if old & mask == 0 {
                let old = self.num_allocs.fetch_add(1, Ordering::Relaxed);
                // TODO: .len is not accurate, need to adjust for header and bitmap blocks
                if (old + 1) as f32 / self.fio.len() as f32 > 0.75 {
                    // TODO: allocate new segment
                }
                let pg_idx_no_head = free_idx as u64 + (x_seg_idx as u64 * 64);
                let chunk_idx = pg_idx_no_head / self.pages_per_chunk();
                let pg_idx = self.add_headers_to_page_index(pg_idx_no_head);
                allocs.allocations.insert(pg_idx);
                let version = seg.chunks[chunk_idx as usize]
                    .version
                    .fetch_add(1, Ordering::AcqRel);
                allocs.dirty.insert(chunk_idx, version + 1);
                return Ok(pg_idx);
            } else {
                // there was conflict move forward one cache line
                CROSS_SEG_IDX.with(|idx| unsafe { *idx.get() = x_seg_idx + CACHE_LINE_SIZE });
            }
        }
    }

    pub fn free(&self, pg_idx: u64) {
        let pg_idx = self.remove_headers_from_page_index(pg_idx);
        let x_seg_idx = pg_idx / BITS_PER_UNIT;
        let seg_idx = x_seg_idx / SEGMENT_LEN;
        let in_seg_idx = x_seg_idx % SEGMENT_LEN;
        let seg = &self.segments[seg_idx as usize];
        let seg = unsafe { &*seg.load(Ordering::Acquire) };
        let mask = !(1u64 << (pg_idx % 64));
        seg.bitfield[in_seg_idx as usize].fetch_and(mask, Ordering::Relaxed);
    }

    async fn flush(&self, chunk_idx: u64, version: u64) -> Result<()> {
        let loc = chunk_idx * self.pages_per_chunk() / BITS_PER_UNIT;
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
        self.fio.page_size() as u64 * BITS_PER_BYTE
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

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use function_name::named;

    use crate::{
        alloc::{BITS_PER_BYTE, NUM_HEADER_PAGES, PageAllocator},
        test_util::TempDir,
    };

    #[named]
    #[tokio::test]
    async fn simple_test() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let fio = temp_dir.fio("file")?;

        assert_eq!(0, fio.len());

        let len = NUM_HEADER_PAGES + 1 + fio.page_size() as u64 * BITS_PER_BYTE;
        println!("setting fio len: {}", len);
        fio.alloc(len).await?;
        println!("getting here?");
        let alloc = PageAllocator::new(fio.clone()).await?;
        let mut allocs = alloc.create_set();
        let pg_idx = alloc.alloc(&mut allocs).await?;

        assert_eq!(4, pg_idx);

        let pg_idx = alloc.alloc(&mut allocs).await?;

        assert_eq!(5, pg_idx);

        let pg_idx = alloc.alloc(&mut allocs).await?;

        assert_eq!(6, pg_idx);

        allocs.flush().await?;

        let buf = fio.read(3).await?;
        // only works on little endian systems
        assert_eq!(0b111, buf.get()[0]);

        assert_eq!(7, alloc.alloc(&mut allocs).await?);

        drop(allocs);

        let mut allocs = alloc.create_set();
        assert_eq!(7, alloc.alloc(&mut allocs).await?);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn many_allocs() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let fio = temp_dir.fio("file")?;

        let num_allocs = fio.page_size() as u64 * BITS_PER_BYTE * 6;

        let alloc = PageAllocator::new(fio.clone()).await?;
        let mut allocs = alloc.create_set();

        for i in 0..num_allocs {
            // println!("alloc #, {}", i);
            alloc.alloc(&mut allocs).await?;
        }

        Ok(())
    }
}
