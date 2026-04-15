use std::{
    alloc::{Layout, alloc},
    cmp,
    collections::VecDeque,
    ffi::c_void,
    fs::{File, OpenOptions},
    os::{
        fd::{AsFd, AsRawFd},
        unix::fs::OpenOptionsExt,
    },
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{self, Poll, Waker},
    thread::{self, JoinHandle},
    vec,
};

use anyhow::{Context, Ok, Result, anyhow};
use crossbeam::queue::{ArrayQueue, SegQueue};
use io_uring::IoUring;
use libc::{O_DIRECT, iovec};
use rustix::fs::fstatvfs;

use crate::spin::SpinLock;

pub const MIN_PAGE_SIZE: usize = 4096; // smallest supported page size and most common filesystem block size

const IO_URING_SQ: u32 = 256;
const IO_URING_CQ: u32 = 512;
const IO_URING_SPIN_LIMIT: u64 = 32;

enum FioOp {
    Read(ReadData),
    Write(WriteData),
    Commit(CommitData),
    Alloc(AllocData),
}

struct ReadData {
    pgidx: usize,
    waker: Waker,
    buf: BufRef,
    state: Arc<SpinLock<ReadState>>,
}

enum ReadState {
    Init,
    Pending,
    Ready(BufRef),
    Done,
}

struct WriteData {
    pgidx: usize,
    buf: BufRef,
}

struct CommitData {
    waker: Waker,
    state: Arc<SpinLock<CommitState>>,
}

pub enum CommitState {
    Init,
    Pending,
    Done,
}

struct AllocData {
    len: usize,
    waker: Waker,
    state: Arc<SpinLock<AllocState>>,
}

pub enum AllocState {
    Init,
    Pending,
    Done,
}

pub struct Fio {
    path: PathBuf,
    file: File,
    inner: Arc<Inner>,
    join: Option<JoinHandle<()>>,
}

struct Inner {
    page_size: usize,
    len: AtomicU64,
    stop: AtomicBool,
    queue: SegQueue<FioOp>,
    bufs: Pin<Box<[u8]>>,
    free_bufs: ArrayQueue<usize>,
}

pub enum BufRef {
    Shared(SharedBuf),
    Owned(Box<[u8]>),
}

impl BufRef {
    pub fn get(&self) -> &[u8] {
        match self {
            BufRef::Shared(shared) => {
                let page_size = shared.fio.page_size;
                unsafe { std::slice::from_raw_parts(shared.ptr(), page_size as usize) }
            }
            BufRef::Owned(buf) => buf,
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        match self {
            BufRef::Shared(shared) => {
                let page_size = shared.fio.page_size;
                unsafe { std::slice::from_raw_parts_mut(shared.ptr(), page_size as usize) }
            }
            BufRef::Owned(buf) => buf,
        }
    }
}

pub struct SharedBuf {
    idx: usize,
    fio: Arc<Inner>,
}

impl SharedBuf {
    pub fn ptr(&self) -> *mut u8 {
        self.fio.bufs[self.idx * self.fio.page_size..].as_ptr() as *mut u8
    }
}

impl Drop for SharedBuf {
    fn drop(&mut self) {
        if let Err(e) = self.fio.free_bufs.push(self.idx) {
            eprintln!(
                "Failed to return buffer idx {} to free pool: {}",
                self.idx, e
            );
        }
    }
}

impl Fio {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT) // TODO: if filesystem block size does not match db page (the file was moved to another computers/filesystem), O_DIRECT will not work
            .open(path.as_ref())?;

        file.try_lock()?; // prevent multiple instances from opening the same file

        let page_size = choose_page_size(path.as_ref())?;

        let fd = file.as_raw_fd();
        let len = file.metadata()?.len() as usize / page_size;

        let stop = AtomicBool::new(false);
        let queue = SegQueue::new();
        let bufs = alloc_aligned_buffer(2 * IO_URING_CQ as usize, page_size)?;
        let free_bufs = ArrayQueue::new(2 * IO_URING_CQ as usize);
        for idx in 0usize..(2 * IO_URING_CQ as usize) {
            free_bufs
                .push(idx)
                .map_err(|idx| anyhow!("Failed to initialize idx {} in free buffer pool", idx))?;
        }

        let inner = Arc::new(Inner {
            page_size,
            len: AtomicU64::new(len as u64),
            stop,
            queue,
            bufs,
            free_bufs,
        });

        let join = {
            let shared = inner.clone();
            thread::spawn(move || {
                if let Err(e) = io_uring_loop(page_size, fd, shared) {
                    panic!("io_uring thread failed with error: {:?}", e);
                }
            })
        };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file,
            inner,
            join: Some(join),
        })
    }

    pub fn page_size(&self) -> usize {
        self.inner.page_size
    }

    pub fn len(&self) -> u64 {
        self.inner.len.load(Ordering::Acquire)
    }

    pub fn get_buf(&self) -> BufRef {
        get_buf(&self.inner)
    }

    pub async fn read(&self, pgidx: usize) -> BufRef {
        pub struct ReadFuture<'a> {
            fio: &'a Fio,
            idx: usize,
            result: Arc<SpinLock<ReadState>>,
        }

        impl<'a> Future for ReadFuture<'a> {
            type Output = BufRef;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut result = self.result.lock();
                let state = std::mem::replace(&mut *result, ReadState::Done);
                match state {
                    ReadState::Init => {
                        let op = FioOp::Read(ReadData {
                            pgidx: self.idx,
                            waker: cx.waker().clone(),
                            buf: self.fio.get_buf(),
                            state: self.result.clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join.as_ref().unwrap().thread().unpark();

                        *result = ReadState::Pending;
                        Poll::Pending
                    }
                    ReadState::Pending => {
                        *result = state;
                        Poll::Pending
                    }
                    ReadState::Ready(data) => {
                        *result = ReadState::Done;
                        Poll::Ready(data)
                    }
                    ReadState::Done => Poll::Pending,
                }
            }
        }

        impl Drop for ReadFuture<'_> {
            fn drop(&mut self) {
                let mut result = self.result.lock();
                *result = ReadState::Done;
            }
        }

        ReadFuture {
            fio: self,
            idx: pgidx,
            result: Arc::new(SpinLock::new(ReadState::Init)),
        }
        .await
    }

    pub fn write(&self, pgidx: usize, buf: BufRef) {
        let op = FioOp::Write(WriteData { pgidx, buf });
        self.inner.queue.push(op);
        self.join.as_ref().unwrap().thread().unpark();
    }

    pub async fn commit(&self) {
        pub struct CommitFuture<'a> {
            fio: &'a Fio,
            result: Arc<SpinLock<CommitState>>,
        }

        impl<'a> Future for CommitFuture<'a> {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut result = self.result.lock();
                let state = std::mem::replace(&mut *result, CommitState::Done);
                match state {
                    CommitState::Init => {
                        let op = FioOp::Commit(CommitData {
                            state: self.result.clone(),
                            waker: cx.waker().clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join.as_ref().unwrap().thread().unpark();

                        *result = CommitState::Pending;
                        Poll::Pending
                    }
                    CommitState::Pending => {
                        *result = state;
                        Poll::Pending
                    }
                    CommitState::Done => Poll::Ready(()),
                }
            }
        }

        impl Drop for CommitFuture<'_> {
            fn drop(&mut self) {
                let mut result = self.result.lock();
                *result = CommitState::Done;
            }
        }

        CommitFuture {
            fio: self,
            result: Arc::new(SpinLock::new(CommitState::Init)),
        }
        .await
    }

    pub async fn alloc(&self, len: usize) {
        pub struct AllocFuture<'a> {
            fio: &'a Fio,
            len: usize,
            result: Arc<SpinLock<AllocState>>,
        }

        impl<'a> Future for AllocFuture<'a> {
            type Output = ();

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut result = self.result.lock();
                let state = std::mem::replace(&mut *result, AllocState::Done);
                match state {
                    AllocState::Init => {
                        let op = FioOp::Alloc(AllocData {
                            len: self.len,
                            state: self.result.clone(),
                            waker: cx.waker().clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join.as_ref().unwrap().thread().unpark();

                        *result = AllocState::Pending;
                        Poll::Pending
                    }
                    AllocState::Pending => {
                        *result = state;
                        Poll::Pending
                    }
                    AllocState::Done => Poll::Ready(()),
                }
            }
        }

        impl Drop for AllocFuture<'_> {
            fn drop(&mut self) {
                let mut result = self.result.lock();
                *result = AllocState::Done;
            }
        }

        AllocFuture {
            fio: self,
            len: len,
            result: Arc::new(SpinLock::new(AllocState::Init)),
        }
        .await
    }
}

fn get_buf(inner: &Arc<Inner>) -> BufRef {
    inner
        .free_bufs
        .pop()
        .map(|idx| {
            BufRef::Shared(SharedBuf {
                idx,
                fio: inner.clone(),
            })
        })
        .unwrap_or_else(|| BufRef::Owned(vec![0u8; inner.page_size as usize].into_boxed_slice()))
}

impl Drop for Fio {
    fn drop(&mut self) {
        if let Err(e) = self.file.unlock() {
            eprintln!("Failed to unlock file: {}", e);
        }
        self.inner.stop.store(true, Ordering::Release);
        let join = self.join.take().unwrap();
        join.thread().unpark();
        let thread_id = join.thread().id();
        if let Err(e) = join.join() {
            eprintln!("Failed to join io_uring thread {:?}: {:?}", thread_id, e);
        }
    }
}

fn io_uring_loop(page_size: usize, fd: i32, inner: Arc<Inner>) -> Result<()> {
    let mut pending: u32 = 0;
    let mut ring = IoUring::builder()
        .setup_cqsize(IO_URING_CQ)
        .build(IO_URING_SQ)?;

    let mut bufs = alloc_aligned_buffer(IO_URING_CQ as usize, page_size)?;
    let mut ids: VecDeque<usize> = (0..IO_URING_CQ as usize).collect();

    let mut ops = Vec::new();
    ops.resize_with(IO_URING_CQ as usize, || None);
    let mut ops = ops.into_boxed_slice();

    let mut spins = 0;

    let shared_offset = IO_URING_CQ as usize;
    let iovecs: Vec<iovec> = (0usize..IO_URING_CQ as usize)
        .map(|i| iovec {
            iov_base: (bufs[i * page_size..].as_mut_ptr()) as *mut c_void,
            iov_len: page_size,
        })
        .chain((0usize..(2 * IO_URING_CQ as usize)).map(|i| iovec {
            iov_base: (inner.bufs[i * page_size..].as_ptr() as *mut u8) as *mut c_void,
            iov_len: page_size,
        }))
        .collect();

    println!(
        "registered address 1: {:p}",
        (inner.bufs[0 * page_size..].as_ptr() as *mut u8)
    );

    unsafe {
        ring.submitter()
            .register_buffers(&iovecs)
            .context("Failed to register buffers")?;
    }

    loop {
        let mut submitted = 0;

        if inner.queue.is_empty() && pending == 0 {
            thread::park();
        }
        if inner.stop.load(Ordering::Acquire) {
            return Ok(());
        }

        while !ids.is_empty()
            && submitted < IO_URING_SQ
            && submitted + pending < IO_URING_CQ
            && let Some(op) = inner.queue.pop()
        {
            submitted += 1;
            match op {
                FioOp::Read(data) => {
                    let id = ids.pop_front().unwrap();
                    let offset = data.pgidx * page_size;

                    let (buf, len, idx) = match &data.buf {
                        BufRef::Shared(shared) => {
                            (shared.ptr(), page_size as u32, (shared_offset + id) as u16)
                        }
                        BufRef::Owned(_) => (
                            bufs[id * page_size..].as_mut_ptr(),
                            page_size as u32,
                            id as u16,
                        ),
                    };

                    let read =
                        io_uring::opcode::ReadFixed::new(io_uring::types::Fd(fd), buf, len, idx)
                            .offset(offset as u64)
                            .build()
                            .user_data(id as u64);
                    unsafe {
                        ring.submission()
                            .push(&read)
                            .context("Failed to push read entry onto submission queue")?;
                    }
                    ops[id] = Some(FioOp::Read(data));
                }
                FioOp::Write(data) => {
                    let id = ids.pop_front().unwrap();
                    let offset = data.pgidx * page_size;

                    let (buf, len, idx) = match &data.buf {
                        BufRef::Shared(shared) => {
                            (shared.ptr(), page_size as u32, (shared_offset + id) as u16)
                        }
                        BufRef::Owned(vec) => {
                            bufs[id * page_size..(id + 1) * page_size].copy_from_slice(vec);
                            (
                                bufs[id * page_size..].as_mut_ptr(),
                                page_size as u32,
                                id as u16,
                            )
                        }
                    };

                    let write =
                        io_uring::opcode::WriteFixed::new(io_uring::types::Fd(fd), buf, len, idx)
                            .offset(offset as u64)
                            .build()
                            .user_data(id as u64);
                    unsafe {
                        ring.submission()
                            .push(&write)
                            .context("Failed to push write entry onto submission queue")?;
                    }
                    ops[id] = Some(FioOp::Write(data));
                }
                FioOp::Commit(data) => {
                    // TODO: combine fsyncs and move to back of batch

                    let id = ids.pop_front().unwrap();
                    let fsync = io_uring::opcode::Fsync::new(io_uring::types::Fd(fd))
                        .build()
                        .user_data(id as u64)
                        .flags(io_uring::squeue::Flags::IO_DRAIN);
                    unsafe {
                        ring.submission()
                            .push(&fsync)
                            .context("Failed to push fsync entry onto submission queue")?;
                    }
                    ops[id] = Some(FioOp::Commit(data));
                }
                FioOp::Alloc(data) => {
                    let id = ids.pop_front().unwrap();
                    let alloc = io_uring::opcode::Fallocate::new(
                        io_uring::types::Fd(fd),
                        (data.len * page_size) as u64,
                    )
                    .build()
                    .user_data(id as u64);
                    unsafe {
                        ring.submission()
                            .push(&alloc)
                            .context("Failed to push alloc entry onto submission queue")?;
                    }
                    ops[id] = Some(FioOp::Alloc(data));
                }
            }
        }

        if !ring.submission().is_empty() {
            ring.submit().context("Failed to submit submission queue")?;
            pending += submitted;
        }

        let cq: io_uring::CompletionQueue = ring.completion();
        let mut completed: u32 = 0;
        for cqe in cq {
            completed += 1;

            println!("result: {}", cqe.result());

            let id = cqe.user_data() as usize;
            match std::mem::take(&mut ops[id]) {
                Some(FioOp::Read(ReadData {
                    pgidx: _,
                    waker,
                    mut buf,
                    state: result,
                })) => {
                    {
                        let mut inner = result.lock();
                        if let BufRef::Owned(buf) = &mut buf {
                            buf.copy_from_slice(&bufs[id * page_size..(id + 1) * page_size]);
                        }
                        *inner = ReadState::Ready(buf);
                    }
                    waker.wake();
                }
                Some(FioOp::Write(_)) => {
                    // no-op
                }
                Some(FioOp::Commit(CommitData { state, waker })) => {
                    {
                        let mut inner = state.lock();
                        let _ = std::mem::replace(&mut *inner, CommitState::Done);
                    }
                    waker.wake();
                }
                Some(FioOp::Alloc(AllocData { len, waker, state })) => {
                    {
                        let mut inner = state.lock();
                        let _ = std::mem::replace(&mut *inner, AllocState::Done);
                    }
                    inner.len.fetch_max(len as u64, Ordering::AcqRel);
                    waker.wake();
                }
                _ => panic!("should never get here"),
            }

            ids.push_back(id);
            pending -= 1;
        }

        if submitted == 0 && completed == 0 {
            spins += 1;
            if spins >= IO_URING_SPIN_LIMIT && pending > 0 {
                ring.submit_and_wait(1)
                    .context("Failed to submit and wait on io_uring")?;
                spins = 0;
            }
        } else {
            spins = 0;
        }
    }
}

pub fn choose_page_size<P: AsRef<Path>>(path: P) -> Result<usize> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    let block_size = fstatvfs.f_bsize as usize;
    if MIN_PAGE_SIZE % block_size == 0 || block_size % MIN_PAGE_SIZE == 0 {
        Ok(cmp::max(block_size, MIN_PAGE_SIZE))
    } else {
        // realistically, there should be no linux filesytems that fail this check
        Err(anyhow!("Unsupported filesystem block size: {}", block_size))
    }
}

pub fn alloc_aligned_buffer(pages: usize, page_size: usize) -> Result<Pin<Box<[u8]>>> {
    let size = pages * page_size;
    let layout = Layout::from_size_align(size, page_size)
        .map_err(|_| anyhow!("Invalid layout for buffer alignment"))?;

    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        return Err(anyhow!("Failed to allocate aligned buffer"));
    }

    unsafe {
        std::ptr::write_bytes(ptr, 0, size);
    }

    Ok(unsafe { Pin::from(Box::from_raw(std::slice::from_raw_parts_mut(ptr, size))) })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
    };

    use function_name::named;

    use crate::test_util::TempDir;

    use super::*;

    #[test]
    fn test_choose_page_size() {
        let page_size = choose_page_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", page_size);
    }

    #[named]
    #[test]
    fn test_buf() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(test_file.clone()).unwrap();
        let mut buf = fio.get_buf();
        assert!(matches!(buf, BufRef::Shared(_)));

        buf.get_mut()[0] = 1;
        assert_eq!(1, buf.get()[0]);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(test_file.clone()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(test_file)?;
        file.write_all(&vec![1u8; fio.page_size()])?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read(0).await;

        if let BufRef::Shared(shared) = &data {
            println!(
                "read page into shared buffer with idx {}, address {:p}",
                shared.idx,
                shared.ptr()
            );
        }

        assert_eq!(vec![1u8; fio.page_size()], data.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_write_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(test_file.clone()).unwrap();
        let mut buf = fio.get_buf();
        buf.get_mut()[0..].fill(1u8);
        fio.write(0, buf);
        fio.commit().await;

        let mut file = OpenOptions::new().read(true).append(true).open(test_file)?;
        let mut buf = vec![0u8; fio.page_size()];
        file.read_exact(&mut buf)?;

        assert_eq!(vec![1u8; fio.page_size()], buf);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_simple_alloc() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(&test_file).unwrap();
        assert_eq!(0, fio.len());

        fio.alloc(1).await;
        assert_eq!(1, fio.len());
        assert_eq!(fio.page_size(), fs::metadata(&test_file)?.len() as usize);

        fio.alloc(3).await;
        assert_eq!(3, fio.len());
        assert_eq!(
            3 * fio.page_size(),
            fs::metadata(&test_file)?.len() as usize
        );

        Ok(())
    }
}
