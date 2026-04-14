use std::{
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
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    vec,
};

use anyhow::{Ok, Result, anyhow};
use crossbeam::queue::{ArrayQueue, SegQueue};
use io_uring::IoUring;
use libc::{O_DIRECT, iovec};
use rustix::fs::fstatvfs;

use crate::spin::SpinLock;

const MIN_BLOCK_SIZE: usize = 4096; // 4kb
const MAX_BLOCK_SIZE: usize = 65536; // 64kb

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
    block: usize,
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
    idx: usize,
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
    join: Option<JoinHandle<Result<()>>>,
}

struct Inner {
    block_size: usize,
    len: AtomicU64,
    stop: AtomicBool,
    queue: SegQueue<FioOp>,
    bufs: Box<[u8]>,
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
                let block_size = shared.fio.block_size;
                unsafe { std::slice::from_raw_parts(shared.ptr(), block_size as usize) }
            }
            BufRef::Owned(buf) => buf,
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        match self {
            BufRef::Shared(shared) => {
                let block_size = shared.fio.block_size;
                unsafe { std::slice::from_raw_parts_mut(shared.ptr(), block_size as usize) }
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
        self.fio.bufs[self.idx * self.fio.block_size..].as_ptr() as *mut u8
    }
}

impl Drop for SharedBuf {
    fn drop(&mut self) {
        let _ = self.fio.free_bufs.push(self.idx); // TODO: handle error
    }
}

impl Fio {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(path.as_ref())?;

        file.try_lock()?; // prevent multiple instances from opening the same file

        let block_size = get_block_size(path.as_ref())?;
        validate_block_size(block_size)?;

        let fd = file.as_raw_fd();
        let len = file.metadata()?.len() as usize / block_size;

        let stop = AtomicBool::new(false);
        let queue = SegQueue::new();
        let bufs = vec![0u8; 2 * IO_URING_CQ as usize * block_size as usize].into_boxed_slice();
        let free_bufs = ArrayQueue::new(2 * IO_URING_CQ as usize);
        for idx in 0usize..(2 * IO_URING_CQ as usize) {
            let _ = free_bufs.push(idx); // TODO: handle error
        }

        let inner = Arc::new(Inner {
            block_size,
            len: AtomicU64::new(len as u64),
            stop,
            queue,
            bufs,
            free_bufs,
        });

        let join = {
            let shared = inner.clone();
            thread::spawn(move || io_uring_loop(block_size, fd, shared))
        };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file,
            inner,
            join: Some(join),
        })
    }

    pub fn block_size(&self) -> usize {
        self.inner.block_size
    }

    pub fn len(&self) -> u64 {
        self.inner.len.load(Ordering::Acquire)
    }

    pub fn get_buf(&self) -> BufRef {
        get_buf(&self.inner)
    }

    pub async fn read(&self, idx: usize) -> BufRef {
        pub struct ReadBlockFuture<'a> {
            fio: &'a Fio,
            idx: usize,
            result: Arc<SpinLock<ReadState>>,
        }

        impl<'a> Future for ReadBlockFuture<'a> {
            type Output = BufRef;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut result = self.result.lock();
                let state = std::mem::replace(&mut *result, ReadState::Done);
                match state {
                    ReadState::Init => {
                        let op = FioOp::Read(ReadData {
                            block: self.idx,
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

        impl Drop for ReadBlockFuture<'_> {
            fn drop(&mut self) {
                let mut result = self.result.lock();
                *result = ReadState::Done;
            }
        }

        ReadBlockFuture {
            fio: self,
            idx,
            result: Arc::new(SpinLock::new(ReadState::Init)),
        }
        .await
    }

    pub fn write(&self, idx: usize, buf: BufRef) {
        let op = FioOp::Write(WriteData { idx, buf });
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

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
        .unwrap_or_else(|| BufRef::Owned(vec![0u8; inner.block_size as usize].into_boxed_slice()))
}

impl Drop for Fio {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        self.inner.stop.store(true, Ordering::Release);
        let join = self.join.take().unwrap();
        join.thread().unpark();
        let _ = join.join();
    }
}

fn io_uring_loop(block_size: usize, fd: i32, inner: Arc<Inner>) -> Result<()> {
    let mut pending: u32 = 0;
    let mut ring = IoUring::builder()
        .setup_cqsize(IO_URING_CQ)
        .build(IO_URING_SQ)?;

    let mut bufs = vec![0u8; IO_URING_CQ as usize * block_size as usize].into_boxed_slice();
    let mut ids: VecDeque<usize> = (0..IO_URING_CQ as usize).collect();

    let mut ops = Vec::new();
    ops.resize_with(IO_URING_CQ as usize, || None);
    let mut ops = ops.into_boxed_slice();

    let mut spins = 0;

    let iovecs: Vec<iovec> = (0usize..IO_URING_CQ as usize)
        .map(|i| iovec {
            iov_base: (bufs[i * block_size..].as_mut_ptr()) as *mut c_void,
            iov_len: block_size,
        })
        .chain((0usize..(2 * IO_URING_CQ as usize)).map(|i| iovec {
            iov_base: (inner.bufs[i * block_size..].as_ptr() as *mut u8) as *mut c_void,
            iov_len: block_size,
        }))
        .collect();

    unsafe {
        ring.submitter()
            .register_buffers(&iovecs)
            .expect("Failed to register buffers");
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
                    let offset = data.block * block_size;

                    let (buf, len, idx) = match &data.buf {
                        BufRef::Shared(shared) => (
                            shared.ptr(),
                            block_size as u32,
                            (IO_URING_SQ as usize + id) as u16,
                        ),
                        BufRef::Owned(_) => (
                            bufs[id * block_size..].as_mut_ptr(),
                            block_size as u32,
                            id as u16,
                        ),
                    };

                    let read =
                        io_uring::opcode::ReadFixed::new(io_uring::types::Fd(fd), buf, len, idx)
                            .offset(offset as u64)
                            .build()
                            .user_data(id as u64);
                    unsafe {
                        let _ = ring.submission().push(&read); // TODO: handle error
                    }
                    ops[id] = Some(FioOp::Read(data));
                }
                FioOp::Write(data) => {
                    let id = ids.pop_front().unwrap();
                    let offset = data.idx * block_size;

                    let (buf, len, idx) = match &data.buf {
                        BufRef::Shared(shared) => (
                            shared.ptr(),
                            block_size as u32,
                            (IO_URING_SQ as usize + id) as u16,
                        ),
                        BufRef::Owned(vec) => {
                            bufs[id * block_size..(id + 1) * block_size].copy_from_slice(vec);
                            (
                                bufs[id * block_size..].as_mut_ptr(),
                                block_size as u32,
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
                        let _ = ring.submission().push(&write); // TODO: handle error
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
                        let _ = ring.submission().push(&fsync); // TODO: handle error
                    }
                    ops[id] = Some(FioOp::Commit(data));
                }
                FioOp::Alloc(data) => {
                    let id = ids.pop_front().unwrap();
                    let alloc = io_uring::opcode::Fallocate::new(
                        io_uring::types::Fd(fd),
                        (data.len * block_size) as u64,
                    )
                    .build()
                    .user_data(id as u64);
                    unsafe {
                        let _ = ring.submission().push(&alloc); // TODO: handle error
                    }
                    ops[id] = Some(FioOp::Alloc(data));
                }
            }
        }

        if !ring.submission().is_empty() {
            let _ = ring.submit();
            pending += submitted;
        }

        let cq: io_uring::CompletionQueue = ring.completion();
        let mut completed: u32 = 0;
        for cqe in cq {
            completed += 1;

            let id = cqe.user_data() as usize;
            match std::mem::take(&mut ops[id]) {
                Some(FioOp::Read(ReadData {
                    block: _,
                    waker,
                    mut buf,
                    state: result,
                })) => {
                    {
                        let mut inner = result.lock();
                        if let BufRef::Owned(buf) = &mut buf {
                            buf.copy_from_slice(&bufs[id * block_size..(id + 1) * block_size]);
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
                let _ = ring.submit_and_wait(1); // TODO: handle errors
                spins = 0;
            }
        } else {
            spins = 0;
        }
    }
}

pub fn get_block_size<P: AsRef<Path>>(path: P) -> Result<usize> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    Ok(fstatvfs.f_bsize as usize)
}

pub fn validate_block_size(block_size: usize) -> Result<()> {
    if block_size >= MIN_BLOCK_SIZE
        && block_size <= MAX_BLOCK_SIZE
        && block_size % MIN_BLOCK_SIZE == 0
    {
        Ok(())
    } else {
        Err(anyhow!(
            "Block size for filesystem is {}, but it must between {} and {} and a multiple of {}",
            block_size,
            MIN_BLOCK_SIZE,
            MAX_BLOCK_SIZE,
            MIN_BLOCK_SIZE
        ))
    }
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
    fn test_pick_block_size() {
        let block_size = get_block_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", block_size);
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_block() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(test_file.clone()).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(test_file)?;
        file.write_all(&vec![1u8; fio.block_size()])?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read(0).await;
        assert_eq!(vec![1u8; fio.block_size()], data.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_write_block() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("db");

        let fio = Fio::open(test_file.clone()).unwrap();
        let mut buf = fio.get_buf();
        buf.get_mut()[0..].fill(1u8);
        fio.write(0, buf);
        fio.commit().await;

        let mut file = OpenOptions::new().read(true).append(true).open(test_file)?;
        let mut buf = vec![0u8; fio.block_size()];
        file.read_exact(&mut buf)?;

        assert_eq!(vec![1u8; fio.block_size()], buf);

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
        assert_eq!(fio.block_size(), fs::metadata(&test_file)?.len() as usize);

        fio.alloc(3).await;
        assert_eq!(3, fio.len());
        assert_eq!(
            3 * fio.block_size(),
            fs::metadata(&test_file)?.len() as usize
        );

        Ok(())
    }
}
