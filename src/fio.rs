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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    task::{self, Poll, Waker},
    thread::{self, JoinHandle},
    vec,
};

use anyhow::{Context, Ok, Result, anyhow};
use bon::bon;
use crossbeam::queue::{ArrayQueue, SegQueue};
use io_uring::IoUring;
use libc::{O_DIRECT, iovec};
use parking_lot::Mutex;
use rustix::fs::fstatvfs;
use thiserror::Error;

use crate::{
    file::DbFile,
    meta::MetaHandler,
    util::{get_fs_block_size, has_valid_checksum, update_checksum},
};

pub const MIN_PAGE_SIZE: u64 = 4096; // smallest supported page size and most common filesystem block size
pub const MAX_PAGE_SIZE: u64 = 65536;

pub const DEFAULT_SQ_SIZE: usize = 128;
pub const DEFAULT_CQ_SIZE: usize = 256;
const IO_URING_SPIN_LIMIT: u64 = 32;

#[derive(Error, Debug)]
pub(crate) enum ReadError {
    #[error("invalid checksum")]
    BadChecksum,
    #[error("io error")]
    Unknown(#[from] anyhow::Error),
}

enum FioOp {
    Read(ReadData),
    Write(WriteData),
    Commit(CommitData),
    CommitFlush(CommitData),
    CommitBatch,
    Alloc(AllocData),
}

#[repr(u32)]
enum GenericOpState {
    Init = 0,
    Pending,
    Ready,
    Err,
}

impl TryFrom<u32> for GenericOpState {
    type Error = anyhow::Error;

    fn try_from(value: u32) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(GenericOpState::Init),
            1 => Ok(GenericOpState::Pending),
            2 => Ok(GenericOpState::Ready),
            3 => Ok(GenericOpState::Err),
            _ => Err(anyhow!("could not convert value {}", value)),
        }
    }
}

struct ReadData {
    pgidx: u64,
    waker: Waker,
    buf: PageBuf,
    state: Arc<Mutex<ReadState>>,
}

enum ReadState {
    Init,
    Pending,
    Ready(PageBuf),
    Done,
    Err,
}

struct WriteData {
    pg_idx: u64,
    buf: PageBuf,
    waker: Waker,
    state: GenericOpStateRef,
}

struct CommitData {
    waker: Waker,
    state: GenericOpStateRef,
}

struct AllocData {
    len: u64,
    waker: Waker,
    state: GenericOpStateRef,
}

pub struct IoLoopHandle {
    join: Option<JoinHandle<()>>,
    inner: Arc<Inner>,
}

#[derive(Clone)]
pub struct Fio {
    inner: Arc<Inner>,
    join: Arc<IoLoopHandle>,
}

#[derive(Debug)]
struct Inner {
    fio_file: File,
    file: Arc<DbFile>,

    page_size: usize,
    len: AtomicU64, // number of pages
    stop: AtomicBool,
    queue: SegQueue<FioOp>,

    bufs: Pin<Box<[u8]>>,
    free_bufs: ArrayQueue<usize>,

    // TODO: fully implement
    // read_states: Box<Mutex<ReadState>>,
    // free_read_states: ArrayQueue<usize>,
    generic_op_states: Box<[AtomicU32]>,
    free_generic_op_states: ArrayQueue<usize>,
}

#[derive(Clone)]
enum GenericOpStateRef {
    Pool(Arc<PoolGenericOpState>),
    Dynamic(Arc<AtomicU32>),
}

struct PoolGenericOpState {
    idx: usize,
    fio: Arc<Inner>,
}

impl PoolGenericOpState {
    fn new(idx: usize, fio: Arc<Inner>) -> Self {
        fio.generic_op_states[idx].store(GenericOpState::Init as u32, Ordering::Release);
        Self { idx, fio }
    }
}

impl GenericOpStateRef {
    pub fn get(&self) -> &AtomicU32 {
        match &self {
            GenericOpStateRef::Pool(pool) => &pool.fio.generic_op_states[pool.idx],
            GenericOpStateRef::Dynamic(arc) => &arc,
        }
    }
}

impl Drop for PoolGenericOpState {
    fn drop(&mut self) {
        if let Err(_) = self.fio.free_generic_op_states.push(self.idx) {
            eprintln!(
                "Failed to return generic op state {} to free pool",
                self.idx
            );
        }
    }
}

#[derive(Debug)]
pub enum PageBuf {
    Pool(PoolBuf),
    Dynamic(Box<[u8]>),
}

impl PageBuf {
    pub fn get(&self) -> &[u8] {
        match self {
            PageBuf::Pool(shared) => {
                let page_size = shared.fio.page_size;
                unsafe { std::slice::from_raw_parts(shared.ptr(), page_size as usize) }
            }
            PageBuf::Dynamic(buf) => buf,
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        match self {
            PageBuf::Pool(shared) => {
                let page_size = shared.fio.page_size;
                unsafe { std::slice::from_raw_parts_mut(shared.ptr(), page_size as usize) }
            }
            PageBuf::Dynamic(buf) => buf,
        }
    }
}

#[derive(Debug)]
pub struct PoolBuf {
    idx: usize,
    fio: Arc<Inner>,
}

impl PoolBuf {
    pub fn ptr(&self) -> *mut u8 {
        self.fio.bufs[self.idx * self.fio.page_size..].as_ptr() as *mut u8
    }
}

impl Drop for PoolBuf {
    fn drop(&mut self) {
        if let Err(_) = self.fio.free_bufs.push(self.idx) {
            eprintln!("Failed to return buffer idx {} to free pool", self.idx);
        }
    }
}

#[bon]
impl Fio {
    #[builder]
    pub(crate) fn builder(
        file: Arc<DbFile>,
        meta: &MetaHandler,
        #[builder(default = DEFAULT_SQ_SIZE)] sq: usize,
        #[builder(default = DEFAULT_CQ_SIZE)] cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        Self::new(file, meta, sq, cq, page_buf_pool, generic_op_state_pool)
    }

    pub(crate) fn new(
        file: Arc<DbFile>,
        meta: &MetaHandler,
        sq: usize,
        cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        let page_buf_pool = page_buf_pool.unwrap_or_else(|| 2 * cq);
        let generic_op_state_pool = generic_op_state_pool.unwrap_or_else(|| cq);

        let fio_file = {
            let mut open = OpenOptions::new();
            open.read(true);
            open.write(true);
            open.create(true);
            if get_fs_block_size(file.path())? == meta.page_size() {
                open.custom_flags(O_DIRECT);
            }
            open.open(file.path())?
        };

        let fd = fio_file.as_raw_fd();
        let len = meta.len();
        let page_size = meta.page_size() as usize;

        let stop = AtomicBool::new(false);
        let queue = SegQueue::new();
        let bufs = alloc_aligned_buffer(page_buf_pool as usize, page_size)?;
        let free_bufs = ArrayQueue::new(cmp::max(1, page_buf_pool as usize));
        for idx in 0usize..(page_buf_pool as usize) {
            free_bufs
                .push(idx)
                .map_err(|idx| anyhow!("Failed to initialize idx {} in free buffer pool", idx))?;
        }
        let mut generic_op_states = Vec::with_capacity(generic_op_state_pool);
        let free_generic_op_states = ArrayQueue::new(cmp::max(1, generic_op_state_pool as usize));
        for idx in 0usize..(generic_op_state_pool as usize) {
            generic_op_states.push(AtomicU32::new(GenericOpState::Init as u32));
            free_generic_op_states.push(idx).map_err(|idx| {
                anyhow!(
                    "Failed to initialize idx {} in free generic ops state pool",
                    idx
                )
            })?;
        }
        let generic_op_states = generic_op_states.into_boxed_slice();

        let inner = Arc::new(Inner {
            fio_file,
            file,
            page_size,
            len: AtomicU64::new(len as u64),
            stop,
            queue,
            bufs,
            free_bufs,
            generic_op_states,
            free_generic_op_states,
        });

        let join = {
            let inner = inner.clone();
            let mut io_loop = IoLoop::new(page_size, sq, cq, page_buf_pool, fd, inner)?;
            thread::spawn(move || io_loop.run())
        };

        Ok(Self {
            inner: inner.clone(),
            join: Arc::new(IoLoopHandle {
                join: Some(join),
                inner,
            }),
        })
    }

    pub fn file(&self) -> &Arc<DbFile> {
        &self.inner.file
    }

    pub fn page_size(&self) -> usize {
        self.inner.page_size
    }

    pub fn get_buf(&self) -> PageBuf {
        get_buf(&self.inner)
    }

    pub fn get_dyn_buf(&self) -> PageBuf {
        get_dyn_buf(&self.inner)
    }

    fn join(&self) -> &JoinHandle<()> {
        self.join.as_ref().join.as_ref().unwrap()
    }

    pub(crate) async fn read(&self, pg_idx: u64) -> std::result::Result<PageBuf, ReadError> {
        let pg = self.read_unchecked(pg_idx).await?;

        if !has_valid_checksum(pg.get()) {
            return Err(ReadError::BadChecksum);
        }

        std::result::Result::Ok(pg)
    }

    pub(crate) async fn read_unchecked(&self, pg_idx: u64) -> Result<PageBuf> {
        struct ReadFuture<'a> {
            fio: &'a Fio,
            idx: u64,
            state: Arc<Mutex<ReadState>>,
        }

        impl<'a> Future for ReadFuture<'a> {
            type Output = Result<PageBuf>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut state = self.state.lock();
                let cur = std::mem::replace(&mut *state, ReadState::Done);
                match cur {
                    ReadState::Init => {
                        let op = FioOp::Read(ReadData {
                            pgidx: self.idx,
                            waker: cx.waker().clone(),
                            buf: self.fio.get_buf(),
                            state: self.state.clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join().thread().unpark();

                        *state = ReadState::Pending;
                        Poll::Pending
                    }
                    ReadState::Pending => {
                        *state = cur;
                        Poll::Pending
                    }
                    ReadState::Ready(data) => {
                        *state = ReadState::Done;
                        Poll::Ready(Ok(data))
                    }
                    ReadState::Done => Poll::Pending,
                    ReadState::Err => {
                        *state = ReadState::Done;
                        Poll::Ready(Err(anyhow!("Failed to read page")))
                    }
                }
            }
        }

        ReadFuture {
            fio: self,
            idx: pg_idx,
            state: Arc::new(Mutex::new(ReadState::Init)),
        }
        .await
    }

    pub(crate) async fn write(&self, pg_idx: u64, mut buf: PageBuf) -> Result<()> {
        update_checksum(buf.get_mut());
        self.write_unchecked(pg_idx, buf).await
    }

    pub(crate) async fn write_unchecked(&self, pg_idx: u64, buf: PageBuf) -> Result<()> {
        struct WriteFuture<'a> {
            fio: &'a Fio,
            pg_idx: u64,
            buf: Option<PageBuf>,
            state: GenericOpStateRef,
        }

        impl<'a> Future for WriteFuture<'a> {
            type Output = Result<()>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let state = self.state.get().load(Ordering::Acquire).try_into().unwrap();
                match state {
                    GenericOpState::Init => {
                        self.state
                            .get()
                            .store(GenericOpState::Pending as u32, Ordering::Release);

                        let op = FioOp::Write(WriteData {
                            pg_idx: self.pg_idx,
                            buf: std::mem::replace(&mut self.buf, None).unwrap(),
                            waker: cx.waker().clone(),
                            state: self.state.clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join().thread().unpark();
                        Poll::Pending
                    }
                    GenericOpState::Pending => Poll::Pending,
                    GenericOpState::Ready => Poll::Ready(Ok(())),
                    GenericOpState::Err => {
                        Poll::Ready(Err(anyhow!("Failed to perform disk write")))
                    }
                }
            }
        }

        WriteFuture {
            fio: self,
            pg_idx,
            buf: Some(buf),
            state: get_generic_op_state(&self.inner),
        }
        .await
    }

    pub(crate) async fn commit(&self) -> Result<()> {
        struct CommitFuture<'a> {
            fio: &'a Fio,
            state: GenericOpStateRef,
        }

        impl<'a> Future for CommitFuture<'a> {
            type Output = Result<()>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let state = self.state.get().load(Ordering::Acquire).try_into().unwrap();
                match state {
                    GenericOpState::Init => {
                        self.state
                            .get()
                            .store(GenericOpState::Pending as u32, Ordering::Release);

                        let op = FioOp::Commit(CommitData {
                            state: self.state.clone(),
                            waker: cx.waker().clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join().thread().unpark();
                        Poll::Pending
                    }
                    GenericOpState::Pending => Poll::Pending,
                    GenericOpState::Ready => Poll::Ready(Ok(())),
                    GenericOpState::Err => {
                        Poll::Ready(Err(anyhow!("Failed to perform disk commit")))
                    }
                }
            }
        }

        CommitFuture {
            fio: self,
            state: get_generic_op_state(&self.inner),
        }
        .await
    }

    pub async fn commit_flush(&self) -> Result<()> {
        pub struct CommitFuture<'a> {
            fio: &'a Fio,
            state: GenericOpStateRef,
        }

        impl<'a> Future for CommitFuture<'a> {
            type Output = Result<()>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let state = self.state.get().load(Ordering::Acquire).try_into().unwrap();
                match state {
                    GenericOpState::Init => {
                        self.state
                            .get()
                            .store(GenericOpState::Pending as u32, Ordering::Release);

                        let op = FioOp::CommitFlush(CommitData {
                            state: self.state.clone(),
                            waker: cx.waker().clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join().thread().unpark();
                        Poll::Pending
                    }
                    GenericOpState::Pending => Poll::Pending,
                    GenericOpState::Ready => Poll::Ready(Ok(())),
                    GenericOpState::Err => {
                        Poll::Ready(Err(anyhow!("Failed to perform disk commit")))
                    }
                }
            }
        }

        CommitFuture {
            fio: self,
            state: get_generic_op_state(&self.inner),
        }
        .await
    }

    pub async fn alloc(&self, len: u64) -> Result<()> {
        pub struct AllocFuture<'a> {
            fio: &'a Fio,
            len: u64,
            state: GenericOpStateRef,
        }

        impl<'a> Future for AllocFuture<'a> {
            type Output = Result<()>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let state = self.state.get().load(Ordering::Acquire).try_into().unwrap();
                match state {
                    GenericOpState::Init => {
                        self.state
                            .get()
                            .store(GenericOpState::Pending as u32, Ordering::Release);

                        let op = FioOp::Alloc(AllocData {
                            len: self.len,
                            state: self.state.clone(),
                            waker: cx.waker().clone(),
                        });
                        self.fio.inner.queue.push(op);
                        self.fio.join().thread().unpark();
                        Poll::Pending
                    }
                    GenericOpState::Pending => Poll::Pending,
                    GenericOpState::Ready => Poll::Ready(Ok(())),
                    GenericOpState::Err => {
                        Poll::Ready(Err(anyhow!("Failed to perform disk alloc")))
                    }
                }
            }
        }

        AllocFuture {
            fio: self,
            len,
            state: get_generic_op_state(&self.inner),
        }
        .await
    }
}

fn get_buf(inner: &Arc<Inner>) -> PageBuf {
    inner
        .free_bufs
        .pop()
        .map(|idx| {
            PageBuf::Pool(PoolBuf {
                idx,
                fio: inner.clone(),
            })
        })
        .unwrap_or_else(|| get_dyn_buf(&inner))
}

fn get_dyn_buf(inner: &Inner) -> PageBuf {
    PageBuf::Dynamic(vec![0u8; inner.page_size as usize].into_boxed_slice())
}

fn get_generic_op_state(inner: &Arc<Inner>) -> GenericOpStateRef {
    inner
        .free_generic_op_states
        .pop()
        .map(|idx| GenericOpStateRef::Pool(Arc::new(PoolGenericOpState::new(idx, inner.clone()))))
        .unwrap_or_else(|| {
            GenericOpStateRef::Dynamic(Arc::new(AtomicU32::new(GenericOpState::Init as u32)))
        })
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Err(e) = self.fio_file.unlock() {
            eprintln!("Failed to unlock file: {}", e);
        }
    }
}

impl Drop for IoLoopHandle {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Release);
        let join = self.join.take().unwrap();
        join.thread().unpark();
        let thread_id = join.thread().id();
        if let Err(e) = join.join() {
            eprintln!("Failed to join io_uring thread {:?}: {:?}", thread_id, e);
        }
    }
}

struct IoLoop {
    pg_size: usize,
    sq_size: usize,
    cq_size: usize,
    pg_buf_pool_size: usize,
    fd: i32,
    inner: Arc<Inner>,

    ring: IoUring,
    bufs: Pin<Box<[u8]>>,
    ops: Box<[Option<FioOp>]>,

    fsync_front: Vec<CommitData>,
    fsync_back: Vec<CommitData>,
}

impl IoLoop {
    fn new(
        page_size: usize,
        sq_size: usize,
        cq_size: usize,
        page_buf_pool_size: usize,
        fd: i32,
        inner: Arc<Inner>,
    ) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_cqsize(cq_size as u32)
            .build(sq_size as u32)?;
        let mut bufs = alloc_aligned_buffer(cq_size as usize, page_size)?;

        let iovecs: Vec<iovec> = (0usize..cq_size as usize)
            .map(|i| iovec {
                iov_base: (bufs[i * page_size..].as_mut_ptr()) as *mut c_void,
                iov_len: page_size,
            })
            .chain((0usize..(page_buf_pool_size as usize)).map(|i| iovec {
                iov_base: (inner.bufs[i * page_size..].as_ptr() as *mut u8) as *mut c_void,
                iov_len: page_size,
            }))
            .collect();

        let mut ops = Vec::new();
        ops.resize_with(cq_size as usize, || None);
        let ops = ops.into_boxed_slice();

        unsafe {
            ring.submitter()
                .register_buffers(&iovecs)
                .context("Failed to register buffers")?;
        }

        Ok(Self {
            pg_size: page_size,
            sq_size,
            cq_size,
            pg_buf_pool_size: page_buf_pool_size,
            fd,
            inner,
            ring,
            bufs,
            ops,
            fsync_front: Vec::new(),
            fsync_back: Vec::new(),
        })
    }

    fn complete_op_with_error(&mut self, op: FioOp) {
        match op {
            FioOp::Read(ReadData { waker, state, .. }) => {
                {
                    let mut state = state.lock();
                    *state = ReadState::Err;
                }
                waker.wake();
            }
            FioOp::Write(WriteData { waker, state, .. }) => {
                state
                    .get()
                    .store(GenericOpState::Err as u32, Ordering::Release);
                waker.wake();
            }
            FioOp::Commit(CommitData { waker, state, .. })
            | FioOp::CommitFlush(CommitData { waker, state, .. }) => {
                state
                    .get()
                    .store(GenericOpState::Err as u32, Ordering::Release);
                waker.wake();
            }
            FioOp::CommitBatch => {
                for CommitData { waker, state, .. } in self.fsync_front.drain(..) {
                    state
                        .get()
                        .store(GenericOpState::Err as u32, Ordering::Release);
                    waker.wake();
                }
            }
            FioOp::Alloc(AllocData { waker, state, .. }) => {
                state
                    .get()
                    .store(GenericOpState::Err as u32, Ordering::Release);
                waker.wake();
            }
        }
    }

    fn run(&mut self) {
        if let Err(e) = self.run_unchecked() {
            eprintln!("io_uring thread failed: {}", e);

            // wake all outstanding operations
            for i in 0..self.ops.len() {
                match std::mem::take(&mut self.ops[i]) {
                    Some(op) => self.complete_op_with_error(op),
                    None => {
                        // no-op
                    }
                }
            }

            // complete future operations immediately with error
            loop {
                if self.inner.queue.is_empty() {
                    thread::park();
                }
                if self.inner.stop.load(Ordering::Acquire) {
                    return;
                }
                while let Some(op) = self.inner.queue.pop() {
                    self.complete_op_with_error(op);
                }
            }
        }
    }

    fn run_unchecked(&mut self) -> Result<()> {
        let mut pending: usize = 0;

        let mut ids: VecDeque<usize> = (0..self.cq_size as usize).collect();

        let mut spins = 0;

        loop {
            let mut submitted = 0;

            if self.inner.queue.is_empty() && pending == 0 && !self.fsync_back.is_empty() {
                thread::park();
            }
            if self.inner.stop.load(Ordering::Acquire) {
                return Ok(());
            }

            if self.fsync_front.is_empty() && !self.fsync_back.is_empty() {
                submitted += 1;
            }

            while submitted < self.sq_size
                && submitted + pending < self.cq_size
                && let Some(op) = self.inner.queue.pop()
            {
                match op {
                    FioOp::Read(data) => {
                        submitted += 1;

                        let id = ids.pop_front().unwrap();
                        let offset = data.pgidx * self.pg_size as u64;

                        let (buf, len, idx) = match &data.buf {
                            PageBuf::Pool(shared) => (
                                shared.ptr(),
                                self.pg_size as u32,
                                (self.cq_size + shared.idx) as u16,
                            ),
                            PageBuf::Dynamic(_) => (
                                self.bufs[id * self.pg_size..].as_mut_ptr(),
                                self.pg_size as u32,
                                id as u16,
                            ),
                        };

                        let read = io_uring::opcode::ReadFixed::new(
                            io_uring::types::Fd(self.fd),
                            buf,
                            len,
                            idx,
                        )
                        .offset(offset)
                        .build()
                        .user_data(id as u64);
                        unsafe {
                            self.ring
                                .submission()
                                .push(&read)
                                .context("Failed to push read entry onto submission queue")?;
                        }
                        self.ops[id] = Some(FioOp::Read(data));
                    }
                    FioOp::Write(data) => {
                        submitted += 1;

                        let id = ids.pop_front().unwrap();
                        let offset = data.pg_idx * self.pg_size as u64;

                        let (buf, len, idx) = match &data.buf {
                            PageBuf::Pool(shared) => (
                                shared.ptr(),
                                self.pg_size as u32,
                                (self.cq_size + shared.idx) as u16,
                            ),
                            PageBuf::Dynamic(pg) => {
                                self.bufs[id * self.pg_size..(id + 1) * self.pg_size]
                                    .copy_from_slice(pg);
                                (
                                    self.bufs[id * self.pg_size..].as_mut_ptr(),
                                    self.pg_size as u32,
                                    id as u16,
                                )
                            }
                        };

                        let write = io_uring::opcode::WriteFixed::new(
                            io_uring::types::Fd(self.fd),
                            buf,
                            len,
                            idx,
                        )
                        .offset(offset)
                        .build()
                        .user_data(id as u64);
                        unsafe {
                            self.ring
                                .submission()
                                .push(&write)
                                .context("Failed to push write entry onto submission queue")?;
                        }
                        self.ops[id] = Some(FioOp::Write(data));
                    }
                    FioOp::Commit(data) => {
                        if self.fsync_front.is_empty() && self.fsync_back.is_empty() {
                            submitted += 1;
                        }
                        self.fsync_back.push(data);
                    }
                    FioOp::CommitFlush(data) => {
                        submitted += 1;

                        let id = ids.pop_front().unwrap();
                        let fsync = io_uring::opcode::Fsync::new(io_uring::types::Fd(self.fd))
                            .build()
                            .user_data(id as u64)
                            .flags(io_uring::squeue::Flags::IO_DRAIN); // wait for all pending operations
                        unsafe {
                            self.ring
                                .submission()
                                .push(&fsync)
                                .context("Failed to push fsync entry onto submission queue")?;
                        }
                        self.ops[id] = Some(FioOp::Commit(data));
                    }
                    FioOp::CommitBatch => {
                        eprintln!("CommitBatch should never be submitted to queue directly");
                    }
                    FioOp::Alloc(data) => {
                        submitted += 1;

                        let id = ids.pop_front().unwrap();
                        let alloc = io_uring::opcode::Fallocate::new(
                            io_uring::types::Fd(self.fd),
                            data.len * self.pg_size as u64,
                        )
                        .build()
                        .user_data(id as u64);
                        unsafe {
                            self.ring
                                .submission()
                                .push(&alloc)
                                .context("Failed to push alloc entry onto submission queue")?;
                        }
                        self.ops[id] = Some(FioOp::Alloc(data));
                    }
                }
            }

            if self.fsync_front.is_empty() && !self.fsync_back.is_empty() {
                std::mem::swap(&mut self.fsync_front, &mut self.fsync_back);

                let id = ids.pop_front().unwrap();
                let fsync = io_uring::opcode::Fsync::new(io_uring::types::Fd(self.fd))
                    .build()
                    .user_data(id as u64);
                unsafe {
                    self.ring
                        .submission()
                        .push(&fsync)
                        .context("Failed to push fsync entry onto submission queue")?;
                }
                self.ops[id] = Some(FioOp::CommitBatch);
            }

            if !self.ring.submission().is_empty() {
                self.ring
                    .submit()
                    .context("Failed to submit submission queue")?;
                pending += submitted;
            }

            let cq: io_uring::CompletionQueue = self.ring.completion();
            let mut completed: u32 = 0;
            for cqe in cq {
                completed += 1;

                let id = cqe.user_data() as usize;
                match std::mem::take(&mut self.ops[id]) {
                    Some(FioOp::Read(ReadData {
                        pgidx: _,
                        waker,
                        mut buf,
                        state: result,
                    })) => {
                        {
                            let mut inner = result.lock();
                            if cqe.result() >= 0 {
                                if let PageBuf::Dynamic(buf) = &mut buf {
                                    buf.copy_from_slice(
                                        &self.bufs[id * self.pg_size..(id + 1) * self.pg_size],
                                    );
                                }
                                *inner = ReadState::Ready(buf);
                            } else {
                                eprintln!("Read failed with error: {}", cqe.result());
                                *inner = ReadState::Err;
                            }
                        }
                        waker.wake();
                    }
                    Some(FioOp::Write(WriteData { waker, state, .. })) => {
                        if cqe.result() >= 0 {
                            state
                                .get()
                                .store(GenericOpState::Ready as u32, Ordering::Release);
                        } else {
                            eprintln!("Write failed with error: {}", cqe.result());
                            state
                                .get()
                                .store(GenericOpState::Err as u32, Ordering::Release);
                        }
                        waker.wake();
                    }
                    Some(FioOp::Commit(CommitData { state, waker }))
                    | Some(FioOp::CommitFlush(CommitData { state, waker })) => {
                        if cqe.result() >= 0 {
                            state
                                .get()
                                .store(GenericOpState::Ready as u32, Ordering::Release);
                        } else {
                            eprintln!("Commit failed with error: {}", cqe.result());
                            state
                                .get()
                                .store(GenericOpState::Err as u32, Ordering::Release);
                        }
                        waker.wake();
                    }
                    Some(FioOp::CommitBatch) => {
                        for CommitData { state, waker } in self.fsync_front.drain(..) {
                            if cqe.result() >= 0 {
                                state
                                    .get()
                                    .store(GenericOpState::Ready as u32, Ordering::Release);
                            } else {
                                eprintln!("Commit failed with error: {}", cqe.result());
                                state
                                    .get()
                                    .store(GenericOpState::Err as u32, Ordering::Release);
                            }
                            waker.wake();
                        }
                    }
                    Some(FioOp::Alloc(AllocData { len, waker, state })) => {
                        if cqe.result() >= 0 {
                            self.inner.len.fetch_max(len as u64, Ordering::AcqRel);
                            state
                                .get()
                                .store(GenericOpState::Ready as u32, Ordering::Release);
                        } else {
                            eprintln!("Alloc failed with error: {}", cqe.result());
                            state
                                .get()
                                .store(GenericOpState::Err as u32, Ordering::Release);
                        }
                        waker.wake();
                    }
                    _ => {
                        eprintln!("completion user data did not match a valid operation");
                    }
                }

                ids.push_back(id);
                pending -= 1;
            }

            if submitted == 0 && completed == 0 {
                spins += 1;
                if spins >= IO_URING_SPIN_LIMIT && pending > 0 {
                    self.ring
                        .submit_and_wait(1)
                        .context("Failed to submit and wait on io_uring")?;
                    spins = 0;
                }
            } else {
                spins = 0;
            }
        }
    }
}

pub fn choose_page_size(file: &File) -> Result<usize> {
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    let block_size = fstatvfs.f_bsize as u64;
    if MIN_PAGE_SIZE % block_size == 0 || block_size % MIN_PAGE_SIZE == 0 {
        Ok(cmp::max(block_size, MIN_PAGE_SIZE) as usize)
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
        path::Path,
    };

    use function_name::named;

    use crate::{meta::NUM_HEADER_PAGES, test_util::TempDir};

    use super::*;

    #[test]
    fn test_choose_page_size() -> Result<()> {
        let page_size = choose_page_size(&File::open(Path::new("/"))?)?;
        println!("Auto picked size: {}", page_size);
        Ok(())
    }

    #[named]
    #[test]
    fn test_buf() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (file, meta) = temp_dir.fio_cust("db")?;

        let fio = Fio::builder()
            .file(file)
            .meta(&meta)
            .sq(2)
            .cq(4)
            .page_buf_pool(2)
            .build()?;
        let mut buf = fio.get_buf();
        assert!(matches!(buf, PageBuf::Pool(_)));

        buf.get_mut()[0] = 1;
        assert_eq!(1, buf.get()[0]);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, _) = temp_dir.fio("db")?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(fio.file().path())?;

        let mut test_buf = vec![1u8; fio.page_size()];
        update_checksum(&mut test_buf);
        file.write_all(&test_buf)?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read(NUM_HEADER_PAGES).await?;

        if let PageBuf::Pool(shared) = &data {
            println!(
                "read page into shared buffer with idx {}, address {:p}",
                shared.idx,
                shared.ptr()
            );
        }

        assert_eq!(&test_buf, data.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_page_dynamic() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (file, meta) = temp_dir.fio_cust("db")?;

        let fio = Fio::builder()
            .file(file.clone())
            .meta(&meta)
            .sq(2)
            .cq(4)
            .page_buf_pool(0)
            .build()?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file.path())?;
        let mut test_buf = vec![1u8; fio.page_size()];
        update_checksum(&mut test_buf);
        file.write_all(&test_buf)?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read(NUM_HEADER_PAGES).await?;

        if let PageBuf::Pool(shared) = &data {
            println!(
                "read page into shared buffer with idx {}, address {:p}",
                shared.idx,
                shared.ptr()
            );
        }

        assert_eq!(&test_buf, data.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_write_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, _) = temp_dir.fio("db")?;

        let mut buf = fio.get_buf();
        buf.get_mut()[0..].fill(1u8);
        fio.write(0, buf).await?;
        fio.commit().await?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(fio.file().path())?;
        let mut buf = vec![0u8; fio.page_size()];
        file.read_exact(&mut buf)?;

        let mut res = vec![1u8; fio.page_size()];
        update_checksum(&mut res);
        assert_eq!(res, buf);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_write_page_dynamic() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (file, meta) = temp_dir.fio_cust("db")?;

        let fio = Fio::builder()
            .file(file.clone())
            .meta(&meta)
            .sq(2)
            .cq(4)
            .page_buf_pool(0)
            .build()?;
        let mut buf = fio.get_buf();
        buf.get_mut()[0..].fill(1u8);
        fio.write(0, buf).await?;
        fio.commit_flush().await?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(file.path())?;
        let mut buf = vec![0u8; fio.page_size()];
        file.read_exact(&mut buf)?;

        let mut res = vec![1u8; fio.page_size()];
        update_checksum(&mut res);
        assert_eq!(res, buf);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_simple_alloc() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, meta) = temp_dir.fio("db")?;
        assert_eq!(NUM_HEADER_PAGES, meta.len());

        fio.alloc(NUM_HEADER_PAGES + 1).await?;
        assert!(NUM_HEADER_PAGES + 1 <= fs::metadata(fio.file().path())?.len());
        assert_eq!(
            fio.page_size() * (NUM_HEADER_PAGES + 1) as usize,
            fs::metadata(fio.file().path())?.len() as usize
        );

        let _read_test = fio.read_unchecked(0).await?;

        fio.alloc(1000).await?;
        assert!(1000 <= fs::metadata(fio.file().path())?.len());
        assert_eq!(
            1000 * fio.page_size(),
            fs::metadata(fio.file().path())?.len() as usize
        );

        let _read_test = fio.read_unchecked(5).await?;

        Ok(())
    }
}
