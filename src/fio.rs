#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    alloc::{Layout, alloc},
    cmp,
    collections::VecDeque,
    ffi::c_void,
    fs::{File, OpenOptions},
    mem::MaybeUninit,
    os::{
        fd::{AsFd, AsRawFd},
        unix::fs::OpenOptionsExt,
    },
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    task::{self, Poll, Waker},
    thread::{self, JoinHandle},
    vec,
};

use anyhow::{Context, Ok, Result, anyhow};
use bon::bon;
use crossbeam::queue::{ArrayQueue, SegQueue};

use io_uring::IoUring;
use libc::{
    _SC_PAGESIZE, O_DIRECT, RLIM_INFINITY, RLIMIT_MEMLOCK, getrlimit, iovec, rlimit, sysconf,
};
use parking_lot::Mutex;
use rustix::fs::fstatvfs;
use thiserror::Error;

use crate::{
    file::DbFile,
    uint::{InPgIdx, PgIdx},
    util::{catch_unwind_anyhow, fs_block_size, has_valid_checksum, update_checksum},
};

pub const MIN_PAGE_SIZE: u64 = 4096; // smallest supported page size and most common filesystem block size
pub const MAX_PAGE_SIZE: u64 = 65536;

pub const DEFAULT_SQ_SIZE: usize = 128;
pub const DEFAULT_CQ_SIZE: usize = 256;

#[cfg(test)]
static DYN_BUFS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static POOL_BUFS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static PAUSES: AtomicUsize = AtomicUsize::new(0);

#[derive(Error, Debug)]
pub(crate) enum ReadError {
    #[error("invalid checksum")]
    BadChecksum,
    #[error("io error")]
    Unknown(#[from] anyhow::Error),
}

enum FioOp {
    Read(ReadData),
    ReadFinish(Waker),
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
    pg_idx: PgIdx,
    buf: PageBuf,
    state: Arc<Mutex<WriteState>>,
}

struct WriteState {
    waker: Option<Waker>,
    state: GenericOpState,
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

#[derive(Debug)]
pub struct IoLoopHandle {
    join: Option<JoinHandle<()>>,
    inner: Arc<Inner>,
}

#[derive(Clone, Debug)]
pub struct Fio {
    inner: Arc<Inner>,
    join: Arc<IoLoopHandle>,
}

#[derive(Debug)]
struct Inner {
    fio_file: File,
    file: Arc<DbFile>,

    page_size: InPgIdx,
    stop: AtomicBool,
    queue: SegQueue<FioOp>,

    bufs: Pin<Box<[u8]>>,
    free_bufs: ArrayQueue<usize>,

    // TODO: fully implement
    // read_states: Box<Mutex<ReadState>>,
    // free_read_states: ArrayQueue<usize>,
    generic_op_states: Box<[AtomicU32]>,
    free_generic_op_states: ArrayQueue<usize>,

    #[cfg(test)]
    park_signal: AtomicBool,
    #[cfg(test)]
    fail: AtomicBool,
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

impl AsRef<[u8]> for PageBuf {
    fn as_ref(&self) -> &[u8] {
        match self {
            PageBuf::Pool(shared) => {
                let page_size = shared.fio.page_size;
                unsafe { std::slice::from_raw_parts(shared.ptr(), page_size as usize) }
            }
            PageBuf::Dynamic(buf) => buf,
        }
    }
}

impl AsMut<[u8]> for PageBuf {
    fn as_mut(&mut self) -> &mut [u8] {
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
        self.fio.bufs[self.idx * self.fio.page_size as usize..].as_ptr() as *mut u8
    }
}

impl Drop for PoolBuf {
    fn drop(&mut self) {
        if let Err(_) = self.fio.free_bufs.push(self.idx) {
            eprintln!("Failed to return buffer idx {} to free pool", self.idx);
        }
    }
}

const MAX_PINNED_PAGES: PgIdx = 1 << 14;
pub(crate) fn max_pinned_pages(page_size: InPgIdx) -> Result<PgIdx> {
    let mut rlim = MaybeUninit::<rlimit>::uninit();

    let ret = unsafe { getrlimit(RLIMIT_MEMLOCK, rlim.as_mut_ptr()) };

    if ret != 0 {
        return Err(std::io::Error::last_os_error()).context("Failed to get RLIMIT_MEMLOCK");
    }

    let rlim = unsafe { rlim.assume_init() };
    let limit = rlim.rlim_cur;
    Ok(if limit == RLIM_INFINITY {
        MAX_PINNED_PAGES
    } else {
        limit / page_size / 2 // this is a hack, figure out why can't allocate full amount
    })
}

pub(crate) fn get_sys_mem_page_size() -> InPgIdx {
    let value = unsafe { sysconf(_SC_PAGESIZE) };
    value as InPgIdx
}

#[bon]
impl Fio {
    #[builder]
    pub(crate) fn builder(
        file: Arc<DbFile>,
        page_size: InPgIdx,
        #[builder(default = DEFAULT_SQ_SIZE)] sq: usize,
        #[builder(default = DEFAULT_CQ_SIZE)] cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        Self::new(
            file,
            page_size,
            sq,
            cq,
            page_buf_pool,
            generic_op_state_pool,
        )
    }

    pub(crate) fn new(
        file: Arc<DbFile>,
        page_size: InPgIdx,
        sq: usize,
        cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        let max_pinned_pages = max_pinned_pages(page_size).unwrap_or(cq as u64);
        let page_buf_pool = page_buf_pool
            .unwrap_or_else(|| cmp::max(cq * 2, cmp::max(cq, max_pinned_pages as usize) - cq));
        let generic_op_state_pool = generic_op_state_pool.unwrap_or_else(|| cq);

        let fio_file = {
            let mut open = OpenOptions::new();
            open.read(true);
            open.write(true);
            open.create(true);
            if page_size % fs_block_size(file.path())? == 0 {
                open.custom_flags(O_DIRECT);
            }
            open.open(file.path())?
        };

        let fd = fio_file.as_raw_fd();

        let stop = AtomicBool::new(false);
        let queue = SegQueue::new();
        let bufs = alloc_aligned_buffer(page_buf_pool, page_size as usize)?;
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
            stop,
            queue,
            bufs,
            free_bufs,
            generic_op_states,
            free_generic_op_states,
            #[cfg(test)]
            park_signal: AtomicBool::new(false),
            #[cfg(test)]
            fail: AtomicBool::new(false),
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

    pub fn page_size(&self) -> InPgIdx {
        self.inner.page_size
    }

    pub fn get_buf(&self) -> PageBuf {
        get_buf(&self.inner)
    }

    pub async fn get_pool_buf(&self) -> PageBuf {
        let idx = self.inner.free_bufs.pop().unwrap();
        PageBuf::Pool(PoolBuf {
            idx,
            fio: self.inner.clone(),
        })
    }

    pub fn get_dyn_buf(&self) -> PageBuf {
        get_dyn_buf(&self.inner)
    }

    fn join(&self) -> &JoinHandle<()> {
        self.join.as_ref().join.as_ref().unwrap()
    }

    pub(crate) async fn read(&self, pg_idx: u64) -> std::result::Result<PageBuf, ReadError> {
        let pg = self.read_unchecked(pg_idx).await?;

        if !has_valid_checksum(pg.as_ref()) {
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

    pub(crate) fn write(&self, pg_idx: u64, mut buf: PageBuf) -> impl Future<Output = Result<()>> {
        update_checksum(buf.as_mut());
        self.write_unchecked(pg_idx, buf)
    }

    pub(crate) fn write_unchecked(
        &self,
        pg_idx: u64,
        buf: PageBuf,
    ) -> impl Future<Output = Result<()>> {
        struct WriteFuture<'a> {
            fio: &'a Fio,
            pg_idx: u64,
            state: Arc<Mutex<WriteState>>,
        }

        impl<'a> Future for WriteFuture<'a> {
            type Output = Result<()>;

            fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
                let mut state = self.state.lock();
                let cur = std::mem::replace(&mut state.state, GenericOpState::Err);
                match cur {
                    GenericOpState::Init => {
                        state.waker = Some(cx.waker().clone());
                        state.state = GenericOpState::Pending;
                        Poll::Pending
                    }
                    GenericOpState::Pending => {
                        state.state = cur;
                        Poll::Pending
                    }
                    GenericOpState::Ready => {
                        state.state = cur;
                        Poll::Ready(Ok(()))
                    }
                    GenericOpState::Err => {
                        state.state = cur;
                        Poll::Ready(Err(anyhow!("Failed to perform disk write")))
                    }
                }
            }
        }

        let state = Arc::new(Mutex::new(WriteState {
            waker: None,
            state: GenericOpState::Init,
        }));
        let op = FioOp::Write(WriteData {
            pg_idx,
            buf,
            state: state.clone(),
        });
        self.inner.queue.push(op);
        self.join().thread().unpark();

        WriteFuture {
            fio: self,
            pg_idx,
            state,
        }
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

    pub async fn alloc(&self, len: PgIdx) -> Result<()> {
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
            #[cfg(test)]
            POOL_BUFS.fetch_add(1, Ordering::AcqRel);

            PageBuf::Pool(PoolBuf {
                idx,
                fio: inner.clone(),
            })
        })
        .unwrap_or_else(|| {
            #[cfg(test)]
            DYN_BUFS.fetch_add(1, Ordering::AcqRel);

            get_dyn_buf(&inner)
        })
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
    pg_size: InPgIdx,
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
        pg_size: InPgIdx,
        sq_size: usize,
        cq_size: usize,
        page_buf_pool_size: usize,
        fd: i32,
        inner: Arc<Inner>,
    ) -> Result<Self> {
        let ring = IoUring::builder()
            .setup_cqsize(cq_size as u32)
            .build(sq_size as u32)?;
        let mut bufs = alloc_aligned_buffer(cq_size, pg_size as usize)?;

        let iovecs: Vec<iovec> = (0usize..cq_size as usize)
            .map(|i| iovec {
                iov_base: (bufs[i * pg_size as usize..].as_mut_ptr()) as *mut c_void,
                iov_len: pg_size as usize,
            })
            .chain((0usize..(page_buf_pool_size as usize)).map(|i| iovec {
                iov_base: (inner.bufs[i * pg_size as usize..].as_ptr() as *mut u8) as *mut c_void,
                iov_len: pg_size as usize,
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
            pg_size,
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
            FioOp::ReadFinish(waker) => {
                waker.wake();
            }
            FioOp::Write(WriteData { state, .. }) => {
                let mut state = state.lock();
                state.state = GenericOpState::Err;
                if let Some(waker) = std::mem::replace(&mut state.waker, None) {
                    waker.wake();
                }
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
        let res = catch_unwind_anyhow(std::panic::AssertUnwindSafe(|| self.run_unchecked()));
        if let Err(e) = res {
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
        let mut completions = VecDeque::with_capacity(self.cq_size as usize);

        loop {
            if self.inner.queue.is_empty() && pending == 0 && self.fsync_back.is_empty() {
                #[cfg(test)]
                self.inner.park_signal.store(true, Ordering::Release);

                #[cfg(test)]
                PAUSES.fetch_add(1, Ordering::AcqRel);

                thread::park();

                #[cfg(test)]
                self.inner.park_signal.store(false, Ordering::Release);
            }
            if self.inner.stop.load(Ordering::Acquire) {
                return Ok(());
            }
            #[cfg(test)]
            {
                if self.inner.fail.load(Ordering::Acquire) {
                    panic!("test induced failure");
                }
            }

            let cq: io_uring::CompletionQueue = self.ring.completion();
            for cqe in cq {
                let id = cqe.user_data() as usize;
                let completion = std::mem::take(&mut self.ops[id]);
                if let Some(completion) = completion {
                    let completion = match completion {
                        FioOp::Read(ReadData {
                            waker,
                            mut buf,
                            state,
                            ..
                        }) => {
                            let mut inner = state.lock();
                            if cqe.result() >= 0 {
                                if let PageBuf::Dynamic(buf) = &mut buf {
                                    buf.copy_from_slice(
                                        &self.bufs[id * self.pg_size as usize
                                            ..(id + 1) * self.pg_size as usize],
                                    );
                                }
                                *inner = ReadState::Ready(buf);
                            } else {
                                eprintln!("Read failed with error: {}", cqe.result());
                                *inner = ReadState::Err;
                            }
                            FioOp::ReadFinish(waker)
                        }
                        completion => completion,
                    };
                    completions.push_back((cqe.result(), completion));
                }
                ids.push_back(id);
                pending -= 1;
            }

            let mut submitted = 0;

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
                                self.bufs[id * self.pg_size as usize..].as_mut_ptr(),
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
                    FioOp::ReadFinish(_) => {
                        eprintln!("read finish should never be submitted to queue");
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
                                let pg_size = self.pg_size as usize;
                                self.bufs[id * pg_size..(id + 1) * pg_size].copy_from_slice(pg);
                                (
                                    self.bufs[id * pg_size..].as_mut_ptr(),
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
                pending += submitted;
                self.ring
                    .submit()
                    .context("Failed to submit submission queue")?;
            } else if pending > 0 {
                self.ring
                    .submit_and_wait(1)
                    .context("Failed to submit submission queue")?;
            }

            while !completions.is_empty() {
                let (cqe_res, completion) = completions.pop_front().unwrap();
                match completion {
                    FioOp::Read(_) => {
                        // no-op
                    }
                    FioOp::ReadFinish(waker) => {
                        waker.wake();
                    }
                    FioOp::Write(WriteData { state, .. }) => {
                        if cqe_res >= 0 {
                            let mut state = state.lock();
                            state.state = GenericOpState::Ready;
                            if let Some(waker) = std::mem::replace(&mut state.waker, None) {
                                waker.wake();
                            }
                        } else {
                            eprintln!("Write failed with error: {}", cqe_res);
                            let mut state = state.lock();
                            state.state = GenericOpState::Err;
                            if let Some(waker) = std::mem::replace(&mut state.waker, None) {
                                waker.wake();
                            }
                        }
                    }
                    FioOp::Commit(CommitData { state, waker })
                    | FioOp::CommitFlush(CommitData { state, waker }) => {
                        if cqe_res >= 0 {
                            state
                                .get()
                                .store(GenericOpState::Ready as u32, Ordering::Release);
                        } else {
                            eprintln!("Commit failed with error: {}", cqe_res);
                            state
                                .get()
                                .store(GenericOpState::Err as u32, Ordering::Release);
                        }
                        waker.wake();
                    }
                    FioOp::CommitBatch => {
                        for CommitData { state, waker } in self.fsync_front.drain(..) {
                            if cqe_res >= 0 {
                                state
                                    .get()
                                    .store(GenericOpState::Ready as u32, Ordering::Release);
                            } else {
                                eprintln!("Commit failed with error: {}", cqe_res);
                                state
                                    .get()
                                    .store(GenericOpState::Err as u32, Ordering::Release);
                            }
                            waker.wake();
                        }
                    }
                    FioOp::Alloc(AllocData { len, waker, state }) => {
                        if cqe_res >= 0 {
                            state
                                .get()
                                .store(GenericOpState::Ready as u32, Ordering::Release);
                        } else {
                            eprintln!("File alloc for len {} failed with error: {}", len, cqe_res);
                            state
                                .get()
                                .store(GenericOpState::Err as u32, Ordering::Release);
                        }
                        waker.wake();
                    }
                }
            }
        }
    }
}

pub fn choose_page_size(file: &File) -> Result<PgIdx> {
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    let block_size = fstatvfs.f_bsize as u64;
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

#[coverage(off)]
#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        path::Path,
        time::{Duration, Instant},
    };

    use function_name::named;
    use futures::future::join_all;
    use rand::{Rng, random, thread_rng};
    use tokio::{spawn, time::sleep};

    use crate::{
        meta::NUM_HEADER_PAGES,
        test::{TempDir, retry_until_success_tokio},
    };

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
            .page_size(meta.page_size())
            .sq(2)
            .cq(4)
            .page_buf_pool(2)
            .build()?;
        let mut buf = fio.get_buf();
        assert!(matches!(buf, PageBuf::Pool(_)));

        buf.as_mut()[0] = 1;
        assert_eq!(1, buf.as_ref()[0]);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, _) = temp_dir.fio_and_meta("db")?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(fio.file().path())?;

        let mut test_buf = vec![1u8; fio.page_size() as usize];
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

        assert_eq!(&test_buf, data.as_ref());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_read_page_dynamic() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (file, meta) = temp_dir.fio_cust("db")?;

        let fio = Fio::builder()
            .file(file.clone())
            .page_size(meta.page_size())
            .sq(2)
            .cq(4)
            .page_buf_pool(0)
            .build()?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file.path())?;
        let mut test_buf = vec![1u8; fio.page_size() as usize];
        update_checksum(&mut test_buf);
        file.write_all(&test_buf)?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read(NUM_HEADER_PAGES).await?;

        assert_eq!(&test_buf, data.as_ref());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_write_page() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, _) = temp_dir.fio_and_meta("db")?;

        let mut buf = fio.get_buf();
        buf.as_mut()[0..].fill(1u8);
        fio.write(0, buf).await?;
        fio.commit().await?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(fio.file().path())?;
        let mut buf = vec![0u8; fio.page_size() as usize];
        file.read_exact(&mut buf)?;

        let mut res = vec![1u8; fio.page_size() as usize];
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
            .page_size(meta.page_size())
            .sq(2)
            .cq(4)
            .page_buf_pool(0)
            .build()?;
        let mut buf = fio.get_buf();
        buf.as_mut()[0..].fill(1u8);
        fio.write(0, buf).await?;
        fio.commit_flush().await?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(file.path())?;
        let mut buf = vec![0u8; fio.page_size() as usize];
        file.read_exact(&mut buf)?;

        let mut res = vec![1u8; fio.page_size() as usize];
        update_checksum(&mut res);
        assert_eq!(res, buf);

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_simple_alloc() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, meta) = temp_dir.fio_and_meta("db")?;
        assert_eq!(NUM_HEADER_PAGES, meta.len());

        fio.alloc(NUM_HEADER_PAGES + 1).await?;
        assert!(NUM_HEADER_PAGES + 1 <= fs::metadata(fio.file().path())?.len());
        assert_eq!(
            fio.page_size() * (NUM_HEADER_PAGES + 1),
            fs::metadata(fio.file().path())?.len()
        );

        let _read_test = fio.read_unchecked(0).await?;

        fio.alloc(1000).await?;
        assert!(1000 <= fs::metadata(fio.file().path())?.len());
        assert_eq!(
            1000 * fio.page_size(),
            fs::metadata(fio.file().path())?.len()
        );

        let _read_test = fio.read_unchecked(5).await?;

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn check_thread_parking() -> Result<()> {
        let tmp = TempDir::new(function_name!())?;
        let (fio, _meta) = tmp.fio_and_meta("db")?;

        retry_until_success_tokio(
            || {
                assert_eq!(true, fio.inner.park_signal.load(Ordering::Acquire));
            },
            Duration::ZERO,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(true, fio.inner.park_signal.load(Ordering::Acquire));

        let run = Arc::new(AtomicBool::new(true));

        let task = {
            let run = run.clone();
            let fio = fio.clone();
            spawn(async move {
                while run.load(Ordering::Acquire) {
                    let mut buf = fio.get_buf();
                    buf.as_mut().fill(0xFF);
                    fio.write(0, buf).await?;
                }
                Ok(())
            })
        };

        retry_until_success_tokio(
            || {
                assert_eq!(false, fio.inner.park_signal.load(Ordering::Acquire));
            },
            Duration::ZERO,
            Duration::from_millis(100),
        )
        .await;

        run.store(false, Ordering::Release);
        task.await??;

        retry_until_success_tokio(
            || {
                assert_eq!(true, fio.inner.park_signal.load(Ordering::Acquire));
            },
            Duration::ZERO,
            Duration::from_millis(100),
        )
        .await;
        assert_eq!(true, fio.inner.park_signal.load(Ordering::Acquire));

        Ok(())
    }

    #[ignore]
    #[named]
    #[tokio::test(flavor = "multi_thread")]
    async fn random_write_stress_test() -> Result<()> {
        const PGS: PgIdx = 1 << 16;
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!())?;
        let file = Arc::new(tmp.file(LOC)?);
        let mut rng = thread_rng();
        let fio = Fio::builder()
            .file(file.clone())
            .page_size(fs_block_size(file.path())?)
            .build()?;

        fio.alloc(PGS).await?;

        {
            let mut writes = Vec::with_capacity(PGS as usize);
            for i in 0..PGS {
                let mut buf = fio.get_buf();
                buf.as_mut().fill(0xFF);
                writes.push(fio.write_unchecked(i, buf));
            }

            let results = join_all(writes).await;
            for result in results {
                result?;
            }

            fio.commit().await?;
        }

        let start = Instant::now();
        let mut writes = Vec::with_capacity(PGS as usize);
        for _ in 0..PGS {
            let mut buf = fio.get_buf();
            buf.as_mut().fill(0xAA);
            buf.as_mut()[0] = random();
            writes.push(fio.write(rng.gen_range(0..PGS), buf));
        }

        let results = join_all(writes).await;
        for result in results {
            result?;
        }
        let dur = (Instant::now() - start).as_secs_f64();

        println!("mem pg size: {}", get_sys_mem_page_size());
        println!(
            "duration: {}, pages: {}, IOPS: {}, throughput: {}MiBps",
            dur,
            PGS,
            PGS as f64 / dur,
            ((PGS as f64 * fio.page_size() as f64) / 1000000f64) / dur
        );
        println!(
            "dyn: {}, stat: {}, ratio: {}",
            DYN_BUFS.load(Ordering::Acquire),
            POOL_BUFS.load(Ordering::Acquire),
            DYN_BUFS.load(Ordering::Acquire) as f64 / POOL_BUFS.load(Ordering::Acquire) as f64,
        );
        println!("pauses: {}", PAUSES.load(Ordering::Acquire));

        fio.commit().await?;

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn thread_failure() -> Result<()> {
        let tmp = TempDir::new(function_name!())?;
        let (fio, _meta) = tmp.fio_and_meta("db")?;

        let _ = fio.read(0).await?;

        fio.inner.fail.store(true, Ordering::Release);
        fio.join().thread().unpark();

        sleep(Duration::from_millis(100)).await;

        let buf = fio.get_dyn_buf();
        assert!(fio.write(0, buf).await.is_err());
        assert!(fio.alloc(3).await.is_err());
        assert!(fio.read(0).await.is_err());
        assert!(fio.commit().await.is_err());

        Ok(())
    }
}
