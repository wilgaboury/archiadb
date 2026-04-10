use std::{
    collections::VecDeque,
    ffi::c_void,
    fs::{File, OpenOptions},
    os::fd::{AsFd, AsRawFd},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
    thread::{self, JoinHandle},
};

use anyhow::{Result, anyhow};
use io_uring::IoUring;
use libc::iovec;
use rustix::fs::fstatvfs;

use crate::{
    intrusive::{IntrusiveList, IntrusiveListNode},
    spin::SpinLock,
};

const MIN_BLOCK_SIZE: u64 = 4096; // 4kb
const MAX_BLOCK_SIZE: u64 = 65536; // 64kb

const QD: u32 = 128;

enum FioOp {
    Read,
    Write,
    Commit,
    Alloc,
}

enum OpResult {
    Read(Vec<u8>),
}

// bridge from async producer to sync consumer
#[derive(Clone)]
pub struct FioQueue {
    inner: Arc<SpinLock<FioQueueInner>>,
}

unsafe impl Send for FioQueue {}
unsafe impl Sync for FioQueue {}

pub struct FioQueueInner {
    queue: VecDeque<FioOp>,

    // wakers, push list
    head: *mut FioFuture,
    tail: *mut FioFuture,
}

impl FioQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SpinLock::new(FioQueueInner {
                queue: VecDeque::with_capacity(QD as usize),
                head: std::ptr::null_mut(),
                tail: std::ptr::null_mut(),
            })),
        }
    }

    pub fn push(&self, op: FioOp) -> FioFuture {
        let mut guard = self.inner.lock();
        guard.queue.push_back(op);

        todo!("finish implementing")
    }

    pub fn pop(&self) -> Option<FioOp> {
        let mut guard = self.inner.lock();
        let ret = guard.queue.pop_front();

        if let Some(future) = guard.pop() {
            let future = unsafe { &mut *future };
            if let FioFutureState::Queued(waker) =
                std::mem::replace(&mut future.state, FioFutureState::Dequeued)
            {
                waker.wake();
            }
        }

        ret
    }
}

impl IntrusiveList for FioQueueInner {
    type Node = FioFuture;

    #[allow(invalid_reference_casting)]
    fn head(&self) -> &mut *mut Self::Node {
        let this = unsafe { &mut *(self as *const Self as *mut Self) };
        &mut this.head
    }

    #[allow(invalid_reference_casting)]
    fn tail(&self) -> &mut *mut Self::Node {
        let this = unsafe { &mut *(self as *const Self as *mut Self) };
        &mut this.tail
    }
}

pub enum FioFutureState {
    Init,
    Queued(Waker),
    Dequeued,
    Result(OpResult),
}

pub struct FioFuture {
    op: FioOp,

    // protected by spin lock
    state: FioFutureState,
    prev: *mut FioFuture,
    next: *mut FioFuture,
}

unsafe impl Send for FioFuture {}
unsafe impl Sync for FioFuture {}

impl IntrusiveListNode for FioFuture {
    type List = FioQueueInner;

    #[allow(invalid_reference_casting)]
    fn prev(&self) -> &mut *mut Self {
        let this = unsafe { &mut *(self as *const Self as *mut Self) };
        &mut this.prev
    }

    #[allow(invalid_reference_casting)]
    fn next(&self) -> &mut *mut Self {
        let this = unsafe { &mut *(self as *const Self as *mut Self) };
        &mut this.next
    }
}

pub struct Fio {
    path: PathBuf,
    block_size: u64,
    file: File,
    fd: i32,
    queue: FioQueue,
    thread: JoinHandle<Result<()>>,
}

pub struct FioBlock {}

impl Fio {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let block_size = get_block_size(path.as_ref())?;
        if !is_valid_block_size(block_size) {
            return Err(anyhow!(
                "Block size for filesystem is {}, but it must between {} and {} and a multiple of {}",
                block_size,
                MIN_BLOCK_SIZE,
                MAX_BLOCK_SIZE,
                MIN_BLOCK_SIZE
            ));
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        file.try_lock()?;

        let fd = file.as_raw_fd();

        let queue = FioQueue::new();

        let stop = AtomicBool::new(false);

        let thread = {
            let queue = queue.clone();
            thread::spawn(move || {
                let ring = IoUring::new(QD)?;
                let mut buffers: Vec<Vec<u8>> =
                    (0..QD).map(|_| vec![0u8; block_size as usize]).collect();

                // The kernel expects an array of `iovec` structures (pointer + length).
                let iovecs: Vec<iovec> = buffers
                    .iter_mut()
                    .map(|buf| iovec {
                        iov_base: buf.as_mut_ptr() as *mut c_void,
                        iov_len: buf.len(),
                    })
                    .collect();

                unsafe {
                    ring.submitter()
                        .register_buffers(&iovecs)
                        .expect("Failed to register buffers");
                }

                loop {
                    loop {
                        if let Some(op) = queue.pop() {
                            todo!("bruh")
                        } else {
                            break;
                        }
                    }

                    // todo!("submit batch of ops to kernel and process results");

                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                }

                Result::Ok(())
            })
        };

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            block_size,
            file,
            fd,
            queue,
            thread,
        })
    }

    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    pub fn len(&self) -> u64 {
        todo!("implement len")
    }

    pub async fn read_block(&self, idx: u64) -> Result<FioBlock> {
        todo!("implement read")
    }

    pub async fn write_block(&self) -> Result<()> {
        todo!("implement write")
    }

    pub async fn commit(&self) -> Result<()> {
        todo!("implement commit")
    }

    pub async fn alloc(&self, blocks: u64) -> Result<()> {
        todo!("implement alloc")
    }

    pub fn delete(self) -> Result<()> {
        todo!("implement delete")
    }
}

impl Drop for Fio {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub fn get_block_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    Ok(fstatvfs.f_bsize)
}

pub fn is_valid_block_size(block_size: u64) -> bool {
    block_size >= MIN_BLOCK_SIZE && block_size <= MAX_BLOCK_SIZE && block_size % MIN_BLOCK_SIZE == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_block_size() {
        let block_size = get_block_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", block_size);
    }
}
