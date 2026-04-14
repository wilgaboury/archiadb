use std::{
    collections::VecDeque,
    ffi::c_void,
    fs::{File, OpenOptions},
    os::fd::{AsFd, AsRawFd},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    vec,
};

use anyhow::{Ok, Result, anyhow};
use crossbeam::queue::SegQueue;
use io_uring::IoUring;
use libc::iovec;
use rustix::fs::fstatvfs;

use crate::spin::SpinLock;

const MIN_BLOCK_SIZE: u64 = 4096; // 4kb
const MAX_BLOCK_SIZE: u64 = 65536; // 64kb

const QD: u32 = 128;

#[derive(Clone)]
enum FioOp {
    Read(ReadData),
}

#[derive(Clone)]
struct ReadData {
    block: u64,
    waker: Waker,
    result: Arc<SpinLock<ReadResultState>>,
}

enum ReadResultState {
    Init,
    Pending(Vec<u8>),
    Ready(Vec<u8>),
    Done,
}

pub struct Fio {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    block_size: u64,
    file: File,
    fd: i32,
    shared: Arc<Shared>,
    join: Option<JoinHandle<Result<()>>>,
}

struct Shared {
    stop: AtomicBool,
    queue: SegQueue<FioOp>,
}

pub struct FioBlock {}

impl Fio {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        file.try_lock()?; // prevent multiple instances from opening the same file

        let block_size = get_block_size(path.as_ref())?;
        validate_block_size(block_size)?;

        let fd = file.as_raw_fd();

        let stop = AtomicBool::new(false);
        let queue = SegQueue::new();
        let shared = Arc::new(Shared { stop, queue });

        let join = {
            let shared = shared.clone();
            thread::spawn(move || io_uring_loop(block_size, fd, shared))
        };

        Ok(Self {
            inner: Arc::new(Inner {
                path: path.as_ref().to_path_buf(),
                block_size,
                file,
                fd,
                shared,
                join: Some(join),
            }),
        })
    }

    pub fn block_size(&self) -> u64 {
        self.inner.block_size
    }

    pub fn len(&self) -> u64 {
        todo!("implement len")
    }

    pub async fn read_block(&self, idx: u64) -> Vec<u8> {
        pub struct ReadBlockFuture<'a> {
            fio: &'a Fio,
            idx: u64,
            result: Arc<SpinLock<ReadResultState>>,
        }

        impl<'a> Future for ReadBlockFuture<'a> {
            type Output = Vec<u8>;

            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let mut result = self.result.lock();
                let state = std::mem::replace(&mut *result, ReadResultState::Done);
                match state {
                    ReadResultState::Init => {
                        let op = FioOp::Read(ReadData {
                            block: self.idx,
                            waker: cx.waker().clone(),
                            result: self.result.clone(),
                        });
                        self.fio.inner.shared.queue.push(op);
                        self.fio.inner.join.as_ref().unwrap().thread().unpark();

                        *result =
                            ReadResultState::Pending(vec![0u8; self.fio.inner.block_size as usize]);
                        Poll::Pending
                    }
                    ReadResultState::Pending(_) => {
                        *result = state;
                        Poll::Pending
                    }
                    ReadResultState::Ready(data) => {
                        *result = ReadResultState::Done;
                        Poll::Ready(data)
                    }
                    ReadResultState::Done => Poll::Pending,
                }
            }
        }

        impl Drop for ReadBlockFuture<'_> {
            fn drop(&mut self) {
                let mut result = self.result.lock();
                *result = ReadResultState::Done;
            }
        }

        ReadBlockFuture {
            fio: self,
            idx,
            result: Arc::new(SpinLock::new(ReadResultState::Init)),
        }
        .await
    }

    // pub async fn write_block(&self) -> Result<()> {
    //     todo!("implement write")
    // }

    // pub async fn commit(&self) -> Result<()> {
    //     todo!("implement commit")
    // }

    // pub async fn alloc(&self, blocks: u64) -> Result<()> {
    //     todo!("implement alloc")
    // }

    // pub fn delete(self) -> Result<()> {
    //     todo!("implement delete")
    // }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        self.shared.stop.store(true, Ordering::Release);
        let join = self.join.take().unwrap();
        join.thread().unpark();
        let _ = join.join();
    }
}

const SPIN_LIMIT: u64 = 32;

fn io_uring_loop(block_size: u64, fd: i32, shared: Arc<Shared>) -> Result<()> {
    let mut pending_operations = 0;
    let mut ring = IoUring::new(QD)?;
    let mut buffers: Vec<Vec<u8>> = (0..QD).map(|_| vec![0u8; block_size as usize]).collect();
    let mut ids: VecDeque<usize> = (0..QD as usize).collect();
    let mut pending_ops: Vec<Option<FioOp>> = vec![None; QD as usize];
    let mut spins = 0;

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

    let mut submission_batch = Vec::with_capacity(QD as usize);

    loop {
        if shared.queue.is_empty() && pending_operations == 0 {
            thread::park();
        }
        if shared.stop.load(Ordering::Acquire) {
            return Ok(());
        }

        while !ids.is_empty()
            && let Some(op) = shared.queue.pop()
        {
            match op {
                FioOp::Read(data) => {
                    let offset = data.block * block_size;
                    let id = ids.pop_front().unwrap();
                    let read = io_uring::opcode::Read::new(
                        io_uring::types::Fd(fd),
                        buffers[id].as_mut_ptr(),
                        block_size as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(id as u64);
                    submission_batch.push(read);
                    pending_ops[id] = Some(FioOp::Read(data));
                }
            }
        }

        if !submission_batch.is_empty() {
            unsafe {
                ring.submission()
                    .push_multiple(&submission_batch)
                    .expect("Failed to push submission queue");
            }
            ring.submit();
            pending_operations += submission_batch.len() as u32;
        }

        let cq = ring.completion();
        let mut had_entry = false;
        for cqe in cq {
            had_entry = true;

            let id = cqe.user_data() as usize;
            match std::mem::take(&mut pending_ops[id]) {
                Some(FioOp::Read(ReadData {
                    block: _,
                    waker,
                    result,
                })) => {
                    let mut inner = result.lock();
                    let state = std::mem::replace(&mut *inner, ReadResultState::Done);
                    if let ReadResultState::Pending(mut data) = state {
                        data.extend_from_slice(&buffers[id]);
                        *inner = ReadResultState::Ready(data);
                    }
                    waker.wake();
                }
                _ => panic!("should never get here"),
            }

            ids.push_back(id);
            pending_operations -= 1;
        }

        if had_entry {
            spins = 0;
        } else {
            spins += 1;
            if spins >= SPIN_LIMIT && pending_operations > 0 {
                ring.submit_and_wait(1);
                spins = 0;
            }
        }
    }
}

pub fn get_block_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    Ok(fstatvfs.f_bsize)
}

pub fn validate_block_size(block_size: u64) -> Result<()> {
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
    use std::io::Write;

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
    async fn test_read_block() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let test_file = temp_dir.path().join("testfile");

        let fio = Fio::open("/tmp/testfile").unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(test_file)?;
        file.write_all(&vec![1u8; fio.block_size() as usize])?;
        file.flush()?;
        file.sync_all()?;

        let data = fio.read_block(0).await;
        assert_eq!(vec![1u8; fio.block_size() as usize], data);

        Ok(())
    }
}
