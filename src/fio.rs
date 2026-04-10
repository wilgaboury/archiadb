use std::{
    collections::{LinkedList, VecDeque},
    fs::{File, OpenOptions},
    os::fd::{AsFd, AsRawFd},
    path::{Path, PathBuf},
    task::Waker,
};

use anyhow::{Result, anyhow};
use rustix::fs::fstatvfs;

use crate::spin::SpinLock;

const MIN_BLOCK_SIZE: u64 = 4096; // 4kb
const MAX_BLOCK_SIZE: u64 = 65536; // 64kb

const QD: u32 = 128;

enum IoOp {
    Read,
    Write,
    Commit,
    Alloc,
}

// bridge from async producer to sync consumer
pub struct OpQueue {
    inner: SpinLock<OpQueueInner>,
}

pub struct OpQueueInner {
    queue: VecDeque<IoOp>,
    wakers: LinkedList<Waker>,
}

pub struct Fio {
    path: PathBuf,
    block_size: u64,
    file: File,
    fd: i32,
    // thread: Thread,
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

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            block_size,
            file,
            fd,
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
