use std::{fs::File, os::fd::AsFd, path::Path};

use anyhow::Result;
use rustix::fs::fstatvfs;

const MIN_BLOCK_SIZE: u64 = 4096; // 4kb
const MAX_BLOCK_SIZE: u64 = 65536; // 64kb

pub struct Fio {}

pub struct FioBlock {}

impl Fio {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        todo!("implement new")
    }

    pub fn block_size(&self) -> u64 {
        todo!("implement block size")
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

pub fn get_block_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    let block_size = fstatvfs.f_bsize;
    if block_size >= MIN_BLOCK_SIZE
        && block_size <= MAX_BLOCK_SIZE
        && block_size % MIN_BLOCK_SIZE == 0
    {
        Ok(block_size)
    } else {
        Ok(MIN_BLOCK_SIZE)
    }
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
