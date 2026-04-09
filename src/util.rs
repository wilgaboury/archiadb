use rustix::fs::fstatvfs;
use std::{fs::File, os::fd::AsFd, path::Path};

use anyhow::Result;

const MIN_BLOCK_SIZE: u64 = 4096; // 4kb
const MAX_BLOCK_SIZE: u64 = 65536; // 64kb

/// file must exist
pub fn pick_block_size<P: AsRef<Path>>(path: P) -> Result<u64> {
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
        let block_size = pick_block_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", block_size);
    }
}
