use rustix::fs::{AtFlags, StatxFlags, statx};
use std::{fs::File, os::fd::AsFd, path::Path};

use anyhow::Result;

const DEFAULT_BLOCK_SIZE: u64 = 4096;

pub fn pick_block_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let statx = statx(fd, "", AtFlags::EMPTY_PATH, StatxFlags::ALL)?;
    let block_size = statx.stx_atomic_write_unit_max as u64;
    if block_size > 0 {
        Ok(block_size)
    } else {
        Ok(DEFAULT_BLOCK_SIZE)
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
