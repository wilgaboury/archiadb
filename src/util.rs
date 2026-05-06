use rustix::fs::fstatvfs;
use std::{fs::File, os::fd::AsFd, path::Path};

use anyhow::Result;

#[macro_export]
macro_rules! const_assert {
    ($($arg:tt)*) => {
        const _: () = assert!($($arg)*);
    };
}

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

pub const CHECKSUM_SIZE: usize = size_of::<u32>();

pub fn update_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = crc32c::crc32c(&buf[..len - CHECKSUM_SIZE]);
    buf[len - CHECKSUM_SIZE..].clone_from_slice(&checksum.to_ne_bytes());
}

fn has_valid_checksum(buf: &[u8]) -> bool {
    let len = buf.len();
    let checksum_bytes: [u8; 4] = buf[len - CHECKSUM_SIZE..]
        .try_into()
        .expect("buffer cannot fit checksum");
    let checksum = u32::from_ne_bytes(checksum_bytes);
    let content_checksum = crc32c::crc32c(&buf[..len - CHECKSUM_SIZE]);
    content_checksum == checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_block_size() {
        let block_size = pick_block_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", block_size);
    }

    #[test]
    fn test_checksum() {
        let mut content: [u8; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0];
        assert!(!has_valid_checksum(&content));
        update_checksum(&mut content);
        assert!(has_valid_checksum(&content));
        content[0] = 0;
        assert!(!has_valid_checksum(&content));
    }
}
