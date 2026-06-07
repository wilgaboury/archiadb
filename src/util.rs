use rustix::fs::fstatvfs;
use std::{fs::File, os::fd::AsFd, path::Path};
use xxhash_rust::xxh3::xxh3_64;

use anyhow::Result;

#[macro_export]
macro_rules! const_assert {
    ($($arg:tt)*) => {
        const _: () = assert!($($arg)*);
    };
}

pub const MAX_KEY_PATH_LEN: usize = u8::MAX as usize;
pub const MAX_KEY_SIZE: usize = u8::MAX as usize;
pub const MIN_PAGE_SIZE: u64 = 4096; // 4kb
pub const MAX_PAGE_SIZE: u64 = 65536; // 64kb

/// file must exist
pub fn pick_page_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    let block_size = fstatvfs.f_bsize;
    if block_size >= MIN_PAGE_SIZE && block_size <= MAX_PAGE_SIZE && block_size % MIN_PAGE_SIZE == 0
    {
        Ok(block_size)
    } else {
        Ok(MIN_PAGE_SIZE)
    }
}

pub type Checksum = u64;
pub const CHECKSUM_SIZE: usize = size_of::<Checksum>();

pub fn update_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = xxh3_64(&buf[..len - CHECKSUM_SIZE]);
    buf[len - CHECKSUM_SIZE..].clone_from_slice(&checksum.to_ne_bytes());
}

pub fn has_valid_checksum(buf: &[u8]) -> bool {
    let len = buf.len();
    let checksum_bytes: [u8; CHECKSUM_SIZE] = buf[len - CHECKSUM_SIZE..]
        .try_into()
        .expect("buffer cannot fit checksum");
    let checksum = Checksum::from_ne_bytes(checksum_bytes);
    let content_checksum = xxh3_64(&buf[..len - CHECKSUM_SIZE]);
    content_checksum == checksum
}

pub fn validate_checksum(buf: &[u8]) -> Result<()> {
    if has_valid_checksum(buf) {
        Ok(())
    } else {
        Err(anyhow::anyhow!("invalid checksum"))
    }
}

pub fn from_bytes<T>(buf: &[u8]) -> &T {
    assert!(buf.len() >= size_of::<T>(), "buffer too small for type");
    assert_eq!(
        buf.as_ptr() as usize % align_of::<T>(),
        0,
        "buffer misaligned for type"
    );
    unsafe { &*(buf.as_ptr() as *const T) }
}

pub fn from_bytes_mut<T>(buf: &mut [u8]) -> &mut T {
    assert!(buf.len() >= size_of::<T>(), "buffer too small for type");
    assert_eq!(
        buf.as_ptr() as usize % align_of::<T>(),
        0,
        "buffer misaligned for type"
    );
    unsafe { &mut *(buf.as_mut_ptr() as *mut T) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_block_size() {
        let block_size = pick_page_size(Path::new("/")).unwrap();
        println!("Auto picked size: {}", block_size);
    }

    #[test]
    fn test_checksum() {
        let mut content: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(!has_valid_checksum(&content));
        assert!(validate_checksum(&content).is_err());
        update_checksum(&mut content);
        assert!(has_valid_checksum(&content));
        assert!(validate_checksum(&content).is_ok());
        content.fill(1);
        assert!(!has_valid_checksum(&content));
        assert!(validate_checksum(&content).is_err());
    }
}
