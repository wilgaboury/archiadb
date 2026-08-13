use rustix::fs::fstatvfs;
use std::{
    fs::File,
    os::fd::AsFd,
    path::Path,
    time::{Duration, Instant},
};
use xxhash_rust::xxh3::xxh3_64;

use anyhow::{Result, bail};

use crate::{
    btree::BTreeRootHeader,
    db::Db,
    fio::PageBuf,
    key::KeyPath,
    uint::{InPgIdx, PgIdx},
};

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

pub fn get_fs_block_size<P: AsRef<Path>>(path: P) -> Result<InPgIdx> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    Ok(fstatvfs.f_bsize)
}

/// file must exist
pub fn pick_page_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let block_size = get_fs_block_size(path)?;
    if block_size >= MIN_PAGE_SIZE && block_size <= MAX_PAGE_SIZE && block_size % MIN_PAGE_SIZE == 0
    {
        Ok(block_size)
    } else {
        Ok(MIN_PAGE_SIZE)
    }
}

pub(crate) type Checksum = u64;
pub(crate) const CHECKSUM_SIZE: usize = size_of::<Checksum>();

pub(crate) fn update_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = xxh3_64(&buf[..len - CHECKSUM_SIZE]);
    buf[len - CHECKSUM_SIZE..].clone_from_slice(&checksum.to_le_bytes());
}

pub(crate) fn has_valid_checksum(buf: &[u8]) -> bool {
    let len = buf.len();
    let checksum_bytes: [u8; CHECKSUM_SIZE] = buf[len - CHECKSUM_SIZE..]
        .try_into()
        .expect("buffer cannot fit checksum");
    let checksum = Checksum::from_le_bytes(checksum_bytes);
    let content_checksum = xxh3_64(&buf[..len - CHECKSUM_SIZE]);
    content_checksum == checksum
}

pub(crate) fn from_bytes<T>(buf: &[u8]) -> &T {
    assert!(buf.len() >= size_of::<T>(), "buffer too small for type");
    assert_eq!(
        buf.as_ptr() as usize % align_of::<T>(),
        0,
        "buffer misaligned for type"
    );
    unsafe { &*(buf.as_ptr() as *const T) }
}

pub(crate) fn from_bytes_mut<T>(buf: &mut [u8]) -> &mut T {
    assert!(buf.len() >= size_of::<T>(), "buffer too small for type");
    assert_eq!(
        buf.as_ptr() as usize % align_of::<T>(),
        0,
        "buffer misaligned for type"
    );
    unsafe { &mut *(buf.as_mut_ptr() as *mut T) }
}

pub(crate) fn order_front_back<V, B: AsRef<[u8]>, F: Fn(&B) -> u64>(
    v1: V,
    b1: B,
    v2: V,
    b2: B,
    version: F,
) -> Result<(V, B, V, B)> {
    let pg1_checksum_valid = has_valid_checksum(&b1.as_ref());
    let pg2_checksum_valid = has_valid_checksum(&b2.as_ref());

    if !pg1_checksum_valid && !pg2_checksum_valid {
        bail!("both invalid")
    } else if pg1_checksum_valid && !pg2_checksum_valid {
        Ok((v1, b1, v2, b2))
    } else if !pg1_checksum_valid && pg2_checksum_valid {
        Ok((v2, b2, v1, b1))
    } else {
        // Both checksums are valid, choose the one with higher version
        let keep_order = version(&b1) >= version(&b2);
        if keep_order {
            Ok((v1, b1, v2, b2))
        } else {
            Ok((v2, b2, v1, b1))
        }
    }
}

pub(crate) async fn read_root_w_retry(
    db: &Db,
    key: &KeyPath,
    pg_idx1: PgIdx,
    pg_idx2: PgIdx,
    timeout: Duration,
) -> Result<(PgIdx, PageBuf, PgIdx, PageBuf)> {
    let version = |pg: &PageBuf| {
        let root = from_bytes::<BTreeRootHeader>(&pg.as_ref());
        root.version.get()
    };

    let start = Instant::now();
    while Instant::now().duration_since(start) < timeout {
        let pg1 = db.inner.fio.read(pg_idx1).await?;
        let pg2 = db.inner.fio.read(pg_idx2).await?;
        if let Ok(ret) = order_front_back(pg_idx1, pg1, pg_idx2, pg2, version) {
            return Ok(ret);
        }
    }

    let carc = db.inner.write_locks.get(key.to_owned());
    let _gaurd = carc.lock().await;
    let pg1 = db.inner.fio.read(pg_idx1).await?;
    let pg2 = db.inner.fio.read(pg_idx2).await?;
    order_front_back(pg_idx1, pg1, pg_idx2, pg2, version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pick_block_size() {
        let block_size = get_fs_block_size(Path::new("/")).unwrap();
        println!("Filesystem block size: {}", block_size);
        let page_size = pick_page_size(Path::new("/")).unwrap();
        println!("Picked page size: {}", page_size);
    }

    #[test]
    fn test_checksum() {
        let mut content: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(!has_valid_checksum(&content));
        update_checksum(&mut content);
        assert!(has_valid_checksum(&content));
        content.fill(1);
        assert!(!has_valid_checksum(&content));
    }
}
