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
    fio::{Fio, PageBuf},
    key::KeyPath,
    uint::{InPgIdx, PgIdx, U64},
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

pub fn fs_block_size<P: AsRef<Path>>(path: P) -> Result<InPgIdx> {
    let file = File::open(path)?;
    let fd = file.as_fd();
    let fstatvfs = fstatvfs(fd)?;
    Ok(fstatvfs.f_bsize)
}

/// file must exist
pub fn default_page_size<P: AsRef<Path>>(path: P) -> Result<u64> {
    let block_size = fs_block_size(path)?;
    if block_size >= MIN_PAGE_SIZE && block_size <= MAX_PAGE_SIZE && block_size % MIN_PAGE_SIZE == 0
    {
        Ok(block_size)
    } else {
        Ok(MIN_PAGE_SIZE)
    }
}

pub(crate) type Checksum = u64;
pub(crate) type ChecksumDisk = U64;

pub(crate) fn update_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = xxh3_64(&buf[..len - size_of::<ChecksumDisk>()]);
    from_bytes_mut::<ChecksumDisk>(&mut buf[len - size_of::<ChecksumDisk>()..]).set(checksum);
}

pub(crate) fn has_valid_checksum(buf: &[u8]) -> bool {
    let len = buf.len();
    let disk_checksum = from_bytes::<ChecksumDisk>(&buf[len - size_of::<ChecksumDisk>()..]).get();
    let content_checksum = xxh3_64(&buf[..len - size_of::<ChecksumDisk>()]);
    disk_checksum == content_checksum
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

pub(crate) fn btree_header_version(pg: &PageBuf) -> u64 {
    let root = from_bytes::<BTreeRootHeader>(&pg.as_ref());
    root.version.get()
}

pub(crate) async fn read_root_w_retry(
    db: &Db,
    key: &KeyPath,
    pg_idx1: PgIdx,
    pg_idx2: PgIdx,
    timeout: Duration,
) -> Result<(PgIdx, PageBuf, PgIdx, PageBuf)> {
    let start = Instant::now();
    while Instant::now().duration_since(start) < timeout {
        let pg1 = db.inner.fio.read_unchecked(pg_idx1).await?;
        let pg2 = db.inner.fio.read_unchecked(pg_idx2).await?;
        if let Ok(ret) = order_front_back(pg_idx1, pg1, pg_idx2, pg2, btree_header_version) {
            return Ok(ret);
        }
    }

    let carc = db.inner.write_locks.get(key.to_owned());
    let _gaurd = carc.lock().await;
    let pg1 = db.inner.fio.read_unchecked(pg_idx1).await?;
    let pg2 = db.inner.fio.read_unchecked(pg_idx2).await?;
    order_front_back(pg_idx1, pg1, pg_idx2, pg2, btree_header_version)
}

pub(crate) fn ceil_div(num: u64, den: u64) -> u64 {
    (num + den - 1) / den
}

pub(crate) struct FrontBack {
    front: PgIdx,
    back: PgIdx,
}

impl FrontBack {
    pub(crate) async fn from_roots(
        fio: &Fio,
        pg_idx_1: PgIdx,
        pg_idx_2: PgIdx,
    ) -> Result<(FrontBack, PageBuf)> {
        let pg1 = fio.read(pg_idx_1).await?;
        let pg2 = fio.read(pg_idx_2).await?;
        let (front, buf, back, _) =
            order_front_back(pg_idx_1, pg1, pg_idx_2, pg2, btree_header_version)?;
        Ok((Self { front, back }, buf))
    }
}

#[cfg(test)]
mod tests {
    use function_name::named;
    use tokio::{
        spawn,
        task::JoinHandle,
        time::{error::Elapsed, sleep, timeout},
    };

    use crate::{
        key_path,
        test::{TempDir, corrupt_checksum},
    };

    use super::*;

    #[test]
    fn check_ciel_div() {
        assert_eq!(1, ceil_div(1, 2));
        assert_eq!(2, ceil_div(3, 2));
    }

    #[test]
    fn pick_block_size() {
        let block_size = fs_block_size(Path::new("/")).unwrap();
        println!("Filesystem block size: {}", block_size);
        let page_size = default_page_size(Path::new("/")).unwrap();
        println!("Picked page size: {}", page_size);
    }

    #[test]
    fn checksum() {
        let mut content: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0];
        assert!(!has_valid_checksum(&content));
        update_checksum(&mut content);
        assert!(has_valid_checksum(&content));
        corrupt_checksum(&mut content);
        assert!(!has_valid_checksum(&content));
        update_checksum(&mut content);
        assert!(has_valid_checksum(&content));
    }

    #[test]
    #[should_panic(expected = "buffer misaligned for type")]
    fn test_from_bytes_misaligned() {
        let buf = [0u8; 16];
        let misaligned = &buf[1..];
        let _: &u64 = from_bytes(misaligned);
    }

    #[test]
    #[should_panic(expected = "buffer misaligned for type")]
    fn test_from_bytes_mut_misaligned() {
        let mut buf = [0u8; 16];
        let misaligned = &mut buf[1..];
        let _: &mut u64 = from_bytes_mut(misaligned);
    }

    #[named]
    #[tokio::test]
    async fn root_rety_no_lock() -> Result<()> {
        let tmp = TempDir::new(function_name!())?;
        let db = tmp.db("db").await?;

        let mut buf: PageBuf = db.inner.fio.read(2).await?;
        corrupt_checksum(buf.get_mut());
        db.inner.fio.write_unchecked(2, buf).await?;
        let mut buf: PageBuf = db.inner.fio.read(3).await?;
        corrupt_checksum(buf.get_mut());
        db.inner.fio.write_unchecked(3, buf).await?;
        db.inner.fio.commit().await?;

        let db2 = db.clone();
        let task: JoinHandle<Result<()>> = tokio::spawn(async move {
            let (pg1, _, pg2, _) =
                read_root_w_retry(&db2, key_path![], 2, 3, Duration::from_secs(10)).await?;

            assert_eq!(pg1, 3);
            assert_eq!(pg2, 2);

            anyhow::Result::Ok(())
        });

        sleep(Duration::from_millis(500)).await;

        assert!(!task.is_finished());

        let buf: PageBuf = db.inner.fio.read_unchecked(3).await?;
        db.inner.fio.write(3, buf).await?;

        task.await??;

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn root_rety_lock() -> Result<()> {
        let tmp = TempDir::new(function_name!())?;
        let db = tmp.db("db").await?;

        let mut buf: PageBuf = db.inner.fio.read(3).await?;
        corrupt_checksum(buf.get_mut());
        db.inner.fio.write_unchecked(3, buf).await?;
        db.inner.fio.commit().await?;

        let carc = db.inner.write_locks.get(key_path![].to_owned());
        let gaurd = carc.lock().await;

        let db2 = db.clone();
        let task: JoinHandle<Result<anyhow::Result<()>, Elapsed>> =
            spawn(timeout(Duration::from_secs(5), async move {
                let (pg1, _, pg2, _) =
                    read_root_w_retry(&db2, key_path![], 2, 3, Duration::from_secs(0)).await?;

                assert_eq!(pg1, 2);
                assert_eq!(pg2, 3);

                anyhow::Result::Ok(())
            }));

        sleep(Duration::from_millis(500)).await;

        assert!(!task.is_finished());

        drop(gaurd);

        task.await???;

        Ok(())
    }
}
