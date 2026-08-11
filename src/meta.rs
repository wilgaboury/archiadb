use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Result, anyhow, bail};
use tokio::sync::Mutex;

use crate::{
    const_assert,
    fio::{Fio, MAX_PAGE_SIZE, MIN_PAGE_SIZE, choose_page_size},
    uint::{InPgIdxDisk, PgIdxDisk, U16, U64, U128},
    util::{CHECKSUM_SIZE, from_bytes, from_bytes_mut, has_valid_checksum, update_checksum},
};

pub(crate) type MagicType = u128;
pub(crate) type MagicTypeDisk = U128;
pub(crate) const MAGIC: MagicType = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
pub(crate) const NUM_HEADER_PAGES: u64 = 2;

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FmtVer {
    V1 = 0,
}

const CUR_FMT_VER: FmtVer = FmtVer::V1;

impl TryFrom<u64> for FmtVer {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> anyhow::Result<Self> {
        match value {
            0 => Ok(FmtVer::V1),
            _ => Err(anyhow!("invalid format version")),
        }
    }
}

#[repr(C, packed)]
pub(crate) struct Meta {
    magic: MagicTypeDisk,

    fmt_ver: U16,
    pg_size: InPgIdxDisk,
    root1: PgIdxDisk,
    root2: PgIdxDisk,

    version: U64,
    open: u8,
    len: PgIdxDisk,

    // global alloc data
    galloc_fidx: PgIdxDisk,
    galloc_bidx: PgIdxDisk,
}

const_assert!(size_of::<Meta>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

impl Meta {
    pub(crate) fn init(&mut self, page_size: u64) {
        self.magic.set(MAGIC);
        self.fmt_ver.set(CUR_FMT_VER as u64);
        self.pg_size.set(page_size as u64);
        self.root1.set(NUM_HEADER_PAGES + 1 as u64);
        self.root2.set(NUM_HEADER_PAGES + 2 as u64);

        self.set_version(0);
        self.set_open(false);
        self.set_len(NUM_HEADER_PAGES);
    }

    pub(crate) fn magic(&self) -> MagicType {
        self.magic.get()
    }

    pub(crate) fn fmt_ver(&self) -> FmtVer {
        self.fmt_ver.get().try_into().unwrap()
    }

    pub(crate) fn pg_size(&self) -> u64 {
        self.pg_size.get()
    }

    pub(crate) fn root1(&self) -> u64 {
        self.root1.get()
    }

    pub(crate) fn root2(&self) -> u64 {
        self.root2.get()
    }

    pub(crate) fn version(&self) -> u64 {
        self.version.get()
    }

    fn set_version(&mut self, ver: u64) {
        self.version.set(ver);
    }

    pub(crate) fn open(&self) -> bool {
        if self.open > 0 { true } else { false }
    }

    pub(crate) fn set_open(&mut self, is_open: bool) {
        self.open = if is_open { 1 } else { 0 };
    }

    pub(crate) fn len(&self) -> u64 {
        self.len.get()
    }

    pub(crate) fn set_len(&mut self, len: u64) {
        self.len.set(len);
    }

    pub(crate) fn galloc_fidx(&self) -> u64 {
        self.galloc_fidx.get()
    }

    pub(crate) fn set_galloc_fidx(&mut self, idx: u64) {
        self.galloc_fidx.set(idx);
    }

    pub(crate) fn galloc_bidx(&self) -> u64 {
        self.galloc_bidx.get()
    }

    pub(crate) fn set_galloc_bidx(&mut self, idx: u64) {
        self.galloc_bidx.set(idx);
    }
}

#[derive(Debug)]
pub(crate) struct MetaHandler {
    fmt_ver: FmtVer,
    pg_size: u64,
    root1: u64,
    root2: u64,

    len: AtomicU64,

    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    version: u64,
    is_first: bool,
    front: Box<[u8]>,
    back: Box<[u8]>,
}

impl MetaHandler {
    pub fn new(file: &File) -> Result<Self> {
        let (is_first, front, back) = if file.metadata()?.len() == 0 {
            let page_size = choose_page_size(file)?;
            let front = Self::create_buf(page_size);
            let back = Self::create_buf(page_size);
            file.write_at(&front, 0)?;
            file.write_at(&back, page_size as u64)?;
            (true, front, back)
        } else {
            let page_size = Self::read_page_size(&file)?;
            let buf1 = Self::read_buf(&file, page_size, 0)?;
            let buf2 = Self::read_buf(&file, page_size, page_size)?;
            let front_back = Self::choose_front_back(buf1, buf2)?;

            if from_bytes::<Meta>(&front_back.1).magic() != MAGIC {
                return Err(anyhow!(
                    "file is not an archia db file or magic number is corrupted",
                ));
            }

            front_back
        };

        let meta = from_bytes::<Meta>(&front);
        Ok(Self {
            fmt_ver: meta.fmt_ver(),
            pg_size: meta.pg_size(),
            root1: meta.root1(),
            root2: meta.root2(),
            len: AtomicU64::new(meta.len()),
            inner: Mutex::new(Inner {
                version: meta.version(),
                is_first,
                front,
                back,
            }),
        })
    }

    pub(crate) fn fmt_ver(&self) -> FmtVer {
        self.fmt_ver
    }

    pub(crate) fn page_size(&self) -> u64 {
        self.pg_size
    }

    pub(crate) fn root1(&self) -> u64 {
        self.root1
    }

    pub(crate) fn root2(&self) -> u64 {
        self.root2
    }

    pub(crate) fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    pub(crate) async fn open_async(&self) -> bool {
        self.access_async(|meta| meta.open()).await
    }

    pub(crate) fn try_mutate(&self, file: &File, f: impl FnOnce(&mut Meta)) -> Result<()> {
        let mut inner_guard = self.inner.try_lock()?;
        let inner = &mut *inner_guard;
        self.mutate_helper(file, f, inner)
    }

    pub(crate) fn mutate(&self, file: &File, f: impl FnOnce(&mut Meta)) -> Result<()> {
        let mut inner_guard = self.inner.blocking_lock();
        let inner = &mut *inner_guard;
        self.mutate_helper(file, f, inner)
    }

    fn mutate_helper(
        &self,
        file: &File,
        f: impl FnOnce(&mut Meta),
        inner: &mut Inner,
    ) -> Result<()> {
        let len = {
            inner.version += 1;
            inner.back.copy_from_slice(&inner.front);
            let meta = from_bytes_mut::<Meta>(&mut inner.back);
            f(meta);
            meta.set_version(inner.version);
            let len = meta.len();
            update_checksum(&mut inner.back);

            let offset = if inner.is_first { self.pg_size } else { 0 };
            file.write_at(&inner.back, offset)?;
            file.sync_all()?;

            std::mem::swap(&mut inner.front, &mut inner.back);
            inner.is_first = !inner.is_first;
            len
        };
        self.len.store(len, Ordering::Release);

        Ok(())
    }

    pub(crate) async fn mutate_async(&self, fio: &Fio, f: impl FnOnce(&mut Meta)) -> Result<()> {
        let mut inner_guard = self.inner.lock().await;
        let inner = &mut *inner_guard;
        let len = {
            inner.version += 1;
            inner.back.copy_from_slice(&inner.front);
            let meta = from_bytes_mut::<Meta>(&mut inner.back);
            f(meta);
            meta.set_version(inner.version);
            let len = meta.len();

            let pg_idx = if inner.is_first { 1 } else { 0 };
            {
                let mut buf = fio.get_buf();
                buf.get_mut().copy_from_slice(&inner.back);
                fio.write(pg_idx, buf).await?;
                fio.commit().await?;
            }

            std::mem::swap(&mut inner.front, &mut inner.back);
            inner.is_first = !inner.is_first;
            len
        };
        self.len.store(len, Ordering::Release);

        Ok(())
    }

    pub(crate) async fn access_async<T>(&self, f: impl FnOnce(&Meta) -> T) -> T {
        let mut inner_guard = self.inner.lock().await;
        let inner = &mut *inner_guard;
        let meta = from_bytes::<Meta>(&inner.front);
        f(meta)
    }

    fn read_page_size(file: &File) -> Result<u64> {
        let offset: u64 = (size_of::<MagicType>() + size_of::<FmtVer>()) as u64;
        let mut buf = [0u8; size_of::<u64>()];
        let read = file.read_at(&mut buf, offset)?;
        if read < size_of::<u64>() {
            bail!("File too small to contain metadata");
        }
        let page_size = u64::from_le_bytes(buf);
        if page_size < MIN_PAGE_SIZE || page_size % MIN_PAGE_SIZE != 0 || page_size > MAX_PAGE_SIZE
        {
            bail!("Invalid page size in metadata");
        }
        Ok(page_size)
    }

    fn read_buf(file: &File, page_size: u64, offset: u64) -> Result<Box<[u8]>> {
        let mut buf = vec![0u8; page_size as usize];
        let read = file.read_at(&mut buf, offset)?;
        if read < size_of::<Meta>() {
            bail!("File too small to contain metadata");
        }
        Ok(buf.into())
    }

    fn choose_front_back(buf1: Box<[u8]>, buf2: Box<[u8]>) -> Result<(bool, Box<[u8]>, Box<[u8]>)> {
        let buf1_checksum_valid = has_valid_checksum(&buf1);
        let buf2_checksum_valid = has_valid_checksum(&buf2);

        if !buf1_checksum_valid && !buf2_checksum_valid {
            bail!("File corrupted, both metadata pages have invalid checksums");
        } else if buf1_checksum_valid && !buf2_checksum_valid {
            Ok((true, buf1, buf2))
        } else if !buf1_checksum_valid && buf2_checksum_valid {
            Ok((false, buf2, buf1))
        } else {
            // Both checksums are valid, choose the one with higher version
            let keep_order = {
                let meta1 = from_bytes::<Meta>(&buf1);
                let meta2 = from_bytes::<Meta>(&buf2);
                meta1.version() >= meta2.version()
            };
            if keep_order {
                Ok((true, buf1, buf2))
            } else {
                Ok((false, buf2, buf1))
            }
        }
    }

    fn create_buf(page_size: usize) -> Box<[u8]> {
        let mut buf = vec![0u8; page_size].into_boxed_slice();
        let meta = from_bytes_mut::<Meta>(&mut buf);
        meta.init(page_size as u64);
        update_checksum(&mut buf);
        buf
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    use anyhow::Result;
    use function_name::named;

    use crate::meta::{CUR_FMT_VER, MAGIC, Meta};
    use crate::test_util::TempDir;
    use crate::util::{from_bytes, from_bytes_mut, update_checksum};

    #[named]
    #[test]
    fn test_mutate_sync() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;

        let file = temp_dir.file_raw("sync.db")?;
        let meta_hand = temp_dir.meta("sync.db")?;

        let mut buf = vec![0u8; meta_hand.pg_size as usize];

        meta_hand.mutate(&file, |m| m.set_len(100))?;
        file.read_at(&mut buf, meta_hand.pg_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 100);

        meta_hand.mutate(&file, |m| m.set_len(101))?;
        file.read_at(&mut buf, 0)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 101);

        meta_hand.mutate(&file, |m| m.set_len(0x1FFFFFFFFFFFFFFF))?;
        file.read_at(&mut buf, meta_hand.pg_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 0x1FFFFFFFFFFFFFFF);

        Ok(())
    }

    #[named]
    #[test]
    fn test_mutate_async() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;

        let (fio, meta_hand) = temp_dir.fio("sync.db")?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let result: Result<()> = rt.block_on(async {
            meta_hand.mutate_async(&fio, |m| m.set_len(100)).await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!(meta.len(), 100);

            meta_hand.mutate_async(&fio, |m| m.set_len(101)).await?;
            let buf = fio.read(0).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!(meta.len(), 101);

            meta_hand
                .mutate_async(&fio, |m| m.set_len(0x1FFFFFFFFFFFFFFF))
                .await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!(meta.len(), 0x1FFFFFFFFFFFFFFF);

            Ok(())
        });
        result?;

        Ok(())
    }

    #[named]
    #[test]
    fn test_get() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let meta_hand = temp_dir.meta("sync.db")?;

        assert_eq!(meta_hand.fmt_ver(), CUR_FMT_VER);
        assert_eq!(meta_hand.root1(), 3);
        assert_eq!(meta_hand.root2(), 4);

        Ok(())
    }

    #[named]
    #[test]
    fn test_corrption() -> Result<()> {
        fn corrupt_page(file: &File, page_size: u64, page_num: u64) -> Result<()> {
            file.write_all_at(&[0u8; 1], page_num * page_size)?;
            Ok(())
        }

        fn fix_corrupt_page(file: &File, page_size: u64, page_num: u64) -> Result<()> {
            file.write_all_at(&MAGIC.to_le_bytes()[0..1], page_num * page_size)?;
            Ok(())
        }

        let temp_dir = TempDir::new(function_name!())?;
        let loc = "meta.db";
        let file = temp_dir.file_raw(loc)?;
        let page_size = {
            let meta_hand = temp_dir.meta(loc)?;
            meta_hand.pg_size
        };

        corrupt_page(&file, page_size, 0)?;
        corrupt_page(&file, page_size, 1)?;
        {
            assert!(temp_dir.meta(loc).is_err());
        }

        fix_corrupt_page(&file, page_size, 0)?;
        {
            assert!(temp_dir.meta(loc)?.inner.blocking_lock().is_first);
        }

        corrupt_page(&file, page_size, 0)?;
        fix_corrupt_page(&file, page_size, 1)?;
        {
            assert!(!temp_dir.meta(loc)?.inner.blocking_lock().is_first);
        }

        fix_corrupt_page(&file, page_size, 0)?;
        {
            assert!(temp_dir.meta(loc)?.inner.blocking_lock().is_first);
        }

        let mut buf = vec![0u8; page_size as usize];
        {
            let meta = from_bytes_mut::<Meta>(&mut buf);
            meta.init(page_size);
            meta.set_version(meta.version() + 1);
        }
        update_checksum(&mut buf);

        file.write_all_at(&buf, page_size)?;
        {
            let meta_hand = temp_dir.meta(loc)?;
            let inner = meta_hand.inner.blocking_lock();
            assert!(!inner.is_first);
            assert_eq!(1, inner.version);
        }

        Ok(())
    }
}
