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
    uint::{InPgIdxDisk, PgIdx, PgIdxDisk, U16, U64, U128},
    util::{Checksum, from_bytes, from_bytes_mut, order_front_back, update_checksum},
};

pub(crate) type MagicType = u128;
pub(crate) type MagicTypeDisk = U128;
pub(crate) const MAGIC: MagicType = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
pub(crate) const NUM_HEADER_PAGES: u64 = 2;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FmtVer {
    V1 = 0,
}
type FmtVerDisk = U16;

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
    pub(crate) magic: MagicTypeDisk,

    pub(crate) fmt_ver: FmtVerDisk,
    pub(crate) pg_size: InPgIdxDisk,
    pub(crate) root1: PgIdxDisk,
    pub(crate) root2: PgIdxDisk,

    pub(crate) version: U64,
    pub(crate) open: u8,
    pub(crate) len: PgIdxDisk,

    // global alloc data
    pub(crate) galloc_fidx: PgIdxDisk,
    pub(crate) galloc_bidx: PgIdxDisk,
    pub(crate) galloc_len: PgIdxDisk,
}

const_assert!(size_of::<Meta>() + size_of::<Checksum>() < MIN_PAGE_SIZE as usize);

impl Meta {
    pub(crate) fn init(&mut self, page_size: u64) {
        self.magic.set(MAGIC);
        self.fmt_ver.set(CUR_FMT_VER as u64);
        self.pg_size.set(page_size);
        self.root1.set(NUM_HEADER_PAGES);
        self.root2.set(NUM_HEADER_PAGES + 1);

        self.set_version(0);
        self.set_len(NUM_HEADER_PAGES);

        self.set_galloc_fidx(0);
        self.set_galloc_bidx(0);
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
            let mut front = Self::create_buf(page_size);
            let mut back = Self::create_buf(page_size);
            update_checksum(&mut front);
            file.write_at(&front, 0)?;
            update_checksum(&mut back);
            file.write_at(&back, page_size as u64)?;
            (true, front, back)
        } else {
            let page_size = Self::read_page_size(&file)?;
            let buf1 = Self::read_buf(&file, page_size, 0)?;
            let buf2 = Self::read_buf(&file, page_size, page_size)?;
            let (is_first, front, _, mut back) = order_front_back(true, buf1, false, buf2, |pg| {
                let root = from_bytes::<Meta>(&pg.as_ref());
                root.version.get()
            })?;
            back.copy_from_slice(&front);

            if from_bytes::<Meta>(&front).magic() != MAGIC {
                return Err(anyhow!(
                    "file is not an archia db file or magic number is corrupted",
                ));
            }

            (is_first, front, back)
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
                buf.as_mut().copy_from_slice(&inner.back);
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
        let offset: u64 = (size_of::<MagicTypeDisk>() + size_of::<FmtVerDisk>()) as u64;
        let mut buf = [0u8; size_of::<InPgIdxDisk>()];
        let read = file.read_at(&mut buf, offset)?;
        if read < size_of::<InPgIdxDisk>() {
            bail!("File too small to contain metadata");
        }
        let page_size = from_bytes::<InPgIdxDisk>(&buf).get();
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

    fn create_buf(page_size: PgIdx) -> Box<[u8]> {
        let mut buf = vec![0u8; page_size as usize].into_boxed_slice();
        from_bytes_mut::<Meta>(&mut buf).init(page_size);
        buf
    }
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    use anyhow::Result;
    use function_name::named;

    use crate::meta::{CUR_FMT_VER, MAGIC, Meta};
    use crate::test::TmpDir;
    use crate::util::{from_bytes, from_bytes_mut, update_checksum};

    #[named]
    #[test]
    fn test_mutate_sync() -> Result<()> {
        let temp_dir = TmpDir::new(function_name!())?;

        let file = temp_dir.file_raw_at("sync.db")?;
        let meta_hand = temp_dir.meta_at("sync.db")?;

        let mut buf = vec![0u8; meta_hand.pg_size as usize];

        meta_hand.mutate(&file, |m| m.set_len(100))?;
        file.read_at(&mut buf, meta_hand.pg_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 100);

        meta_hand.mutate(&file, |m| m.set_len(101))?;
        file.read_at(&mut buf, 0)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 101);

        meta_hand.mutate(&file, |m| m.set_len(0x0000005544332211))?;
        file.read_at(&mut buf, meta_hand.pg_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!(meta.len(), 0x0000005544332211);

        Ok(())
    }

    #[named]
    #[test]
    fn test_mutate_async() -> Result<()> {
        let temp_dir = TmpDir::new(function_name!())?;

        let (fio, meta_hand) = temp_dir.fio_and_meta_at("sync.db")?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let result: Result<()> = rt.block_on(async {
            meta_hand.mutate_async(&fio, |m| m.set_len(100)).await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.as_ref());
            assert_eq!(meta.len(), 100);

            meta_hand.mutate_async(&fio, |m| m.set_len(101)).await?;
            let buf = fio.read(0).await?;
            let meta = from_bytes::<Meta>(&buf.as_ref());
            assert_eq!(meta.len(), 101);

            meta_hand
                .mutate_async(&fio, |m| m.set_len(0x0000005544332211))
                .await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.as_ref());
            assert_eq!(meta.len(), 0x0000005544332211);

            Ok(())
        });
        result?;

        Ok(())
    }

    #[named]
    #[test]
    fn test_get() -> Result<()> {
        let temp_dir = TmpDir::new(function_name!())?;
        let meta_hand = temp_dir.meta_at("sync.db")?;

        assert_eq!(meta_hand.fmt_ver(), CUR_FMT_VER);
        assert_eq!(meta_hand.root1(), 2);
        assert_eq!(meta_hand.root2(), 3);

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

        let temp_dir = TmpDir::new(function_name!())?;
        let loc = "meta.db";
        let file = temp_dir.file_raw_at(loc)?;
        let page_size = {
            let meta_hand = temp_dir.meta_at(loc)?;
            meta_hand.pg_size
        };

        corrupt_page(&file, page_size, 0)?;
        corrupt_page(&file, page_size, 1)?;
        {
            assert!(temp_dir.meta_at(loc).is_err());
        }

        fix_corrupt_page(&file, page_size, 0)?;
        {
            assert!(temp_dir.meta_at(loc)?.inner.blocking_lock().is_first);
        }

        corrupt_page(&file, page_size, 0)?;
        fix_corrupt_page(&file, page_size, 1)?;
        {
            assert!(!temp_dir.meta_at(loc)?.inner.blocking_lock().is_first);
        }

        fix_corrupt_page(&file, page_size, 0)?;
        {
            assert!(temp_dir.meta_at(loc)?.inner.blocking_lock().is_first);
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
            let meta_hand = temp_dir.meta_at(loc)?;
            let inner = meta_hand.inner.blocking_lock();
            assert!(!inner.is_first);
            assert_eq!(1, inner.version);
        }

        Ok(())
    }
}
