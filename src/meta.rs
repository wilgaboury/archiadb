use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Result, bail};
use tokio::sync::Mutex;

use crate::{
    const_assert,
    fio::{Fio, MAX_PAGE_SIZE, MIN_PAGE_SIZE, choose_page_size},
    util::{CHECKSUM_SIZE, from_bytes, from_bytes_mut, has_valid_checksum, update_checksum},
};

type MagicType = u128;
const MAGIC: MagicType = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
type FmtVersionType = u64;
const FMT_VERSION: FmtVersionType = 0;
pub const NUM_HEADER_PAGES: u64 = 2;

#[repr(C, packed)]
pub struct Meta {
    magic: MagicType,

    fmt_version: FmtVersionType,
    page_size: u64,
    root1: u64,
    root2: u64,

    version: u64,
    pub open: u8,
    pub len: u64,
}

const_assert!(size_of::<Meta>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

impl Meta {
    pub fn init(&mut self, page_size: u64) {
        self.magic = MAGIC;
        self.fmt_version = FMT_VERSION;
        self.page_size = page_size;
        self.root1 = NUM_HEADER_PAGES + 1;
        self.root2 = NUM_HEADER_PAGES + 2;
        self.version = 0;
        self.open = 1;
        self.len = NUM_HEADER_PAGES;
    }
}

pub struct MetaHandler {
    fmt_version: FmtVersionType,
    page_size: u64,
    root1: u64,
    root2: u64,

    len: AtomicU64,

    inner: Mutex<Inner>,
}

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
            Self::choose_front_back(buf1, buf2)?
        };

        let meta = from_bytes::<Meta>(&front);
        Ok(Self {
            fmt_version: meta.fmt_version,
            page_size: meta.page_size,
            root1: meta.root1,
            root2: meta.root2,
            len: AtomicU64::new(meta.len),
            inner: Mutex::new(Inner {
                version: meta.version,
                is_first,
                front,
                back,
            }),
        })
    }

    pub fn fmt_version(&self) -> FmtVersionType {
        self.fmt_version
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    pub fn root1(&self) -> u64 {
        self.root1
    }

    pub fn root2(&self) -> u64 {
        self.root2
    }

    pub fn len(&self) -> u64 {
        self.len.load(Ordering::Acquire)
    }

    pub fn mutate(&self, file: &File, f: impl FnOnce(&mut Meta)) -> Result<()> {
        let mut inner_guard = self.inner.blocking_lock();
        let len = {
            let inner = &mut *inner_guard;
            inner.version += 1;
            inner.back.copy_from_slice(&inner.front);
            let meta = from_bytes_mut::<Meta>(&mut inner.back);
            f(meta);
            meta.version = inner.version;
            let len = meta.len;
            update_checksum(&mut inner.back);

            let offset = if inner.is_first { self.page_size } else { 0 };
            file.write_at(&inner.back, offset)?;

            std::mem::swap(&mut inner.front, &mut inner.back);
            inner.is_first = !inner.is_first;
            len
        };
        self.len.store(len, Ordering::Release);

        Ok(())
    }

    pub async fn mutate_async(&self, fio: &Fio, f: impl FnOnce(&mut Meta)) -> Result<()> {
        let mut inner_guard = self.inner.lock().await;
        let len = {
            let inner = &mut *inner_guard;
            inner.version += 1;
            inner.back.copy_from_slice(&inner.front);
            let meta = from_bytes_mut::<Meta>(&mut inner.back);
            f(meta);
            meta.version = inner.version;
            let len = meta.len;
            update_checksum(&mut inner.back);

            let pg_idx = if inner.is_first { 1 } else { 0 };
            {
                let mut buf = fio.get_buf();
                buf.get_mut().copy_from_slice(&inner.back);
                fio.write(pg_idx, buf).await?;
            }

            std::mem::swap(&mut inner.front, &mut inner.back);
            inner.is_first = !inner.is_first;
            len
        };
        self.len.store(len, Ordering::Release);

        Ok(())
    }

    fn read_page_size(file: &File) -> Result<u64> {
        let offset: u64 = (size_of::<MagicType>() + size_of::<FmtVersionType>()) as u64;
        let mut buf = [0u8; size_of::<u64>()];
        let read = file.read_at(&mut buf, offset)?;
        if read < size_of::<u64>() {
            bail!("File too small to contain metadata");
        }
        let page_size = u64::from_ne_bytes(buf);
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
                meta1.version >= meta2.version
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

    use crate::meta::Meta;
    use crate::util::from_bytes;
    use crate::{meta::MagicType, test_util::TempDir};

    fn corrupt_page(file: &File, page_size: u64, page_num: u64) -> Result<()> {
        let offset = page_num * page_size;
        let garbage = [0u8; size_of::<MagicType>()];
        file.write_all_at(&garbage, offset)?;
        Ok(())
    }

    #[named]
    #[test]
    fn test_mutate_sync() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;

        let file = temp_dir.file("sync.db")?;
        let meta_hand = temp_dir.meta("sync.db")?;

        let mut buf = vec![0u8; meta_hand.page_size as usize];

        meta_hand.mutate(&file, |m| m.len = 100)?;
        file.read_at(&mut buf, meta_hand.page_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!({ meta.len }, 100);

        meta_hand.mutate(&file, |m| m.len = 101)?;
        file.read_at(&mut buf, 0)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!({ meta.len }, 101);

        meta_hand.mutate(&file, |m| m.len = 102)?;
        file.read_at(&mut buf, meta_hand.page_size)?;
        let meta = from_bytes::<Meta>(&buf);
        assert_eq!({ meta.len }, 102);

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
            meta_hand.mutate_async(&fio, |m| m.len = 100).await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!({ meta.len }, 100);

            meta_hand.mutate_async(&fio, |m| m.len = 101).await?;
            let buf = fio.read(0).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!({ meta.len }, 101);

            meta_hand.mutate_async(&fio, |m| m.len = 102).await?;
            let buf = fio.read(1).await?;
            let meta = from_bytes::<Meta>(&buf.get());
            assert_eq!({ meta.len }, 102);

            Ok(())
        });
        result?;

        Ok(())
    }
}
