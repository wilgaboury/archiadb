use std::{
    fs::{File, OpenOptions},
    panic,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;

use crate::{
    bio::{BlockIo, BlockIoDefault, BlockIoRapidHash},
    bnode::BNodeRootHeader,
    db::{BRootInfo, Db},
    defer::{Defer, DeferGuard},
    file::DbFile,
    galloc::Galloc,
    lalloc::{Arena, LallocCtx},
    meta::{Meta, MetaHandler},
    uint::PgIdx,
    uring::BlockIoUring,
    util::{ChecksumDisk, FrontBack, from_bytes_mut},
};

pub(crate) const DEFAULT_FILE_PATH: &str = "test.db";

pub(crate) fn corrupt_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = from_bytes_mut::<ChecksumDisk>(&mut buf[len - size_of::<ChecksumDisk>()..]);
    checksum.set(checksum.get() + 1);
}

pub(crate) async fn retry_until_success_tokio<F, R>(
    f: F,
    retry_delay: Duration,
    timeout: Duration,
) -> R
where
    F: Fn() -> R + panic::UnwindSafe,
{
    let start = Instant::now();

    loop {
        if start.elapsed() > timeout {
            panic!("Retry timed out after {:?}", timeout);
        }

        match panic::catch_unwind(panic::AssertUnwindSafe(&f)) {
            Ok(result) => return result,
            Err(_) => {
                tokio::time::sleep(retry_delay).await;
            }
        }
    }
}

pub(crate) struct TmpDir {
    path: PathBuf,
}

impl TmpDir {
    pub fn new(suffix: &str) -> Result<Self> {
        let path = std::env::current_dir()?.join(format!(
            "tdat/{}_{}_{}",
            suffix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    pub fn root(&self) -> &Path {
        &self.path
    }

    pub fn path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.path.join(path)
    }

    pub async fn db(&self) -> Result<Db<BlockIoDefault>> {
        self.db_at(DEFAULT_FILE_PATH).await
    }

    pub async fn db_at<P: AsRef<Path>>(&self, path: P) -> Result<Db<BlockIoDefault>> {
        Db::builder()
            .path(self.path(path))
            .sq(2)
            .cq(4)
            .page_buf_pool(2)
            .build()
            .await
    }

    pub fn file(&self) -> Result<DbFile> {
        self.file_at(DEFAULT_FILE_PATH)
    }

    pub fn file_at<P: AsRef<Path>>(&self, path: P) -> Result<DbFile> {
        DbFile::open(self.path.join(path.as_ref()))
    }

    pub fn file_raw(&self) -> Result<File> {
        self.file_raw_at(DEFAULT_FILE_PATH)
    }

    pub fn file_raw_at<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path.join(path.as_ref()))?)
    }

    pub fn meta(&self) -> Result<MetaHandler> {
        self.meta_at(DEFAULT_FILE_PATH)
    }

    pub fn meta_at<P: AsRef<Path>>(&self, path: P) -> Result<MetaHandler> {
        let file = self.file_raw_at(path)?;
        MetaHandler::from_file(&file)
    }

    pub fn fio(&self) -> Result<BlockIoDefault> {
        self.fio_at(DEFAULT_FILE_PATH)
    }

    pub fn fio_at<P: AsRef<Path>>(&self, path: P) -> Result<BlockIoDefault> {
        let file = self.file_at(path)?;
        let meta = MetaHandler::from_file(file.file())?;
        Ok(BlockIoRapidHash::new(
            BlockIoUring::builder()
                .file(Arc::new(file))
                .page_size(meta.page_size())
                .sq(2)
                .cq(4)
                .page_buf_pool(2)
                .build()?,
        ))
    }

    pub fn fio_and_meta(&self) -> Result<(BlockIoDefault, MetaHandler)> {
        self.bio_and_meta_at(DEFAULT_FILE_PATH)
    }

    pub fn bio_and_meta_at<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(BlockIoDefault, MetaHandler)> {
        let file = self.file_at(path)?;
        let meta = MetaHandler::from_file(file.file())?;
        Ok((
            BlockIoRapidHash::new(
                BlockIoUring::builder()
                    .file(Arc::new(file))
                    .page_size(meta.page_size())
                    .sq(2)
                    .cq(4)
                    .page_buf_pool(2)
                    .build()?,
            ),
            meta,
        ))
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub(crate) struct TestLallocCtx<'d, B: BlockIo> {
    pub(crate) galloc: Galloc,
    pub(crate) meta: MetaHandler,
    pub(crate) bio: B,
    pub(crate) defer_guard: DeferGuard<'d>,
    pub(crate) root: BRootInfo,
}

impl<'d, B: BlockIo> TestLallocCtx<'d, B> {
    pub(crate) fn ctx(&mut self) -> LallocCtx<'_, 'd, B> {
        LallocCtx {
            galloc: &mut self.galloc,
            meta: &self.meta,
            bio: &self.bio,
            defer_guard: &mut self.defer_guard,
            root: &mut self.root,
        }
    }
}

pub(crate) async fn init_basic_mem_db<'a, B: BlockIo>(
    bio: &B,
    len: PgIdx,
    defer: &'a Defer,
) -> Result<TestLallocCtx<'a, B>> {
    bio.alloc(4 + len).await?;
    let arena = if len > 0 {
        Arena::new(4, len)
    } else {
        Arena::new(0, 0)
    };

    {
        let mut buf1 = bio.buf();
        let meta1 = from_bytes_mut::<Meta>(buf1.as_mut());
        meta1.init(bio.page_size());
        meta1.len.set(4 + len);
        bio.write(0, buf1).await?;

        let mut buf2 = bio.buf();
        let meta2 = from_bytes_mut::<Meta>(buf2.as_mut());
        meta2.init(bio.page_size());
        meta2.len.set(4 + len);
        bio.write(1, buf2).await?;
    }

    {
        let mut buf1 = bio.buf();
        let header1 = from_bytes_mut::<BNodeRootHeader>(buf1.as_mut());
        header1.init();
        header1.arena.from(&arena);
        bio.write(2, buf1).await?;

        let mut buf2 = bio.buf();
        let header2 = from_bytes_mut::<BNodeRootHeader>(buf2.as_mut());
        header2.init();
        header2.arena.from(&arena);
        bio.write(3, buf2).await?;
    }

    Ok(TestLallocCtx {
        galloc: Galloc::new(),
        meta: MetaHandler::from_bio(bio).await?,
        bio: bio.clone(),
        defer_guard: defer.begin(),
        root: BRootInfo::new(FrontBack::new(2, 3), arena.clone()),
    })
}
