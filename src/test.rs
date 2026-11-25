use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    panic,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;

use crate::{
    bio::{BlockIo, BlockIoDefault, BlockIoRapidHash},
    bnode::{BNodeHeader, BNodeRootHeader, get_page_ptr, get_value_leaf, in_pg_size_of},
    db::{BRootInfo, Db},
    defer::{Defer, DeferGuard},
    file::DbFile,
    galloc::Galloc,
    lalloc::{Arena, FreeListHeader, LallocCtx, idxs_per_fl_page, read_fl_idx},
    meta::{Meta, MetaHandler},
    uint::{PgIdx, PgIdxDisk},
    uring::BlockIoUring,
    util::{
        ChecksumDisk, FrontBack, btree_header_version, from_bytes, from_bytes_mut,
        meta_header_version, order_front_back,
    },
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
    pub(crate) root: BRootInfo<B>,
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

    let root_buf = bio.read(2).await?;

    Ok(TestLallocCtx {
        galloc: Galloc::new(),
        meta: MetaHandler::from_bio(bio).await?,
        bio: bio.clone(),
        defer_guard: defer.begin(),
        root: BRootInfo::new(FrontBack::new(2, 3), root_buf, arena.clone()),
    })
}

pub(crate) async fn validate_reachability_db<B: BlockIo>(bio: &B) -> Result<()> {
    let m1 = bio.read(0).await?;
    let m2 = bio.read(1).await?;
    let (_, meta_buf, _, _) = order_front_back((), m1, (), m2, meta_header_version)?;

    let meta = from_bytes::<Meta>(meta_buf.as_ref());

    let mut idxs = HashSet::new();
    for i in 2..meta.len.get() {
        idxs.insert(i);
    }

    validate_reachability_btree_root(bio, 2, 3, &mut idxs).await?;

    assert!(idxs.is_empty(), "there are unreachable pages: {:?}", idxs);

    Ok(())
}

pub(crate) async fn validate_reachability_btree_root<B: BlockIo>(
    bio: &B,
    root_idx_1: PgIdx,
    root_idx_2: PgIdx,
    idxs: &mut HashSet<PgIdx>,
) -> Result<()> {
    idxs.remove(&root_idx_1);
    idxs.remove(&root_idx_2);

    let r1 = bio.read(root_idx_1).await?;
    let r2 = bio.read(root_idx_2).await?;
    let (_, root_buf, _, _) = order_front_back((), r1, (), r2, btree_header_version)?;

    let root = from_bytes::<BNodeRootHeader>(root_buf.as_ref());

    for i in root.arena.next.get()..root.arena.start.get() + root.arena.len.get() {
        idxs.remove(&i);
    }

    let mut next = root.free.get();
    let idxs_per = idxs_per_fl_page(bio.page_size());
    while next != 0 {
        idxs.remove(&next);
        let fl_buf = bio.read(next).await?;

        for i in 0..idxs_per {
            let fl_idx = read_fl_idx(fl_buf.as_ref(), i);
            if fl_idx != 0 {
                idxs.remove(&fl_idx);
            } else {
                break;
            }
        }

        next = from_bytes::<FreeListHeader>(fl_buf.as_ref()).next.get();
    }

    for i in 0..root.node.len.get() {
        let pg_idx = get_page_ptr(root_buf.as_ref(), i);
        validate_reachability_btree_node(bio, pg_idx, idxs).await?;
    }

    Ok(())
}

pub(crate) async fn validate_reachability_btree_node<B: BlockIo>(
    bio: &B,
    pg_idx: PgIdx,
    idxs: &mut HashSet<PgIdx>,
) -> Result<()> {
    idxs.remove(&pg_idx);

    let buf = bio.read(pg_idx).await?;
    let header = from_bytes::<BNodeHeader>(buf.as_ref());

    match header.kind {
        crate::bnode::BNodeKind::Root => panic!("method should not be called on root"),
        crate::bnode::BNodeKind::Inner => {
            for i in 0..header.len.get() {
                let pg_idx = get_page_ptr(buf.as_ref(), i);
                validate_reachability_btree_node(bio, pg_idx, idxs).await?;
            }
        }
        crate::bnode::BNodeKind::Leaf => {
            for i in 0..header.len.get() {
                let value = get_value_leaf(buf.as_ref(), i);
                match value {
                    crate::bnode::LeafValueGetResult::Btree { pg_idx_1, pg_idx_2 } => {
                        Box::pin(validate_reachability_btree_root(
                            bio, pg_idx_1, pg_idx_2, idxs,
                        ))
                        .await?;
                    }
                    crate::bnode::LeafValueGetResult::ValueEmbedded { .. } => {
                        // no-op
                    }
                    crate::bnode::LeafValueGetResult::ValueLinkedList { mut pg_idx, .. } => {
                        let chunk_size = (bio.page_size()
                            - in_pg_size_of::<PgIdxDisk>()
                            - in_pg_size_of::<ChecksumDisk>())
                            as usize;
                        while pg_idx != 0 {
                            idxs.remove(&pg_idx);
                            let buf = bio.read(pg_idx).await?;
                            pg_idx = from_bytes::<PgIdxDisk>(&buf.as_ref()[chunk_size..]).get();
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
