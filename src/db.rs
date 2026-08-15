use std::{path::Path, sync::Arc};

use anyhow::{Context, Result, anyhow};
use bon::bon;
use tokio::sync::Mutex;

use crate::{
    concache::ConCache,
    defer::{Defer, DeferGaurd},
    file::DbFile,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    flux::Flux,
    galloc::{Galloc, galloc_recover, init_root},
    key::{KeyPath, KeyPathBuf},
    lock::{Lock, LockGuard, LockType},
    meta::{MetaHandler, NUM_HEADER_PAGES},
    trie::KeyTrie,
};

#[derive(Debug, Clone)]
pub struct Db {
    pub(crate) inner: Arc<DbInner>,
}

#[derive(Debug)]
pub(crate) struct DbInner {
    pub(crate) file: Arc<DbFile>,
    pub(crate) meta: MetaHandler,
    pub(crate) fio: Fio,
    pub(crate) galloc: Galloc,
    pub(crate) defer: Defer,
    pub(crate) read_locks: ConCache<KeyPathBuf, Lock>,
    pub(crate) write_locks: ConCache<KeyPathBuf, Mutex<()>>,
}

#[bon]
impl Db {
    #[builder]
    pub async fn new<P: AsRef<Path>>(
        path: P,
        #[builder(default = DEFAULT_SQ_SIZE)] sq: usize,
        #[builder(default = DEFAULT_CQ_SIZE)] cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        let file = Arc::new(DbFile::open(path)?);
        let meta = MetaHandler::new(&file.file())?;
        let fio = Fio::new(
            file.clone(),
            meta.page_size(),
            sq,
            cq,
            page_buf_pool,
            generic_op_state_pool,
        )?;

        // check if database file suffered dirty shutdown
        if meta.open_async().await {
            galloc_recover(&meta, &fio).await?;
        }

        meta.mutate_async(&fio, |meta| {
            meta.set_open(true);
        })
        .await?;

        if meta.len() == NUM_HEADER_PAGES {
            init_root(&meta, &fio).await?;
        }

        Ok(Self {
            inner: Arc::new(DbInner {
                file,
                meta,
                fio,
                galloc: Galloc::new(),
                defer: Defer::new(),
                read_locks: ConCache::new(Box::new(|| Lock::new())),
                write_locks: ConCache::new(Box::new(|| Mutex::new(()))),
            }),
        })
    }

    pub fn page_size(&self) -> usize {
        self.inner.meta.page_size() as usize
    }

    pub fn txn(&self) -> TxnBuilder<'_> {
        TxnBuilder {
            db: &self.inner,
            ops: KeyTrie::new(),
        }
    }

    pub fn try_close(self) -> Result<()> {
        let _inner = Arc::try_unwrap(self.inner)
            .map_err(|_| anyhow!("could not close database, multiple references still exist"))?;
        Ok(())
    }
}

impl Drop for DbInner {
    fn drop(&mut self) {
        if let Err(e) = self.meta.try_mutate(self.file.file(), |meta| {
            meta.set_open(false);
        }) {
            eprintln!("Failed to set close flag: {}", e);
        }
    }
}

pub struct TxnBuilder<'a> {
    db: &'a DbInner,
    ops: KeyTrie<LockType>,
}

impl<'a> TxnBuilder<'a> {
    pub fn read(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert_lock(path, LockType::Read)?;
        Ok(self)
    }

    pub fn write(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert_lock(path, LockType::Write)?;
        Ok(self)
    }

    pub fn read_recur(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert_lock(path, LockType::ReadRecursive)?;
        Ok(self)
    }

    pub async fn begin(self) -> Txn<'a> {
        let defer_gaurd = self.db.defer.begin();
        let mut guards = Vec::new();
        for (path, lock_type) in self.ops.bfs_iter() {
            guards.push(self.db.read_locks.get(path).acquire(*lock_type).await);
        }
        guards.reverse();

        Txn {
            db: self.db,
            defer_gaurd,
            guards,
            free: Vec::new(),
            ops: self.ops,
            flux: Flux::new(),
            writes: KeyTrie::new(),
        }
    }
}

pub struct Txn<'a> {
    pub(crate) db: &'a DbInner,
    pub(crate) defer_gaurd: DeferGaurd<'a>,
    pub(crate) guards: Vec<LockGuard>,
    pub(crate) free: Vec<u64>,
    pub(crate) ops: KeyTrie<LockType>,
    pub(crate) flux: Flux,
    pub(crate) writes: KeyTrie<Option<u64>>,
}

impl<'a> Txn<'a> {
    pub async fn read(&self, _path: &KeyPath) -> Result<&[u8]> {
        self.ops
            .validate_read(_path)
            .context("read validation failed")?;

        todo!()
    }

    pub async fn write(&mut self, _path: &KeyPath, _value: &[u8]) -> Result<()> {
        self.ops
            .validate_write(_path)
            .context("write validation failed")?;

        todo!()
    }

    pub async fn scan(
        &self,
        _root: &KeyPath,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
    ) -> impl Iterator<Item = (KeyPathBuf, Vec<u8>)> {
        todo!();
        #[allow(unreachable_code)]
        std::iter::empty()
    }

    // TODO: significant work idea, batch all write futures from btree operations and allocations
    // at once into a batch. That way each commit would be
    // 1. submit batch of all writes (btree + allocations)
    // 2. fsync
    // 3. swap buf root
    // 4. fysnc
    pub async fn commit(&mut self) {
        if let Some(lca) = self.writes.lca(|v| v.is_some()) {
            for (key, _node) in self.ops.dfs_iter_mut() {
                if key.as_path().starts_with(&lca) {
                    todo!("implement")
                }
            }

            todo!("implement")
        }

        self.defer_gaurd.flush();
    }
}

impl<'a> Drop for Txn<'a> {
    fn drop(&mut self) {
        // no-op
        // TODO: idk, does anything rly need to be done here
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, os::unix::fs::FileExt};

    use crate::{
        key_path,
        meta::{MAGIC, MagicType},
        test_util::TempDir,
        util::update_checksum,
    };

    use function_name::named;

    use super::*;

    #[named]
    #[tokio::test]
    async fn magic_check() -> Result<()> {
        fn corrput_magic(page_size: usize, file: &File, loc: usize) -> Result<()> {
            let mut buf = vec![0u8; page_size];
            file.read_exact_at(&mut buf, (loc * page_size) as u64)?;
            let bad_magic = MagicType::to_le_bytes(MAGIC + 1);
            buf[0..size_of::<MagicType>()].copy_from_slice(&bad_magic);
            update_checksum(&mut buf);
            file.write_all_at(&buf, (loc * page_size) as u64)?;
            Ok(())
        }

        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;
        let page_size = db.page_size();
        db.try_close()?;

        corrput_magic(page_size, &tmp.file_raw(LOC)?, 0)?;
        corrput_magic(page_size, &tmp.file_raw(LOC)?, 1)?;

        let err = tmp.db(LOC).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("file is not an archia db file or magic number is corrupted")
        );

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn db_open_meta_flag() -> Result<()> {
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db("db").await?;
        {
            let _t1 = db.txn().read(key_path![b"key1"])?.begin().await;
        }
        db.try_close()?;

        {
            let meta = tmp.meta("db")?;
            assert_eq!(false, meta.open_async().await);
        }

        let _db = tmp.db("db").await?;
        {
            let meta = tmp.meta("db")?;
            assert_eq!(true, meta.open_async().await);
        }

        Ok(())
    }
}
