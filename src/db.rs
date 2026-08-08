use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use bon::bon;
use tokio::sync::Mutex;

use crate::{
    concache::ConCache,
    file::DbFile,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    flux::Flux,
    key::{KeyPath, KeyPathBuf},
    lock::{Lock, LockGuard, LockType},
    meta::MetaHandler,
    trie::TxnKeyTrie,
    txnmap::TxnFreeDeferMap,
};

#[derive(Clone)]
pub struct Db {
    pub(crate) inner: Arc<DbInner>,
}

pub(crate) struct DbInner {
    pub(crate) file: Arc<DbFile>,
    pub(crate) meta: MetaHandler,
    pub(crate) fio: Fio,
    pub(crate) txn_free_defer_map: TxnFreeDeferMap,
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
        file.file().try_lock()?; // prevent multiple processes from operating on the same file

        let meta = MetaHandler::new(&file.file())?;
        let fio = Fio::new(
            file.clone(),
            meta.page_size(),
            sq,
            cq,
            page_buf_pool,
            generic_op_state_pool,
        )?;

        meta.mutate_async(&fio, |meta| {
            meta.set_open(true);
        })
        .await?;

        if meta.open_async().await {
            // TODO: run recovery
        }

        Ok(Self {
            inner: Arc::new(DbInner {
                file,
                meta,
                fio,
                txn_free_defer_map: TxnFreeDeferMap::new(),
                read_locks: ConCache::new(Box::new(|| Lock::new())),
                write_locks: ConCache::new(Box::new(|| Mutex::new(()))),
            }),
        })
    }

    pub fn page_size(&self) -> usize {
        self.inner.meta.page_size() as usize
    }

    pub fn txn(&self) -> TxnBuilder {
        TxnBuilder {
            db: self.clone(),
            ops: TxnKeyTrie::new(),
        }
    }

    pub fn close(self) {
        drop(self)
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

pub struct TxnBuilder {
    db: Db,
    ops: TxnKeyTrie<LockType>,
}

impl TxnBuilder {
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

    pub async fn begin(self) -> Txn {
        let mut guards = Vec::new();
        for (path, lock_type) in self.ops.bfs_iter() {
            guards.push(self.db.inner.read_locks.get(path).acquire(*lock_type).await);
        }
        guards.reverse();

        // There can be no failable code between this line and struct initialization
        let txn_free_defer_id = self.db.inner.txn_free_defer_map.begin();
        Txn {
            txn_free_defer_id,
            db: self.db,
            guards,
            free: Vec::new(),
            ops: self.ops,
            flux: Flux::new(),
            writes: TxnKeyTrie::new(),
        }
    }
}

pub struct Txn {
    pub(crate) txn_free_defer_id: u64,
    pub(crate) db: Db,
    pub(crate) guards: Vec<LockGuard>,
    pub(crate) free: Vec<u64>,
    pub(crate) ops: TxnKeyTrie<LockType>,
    pub(crate) flux: Flux,
    pub(crate) writes: TxnKeyTrie<Option<u64>>,
}

impl Txn {
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
    }
}

impl Drop for Txn {
    fn drop(&mut self) {
        self.db
            .inner
            .txn_free_defer_map
            .finish(self.txn_free_defer_id, &mut self.free);
    }
}

#[cfg(test)]
mod tests {
    use crate::{key_path, test_util::TempDir};

    use function_name::named;

    use super::*;

    #[named]
    #[tokio::test]
    async fn test_db_open_meta_flag() -> Result<()> {
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db("db").await?;
        {
            let _t1 = db.txn().read(key_path![b"key1"])?.begin().await;
        }
        db.close();

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
