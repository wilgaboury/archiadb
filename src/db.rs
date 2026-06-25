use std::{
    path::Path,
    sync::{Arc, atomic::AtomicU64},
};

use anyhow::Result;
use bon::bon;
use parking_lot::Mutex;

use crate::{
    alloc::PageAllocator,
    concache::ConCache,
    file::DbFile,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    key::{KeyPath, KeyPathBuf},
    lock::{Lock, LockGuard, LockType},
    meta::MetaHandler,
    trie::TxnKeyTrie,
    txnmap::TxnFreeDeferMap,
};

#[derive(Clone)]
struct Db {
    inner: Arc<DbInner>,
}

struct DbInner {
    file: Arc<DbFile>,
    meta: MetaHandler,
    fio: Fio,
    alloc: PageAllocator,
    txn_free_defer_map: TxnFreeDeferMap,
    read_locks: ConCache<KeyPathBuf, Lock>,
    write_locks: ConCache<KeyPathBuf, Mutex<()>>,
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
            &meta,
            sq,
            cq,
            page_buf_pool,
            generic_op_state_pool,
        )?;
        let alloc = PageAllocator::new(fio.clone(), &meta).await?;
        Ok(Self {
            inner: Arc::new(DbInner {
                file,
                meta,
                fio,
                alloc,
                txn_free_defer_map: TxnFreeDeferMap::new(),
                read_locks: ConCache::new(Box::new(|| Lock::new())),
                write_locks: ConCache::new(Box::new(|| Mutex::new(()))),
            }),
        })
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

struct TxnBuilder {
    db: Db,
    ops: TxnKeyTrie,
}

impl TxnBuilder {
    pub fn read(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert(path, LockType::Read)?;
        Ok(self)
    }

    pub fn write(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert(path, LockType::Write)?;
        Ok(self)
    }

    pub fn read_recur(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert(path, LockType::ReadRecursive)?;
        Ok(self)
    }

    pub async fn begin(self) -> Txn {
        let mut guards = Vec::new();
        for (lock_path, lock_type) in self.ops.level_order_iter() {
            guards.push(
                self.db
                    .inner
                    .read_locks
                    .get(lock_path)
                    .acquire(lock_type)
                    .await,
            );
        }
        guards.reverse();
        Txn {
            db: self.db,
            guards,
        }
    }
}

pub struct Txn {
    db: Db,
    guards: Vec<LockGuard>,
}

impl Txn {
    pub async fn read(&self, path: &KeyPath) -> &[u8] {
        todo!()
    }

    pub async fn scan(
        &self,
        root: &KeyPath,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = (KeyPathBuf, Vec<u8>)> {
        todo!();
        #[allow(unreachable_code)]
        std::iter::empty()
    }

    pub async fn write(&mut self, path: &KeyPath, value: &[u8]) {
        todo!()
    }

    pub async fn commit(&mut self) {
        // collect write paths and then acquire in reverse/bottom-up order
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{key_path, test_util::TempDir};

    use function_name::named;

    use super::*;

    #[named]
    #[tokio::test]
    async fn test_init() -> Result<()> {
        let dir = TempDir::new(function_name!()).unwrap();
        let db = Db::builder().path(dir.path().join("file")).build().await?;
        {
            let _t1 = db.txn().read(key_path![b"key1"])?.begin().await;
        }

        Ok(())
    }
}
