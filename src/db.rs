use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use bon::bon;
use tokio::sync::Mutex;

use crate::{
    btree::BTreeRootHeader,
    defer::{Defer, DeferGaurd},
    file::DbFile,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    flux::Flux,
    galloc::{Galloc, galloc_recover},
    karc::Karc,
    key::{KeyPath, KeyPathBuf},
    key_path,
    lalloc::{Arena, Lalloc},
    lock::{Lock, LockGuard, LockType},
    meta::MetaHandler,
    trie::KeyTrie,
    uint::PgIdx,
    util::{
        FrontBack, LockVec, collect_intermediary_decendants, from_bytes, from_bytes_mut, lca,
        read_root_w_retry,
    },
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
    pub(crate) read_locks: Karc<KeyPathBuf, Lock>,
    pub(crate) write_locks: Karc<KeyPathBuf, Mutex<()>>,
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

        galloc_recover(&meta, &fio).await?;

        if meta.len() == 2 {
            init_db_root(&meta, &fio).await?;
        }

        Ok(Self {
            inner: Arc::new(DbInner {
                file,
                meta,
                fio,
                galloc: Galloc::new(),
                defer: Defer::new(),
                read_locks: Karc::new(Box::new(|| Lock::new())),
                write_locks: Karc::new(Box::new(|| Mutex::new(()))),
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

pub(crate) async fn init_db_root(meta: &MetaHandler, fio: &Fio) -> Result<()> {
    let front_idx = 2;
    let back_idx = 3;
    fio.alloc(4).await?;

    let mut front = fio.get_buf();
    let mut back = fio.get_buf();
    from_bytes_mut::<BTreeRootHeader>(front.as_mut()).init();
    from_bytes_mut::<BTreeRootHeader>(back.as_mut()).init();
    fio.write(front_idx, front).await?;
    fio.write(back_idx, back).await?;

    fio.commit().await?;

    meta.mutate_async(&fio, |meta| {
        meta.set_len(4);
    })
    .await?;

    Ok(())
}

pub struct TxnBuilder<'txn> {
    db: &'txn DbInner,
    ops: KeyTrie<LockType>,
}

impl<'txn> TxnBuilder<'txn> {
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

    pub async fn begin(self) -> Txn<'txn> {
        let defer_gaurd = self.db.defer.begin();
        let mut guards = LockVec(Vec::new());
        for (path, lock_type) in self.ops.bfs_iter() {
            guards
                .0
                .push(self.db.read_locks.get(path).acquire(*lock_type).await);
        }

        Txn {
            db: self.db,
            defer_gaurd,
            guards,
            free: Vec::new(),
            ops: self.ops,
            flux: Flux::new(),
            writes: HashMap::new(),
        }
    }
}

pub(crate) struct DirtyEntry {
    pub(crate) fb: FrontBack,
    pub(crate) lalloc: Lalloc,
}

impl DirtyEntry {
    pub fn new(fb: FrontBack, arena: Arena) -> Self {
        Self {
            fb,
            lalloc: Lalloc::new(arena),
        }
    }
}

pub struct Txn<'txn> {
    pub(crate) db: &'txn DbInner,
    pub(crate) defer_gaurd: DeferGaurd<'txn>,
    pub(crate) guards: LockVec<LockGuard>,
    pub(crate) free: Vec<u64>,
    pub(crate) ops: KeyTrie<LockType>,
    pub(crate) flux: Flux,
    pub(crate) writes: HashMap<KeyPathBuf, DirtyEntry>,
}

impl<'txn> Txn<'txn> {
    pub(crate) async fn create_root_dirty_entry(&self) -> Result<DirtyEntry> {
        self.create_dirty_entry(key_path![], self.db.meta.root1(), self.db.meta.root2())
            .await
    }

    pub(crate) async fn create_dirty_entry(
        &self,
        key: &KeyPath,
        pg_idx_1: PgIdx,
        pg_idx_2: PgIdx,
    ) -> Result<DirtyEntry> {
        let (pg_idx_1, pg1, pg_idx_2, _pg2) =
            read_root_w_retry(self.db, key, pg_idx_1, pg_idx_2, Duration::from_secs(1)).await?;
        let root = from_bytes::<BTreeRootHeader>(pg1.as_ref());
        Ok(DirtyEntry::new(
            FrontBack::new(pg_idx_1, pg_idx_2),
            root.arena.to_mem(),
        ))
    }

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
    // 1. submit batch of all writes
    // 2. fsync
    // 3. swap buf root
    // 4. fysnc
    pub async fn commit(&mut self) -> Result<()> {
        let lca = lca(self.writes.keys().map(|k| k.as_ref()));

        // TODO: update intermediary roots in btrees (make sure they are in flux)

        self.flux_drain().await?;
        self.db.fio.commit().await?;

        let mut wlock_keys =
            collect_intermediary_decendants(lca.as_ref(), self.writes.keys().map(|k| k.as_ref()));
        wlock_keys.reverse(); // aquired in bottom-up order

        let wlocks: Vec<_> = wlock_keys
            .into_iter()
            .map(|k| self.db.write_locks.get(k))
            .collect();
        let mut wgaurds = LockVec(Vec::with_capacity(wlocks.len()));
        for wlock in wlocks.iter() {
            wgaurds.0.push(wlock.lock().await);
        }

        // TODO: implement btree root writes

        // free page in-mem bookkeeping
        self.defer_gaurd.flush();

        Ok(())
    }

    async fn upsert_dirty_entries_to_lca(&mut self, _lca: &KeyPath) -> Result<()> {
        Ok(())
    }

    async fn flux_drain(&mut self) -> Result<()> {
        let mut flux_writes: Vec<_> = Vec::with_capacity(self.flux.map.len());
        for (idx, buf) in self.flux.map.drain() {
            let buf = buf.unwrap(); // critical logic failure if any of these are none
            flux_writes.push(self.db.fio.write(idx, buf));
        }
        for flux_write in flux_writes {
            flux_write.await?;
        }
        Ok(())
    }
}

impl<'txn> Drop for Txn<'txn> {
    fn drop(&mut self) {
        // no-op
        // TODO: idk, does anything rly need to be done here
    }
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use std::{fs::File, os::unix::fs::FileExt};

    use crate::{
        meta::{MAGIC, MagicType},
        test::TmpDir,
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

        let tmp = TmpDir::new(function_name!()).unwrap();
        let db = tmp.db().await?;
        let page_size = db.page_size();
        db.try_close()?;

        corrput_magic(page_size, &tmp.file_raw()?, 0)?;
        corrput_magic(page_size, &tmp.file_raw()?, 1)?;

        let err = tmp.db().await.unwrap_err();
        assert!(
            err.to_string()
                .contains("file is not an archia db file or magic number is corrupted")
        );

        Ok(())
    }
}
