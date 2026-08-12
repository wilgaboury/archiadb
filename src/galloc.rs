use anyhow::Result;
use tokio::sync::Mutex;

use crate::{
    btree::BTreeRootHeader,
    db::DbInner,
    fio::{Fio, PageBuf},
    meta::MetaHandler,
    uint::PgIdx,
    util::from_bytes_mut,
};

pub(crate) const MIN_ARENA_LEN: u64 = 1 << 2;
pub(crate) const MAX_ARENA_LEN: u64 = 1 << 18; // 1GB with 4kb page

#[derive(Debug)]
pub(crate) struct Galloc {
    lock: Mutex<()>,
}

impl Galloc {
    pub(crate) fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
    }
}

impl DbInner {
    pub(crate) async fn galloc(&self, root: PageBuf, front_idx: u64, back_idx: u64) -> Result<()> {
        let _gaurd = self.galloc.lock.lock().await;
        galloc_helper(&self.meta, &self.fio, front_idx, back_idx).await?;
        Ok(())
    }
}

pub(crate) async fn galloc_helper(
    meta: &MetaHandler,
    fio: &Fio,
    front_idx: u64,
    back_idx: u64,
) -> Result<()> {
    let mut root = fio.read(front_idx).await?;
    let btree = from_bytes_mut::<BTreeRootHeader>(root.get_mut());
    let prev_len = meta.len();
    let alen = PgIdx::max(
        MIN_ARENA_LEN,
        PgIdx::min(MAX_ARENA_LEN, 2 * btree.arena.len.get()),
    );
    let len = meta.len() + alen;

    fio.alloc(len).await?;

    meta.mutate_async(&fio, |meta| {
        meta.set_len(len);
        meta.set_galloc_fidx(front_idx);
        meta.set_galloc_bidx(back_idx);
    })
    .await?;

    btree.version.set(btree.version.get() + 1);
    btree.arena.start.set(prev_len);
    btree.arena.len.set(alen);
    btree.arena.next.set(0);
    fio.write(back_idx, root).await?;
    fio.commit().await?;

    meta.mutate_async(&fio, |meta| {
        meta.set_galloc_fidx(0);
        meta.set_galloc_bidx(0);
    })
    .await?;

    Ok(())
}

pub(crate) async fn galloc_recover(meta: &MetaHandler, fio: &Fio) -> Result<()> {
    let (front_idx, back_idx) = meta
        .access_async(|meta| (meta.galloc_fidx(), meta.galloc_bidx()))
        .await;
    if front_idx != 0 && back_idx != 0 {
        galloc_helper(meta, fio, front_idx, back_idx).await?;
    }
    Ok(())
}

pub(crate) async fn init_root(meta: &MetaHandler, fio: &Fio) -> Result<()> {
    let len = meta.len();
    let front_idx = len;
    let back_idx = len + 1;
    fio.alloc(len + 2).await?;

    let mut front = fio.get_buf();
    let mut back = fio.get_buf();
    from_bytes_mut::<BTreeRootHeader>(front.get_mut()).init();
    from_bytes_mut::<BTreeRootHeader>(back.get_mut()).init();
    fio.write(front_idx, front).await?;
    fio.write(back_idx, back).await?;

    fio.commit().await?;

    meta.mutate_async(&fio, |meta| {
        meta.set_len(len + 2);
    })
    .await?;

    galloc_helper(meta, fio, front_idx, back_idx).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use function_name::named;

    use crate::{
        meta::{Meta, NUM_HEADER_PAGES},
        test_util::TempDir,
        util::from_bytes,
    };

    use super::*;

    #[named]
    #[tokio::test]
    async fn basic_galloc() -> Result<()> {
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;

        assert_eq!(NUM_HEADER_PAGES + 2 + MIN_ARENA_LEN, db.inner.meta.len());

        let buf = db.inner.fio.read(3).await?;
        let root = from_bytes::<BTreeRootHeader>(&buf.get());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(MIN_ARENA_LEN, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn galloc_recover() -> Result<()> {
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;
        db.try_close()?;

        {
            let (fio, _meta) = tmp.fio(LOC)?;
            let mut buf = fio.read(0).await?;
            let meta = from_bytes_mut::<Meta>(buf.get_mut());
            meta.open = 0;
            assert_eq!(4, meta.version.get());
            meta.version.set(meta.version.get() + 1);
            meta.galloc_fidx.set(2);
            meta.galloc_bidx.set(3);
            fio.write(0, buf).await?;

            fio.commit().await?;
        }

        let db = tmp.db(LOC).await?;
        let buf = db.inner.fio.read(3).await?;
        let root = from_bytes::<BTreeRootHeader>(&buf.get());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(MIN_ARENA_LEN, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }
}
