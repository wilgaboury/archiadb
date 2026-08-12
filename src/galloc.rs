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

pub(crate) const MIN_ARENA_LEN: u64 = 2 ^ 2;
pub(crate) const MAX_ARENA_LEN: u64 = 2 ^ 18; // 1GB with 4kb page

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
        galloc_helper(&self.meta, &self.fio, root, front_idx, back_idx).await?;
        Ok(())
    }
}

pub(crate) async fn galloc_helper(
    meta: &MetaHandler,
    fio: &Fio,
    mut root: PageBuf,
    front_idx: u64,
    back_idx: u64,
) -> Result<()> {
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
        let root = fio.read(front_idx).await?;
        galloc_helper(meta, fio, root, front_idx, back_idx).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use function_name::named;

    use crate::test_util::TempDir;

    use super::*;

    #[named]
    #[tokio::test]
    async fn basic_galloc() -> Result<()> {
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;

        assert_eq!(2, db.inner.meta.len());

        db.try_close()?;

        Ok(())
    }
}
