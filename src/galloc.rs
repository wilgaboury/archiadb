use anyhow::Result;
use tokio::sync::Mutex;

use crate::{
    btree::BTreeRootHeader,
    db::DbInner,
    fio::Fio,
    lalloc::Arena,
    meta::MetaHandler,
    uint::PgIdx,
    util::{FrontBack, from_bytes_mut},
};

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
    pub(crate) async fn galloc(&self, fb: &mut FrontBack, len: PgIdx) -> Result<Arena> {
        galloc_w_lock(&self.galloc, &self.meta, &self.fio, fb, len).await
    }
}

pub(crate) async fn galloc_w_lock(
    lock: &Galloc,
    meta: &MetaHandler,
    fio: &Fio,
    fb: &mut FrontBack,
    len: PgIdx,
) -> Result<Arena> {
    let _gaurd = lock.lock.lock().await;
    galloc_io_proc(meta, fio, fb, len).await
}

pub(crate) async fn galloc_io_proc(
    meta: &MetaHandler,
    fio: &Fio,
    fb: &mut FrontBack,
    len: PgIdx,
) -> Result<Arena> {
    let mut root = fio.read(fb.front()).await?;
    let btree = from_bytes_mut::<BTreeRootHeader>(root.as_mut());
    let start = meta.len();
    let flen = meta.len() + len;

    fio.alloc(flen).await?;

    meta.mutate_async(&fio, |meta| {
        meta.galloc_fidx.set(fb.front());
        meta.galloc_bidx.set(fb.back());
        meta.galloc_len.set(len);
    })
    .await?;

    btree.version.set(btree.version.get() + 1);
    btree.arena.start.set(start);
    btree.arena.len.set(len);
    btree.arena.next.set(0);
    fio.write(fb.back(), root).await?;
    fio.commit().await?;

    meta.mutate_async(&fio, |meta| {
        meta.len.set(flen);
        meta.galloc_fidx.set(0);
        meta.galloc_bidx.set(0);
        meta.galloc_len.set(0);
    })
    .await?;

    fb.flip();

    Ok(Arena::new(start, len))
}

pub(crate) async fn galloc_recover(meta: &MetaHandler, fio: &Fio) -> Result<()> {
    let (front_idx, back_idx, alen) = meta
        .access_async(|meta| {
            (
                meta.galloc_fidx.get(),
                meta.galloc_bidx.get(),
                meta.galloc_len.get(),
            )
        })
        .await;
    if front_idx != 0 && back_idx != 0 {
        galloc_io_proc(meta, fio, &mut FrontBack::new(front_idx, back_idx), alen).await?;
    }
    Ok(())
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use function_name::named;

    use crate::{meta::Meta, test::TempDir, util::from_bytes};

    use super::*;

    #[named]
    #[tokio::test]
    async fn basic_galloc() -> Result<()> {
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;

        assert_eq!(4, db.inner.meta.len());

        db.inner
            .galloc(&mut FrontBack::from_roots(&db.inner.fio, 2, 3).await?.0, 4)
            .await?;
        assert_eq!(8, db.inner.meta.len());

        let buf = db.inner.fio.read(3).await?;
        let root = from_bytes::<BTreeRootHeader>(&buf.as_ref());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(4, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn galloc_recover_finish_galloc() -> Result<()> {
        const LOC: &str = "db";
        let tmp = TempDir::new(function_name!()).unwrap();
        let db = tmp.db(LOC).await?;
        db.try_close()?;

        {
            let (fio, _meta) = tmp.fio(LOC)?;
            let mut buf = fio.read(0).await?;
            let meta = from_bytes_mut::<Meta>(buf.as_mut());
            meta.open = 0;
            assert_eq!(0, meta.version.get());
            meta.version.set(meta.version.get() + 1);
            meta.galloc_fidx.set(2);
            meta.galloc_bidx.set(3);
            meta.galloc_len.set(4);
            meta.len.set(4);
            fio.write(0, buf).await?;

            let mut buf = fio.read(3).await?;
            let root = from_bytes_mut::<BTreeRootHeader>(buf.as_mut());
            root.arena.start.set(0);

            fio.commit().await?;
        }

        let db = tmp.db(LOC).await?;
        let buf = db.inner.fio.read(3).await?;
        let root = from_bytes::<BTreeRootHeader>(buf.as_ref());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(4, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }
}
