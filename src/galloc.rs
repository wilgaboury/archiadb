use anyhow::Result;
use tokio::sync::Mutex;

use crate::{
    block::BlockIo,
    bnode::BNodeRootHeader,
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

pub(crate) struct GallocCtx<'a, B: BlockIo> {
    pub(crate) galloc: &'a Galloc,
    pub(crate) meta: &'a MetaHandler,
    pub(crate) bio: &'a B,
    pub(crate) fb: &'a mut FrontBack,
}

pub(crate) async fn galloc<'a, B: BlockIo>(ctx: GallocCtx<'a, B>, len: PgIdx) -> Result<Arena> {
    let _gaurd = ctx.galloc.lock.lock().await;
    galloc_no_lock(ctx.meta, ctx.bio, ctx.fb, len).await
}

pub(crate) async fn galloc_no_lock<B: BlockIo>(
    meta: &MetaHandler,
    bio: &B,
    fb: &mut FrontBack,
    len: PgIdx,
) -> Result<Arena> {
    let mut root = bio.read(fb.front()).await?;
    let btree = from_bytes_mut::<BNodeRootHeader>(root.as_mut());
    let start = meta.len();
    let flen = meta.len() + len;

    bio.alloc(flen).await?;

    meta.mutate_async(bio, |meta| {
        meta.galloc_fidx.set(fb.front());
        meta.galloc_bidx.set(fb.back());
        meta.galloc_len.set(len);
    })
    .await?;

    btree.version.set(btree.version.get() + 1);
    btree.arena.start.set(start);
    btree.arena.len.set(len);
    btree.arena.next.set(0);
    bio.write(fb.back(), root).await?;
    bio.sync().await?;
    fb.flip();

    meta.mutate_async(bio, |meta| {
        meta.len.set(flen);
        meta.galloc_fidx.set(0);
        meta.galloc_bidx.set(0);
        meta.galloc_len.set(0);
    })
    .await?;

    Ok(Arena::new(start, len))
}

pub(crate) async fn galloc_recover<B: BlockIo>(meta: &MetaHandler, bio: &B) -> Result<()> {
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
        galloc_no_lock(meta, bio, &mut FrontBack::new(front_idx, back_idx), alen).await?;
    }
    Ok(())
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use function_name::named;

    use crate::{db::DbInner, meta::Meta, test::TmpDir, util::from_bytes};

    use super::*;

    impl<B: BlockIo> DbInner<B> {
        pub(crate) fn to_galloc_ctx<'a>(&'a self, fb: &'a mut FrontBack) -> GallocCtx<'a, B> {
            GallocCtx {
                galloc: &self.galloc,
                meta: &self.meta,
                bio: &self.bio,
                fb,
            }
        }
    }

    #[named]
    #[tokio::test]
    async fn basic_galloc() -> Result<()> {
        let tmp = TmpDir::new(function_name!()).unwrap();
        let db = tmp.db().await?;

        let mut fb = FrontBack::from_roots(&db.inner.bio, 2, 3).await?.0;
        let ctx = db.inner.to_galloc_ctx(&mut fb);

        assert_eq!(4, db.inner.meta.len());

        galloc(ctx, 4).await?;
        assert_eq!(8, db.inner.meta.len());

        let buf = db.inner.bio.read(3).await?;
        let root = from_bytes::<BNodeRootHeader>(&buf.as_ref());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(4, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn galloc_recover_finish_galloc() -> Result<()> {
        let tmp = TmpDir::new(function_name!()).unwrap();
        let db = tmp.db().await?;
        db.try_close()?;

        {
            let fio = tmp.fio()?;
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

            fio.sync().await?;
        }

        let db = tmp.db().await?;
        let buf = db.inner.bio.read(3).await?;
        let root = from_bytes::<BNodeRootHeader>(buf.as_ref());
        assert_eq!(4, root.arena.start.get());
        assert_eq!(4, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }
}
