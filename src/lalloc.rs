use std::{cmp, collections::HashMap};

use anyhow::Result;

use crate::{
    block::BlockIo,
    bnode::BNodeRootHeader,
    db::{BRootInfo, Txn},
    defer::DeferGaurd,
    galloc::{Galloc, GallocCtx, galloc},
    key::KeyPathBuf,
    meta::MetaHandler,
    uint::{InPgIdx, PgIdx, PgIdxDisk},
    util::{from_bytes, from_bytes_mut},
};

const INIT_ARENA_SIZE: PgIdx = 8;
const MAX_ARENA_EXP: u64 = 18;

// Page format for free list node
// header, idxs, 0
// null terminated, or terminated by max idxs in page
// this keeps iteration simple and is space efficent

#[repr(C, packed)]
pub(crate) struct FreeListHeader {
    pub(crate) next: PgIdxDisk,
}

pub(crate) fn read_fl_idx(buf: &[u8], idx: InPgIdx) -> PgIdx {
    from_bytes::<PgIdxDisk>(
        &buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
    )
    .get()
}

pub(crate) fn write_fl_idx(buf: &mut [u8], idx: InPgIdx, pg_idx: PgIdx) {
    from_bytes_mut::<PgIdxDisk>(
        &mut buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
    )
    .set(pg_idx)
}

pub(crate) fn term_fl(buf: &mut [u8], idx: InPgIdx) {
    let idxs_per = idxs_per_fl_page(buf.len() as u64);
    if idx < idxs_per {
        from_bytes_mut::<PgIdxDisk>(
            &mut buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
        )
        .set(0)
    }
}

pub(crate) fn idxs_per_fl_page(pg_sz: InPgIdx) -> InPgIdx {
    (pg_sz - size_of::<FreeListHeader>() as InPgIdx) / size_of::<PgIdxDisk>() as InPgIdx
}

#[derive(Clone, Copy)]
pub(crate) struct Arena {
    pub(crate) start: PgIdx,
    pub(crate) len: PgIdx,
    pub(crate) next: PgIdx,
}

impl Arena {
    pub(crate) fn new(start: PgIdx, len: PgIdx) -> Self {
        Self {
            start,
            len,
            next: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.next >= self.len
    }

    pub(crate) fn pop(&mut self) -> Option<PgIdx> {
        if self.len - self.next > 0 {
            let ret = self.start + self.next;
            self.next += 1;
            Some(ret)
        } else {
            None
        }
    }

    pub(crate) fn pop_w_reserve(&mut self, reserve: PgIdx) -> Option<PgIdx> {
        if self.len - self.next > reserve {
            let ret = self.start + self.next;
            self.next += 1;
            Some(ret)
        } else {
            None
        }
    }
}

pub(crate) struct LallocMap {
    map: HashMap<KeyPathBuf, Lalloc>,
}

// TODO: add allocated field (tracks all allocations in a given txn), replaces flux free conditional
pub(crate) struct Lalloc {
    arena: Arena,
    orig_next: PgIdx,

    clean: Option<PgIdx>, // pointer to unmodified tail of free list
    free: Vec<PgIdx>,     // usable pointers
    deferred: Vec<PgIdx>, // freed pgs and ones that fail the deffered set check
}

impl Lalloc {
    pub(crate) fn new(arena: Arena) -> Self {
        let orig_next = arena.next;
        Self {
            arena,
            orig_next,

            clean: None,
            free: Vec::new(),
            deferred: Vec::new(),
        }
    }

    pub(crate) fn free_in_mem_page(&mut self, pg_idx: PgIdx) {
        self.free.push(pg_idx);
    }

    pub(crate) fn pages_to_fl_encode_arena(&self, pg_size: PgIdx) -> PgIdx {
        let idxs_per = idxs_per_fl_page(pg_size);
        let idxs = self.arena.len - self.orig_next;
        // +1 is accounting for page needed to store free list node
        let div = idxs / (idxs_per + 1);
        let rem = idxs % (idxs_per + 1);
        div + (if rem > 0 { 1 } else { 0 })
    }

    pub(crate) fn pop_arena_with_encode_reserve(&mut self, pg_sz: PgIdx) -> Option<PgIdx> {
        let reserve = self.pages_to_fl_encode_arena(pg_sz);
        self.arena.pop_w_reserve(reserve)
    }

    pub(crate) fn clean_or(&self, root: &BNodeRootHeader) -> PgIdx {
        if let Some(clean) = self.clean.as_ref() {
            *clean
        } else {
            root.free.get()
        }
    }

    pub(crate) fn pop_deferred_then_free(&mut self) -> Option<PgIdx> {
        self.deferred.pop().or_else(|| self.free.pop())
    }
}

pub(crate) async fn write_fl<'a, 'd, B: BlockIo>(
    ctx: &mut LallocCtx<'a, 'd, B>,
    header: &BNodeRootHeader,
) -> Result<PgIdx> {
    // TODO: this function could be more complicated and do proper ordering of deferred/free pg idxs for better effiecency

    let mut next = ctx.root.lalloc.clean.unwrap_or(0);
    loop {
        let maybe_load = ctx.root.lalloc.pop_deferred_then_free();
        let mut free_idx = if let Some(v) = maybe_load { v } else { break };

        let n_pg_idx = lalloc_from_dirty_or_arena(ctx, header).await?;
        let mut buf = ctx.bio.buf();
        from_bytes_mut::<FreeListHeader>(buf.as_mut())
            .next
            .set(next);

        let mut i = 0;
        let idxs_per = idxs_per_fl_page(ctx.bio.page_size());
        while i < idxs_per {
            write_fl_idx(buf.as_mut(), i, free_idx);

            i += 1;
            let maybe_load = ctx.root.lalloc.pop_deferred_then_free();
            free_idx = if let Some(v) = maybe_load { v } else { break };
        }
        if i < idxs_per {
            term_fl(buf.as_mut(), i);
        }

        ctx.bio.write(n_pg_idx, buf).await?;
        next = n_pg_idx;
    }

    Ok(next)
}

impl<'a, B: BlockIo> Txn<'a, B> {
    pub(crate) async fn lalloc<'b>(&'b mut self, root: &'b mut BRootInfo) -> Result<PgIdx> {
        lalloc(&mut self.to_lalloc_ctx(root)).await
    }
}

pub(crate) fn free_on_disk_pg(lalloc: &mut Lalloc, defer_gaurd: &mut DeferGaurd, pg_idx: PgIdx) {
    lalloc.deferred.push(pg_idx);
    defer_gaurd.free(pg_idx);
}

pub(crate) struct LallocCtx<'a, 'd, B: BlockIo> {
    pub(crate) galloc: &'a Galloc,
    pub(crate) meta: &'a MetaHandler,
    pub(crate) bio: &'a B,
    pub(crate) defer_gaurd: &'a mut DeferGaurd<'d>,
    pub(crate) root: &'a mut BRootInfo,
}

impl<'a, 'd, B: BlockIo> LallocCtx<'a, 'd, B> {
    pub(crate) fn to_galloc_ctx(&mut self) -> GallocCtx<'_, B> {
        GallocCtx {
            galloc: self.galloc,
            meta: self.meta,
            bio: self.bio,
            fb: &mut self.root.fb,
        }
    }
}

pub(crate) async fn lalloc<'a, 'd, B: BlockIo>(ctx: &mut LallocCtx<'a, 'd, B>) -> Result<PgIdx> {
    let buf = ctx.bio.read(ctx.root.fb.front()).await?;
    let header = from_bytes::<BNodeRootHeader>(buf.as_ref());

    if should_init_arena(header) {
        // I'm not even sure what I meant by comment below
        // unlikely case, but post arena encode crash + fl depletion could leave inlavid arena with large len. use that len for galloc not init.
        let len = cmp::max(INIT_ARENA_SIZE, next_arena_len(header.arena.len.get()));
        let arena = {
            let gctx = ctx.to_galloc_ctx();
            galloc(gctx, len).await?
        };
        ctx.root.lalloc.arena = arena;
        ctx.root.lalloc.orig_next = ctx.root.lalloc.arena.next;
        return Ok(ctx.root.lalloc.arena.pop().unwrap());
    }

    if let Some(free) =
        find_free_idx_and_consume_fl(ctx.defer_gaurd, ctx.bio, header, &mut ctx.root.lalloc).await?
    {
        return Ok(free);
    }

    lalloc_from_arena_or_galloc(ctx, header).await
}

pub(crate) fn should_init_arena(root: &BNodeRootHeader) -> bool {
    root.free.get() == 0 && !root.arena.is_valid()
}

pub(crate) async fn find_free_idx_and_consume_fl<B: BlockIo>(
    defer_gaurd: &mut DeferGaurd<'_>,
    fio: &B,
    header: &BNodeRootHeader,
    lalloc: &mut Lalloc,
) -> Result<Option<PgIdx>> {
    let mut clean = lalloc.clean_or(header);
    loop {
        if let Some(free) = lalloc.free.pop() {
            return Ok(Some(free));
        }

        if clean == 0 {
            break;
        }

        clean = load_fl_node(defer_gaurd, fio, lalloc, clean).await?;
        lalloc.clean = Some(clean);
    }

    Ok(None)
}

// TODO: need test with long fl to test this
pub(crate) async fn load_fl_node<B: BlockIo>(
    defer_gaurd: &mut DeferGaurd<'_>,
    bio: &B,
    lalloc: &mut Lalloc,
    fl_n_idx: PgIdx,
) -> Result<PgIdx> {
    let buf = bio.read(fl_n_idx).await?;
    free_on_disk_pg(lalloc, defer_gaurd, fl_n_idx);

    let header = from_bytes::<FreeListHeader>(buf.as_ref());
    let idxs_per = idxs_per_fl_page(bio.page_size());
    for i in 0..idxs_per {
        let free_pg_idx = read_fl_idx(buf.as_ref(), i);
        if free_pg_idx == 0 {
            break;
        }

        if defer_gaurd.defer.contains(free_pg_idx) {
            lalloc.deferred.push(free_pg_idx);
        } else {
            lalloc.free.push(free_pg_idx);
        }
    }
    Ok(header.next.get())
}

pub(crate) async fn lalloc_from_dirty_or_arena<'a, 'd, B: BlockIo>(
    ctx: &mut LallocCtx<'a, 'd, B>,
    header: &BNodeRootHeader,
) -> Result<PgIdx> {
    if let Some(free) = ctx.root.lalloc.free.pop() {
        Ok(free)
    } else {
        lalloc_from_arena_or_galloc(ctx, header).await
    }
}

pub(crate) async fn lalloc_from_arena_or_galloc<'a, 'd, B: BlockIo>(
    ctx: &mut LallocCtx<'a, 'd, B>,
    header: &BNodeRootHeader,
) -> Result<PgIdx> {
    if let Some(free) = ctx
        .root
        .lalloc
        .pop_arena_with_encode_reserve(ctx.bio.page_size())
    {
        Ok(free)
    } else {
        encode_arena_and_sync(ctx.bio, ctx.root).await?;
        let next_len = next_arena_len(header.arena.len.get());
        ctx.root.lalloc.arena = galloc(ctx.to_galloc_ctx(), next_len).await?;
        ctx.root.lalloc.orig_next = ctx.root.lalloc.arena.next;
        Ok(ctx.root.lalloc.arena.pop().unwrap())
    }
}

pub(crate) fn next_arena_len(prev: PgIdx) -> PgIdx {
    if prev < INIT_ARENA_SIZE {
        INIT_ARENA_SIZE
    } else {
        1 << cmp::min(MAX_ARENA_EXP, prev.ilog2() as u64 + 1)
    }
}

// for simplity this makes no attempt to compact the previous head node
pub(crate) async fn encode_arena_and_sync<B: BlockIo>(bio: &B, root: &mut BRootInfo) -> Result<()> {
    let to_encode = root.lalloc.pages_to_fl_encode_arena(bio.page_size());
    let left = root.lalloc.arena.len - root.lalloc.arena.next;
    assert!(left == to_encode);

    let idxs_per = idxs_per_fl_page(bio.page_size());
    let mut root_buf = bio.read(root.fb.front()).await?;
    let header = from_bytes_mut::<BNodeRootHeader>(root_buf.as_mut());

    let arena = &root.lalloc.arena;
    let mut encode_idx = root.lalloc.orig_next;
    let mut alloc_idx = arena.next;

    let mut next = header.free.get();
    while encode_idx < arena.next {
        let mut fl_buf = bio.buf();
        from_bytes_mut::<FreeListHeader>(fl_buf.as_mut())
            .next
            .set(next);

        let mut i = 0;
        while i < idxs_per && encode_idx < arena.next {
            write_fl_idx(fl_buf.as_mut(), i, arena.start + encode_idx);

            i += 1;
            encode_idx += 1;
        }
        if i < idxs_per {
            term_fl(fl_buf.as_mut(), i);
        }

        next = arena.start + alloc_idx;
        alloc_idx += 1;
        bio.write(next, fl_buf).await?;
    }

    header.arena.invalidate();
    header.free.set(next);
    let version = header.version.get() + 1;
    header.version.set(version);
    bio.write(root.fb.back(), root_buf).await?;
    bio.sync().await?;
    root.fb.flip();

    Ok(())
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use anyhow::Result;
    use function_name::named;

    use crate::{
        block::BlockIo,
        bnode::BNodeRootHeader,
        db::{BRootInfo, Txn},
        key_path,
        lalloc::{FreeListHeader, INIT_ARENA_SIZE, free_on_disk_pg, read_fl_idx, write_fl},
        test::TmpDir,
        uint::PgIdx,
        util::from_bytes,
    };

    impl<'a, B: BlockIo> Txn<'a, B> {
        pub(crate) async fn write_fl<'b>(&'b mut self, root: &'b mut BRootInfo) -> Result<PgIdx> {
            let root_buf = self.db.bio.read(root.fb.front()).await?;
            let header = from_bytes::<BNodeRootHeader>(root_buf.as_ref());

            write_fl(&mut self.to_lalloc_ctx(root), header).await
        }
    }

    #[named]
    #[tokio::test]
    async fn test_single_encode_arena() -> Result<()> {
        let tmp = TmpDir::new(function_name!())?;
        let db = tmp.db().await?;
        let bio = &db.inner.bio;
        let mut txn = db.txn().write(key_path![])?.begin().await;
        let mut root = txn.create_root_root_info().await?;
        assert_eq!(4, txn.lalloc(&mut root).await?);

        {
            assert_eq!(3, root.fb.front());
            assert_eq!(2, root.fb.back());
        }

        assert_eq!(5, txn.lalloc(&mut root).await?);
        assert_eq!(6, txn.lalloc(&mut root).await?);
        assert_eq!(7, txn.lalloc(&mut root).await?);
        assert_eq!(8, txn.lalloc(&mut root).await?);
        assert_eq!(9, txn.lalloc(&mut root).await?);
        assert_eq!(10, txn.lalloc(&mut root).await?);

        let root_buf = bio.read(root.fb.front()).await?;
        let header = from_bytes::<BNodeRootHeader>(root_buf.as_ref());
        let start_version = header.version.get();
        assert_eq!(12, txn.lalloc(&mut root).await?);

        assert_eq!(3, root.fb.front());
        assert_eq!(2, root.fb.back());
        let root_buf = bio.read(root.fb.front()).await?;
        let root = from_bytes::<BNodeRootHeader>(root_buf.as_ref());
        assert_eq!(11, root.free.get());
        assert_eq!(start_version + 2, root.version.get());

        let fl_buf = bio.read(root.free.get()).await?;
        assert_eq!(0, from_bytes::<FreeListHeader>(fl_buf.as_ref()).next.get());
        assert_eq!(4, read_fl_idx(fl_buf.as_ref(), 0));
        assert_eq!(5, read_fl_idx(fl_buf.as_ref(), 1));
        assert_eq!(6, read_fl_idx(fl_buf.as_ref(), 2));
        assert_eq!(7, read_fl_idx(fl_buf.as_ref(), 3));
        assert_eq!(8, read_fl_idx(fl_buf.as_ref(), 4));
        assert_eq!(9, read_fl_idx(fl_buf.as_ref(), 5));
        assert_eq!(10, read_fl_idx(fl_buf.as_ref(), 6));
        assert_eq!(0, read_fl_idx(fl_buf.as_ref(), 7)); // relies on fio get_buf test version to actually be useful

        assert_eq!(12, root.arena.start.get());
        assert_eq!(INIT_ARENA_SIZE * 2, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_node_write_fl() -> Result<()> {
        let tmp = TmpDir::new(function_name!())?;
        let db = tmp.db().await?;
        let bio = &db.inner.bio;
        let mut txn = db.txn().write(key_path![])?.begin().await;
        let mut root = txn.create_root_root_info().await?;

        let free = txn.write_fl(&mut root).await?;
        assert_eq!(0, free);

        assert_eq!(4, txn.lalloc(&mut root).await?);
        assert_eq!(5, txn.lalloc(&mut root).await?);
        free_on_disk_pg(&mut root.lalloc, &mut txn.defer_gaurd, 4);
        free_on_disk_pg(&mut root.lalloc, &mut txn.defer_gaurd, 5);

        let free = txn.write_fl(&mut root).await?;
        assert_eq!(6, free);
        bio.sync().await?;

        let fl_buf = bio.read(free).await?;
        assert_eq!(0, from_bytes::<FreeListHeader>(fl_buf.as_ref()).next.get());
        assert_eq!(5, read_fl_idx(fl_buf.as_ref(), 0));
        assert_eq!(4, read_fl_idx(fl_buf.as_ref(), 1));
        assert_eq!(0, read_fl_idx(fl_buf.as_ref(), 2));

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_long_fl() -> Result<()> {
        Ok(())
    }
}
