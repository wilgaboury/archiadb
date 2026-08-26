use std::{cmp, collections::HashMap};

use anyhow::Result;

use crate::{
    btree::BTreeRootHeader,
    db::{DirtyEntry, Txn},
    defer::Defer,
    fio::Fio,
    galloc::{Galloc, galloc_w_lock},
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

pub(crate) fn read_fl(buf: &[u8], idx: InPgIdx) -> PgIdx {
    from_bytes::<PgIdxDisk>(
        &buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
    )
    .get()
}

pub(crate) fn write_fl(buf: &mut [u8], idx: InPgIdx, pg_idx: PgIdx) {
    from_bytes_mut::<PgIdxDisk>(
        &mut buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
    )
    .set(pg_idx)
}

pub(crate) fn cap_fl(buf: &mut [u8], idx: InPgIdx) {
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

pub(crate) struct Lalloc {
    arena: Arena,
    orig_next: PgIdx,

    clean: Option<PgIdx>, // pointer to unmodified tail of free list
    free: Vec<PgIdx>,     // usable pointers
    deferred: Vec<PgIdx>, // pgs that fail the deffered set check
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

    pub(crate) fn free(&mut self, txn: &mut Txn, pg_idx: PgIdx) {
        self.deferred.push(pg_idx);
        txn.defer_gaurd.free(pg_idx);
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

    pub(crate) fn clean_or(&self, root: &BTreeRootHeader) -> PgIdx {
        if let Some(clean) = self.clean.as_ref() {
            *clean
        } else {
            root.free.get()
        }
    }
}

impl<'txn> Txn<'txn> {
    pub(crate) async fn lalloc(&self, dirty: &mut DirtyEntry) -> Result<PgIdx> {
        lalloc(
            &self.db.galloc,
            &self.db.defer,
            &self.db.meta,
            &self.db.fio,
            dirty,
        )
        .await
    }
}

pub(crate) async fn lalloc(
    galloc: &Galloc,
    defer: &Defer,
    meta: &MetaHandler,
    fio: &Fio,
    dirty: &mut DirtyEntry,
) -> Result<PgIdx> {
    let root_buf = fio.read(dirty.fb.front()).await?;
    let root = from_bytes::<BTreeRootHeader>(root_buf.as_ref());

    if should_init_arena(root) {
        // unlikely case, but post arena encode crash + fl depletion could leave inlavid arena with large len. use that len for galloc not init.
        let len = cmp::max(INIT_ARENA_SIZE, next_arena_len(root.arena.len.get()));
        dirty.lalloc.arena = galloc_w_lock(galloc, meta, fio, &mut dirty.fb, len).await?;
        dirty.lalloc.orig_next = dirty.lalloc.arena.next;
        return Ok(dirty.lalloc.arena.pop().unwrap());
    }

    if let Some(free) = find_free_idx_in_fl(defer, fio, root, &mut dirty.lalloc).await? {
        return Ok(free);
    }

    Ok(
        if let Some(free) = dirty.lalloc.pop_arena_with_encode_reserve(fio.page_size()) {
            free
        } else {
            encode_arena_and_commit(fio, dirty).await?;
            let next_len = next_arena_len(root.arena.len.get());
            dirty.lalloc.arena = galloc_w_lock(galloc, meta, fio, &mut dirty.fb, next_len).await?;
            dirty.lalloc.orig_next = dirty.lalloc.arena.next;
            dirty.lalloc.arena.pop().unwrap()
        },
    )
}

pub(crate) fn should_init_arena(root: &BTreeRootHeader) -> bool {
    root.free.get() == 0 && root.arena.is_valid()
}

pub(crate) async fn find_free_idx_in_fl(
    defer: &Defer,
    fio: &Fio,
    root: &BTreeRootHeader,
    lalloc: &mut Lalloc,
) -> Result<Option<PgIdx>> {
    let mut clean = lalloc.clean_or(root);
    loop {
        if let Some(free) = lalloc.free.pop() {
            return Ok(Some(free));
        }

        if clean == 0 {
            break;
        }

        clean = load_fl_node(defer, fio, lalloc, clean).await?;
        lalloc.clean = Some(clean);
    }

    Ok(None)
}

pub(crate) async fn load_fl_node(
    defer: &Defer,
    fio: &Fio,
    lalloc: &mut Lalloc,
    fl_n_idx: PgIdx,
) -> Result<PgIdx> {
    let fl_pg = fio.read(fl_n_idx).await?;
    let header = from_bytes::<FreeListHeader>(fl_pg.as_ref());
    let idxs_per = idxs_per_fl_page(fio.page_size());
    for i in 0..idxs_per {
        let free_pg_idx = read_fl(fl_pg.as_ref(), i);
        if free_pg_idx == 0 {
            break;
        }

        if defer.contains(free_pg_idx) {
            lalloc.deferred.push(free_pg_idx);
        } else {
            lalloc.free.push(free_pg_idx);
        }
    }
    Ok(header.next.get())
}

pub(crate) fn next_arena_len(prev: PgIdx) -> PgIdx {
    if prev < INIT_ARENA_SIZE {
        INIT_ARENA_SIZE
    } else {
        1 << cmp::min(MAX_ARENA_EXP, prev.ilog2() as u64 + 1)
    }
}

// for simplity this makes no attempt to compact the previous head node
pub(crate) async fn encode_arena_and_commit(fio: &Fio, dirty: &mut DirtyEntry) -> Result<()> {
    let to_encode = dirty.lalloc.pages_to_fl_encode_arena(fio.page_size());
    let left = dirty.lalloc.arena.len - dirty.lalloc.arena.next;
    assert!(left == to_encode);

    let idxs_per = idxs_per_fl_page(fio.page_size());
    let mut root_buf = fio.read(dirty.fb.front()).await?;
    let root = from_bytes_mut::<BTreeRootHeader>(root_buf.as_mut());

    let arena = &dirty.lalloc.arena;
    let mut encode_idx = dirty.lalloc.orig_next;
    let mut alloc_idx = arena.next;

    let mut next = root.free.get();
    while encode_idx < arena.next {
        let mut fl_buf = fio.get_buf();
        from_bytes_mut::<FreeListHeader>(fl_buf.as_mut())
            .next
            .set(next);

        let mut i = 0;
        while i < idxs_per && encode_idx < arena.next {
            write_fl(fl_buf.as_mut(), i, arena.start + encode_idx);

            i += 1;
            encode_idx += 1;
        }

        next = arena.start + alloc_idx;
        alloc_idx += 1;
        fio.write(next, fl_buf).await?;
    }

    root.arena.invalidate();
    root.free.set(next);
    let version = root.version.get() + 1;
    root.version.set(version);
    fio.write(dirty.fb.back(), root_buf).await?;
    fio.commit().await?;
    dirty.fb.flip();

    Ok(())
}

#[coverage(off)]
#[cfg(test)]
mod tests {
    use anyhow::Result;
    use function_name::named;

    use crate::{
        btree::BTreeRootHeader,
        db::DirtyEntry,
        key_path,
        lalloc::{Arena, FreeListHeader, INIT_ARENA_SIZE, read_fl},
        test::TmpDir,
        util::{FrontBack, from_bytes},
    };

    #[named]
    #[tokio::test]
    async fn test_single_encode_arena() -> Result<()> {
        let tmp = TmpDir::new(function_name!())?;
        let db = tmp.db().await?;
        let fio = &db.inner.fio;
        let txn = db.txn().write(key_path![])?.begin().await;
        let mut dirty = DirtyEntry::new(FrontBack::new(2, 3), Arena::new(0, 0));
        assert_eq!(4, txn.lalloc(&mut dirty).await?);

        {
            assert_eq!(3, dirty.fb.front());
            assert_eq!(2, dirty.fb.back());
        }

        assert_eq!(5, txn.lalloc(&mut dirty).await?);
        assert_eq!(6, txn.lalloc(&mut dirty).await?);
        assert_eq!(7, txn.lalloc(&mut dirty).await?);
        assert_eq!(8, txn.lalloc(&mut dirty).await?);
        assert_eq!(9, txn.lalloc(&mut dirty).await?);
        assert_eq!(10, txn.lalloc(&mut dirty).await?);

        let root_buf = fio.read(dirty.fb.front()).await?;
        let root = from_bytes::<BTreeRootHeader>(root_buf.as_ref());
        let start_version = root.version.get();
        assert_eq!(12, txn.lalloc(&mut dirty).await?);

        assert_eq!(3, dirty.fb.front());
        assert_eq!(2, dirty.fb.back());
        let root_buf = fio.read(dirty.fb.front()).await?;
        let root = from_bytes::<BTreeRootHeader>(root_buf.as_ref());
        assert_eq!(11, root.free.get());
        assert_eq!(start_version + 2, root.version.get());

        let fl_buf = fio.read(root.free.get()).await?;
        assert_eq!(0, from_bytes::<FreeListHeader>(fl_buf.as_ref()).next.get());
        assert_eq!(4, read_fl(fl_buf.as_ref(), 0));
        assert_eq!(5, read_fl(fl_buf.as_ref(), 1));
        assert_eq!(6, read_fl(fl_buf.as_ref(), 2));
        assert_eq!(7, read_fl(fl_buf.as_ref(), 3));
        assert_eq!(8, read_fl(fl_buf.as_ref(), 4));
        assert_eq!(9, read_fl(fl_buf.as_ref(), 5));
        assert_eq!(10, read_fl(fl_buf.as_ref(), 6));
        assert_eq!(0, read_fl(fl_buf.as_ref(), 7));

        assert_eq!(12, root.arena.start.get());
        assert_eq!(INIT_ARENA_SIZE * 2, root.arena.len.get());
        assert_eq!(0, root.arena.next.get());

        Ok(())
    }
}
