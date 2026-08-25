use std::collections::HashMap;

use anyhow::Result;

use crate::{
    btree::BTreeRootHeader,
    db::{DirtyEntry, Txn},
    fio::PageBuf,
    key::KeyPathBuf,
    uint::{InPgIdx, PgIdx, PgIdxDisk},
    util::{from_bytes, from_bytes_mut},
};

const INIT_ARENA_SIZE: PgIdx = 8;

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

    pub(crate) fn pop_w_resv(&mut self, reserve: PgIdx) -> Option<PgIdx> {
        if self.len - self.next < reserve {
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
    pub(crate) fn free(&mut self, txn: &mut Txn, pg_idx: PgIdx) {
        self.deferred.push(pg_idx);
        txn.defer_gaurd.free(pg_idx);
    }

    pub(crate) fn pages_to_fl_encode_arena(&self, pg_size: PgIdx) -> PgIdx {
        let idxs_per = idxs_per_fl_page(pg_size);
        let idxs = self.arena.len - self.orig_next;
        // +1 is accounting for page needed to store free list node
        let div = idxs / (idxs_per + 1);
        div + 1 // change to +2 if moving to head compaction on pre-galloc
    }
}

impl<'txn> Txn<'txn> {
    pub(crate) async fn lalloc(&self, dirty: &mut DirtyEntry, pg: PageBuf) -> Result<PgIdx> {
        let root = from_bytes::<BTreeRootHeader>(pg.as_ref());

        // bootstrap condition
        if root.free.get() == 0 && root.arena.start.get() == 0 {
            dirty.lalloc.arena = self.db.galloc(&mut dirty.fb, INIT_ARENA_SIZE).await?;
            dirty.lalloc.orig_next = dirty.lalloc.arena.next;
            return Ok(dirty.lalloc.arena.pop().unwrap());
        }

        let mut clean = if let Some(clean) = dirty.lalloc.clean.as_ref() {
            *clean
        } else {
            root.free.get()
        };

        while clean != 0 {
            if !dirty.lalloc.free.is_empty() {
                return Ok(dirty.lalloc.free.pop().unwrap());
            }

            let fl_pg = self.db.fio.read(clean).await?;
            let header = from_bytes::<FreeListHeader>(fl_pg.as_ref());
            let idxs_per = idxs_per_fl_page(self.db.meta.page_size());
            for i in 0..idxs_per {
                let free_pg_idx = read_fl(fl_pg.as_ref(), i);
                if free_pg_idx == 0 {
                    break;
                }

                if self.db.defer.contains(free_pg_idx) {
                    dirty.lalloc.deferred.push(free_pg_idx);
                } else {
                    dirty.lalloc.free.push(free_pg_idx);
                }
            }
            clean = header.next.get();
            dirty.lalloc.clean = Some(clean);
        }

        let arena_reserve = dirty
            .lalloc
            .pages_to_fl_encode_arena(self.db.meta.page_size());
        Ok(
            if let Some(free_pg) = dirty.lalloc.arena.pop_w_resv(arena_reserve) {
                free_pg
            } else {
                self.encode_arena(dirty).await?;
                dirty.lalloc.arena = self.db.galloc(&mut dirty.fb, INIT_ARENA_SIZE).await?;
                dirty.lalloc.orig_next = 0;
                dirty.lalloc.arena.pop().unwrap()
            },
        )
    }

    pub(crate) async fn encode_arena(&self, dirty: &mut DirtyEntry) -> Result<()> {
        let to_encode = dirty
            .lalloc
            .pages_to_fl_encode_arena(self.db.meta.page_size());
        assert!(dirty.lalloc.arena.len - dirty.lalloc.arena.next == to_encode);

        let idxs_per = idxs_per_fl_page(self.db.meta.page_size());
        let mut root_buf = self.db.fio.read(dirty.fb.front()).await?;
        let root = from_bytes_mut::<BTreeRootHeader>(root_buf.as_mut());

        // TODO: for simplity not compacting previous head node, consider implementing this later

        let arena = &dirty.lalloc.arena;
        let mut encode_idx = dirty.lalloc.orig_next;
        let mut alloc_idx = arena.next;

        let mut next = root.free.get();
        while encode_idx < arena.next {
            let mut fl_buf = self.db.fio.get_buf();
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
            self.db.fio.write(next, fl_buf).await?;
        }

        root.free.set(next);
        self.db.fio.write(dirty.fb.back(), root_buf).await?;
        self.db.fio.commit().await?;
        dirty.fb.flip();

        Ok(())
    }
}
