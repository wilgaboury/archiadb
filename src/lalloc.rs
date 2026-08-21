use std::collections::HashMap;

use anyhow::Result;

use crate::{
    btree::BTreeRootHeader,
    db::{DirtyEntry, Txn},
    fio::PageBuf,
    key::KeyPathBuf,
    uint::{InPgIdx, InPgIdxDisk, PgIdx, PgIdxDisk},
    util::{ceil_div, from_bytes},
};

const INIT_ARENA_SIZE: PgIdx = 8;

#[repr(C, packed)]
pub(crate) struct FreeListHeader {
    pub(crate) next: PgIdxDisk,
    pub(crate) len: InPgIdxDisk,
}

pub(crate) fn read_fl(buf: &[u8], idx: InPgIdx) -> PgIdx {
    from_bytes::<PgIdxDisk>(
        &buf[size_of::<FreeListHeader>() + (idx as usize * size_of::<PgIdxDisk>())..],
    )
    .get()
}

pub(crate) struct Arena {
    pub(crate) start: PgIdx,
    pub(crate) len: PgIdx,
    pub(crate) next: PgIdx,
}

impl Arena {
    pub(crate) fn pop_w_resv(&mut self, reserve: PgIdx) -> Option<PgIdx> {
        if self.len - self.next < reserve + 1 {
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
    from_arena: PgIdx, // # of allocns via this arena, needs to be flushed to disk before galloc

    clean: Option<PgIdx>, // pointer to unmodified tail of free list
    free: Vec<PgIdx>,     // usable pointers
    deferred: Vec<PgIdx>, // pgs that fail the deffered set check
}

impl Lalloc {
    pub(crate) fn pages_to_encode_arena(&self, pg_size: PgIdx, free_len: PgIdx) -> PgIdx {
        let idxs_per = pg_size / size_of::<PgIdxDisk>() as PgIdx;
        let from_div = self.from_arena / idxs_per;
        let from_rem = self.from_arena % idxs_per;
        let in_arena = self.arena.len - self.arena.next;
        let in_div = in_arena / (idxs_per + 1);
        let in_rem = in_arena % (idxs_per + 1);
        let head_rem = free_len % idxs_per;
        from_div + in_div + ceil_div(from_rem + in_rem + head_rem, idxs_per)
    }
}

impl<'txn> Txn<'txn> {
    pub(crate) async fn lalloc(&self, dirty: &mut DirtyEntry, pg: PageBuf) -> Result<PgIdx> {
        let root = from_bytes::<BTreeRootHeader>(pg.as_ref());
        if root.free.get() == 0 && root.arena.start.get() == 0 {
            self.db.galloc(&mut dirty.fb, INIT_ARENA_SIZE).await?;
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
            for i in 0..header.len.get() {
                let free_pg_idx = read_fl(fl_pg.as_ref(), i);
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
            .pages_to_encode_arena(self.db.meta.page_size(), root.free_len.get());

        Ok(
            if let Some(free_pg) = dirty.lalloc.arena.pop_w_resv(arena_reserve) {
                free_pg
            } else {
                todo!("encode arena, galloc, and return")
            },
        )
    }
}
