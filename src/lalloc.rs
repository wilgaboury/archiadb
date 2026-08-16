use std::collections::HashMap;

use crate::{
    db::Txn,
    fio::PageBuf,
    key::KeyPathBuf,
    uint::{PgIdx, PgIdxDisk},
    util::{FrontBack, ceil_div},
};

pub(crate) struct Arena {
    pub(crate) start: PgIdx,
    pub(crate) len: PgIdx,
    pub(crate) next: PgIdx,
}

pub(crate) struct LallocMap {
    map: HashMap<KeyPathBuf, Lalloc>,
}

pub(crate) struct Lalloc {
    arena: Arena,
    from_arena: PgIdx, // # of allocns via this arena, needs to be flushed to disk before galloc

    clean: Option<PgIdx>, // pointer to unmodified tail of free list
    free: Option<PgIdx>,  // usable pointers
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

impl<'a> Txn<'a> {
    pub(crate) async fn lalloc(&self, _fb: FrontBack, _pg: PageBuf) -> u64 {
        todo!("implement lalloc");
    }
}
