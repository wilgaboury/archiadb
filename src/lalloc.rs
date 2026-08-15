use std::collections::HashMap;

use crate::{db::Txn, key::KeyPathBuf, uint::PgIdx};

pub(crate) struct LallocMap {
    map: HashMap<KeyPathBuf, Lalloc>,
}

pub(crate) struct Lalloc {
    arena: Vec<PgIdx>, // allocated via arena, needs to be flushed to disk before galloc

    clean: Option<PgIdx>, // pointer to unmodified tail of free list
    free: Option<PgIdx>,  // usable pointers
    deferred: Vec<PgIdx>, // pgs that fail the deffered set check
}

impl<'a> Txn<'a> {
    pub async fn lalloc(&self) -> u64 {
        todo!("implement lalloc");
    }
}
