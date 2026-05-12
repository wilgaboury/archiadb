use std::{path::Path, sync::Arc};

use anyhow::Result;
use bon::bon;

use crate::{
    alloc::PageAllocator,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
};

#[derive(Clone)]
struct Db {
    inner: Arc<Inner>,
}

struct Inner {
    alloc: PageAllocator,
    fio: Fio,
}

#[bon]
impl Db {
    #[builder]
    pub async fn new<P: AsRef<Path>>(
        path: P,
        #[builder(default = DEFAULT_SQ_SIZE)] sq: usize,
        #[builder(default = DEFAULT_CQ_SIZE)] cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        let fio = Fio::new(path, sq, cq, page_buf_pool, generic_op_state_pool)?;
        let alloc = PageAllocator::new(fio.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner { alloc, fio }),
        })
    }

    pub fn txn(&self) -> TxnBuilder {
        TxnBuilder { db: self.clone() }
    }
}

struct TxnBuilder {
    db: Db,
}
