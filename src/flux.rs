use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ptr,
    rc::Rc,
};

use anyhow::Result;

use crate::{db::Txn, fio::PageBuf};

/// Stores "in-flux" transaction pages in memory. That way non-persisted pages are not
/// copied on write multiple times.
pub(crate) struct Flux {
    map: HashMap<u64, PageBuf>,
    free: Rc<RefCell<HashSet<u64>>>,
}

impl Flux {
    pub(crate) fn new() -> Self {
        Flux {
            map: HashMap::new(),
            free: Rc::new(RefCell::new(HashSet::new())),
        }
    }
}

#[derive(Debug)]
pub enum FluxBuf {
    Unalloc(PageBuf),
    Alloc(FluxBufAlloc),
}

#[derive(Debug)]
pub struct FluxBufAlloc {
    idx: u64,
    buf: PageBuf,
    free: Rc<RefCell<HashSet<u64>>>,
}

impl FluxBuf {
    pub fn get(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.get(),
            FluxBuf::Alloc(data) => data.buf.get(),
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.get_mut(),
            FluxBuf::Alloc(data) => data.buf.get_mut(),
        }
    }
}

impl<'a> Txn<'a> {
    pub(crate) fn flux_buf(&mut self) -> FluxBuf {
        FluxBuf::Unalloc(self.db.fio.get_buf())
    }

    pub(crate) async fn flux_read(&mut self, pg_idx: u64) -> Result<FluxBuf> {
        if let Some(buf) = self.flux.map.remove(&pg_idx) {
            Ok(FluxBuf::Alloc(FluxBufAlloc {
                idx: pg_idx,
                buf,
                free: self.flux.free.clone(),
            }))
        } else {
            Ok(FluxBuf::Unalloc(self.db.fio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn flux_write(&mut self, buf: FluxBuf) -> Result<u64> {
        match buf {
            FluxBuf::Unalloc(buf) => {
                let idx = self.alloc().await?;
                self.flux.map.insert(idx, buf);
                Ok(idx)
            }
            FluxBuf::Alloc(data) => {
                let idx = data.idx;
                let buf = unsafe { ptr::read(&data.buf) };
                std::mem::forget(data);
                self.flux.map.insert(idx, buf);
                Ok(idx)
            }
        }
    }

    pub(crate) fn flux_free(&mut self, idx: u64) {
        if self.flux.map.remove(&idx).is_none() {
            // TODO: replace alloc system
            // self.db.inner.alloc.free(idx);
        }
    }
}

impl Drop for FluxBufAlloc {
    fn drop(&mut self) {
        self.free.borrow_mut().insert(self.idx);
    }
}
