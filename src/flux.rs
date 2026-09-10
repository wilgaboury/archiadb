use std::{cell::RefCell, collections::HashMap, ptr, rc::Rc};

use anyhow::Result;

use crate::{
    db::{DirtyEntry, Txn},
    fio::PageBuf,
    lalloc::free_on_disk_pg,
    uint::PgIdx,
};

/// Stores "in-flux" transaction pages in memory. That way non-persisted pages are not
/// copied on write multiple times.
pub(crate) struct Flux {
    pub(crate) inner: FluxInner,
}

#[derive(Debug, Clone)]
pub(crate) struct FluxInner {
    pub(crate) map: Rc<RefCell<HashMap<PgIdx, Option<PageBuf>>>>,
}

impl Flux {
    pub(crate) fn new() -> Self {
        Flux {
            inner: FluxInner {
                map: Rc::new(RefCell::new(HashMap::new())),
            },
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
    buf: Option<PageBuf>,
    inner: FluxInner,
}

impl AsRef<[u8]> for FluxBuf {
    fn as_ref(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_ref(),
            FluxBuf::Alloc(data) => data.buf.as_ref().unwrap().as_ref(),
        }
    }
}

impl AsMut<[u8]> for FluxBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_mut(),
            FluxBuf::Alloc(data) => data.buf.as_mut().unwrap().as_mut(),
        }
    }
}

impl FluxBuf {
    fn extract(self) -> (Option<PgIdx>, PageBuf) {
        match self {
            FluxBuf::Unalloc(buf) => (None, buf),
            FluxBuf::Alloc(data) => {
                let idx = data.idx;
                let buf = unsafe { ptr::read(&data.buf) };
                std::mem::forget(data);
                (Some(idx), buf.unwrap())
            }
        }
    }
}

impl<'a> Txn<'a> {
    pub(crate) fn flux_buf(&mut self) -> FluxBuf {
        FluxBuf::Unalloc(self.db.fio.get_buf())
    }

    pub(crate) async fn flux_read(&mut self, pg_idx: u64) -> Result<FluxBuf> {
        if let Some(buf) = self.flux.inner.map.borrow_mut().get_mut(&pg_idx) {
            if buf.is_none() {
                panic!("cannot read already owned page")
            }
            let buf = std::mem::replace(buf, None).unwrap();
            Ok(FluxBuf::Alloc(FluxBufAlloc {
                idx: pg_idx,
                buf: Some(buf),
                inner: self.flux.inner.clone(),
            }))
        } else {
            Ok(FluxBuf::Unalloc(self.db.fio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn flux_write(
        &mut self,
        dirty: &mut DirtyEntry,
        buf: FluxBuf,
    ) -> Result<PgIdx> {
        let (maybe_idx, buf) = buf.extract();
        let idx = if let Some(idx) = maybe_idx {
            idx
        } else {
            self.lalloc(dirty).await?
        };
        self.flux.inner.map.borrow_mut().insert(idx, Some(buf));
        Ok(idx)
    }

    pub(crate) fn flux_free(&mut self, dirty: &mut DirtyEntry, idx: u64) {
        if self.flux.inner.map.borrow_mut().remove(&idx).is_some() {
            dirty.lalloc.free_in_mem_page(idx);
        } else {
            free_on_disk_pg(&mut dirty.lalloc, &mut self.defer_gaurd, idx);
        }
    }
}

impl Drop for FluxBufAlloc {
    fn drop(&mut self) {
        self.inner
            .map
            .borrow_mut()
            .insert(self.idx, std::mem::take(&mut self.buf));
    }
}
