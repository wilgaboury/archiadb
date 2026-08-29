use std::{collections::HashMap, ptr};

use anyhow::Result;

use crate::{
    db::{DirtyEntry, Txn},
    fio::PageBuf,
    uint::PgIdx,
};

/// Stores "in-flux" transaction pages in memory. That way non-persisted pages are not
/// copied on write multiple times.
pub(crate) struct Flux {
    map: HashMap<PgIdx, Option<PageBuf>>,
}

impl Flux {
    pub(crate) fn new() -> Self {
        Flux {
            map: HashMap::new(),
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
}

impl AsRef<[u8]> for FluxBuf {
    fn as_ref(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_ref(),
            FluxBuf::Alloc(data) => data.buf.as_ref(),
        }
    }
}

impl AsMut<[u8]> for FluxBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_mut(),
            FluxBuf::Alloc(data) => data.buf.as_mut(),
        }
    }
}

impl<'a> Txn<'a> {
    pub(crate) fn flux_buf(&mut self) -> FluxBuf {
        FluxBuf::Unalloc(self.db.fio.get_buf())
    }

    pub(crate) async fn flux_read(&mut self, pg_idx: u64) -> Result<FluxBuf> {
        if let Some(buf) = self.flux.map.get_mut(&pg_idx) {
            if buf.is_none() {
                panic!("cannot read already owned page")
            }
            let buf = std::mem::replace(buf, None).unwrap();
            Ok(FluxBuf::Alloc(FluxBufAlloc { idx: pg_idx, buf }))
        } else {
            Ok(FluxBuf::Unalloc(self.db.fio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn flux_write(
        &mut self,
        dirty: &mut DirtyEntry,
        buf: FluxBuf,
    ) -> Result<PgIdx> {
        match buf {
            FluxBuf::Unalloc(buf) => {
                let idx = self.lalloc(dirty).await?;
                self.flux.map.insert(idx, Some(buf));
                Ok(idx)
            }
            FluxBuf::Alloc(data) => {
                let idx = data.idx;
                let buf = unsafe { ptr::read(&data.buf) };
                std::mem::forget(data);
                self.flux.map.insert(idx, Some(buf));
                Ok(idx)
            }
        }
    }

    pub(crate) fn flux_free(&mut self, dirty: &mut DirtyEntry, idx: u64) {
        if self.flux.map.remove(&idx).is_some() {
            dirty.lalloc.add_free(idx);
        }
    }
}

impl Drop for FluxBufAlloc {
    fn drop(&mut self) {
        panic!("allocated flux buf should cannot be dropped or it causes leaked page allocations")
    }
}
