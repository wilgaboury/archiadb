use std::collections::HashMap;

use anyhow::Result;

use crate:: {db::Txn, fio::PageBuf};

/// Stores "in-flux" transaction pages in memory. That way non-persisted pages are not
/// copied on write multiple times.
pub(crate) struct Flux {
    map: HashMap<u64, PageBuf>,
}

impl Flux {
    pub(crate) fn new() -> Self {
        Flux { map: HashMap::new() }
    }
}

pub enum FluxBuf {
    Unalloc(PageBuf),
    Alloc {
        idx: u64,
        buf: PageBuf
    }
}

impl FluxBuf {
    pub fn get(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.get(),
            FluxBuf::Alloc { buf, .. } => buf.get(),
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.get_mut(),
            FluxBuf::Alloc { buf, .. } => buf.get_mut(),
        }
    }
}

impl Txn {
    pub(crate) fn flux_buf(&mut self) -> FluxBuf {
        FluxBuf::Unalloc(self.db.inner.fio.get_buf())
    }

    pub(crate) async fn flux_read(&mut self, pg_idx: u64) -> Result<FluxBuf> {
        if let Some(buf) = self.flux.map.remove(&pg_idx) {
            Ok(FluxBuf::Alloc { idx: pg_idx, buf })
        } else {
            Ok(FluxBuf::Unalloc(self.db.inner.fio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn flux_write(&mut self, buf: FluxBuf) -> Result<u64> {
        match buf {
            FluxBuf::Unalloc(buf) => {
                let idx = self.db.inner.alloc.alloc(&self.db.inner.meta, &mut self.allocs).await?;
                self.flux.map.insert(idx, buf);
                Ok(idx)
            }
            FluxBuf::Alloc { idx, buf } => {
                self.flux.map.insert(idx, buf);
                Ok(idx)
            }
        }
    }

    // TODO: need to add flux_free
}