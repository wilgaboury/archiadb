use std::{cell::RefCell, collections::HashMap, ptr, rc::Rc};

use anyhow::Result;

use crate::{block::BlockIo, uint::PgIdx};

/// Answers this question: was a given page allocated during the current transaction?
///
/// This is important for page writes and creates a conditional that should be encapsulated/reused:
/// 1. if yes, it can be safely written to it's current index.
/// 2. if no, it must allocate an index for COW.
///
/// Another way of describing the purpose is to dedup COW allocations during
/// a transaction, which may edit the same page multiple times.
///
/// These allocations/pages are "in flux" because they are not yet persisted to disk.
pub(crate) struct Flux<B: BlockIo> {
    pub(crate) inner: FluxInnerRef<B>,
    pub(crate) bio: B,
}

#[derive(Debug)]
pub(crate) enum FluxMapValue<B: BlockIo> {
    Flushed,
    Borrowed,
    Owned(B::Buf),
}

type FluxInnerRef<U> = Rc<RefCell<FluxInner<U>>>;

#[derive(Debug)]
pub(crate) struct FluxInner<B: BlockIo> {
    pub(crate) map: HashMap<PgIdx, FluxMapValue<B>>,
}

impl<B: BlockIo> Flux<B> {
    pub(crate) fn new(bio: B) -> Self {
        Flux {
            inner: Rc::new(RefCell::new(FluxInner {
                map: HashMap::new(),
            })),
            bio,
        }
    }

    pub(crate) fn buf(&mut self) -> FluxBuf<B> {
        FluxBuf::Unalloc(self.bio.buf())
    }

    pub(crate) async fn read(&mut self, pg_idx: u64) -> Result<FluxBuf<B>> {
        if let Some(val) = self.inner.borrow_mut().map.get_mut(&pg_idx) {
            match val {
                FluxMapValue::Flushed => {
                    *val = FluxMapValue::Borrowed;
                    Ok(FluxBuf::Alloc(FluxBufAlloc {
                        idx: pg_idx,
                        buf: Some(self.bio.read(pg_idx).await?),
                        inner: self.inner.clone(),
                    }))
                }
                FluxMapValue::Borrowed => {
                    panic!("cannot read already borrowed page")
                }
                FluxMapValue::Owned(_) => {
                    let value = self
                        .inner
                        .borrow_mut()
                        .map
                        .insert(pg_idx, FluxMapValue::Borrowed)
                        .unwrap();
                    match value {
                        FluxMapValue::Owned(buf) => Ok(FluxBuf::Alloc(FluxBufAlloc {
                            idx: pg_idx,
                            buf: Some(buf),
                            inner: self.inner.clone(),
                        })),
                        _ => panic!("unexpected value in flux map"),
                    }
                }
            }
        } else {
            Ok(FluxBuf::Unalloc(self.bio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn write<F, Fut>(&mut self, buf: FluxBuf<B>, alloc: F) -> Result<PgIdx>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<PgIdx>>,
    {
        let (maybe_idx, buf) = buf.extract();
        let idx = if let Some(idx) = maybe_idx {
            idx
        } else {
            alloc().await?
        };
        self.inner
            .borrow_mut()
            .map
            .insert(idx, FluxMapValue::Owned(buf));
        Ok(idx)
    }

    pub(crate) fn free(&mut self, idx: PgIdx) {
        self.inner.borrow_mut().map.remove(&idx);
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        let map = &mut self.inner.borrow_mut().map;
        let mut futs = Vec::with_capacity(map.len());
        for (pg_idx, buf) in map.iter_mut() {
            if let FluxMapValue::Owned(buf) = std::mem::replace(buf, FluxMapValue::Flushed) {
                futs.push(self.bio.write(*pg_idx, buf));
            }
        }
        for fut in futs {
            fut.await?;
        }
        Ok(())
    }

    /// intended to be called on transaction commit
    pub(crate) fn clear(&mut self) {
        self.inner.borrow_mut().map.clear();
    }
}

#[derive(Debug)]
pub enum FluxBuf<B: BlockIo> {
    Unalloc(B::Buf),
    Alloc(FluxBufAlloc<B>),
}

#[derive(Debug)]
pub struct FluxBufAlloc<B: BlockIo> {
    idx: u64,
    buf: Option<B::Buf>,
    inner: FluxInnerRef<B>,
}

impl<B: BlockIo> AsRef<[u8]> for FluxBuf<B> {
    fn as_ref(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_ref(),
            FluxBuf::Alloc(data) => data.buf.as_ref().unwrap().as_ref(),
        }
    }
}

impl<B: BlockIo> AsMut<[u8]> for FluxBuf<B> {
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_mut(),
            FluxBuf::Alloc(data) => data.buf.as_mut().unwrap().as_mut(),
        }
    }
}

impl<B: BlockIo> FluxBuf<B> {
    fn extract(self) -> (Option<PgIdx>, B::Buf) {
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

impl<B: BlockIo> Drop for FluxBufAlloc<B> {
    fn drop(&mut self) {
        let map = &mut self.inner.borrow_mut().map;
        if map.get(&self.idx).is_some() {
            map.insert(
                self.idx,
                FluxMapValue::Owned(std::mem::take(&mut self.buf).unwrap()),
            );
        }
    }
}
