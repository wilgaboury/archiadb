use std::{cell::RefCell, collections::HashMap, ptr, rc::Rc};

use anyhow::Result;

use crate::{block::BlockIo, uint::PgIdx};

/// Stores "in-flux" transaction pages in memory, so non-persisted pages are not copied on write multiple times.
pub(crate) struct Flux<B, U>
where
    B: BlockIo<Buf = U>,
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    pub(crate) inner: FluxInner<U>,
    pub(crate) bio: B,
}

#[derive(Debug)]
pub(crate) struct FluxInner<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    pub(crate) map: Rc<RefCell<HashMap<PgIdx, Option<U>>>>,
}

impl<U> Clone for FluxInner<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

impl<B, U> Flux<B, U>
where
    B: BlockIo<Buf = U>,
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    pub(crate) fn new(bio: B) -> Self {
        Flux {
            inner: FluxInner {
                map: Rc::new(RefCell::new(HashMap::new())),
            },
            bio,
        }
    }

    pub(crate) fn buf(&mut self) -> FluxBuf<U> {
        FluxBuf::Unalloc(self.bio.buf())
    }

    pub(crate) async fn read(&mut self, pg_idx: u64) -> Result<FluxBuf<U>> {
        if let Some(buf) = self.inner.map.borrow_mut().get_mut(&pg_idx) {
            if buf.is_none() {
                panic!("cannot read already owned page")
            }
            let buf = std::mem::replace(buf, None).unwrap();
            Ok(FluxBuf::Alloc(FluxBufAlloc {
                idx: pg_idx,
                buf: Some(buf),
                inner: self.inner.clone(),
            }))
        } else {
            Ok(FluxBuf::Unalloc(self.bio.read(pg_idx).await?))
        }
    }

    pub(crate) async fn write<F, Fut>(&mut self, buf: FluxBuf<U>, alloc: F) -> Result<PgIdx>
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
        self.inner.map.borrow_mut().insert(idx, Some(buf));
        Ok(idx)
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        let mut map = self.inner.map.borrow_mut();
        let mut futs = Vec::with_capacity(map.len());
        for (pg_idx, buf) in map.drain() {
            if let Some(buf) = buf {
                futs.push(self.bio.write(pg_idx, buf));
            }
        }
        for fut in futs {
            fut.await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum FluxBuf<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    Unalloc(U),
    Alloc(FluxBufAlloc<U>),
}

#[derive(Debug)]
pub struct FluxBufAlloc<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    idx: u64,
    buf: Option<U>,
    inner: FluxInner<U>,
}

impl<U> AsRef<[u8]> for FluxBuf<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn as_ref(&self) -> &[u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_ref(),
            FluxBuf::Alloc(data) => data.buf.as_ref().unwrap().as_ref(),
        }
    }
}

impl<U> AsMut<[u8]> for FluxBuf<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn as_mut(&mut self) -> &mut [u8] {
        match self {
            FluxBuf::Unalloc(buf) => buf.as_mut(),
            FluxBuf::Alloc(data) => data.buf.as_mut().unwrap().as_mut(),
        }
    }
}

impl<U> FluxBuf<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn extract(self) -> (Option<PgIdx>, U) {
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

impl<U> Drop for FluxBufAlloc<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn drop(&mut self) {
        let mut map = self.inner.map.borrow_mut();
        if map.get(&self.idx).is_some() {
            map.insert(self.idx, std::mem::take(&mut self.buf));
        }
    }
}
