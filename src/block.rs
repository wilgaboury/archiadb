use std::{collections::HashMap, sync::Arc};

use anyhow::{Result, bail};
use parking_lot::Mutex;

use crate::{
    fio::BlockIoUring,
    uint::{InPgIdx, PgIdx},
    util::{has_valid_checksum, update_checksum},
};

pub(crate) type BlockIoDefault = BlockIoRapidHash<BlockIoUring>;

pub(crate) trait BlockIo {
    type Buf: AsMut<[u8]> + AsRef<[u8]> + Send;

    fn page_size(&self) -> InPgIdx;
    fn buf(&self) -> Self::Buf;
    async fn read(&self, pg_idx: PgIdx) -> Result<Self::Buf>;
    async fn read_unchecked(&self, pg_idx: PgIdx) -> Result<Self::Buf> {
        self.read(pg_idx).await
    }
    async fn write(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()>;
    async fn write_unchecked(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()> {
        self.write(pg_idx, buf).await
    }
    async fn alloc(&self, size: PgIdx) -> Result<()>;
    async fn sync(&self) -> Result<()>;
}

pub(crate) struct BlockIoMemory {
    data: Mutex<Vec<u8>>,
    page_size: InPgIdx,
}

impl BlockIoMemory {
    fn new(page_size: InPgIdx) -> Self {
        Self {
            data: Mutex::new(Vec::new()),
            page_size,
        }
    }
}

impl BlockIo for BlockIoMemory {
    type Buf = Vec<u8>;

    fn page_size(&self) -> InPgIdx {
        self.page_size
    }

    fn buf(&self) -> Self::Buf {
        vec![0; self.page_size as usize]
    }

    async fn read(&self, pg_idx: PgIdx) -> Result<Self::Buf> {
        let start = pg_idx * self.page_size;
        let end = start + self.page_size;
        let data = self.data.lock();
        if end > data.len() as PgIdx {
            return Err(anyhow::anyhow!("Read out of bounds"));
        }
        Ok(data[start as usize..end as usize].to_vec())
    }

    async fn write(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()> {
        let start = pg_idx * self.page_size;
        let end = start + self.page_size;
        let mut data = self.data.lock();
        if end > data.len() as PgIdx {
            return Err(anyhow::anyhow!("Write out of bounds"));
        }
        data[start as usize..end as usize].copy_from_slice(&buf);
        Ok(())
    }

    async fn alloc(&self, size: PgIdx) -> Result<()> {
        let mut data = self.data.lock();
        let new_len = data.len() + size as usize;
        data.resize(new_len, 0);
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        // no-op
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct BlockIoRapidHash<T: BlockIo> {
    pub inner: T,
}

impl<T: BlockIo> BlockIoRapidHash<T> {
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: Clone + BlockIo> Clone for BlockIoRapidHash<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<B: BlockIo> BlockIo for BlockIoRapidHash<B> {
    type Buf = B::Buf;

    fn page_size(&self) -> InPgIdx {
        self.inner.page_size()
    }

    fn buf(&self) -> Self::Buf {
        self.inner.buf()
    }

    async fn read(&self, pg_idx: PgIdx) -> Result<Self::Buf> {
        let pg = self.inner.read(pg_idx).await?;

        if !has_valid_checksum(pg.as_ref()) {
            bail!("checksum mismatch")
        }

        std::result::Result::Ok(pg)
    }

    async fn read_unchecked(&self, pg_idx: PgIdx) -> Result<Self::Buf> {
        self.inner.read(pg_idx).await
    }

    async fn write(&self, pg_idx: PgIdx, mut buf: Self::Buf) -> Result<()> {
        update_checksum(buf.as_mut());
        self.inner.write(pg_idx, buf).await
    }

    async fn write_unchecked(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()> {
        self.inner.write(pg_idx, buf).await
    }

    async fn alloc(&self, size: PgIdx) -> Result<()> {
        self.inner.alloc(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }
}

pub(crate) struct BlockIoCached<B: BlockIo> {
    pub(crate) inner: BlockIoCachedInner<B::Buf>,
    pub(crate) bio: B,
}

impl<B: Clone + BlockIo> Clone for BlockIoCached<B> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            bio: self.bio.clone(),
        }
    }
}

pub(crate) struct BlockIoCachedInner<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    pub(crate) map: Arc<Mutex<HashMap<PgIdx, U>>>,
}

impl<U> Clone for BlockIoCachedInner<U>
where
    U: AsMut<[u8]> + AsRef<[u8]> + Send,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

impl<B: BlockIo> BlockIoCached<B> {
    pub(crate) fn new(bio: B) -> Self {
        BlockIoCached {
            inner: BlockIoCachedInner {
                map: Arc::new(Mutex::new(HashMap::new())),
            },
            bio,
        }
    }
}

impl<B: BlockIo> BlockIo for BlockIoCached<B> {
    type Buf = B::Buf;

    fn page_size(&self) -> InPgIdx {
        self.bio.page_size()
    }

    fn buf(&self) -> Self::Buf {
        self.bio.buf()
    }

    async fn read(&self, pg_idx: u64) -> Result<Self::Buf> {
        let mut map = self.inner.map.lock();
        match map.remove(&pg_idx) {
            Some(buf) => Ok(buf),
            None => self.bio.read(pg_idx).await,
        }
    }

    async fn read_unchecked(&self, pg_idx: PgIdx) -> Result<Self::Buf> {
        self.bio.read_unchecked(pg_idx).await
    }

    async fn write(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()> {
        self.inner.map.lock().insert(pg_idx, buf);
        Ok(())
    }

    async fn write_unchecked(&self, pg_idx: PgIdx, buf: Self::Buf) -> Result<()> {
        self.bio.write_unchecked(pg_idx, buf).await
    }

    async fn alloc(&self, size: PgIdx) -> Result<()> {
        self.bio.alloc(size).await
    }

    async fn sync(&self) -> Result<()> {
        let mut map = self.inner.map.lock();
        for (pg_idx, buf) in map.drain() {
            self.bio.write(pg_idx, buf).await?;
        }
        self.bio.sync().await;
        Ok(())
    }
}
