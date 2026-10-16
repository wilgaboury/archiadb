use anyhow::{Result, bail};
use parking_lot::Mutex;

use crate::util::{has_valid_checksum, update_checksum};

trait BlockIo {
    type Buf: AsMut<[u8]> + AsRef<[u8]> + Send + Sync;

    fn page_size(&self) -> usize;
    fn buf(&self) -> Self::Buf;
    async fn read(&self, addr: usize) -> Result<Self::Buf>;
    async fn write(&self, addr: usize, buf: Self::Buf) -> Result<()>;
    async fn alloc(&self, size: usize) -> Result<()>;
    async fn sync(&self) -> Result<()>;
}

trait BlockIoChecked: BlockIo {
    async fn read_unchecked(&self, addr: usize) -> Result<Self::Buf>;
    async fn write_unchecked(&self, addr: usize, buf: Self::Buf) -> Result<()>;
}

struct BlockIoMemory {
    data: Mutex<Vec<u8>>,
    page_size: usize,
}

impl BlockIoMemory {
    fn new(page_size: usize) -> Self {
        Self {
            data: Mutex::new(Vec::new()),
            page_size,
        }
    }
}

impl BlockIo for BlockIoMemory {
    type Buf = Vec<u8>;

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn buf(&self) -> Self::Buf {
        vec![0; self.page_size]
    }

    async fn read(&self, pg_idx: usize) -> Result<Self::Buf> {
        let start = pg_idx * self.page_size;
        let end = start + self.page_size;
        let data = self.data.lock();
        if end > data.len() {
            return Err(anyhow::anyhow!("Read out of bounds"));
        }
        Ok(data[start..end].to_vec())
    }

    async fn write(&self, pg_idx: usize, buf: Self::Buf) -> Result<()> {
        let start = pg_idx * self.page_size;
        let end = start + self.page_size;
        let mut data = self.data.lock();
        if end > data.len() {
            return Err(anyhow::anyhow!("Write out of bounds"));
        }
        data[start..end].copy_from_slice(&buf);
        Ok(())
    }

    async fn alloc(&self, size: usize) -> Result<()> {
        let mut data = self.data.lock();
        let new_len = data.len() + size;
        data.resize(new_len, 0);
        Ok(())
    }

    async fn sync(&self) -> Result<()> {
        // no-op
        Ok(())
    }
}

struct BlockIoCheckedRapidHash<T: BlockIo> {
    inner: T,
}

impl<T: BlockIo> BlockIoChecked for BlockIoCheckedRapidHash<T> {
    async fn read_unchecked(&self, pg_idx: usize) -> Result<Self::Buf> {
        self.inner.read(pg_idx).await
    }

    async fn write_unchecked(&self, pg_idx: usize, buf: Self::Buf) -> Result<()> {
        self.inner.write(pg_idx, buf).await
    }
}

impl<T: BlockIo> BlockIo for BlockIoCheckedRapidHash<T> {
    type Buf = T::Buf;

    fn page_size(&self) -> usize {
        self.inner.page_size()
    }

    fn buf(&self) -> Self::Buf {
        self.inner.buf()
    }

    async fn read(&self, pg_idx: usize) -> Result<Self::Buf> {
        let pg = self.inner.read(pg_idx).await?;

        if !has_valid_checksum(pg.as_ref()) {
            bail!("checksum mismatch")
        }

        std::result::Result::Ok(pg)
    }

    async fn write(&self, pg_idx: usize, mut buf: Self::Buf) -> Result<()> {
        update_checksum(buf.as_mut());
        self.inner.write(pg_idx, buf).await
    }

    async fn alloc(&self, size: usize) -> Result<()> {
        self.inner.alloc(size).await
    }

    async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }
}
