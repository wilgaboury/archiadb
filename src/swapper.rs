use std::marker::PhantomData;

use anyhow::{Context, Result};
use tokio_uring::fs::File;

pub trait ByteSliceConvert: Sized {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self>;
    fn try_into_bytes(&self) -> Result<&[u8]>;
}

pub struct Swapper<T: ByteSliceConvert> {
    file: File,
    block_size: u64,
    canon: u64,
    prev: u64,
    phantom: PhantomData<T>,
}

impl<T: ByteSliceConvert> Swapper<T> {
    pub async fn read(&self) -> Result<T> {
        let buf = vec![0u8; self.block_size as usize]; // TODO: dynamic allocation
        let (result, buf) = self.file.read_at(buf, self.canon).await;
        result.context("Failed to read from file")?;
        T::try_from_bytes(&buf)
    }

    pub async fn write(&mut self, data: &T) -> Result<()> {
        let bytes = data.try_into_bytes()?.to_vec(); // TODO: unnecessary copy
        let (result, _) = self.file.write_all_at(bytes, self.prev).await;
        result.context("Failed to write to file")?;
        let tmp = self.canon;
        self.canon = self.prev;
        self.prev = tmp;
        Ok(())
    }
}
