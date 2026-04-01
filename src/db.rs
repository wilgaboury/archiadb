use std::{path::{Path, PathBuf}, sync::Arc};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio_uring::fs::{File, OpenOptions};

use crate::util::pick_block_size;

struct Db {
    inner: Arc<Inner>
}

struct Inner {
    path: PathBuf,
    file: File,
    block_size: u64
}

impl Db {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner { 
                path: path.as_ref().to_path_buf(),
                file: open_file(&path).await?,
                block_size: pick_block_size(&path)?
            })
        })
    }
}

impl Inner {
    pub async fn write_and_flip(&self, canon: &mut u64, prev: &mut u64, block: Bytes) -> Result<()> {
        self.file.write_all_at(block, *prev).await.0.context("Failed to write to file")?;
        self.file.sync_all().await.context("Failed to sync file")?;
        let tmp = *canon;
        *canon = *prev;
        *prev = tmp;
        Ok(())
    }
}

async fn open_file<P: AsRef<Path>>(path: P) -> Result<File> {
    Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?)
}
