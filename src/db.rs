use std::{path::{Path, PathBuf}, sync::Arc};

use anyhow::{Context, Result};
use tokio_uring::fs::{self, File, OpenOptions};
use zerocopy::IntoBytes;

use crate::{meta::Meta, util::pick_block_size};

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
        Self::open_with_block_size(path.as_ref(), pick_block_size(path.as_ref())?).await
    }

    pub async fn open_with_block_size<P: AsRef<Path>>(path: P, block_size: u64) -> Result<Self> {
        if file_exists(path.as_ref()).await {
            panic!("TODO: implement reading meta and initializing")
        } else {
            match (|| async {
                let file = open_file(&path).await?;

                // allocate meta + first chunk
                file.fallocate(0, 2 + block_size * block_size, 0 as i32).await?;

                let mut buf = vec![0u8; block_size as usize];

                let meta = Meta::new(block_size);
                let meta_bytes = meta.as_bytes();
                buf[..meta_bytes.len()].copy_from_slice(meta_bytes);
                let (result, mut buf) = file.write_all_at(buf, 0).await;
                result.context("Failed to write meta block to file")?;

                buf.fill(0);
                buf[0] = 1;
                buf[1] = 1;
                let (result, mut buf) = file.write_all_at(buf, block_size).await;
                result.context("Failed to write first chunk header to file")?;

                // TODO: write btree root nodes, initalize allocator state

                Result::<_, anyhow::Error>::Ok(Self {
                    inner: Arc::new(Inner {
                        path: path.as_ref().to_path_buf(),
                        file,
                        block_size: pick_block_size(&path)?
                    })
                })
            })().await {
                Ok(db) => Ok(db),
                e => {
                    // try to cleanup file on error
                    let did_delete = fs::remove_file(&path).await.is_ok();
                    let context = if did_delete {
                        "Failed to initialize database file"
                    } else {
                        "Failed to initialize database file, and failed to delete the partially inatialized file. Please delete manually before trying again."
                    };
                    e.context(context)
                }
            }
        }
    }
}

impl Inner {
    pub async fn write_and_flip(&self, canon: &mut u64, prev: &mut u64, block: Vec<u8>) -> Result<()> {
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

async fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::statx(path).await.is_ok()
}