use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result};
use crossbeam::queue::SegQueue;
use tokio::sync::{Mutex, RwLock};
use tokio_uring::fs::{self, File, OpenOptions};
use zerocopy::IntoBytes;

use crate::{meta::Meta, util::pick_block_size};

struct Db {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    file: File,
    block_size: u64,

    free: SegQueue<u64>, // TODO: this same data structure but as a stack would be better because of less allocations
    chunks: RwLock<Vec<Chunk>>,
}

struct Chunk {
    header_lock: Mutex<()>, // protects concurrent reading/writing of the chunk header
    frontier: AtomicU32,
}

impl Db {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_block_size(path.as_ref(), pick_block_size(path.as_ref())?).await
    }

    pub async fn open_with_block_size<P: AsRef<Path>>(path: P, block_size: u64) -> Result<Self> {
        if file_exists(path.as_ref()).await {
            todo!("implement reading meta and initializing")
        } else {
            match (|| async {
                let file = open_file(&path).await?;

                let db = Self {
                    inner: Arc::new(Inner {
                        path: path.as_ref().to_path_buf(),
                        file,
                        block_size: pick_block_size(&path)?,

                        free: SegQueue::new(),
                        chunks: RwLock::new(vec![Chunk {
                            header_lock: Mutex::new(()),
                            frontier: AtomicU32::new(2),
                        }]), // first chunk is allocated for root nodes
                    }),
                };

                // allocate meta + first chunk
                db.inner
                    .file
                    .fallocate(0, 2 + block_size * block_size, 0 as i32)
                    .await?;

                let mut buf = db.alloc_vec_block_buf();

                let meta = Meta::new(block_size);
                let meta_bytes = meta.as_bytes();
                buf[..meta_bytes.len()].copy_from_slice(meta_bytes);
                let (result, mut buf) = db.inner.file.write_all_at(buf, 0).await;
                result.context("Failed to write meta block to file")?;

                buf.fill(0);
                buf[0] = 0b11000000; // first two blocks are root of btree, so mark them as allocated
                let (result, mut buf) = db.inner.file.write_all_at(buf, block_size).await;
                result.context("Failed to write first chunk header to file")?;

                // TODO: write btree root nodes, initalize allocator state

                Result::<_, anyhow::Error>::Ok(db)
            })()
            .await
            {
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

    fn alloc_vec_block_buf(&self) -> Vec<u8> {
        vec![0; self.inner.block_size as usize]
    }

    /// returns block idx
    async fn alloc(&self) -> Result<u64> {
        let idx = 'outer: loop {
            if let Some(idx) = self.inner.free.pop() {
                break 'outer idx;
            } else {
                let len = {
                    let chunks = self.inner.chunks.read().await;
                    for (i, chunk) in (*chunks).iter().enumerate() {
                        if let Some(idx) =
                            try_increment(&chunk.frontier, self.inner.block_size as u32)
                        {
                            break 'outer (i as u64 * (self.inner.block_size + 1)) + idx as u64;
                        }
                    }
                    chunks.len()
                };
                {
                    let mut chunks = self.inner.chunks.write().await;
                    if chunks.len() == len {
                        chunks.push(Chunk {
                            header_lock: Mutex::new(()),
                            frontier: AtomicU32::new(1),
                        });
                        break 'outer len as u64 * (self.inner.block_size + 1);
                    }
                }
            }
        };

        self.commit_alloc_to_disc(idx, true).await?;

        Ok(idx)
    }

    /// takes block idx
    async fn free(&self, idx: u64) -> Result<()> {
        self.commit_alloc_to_disc(idx, false).await?;
        self.inner.free.push(idx);
        Ok(())
    }

    async fn commit_alloc_to_disc(&self, idx: u64, alloc: bool) -> Result<()> {
        let buf = self.alloc_vec_block_buf();

        let chunks = self.inner.chunks.read().await;
        let _header = chunks[self.block_idx_to_chunk_idx(idx)]
            .header_lock
            .lock()
            .await;
        let (result, mut buf) = self
            .inner
            .file
            .read_exact_at(buf, self.block_idx_to_disk_idx(idx))
            .await;
        result.context("Failed to read chunk header for free")?;
        Self::bitfield_set(&mut buf, idx % self.inner.block_size, alloc);
        let (result, _) = self
            .inner
            .file
            .write_all_at(buf, self.block_idx_to_disk_idx(idx))
            .await;
        result.context("Failed to write chunk header for free")?;
        Ok(())
    }

    fn block_idx_to_chunk_idx(&self, idx: u64) -> usize {
        (idx / self.inner.block_size) as usize
    }

    fn block_idx_to_disk_idx(&self, idx: u64) -> u64 {
        self.inner.block_size + (idx * (self.inner.block_size + 1))
    }

    fn bitfield_set(buf: &mut [u8], idx: u64, as_one: bool) {
        let byte_idx = (idx / 8) as usize;
        let bit_idx = (idx % 8) as u8;
        if as_one {
            buf[byte_idx] |= 1 << bit_idx;
        } else {
            buf[byte_idx] &= !(1 << bit_idx);
        }
    }
}

impl Inner {
    pub async fn write_and_flip(
        &self,
        canon: &mut u64,
        prev: &mut u64,
        block: Vec<u8>,
    ) -> Result<()> {
        self.file
            .write_all_at(block, *prev)
            .await
            .0
            .context("Failed to write to file")?;
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

fn try_increment(counter: &AtomicU32, max: u32) -> Option<u32> {
    loop {
        let cur = counter.load(Ordering::Relaxed);
        if cur >= max {
            return None;
        }
        if counter
            .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return Some(cur);
        }
    }
}
