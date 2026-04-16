use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, RwLock};

use crate::fio::Fio;

struct Db {
    inner: Arc<Inner>,
}

struct Inner {
    path: PathBuf,
    fio: Fio,

    free: std::sync::Mutex<BinaryHeap<Reverse<usize>>>, // min-heap free list ensures that allocations natrually gather toward front of address space, so database shrinking is possible or requires fewer operations
    chunks: RwLock<Vec<Chunk>>,
}

struct Chunk {
    header_lock: Mutex<()>, // protects concurrent reading/writing of the chunk header
    frontier: AtomicU32,
}

struct BlockPartialAlloc {
    db: Arc<Inner>,
    blocks: Vec<usize>,
}

impl BlockPartialAlloc {
    async fn commit(self, db: &Db, alloc: bool) -> Result<()> {
        // TODO: could easily be optimized to batch commits belonging to the same chunk
        for idx in &self.blocks {
            db.commit_alloc_to_disc(*idx, alloc).await?;
        }
        Ok(())
    }
}

impl Drop for BlockPartialAlloc {
    fn drop(&mut self) {
        self.db
            .free
            .lock()
            .unwrap()
            .extend(self.blocks.iter().map(|idx| Reverse(*idx)));
    }
}

impl Db {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let fio = Fio::builder().path(path.as_ref()).build()?;

        if fio.len() > 0 {
            todo!("implement reading meta and initializing")
        } else {
            match (|| async {
                // let file = open_file(&path).await?;

                // let block_size = if let Some(block_size) = block_size {
                //     block_size
                // } else {
                //     pick_block_size(&path)?
                // };

                let db = Self {
                    inner: Arc::new(Inner {
                        path: path.as_ref().to_path_buf(),
                        fio,

                        free: std::sync::Mutex::new(BinaryHeap::new()),
                        chunks: RwLock::new(vec![Chunk {
                            header_lock: Mutex::new(()),
                            frontier: AtomicU32::new(2),
                        }]), // first chunk is allocated for root nodes
                    }),
                };

                // // allocate meta + first chunk
                // db.inner
                //     .file
                //     .fallocate(0, 2 + block_size * block_size, 0 as i32)
                //     .await?;

                // let mut buf = db.alloc_vec_block_buf();

                // let meta = Meta::new(block_size);
                // let meta_bytes = meta.as_bytes();
                // buf[..meta_bytes.len()].copy_from_slice(meta_bytes);
                // let (result, mut buf) = db.inner.file.write_all_at(buf, 0).await;
                // result.context("Failed to write meta block to file")?;

                // buf.fill(0);
                // buf[0] = 0b11000000; // first two blocks are root of btree, so mark them as allocated
                // let (result, mut buf) = db.inner.file.write_all_at(buf, block_size).await;
                // result.context("Failed to write first chunk header to file")?;

                // TODO: write btree root nodes, initalize allocator state

                Result::<_, anyhow::Error>::Ok(db)
            })()
            .await
            {
                Ok(db) => Ok(db),
                e => {
                    // try to cleanup file on error
                    let did_delete = true; // TODO: delete path, path.delete().is_ok();
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
        vec![0; self.inner.fio.page_size() as usize]
    }

    /// returns block idx
    async fn alloc(&self, partial_alloc: &mut BlockPartialAlloc) -> Result<()> {
        let idx = 'outer: loop {
            let pop = {
                let mut free = self.inner.free.lock().unwrap();
                free.pop()
            };

            if let Some(Reverse(idx)) = pop {
                break 'outer idx;
            } else {
                let len = {
                    let chunks = self.inner.chunks.read().await;
                    for (i, chunk) in (*chunks).iter().enumerate() {
                        if let Some(idx) =
                            try_increment(&chunk.frontier, self.inner.fio.page_size() as u32)
                        {
                            break 'outer (i * (self.inner.fio.page_size() + 1)) + idx as usize;
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
                        break 'outer len * (self.inner.fio.page_size() + 1);
                    }
                }
            }
        };

        partial_alloc.blocks.push(idx);

        Ok(())
    }

    /// takes block idx
    async fn free(&self, idx: usize) -> Result<()> {
        self.commit_alloc_to_disc(idx, false).await?;
        self.inner.free.lock().unwrap().push(Reverse(idx));
        Ok(())
    }

    async fn commit_alloc_to_disc(&self, idx: usize, alloc: bool) -> Result<()> {
        let buf = self.alloc_vec_block_buf();

        let chunks = self.inner.chunks.read().await;
        let _header = chunks[self.block_idx_to_chunk_idx(idx)]
            .header_lock
            .lock()
            .await;
        // let (result, mut buf) = self
        //     .inner
        //     .file
        //     .read_exact_at(buf, self.block_idx_to_disk_idx(idx))
        //     .await;
        // result.context("Failed to read chunk header for free")?;
        // Self::bitfield_set(&mut buf, idx % self.inner.block_size, alloc);
        // let (result, _) = self
        //     .inner
        //     .file
        //     .write_all_at(buf, self.block_idx_to_disk_idx(idx))
        //     .await;
        // result.context("Failed to write chunk header for free")?;
        Ok(())
    }

    fn block_idx_to_chunk_idx(&self, idx: usize) -> usize {
        idx / self.inner.fio.page_size()
    }

    fn block_idx_to_disk_idx(&self, idx: usize) -> usize {
        self.inner.fio.page_size() + (idx * (self.inner.fio.page_size() + 1))
    }

    fn bitfield_set(buf: &mut [u8], idx: usize, as_one: bool) {
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
        // self.file
        //     .write_all_at(block, *prev)
        //     .await
        //     .0
        //     .context("Failed to write to file")?;
        // self.fio.commit().await.context("Failed to sync file")?;
        let tmp = *canon;
        *canon = *prev;
        *prev = tmp;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use function_name::named;

    // #[tokio::test]
    #[named]
    async fn test_db_open() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;

        let db_path = temp_dir.path().join("db");
        let _db = Db::open(&db_path).await?;

        Ok(())
    }

    // Utils

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(suffix: &str) -> Result<Self> {
            let path = std::env::temp_dir().join(format!(
                "{}_{}_{}",
                suffix,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&path)?;
            Ok(Self { path })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
