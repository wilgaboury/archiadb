use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::{AtomicU32, Ordering}},
};

use anyhow::Result;
use crossbeam::queue::SegQueue;
use tokio::sync::RwLock;
use tokio_uring::fs::{File, OpenOptions};

struct Inner {}

struct BlockAllocator {
    path: PathBuf,
    file: File,
    block_size: u64,
    free: Arc<SegQueue<u64>>,
    chunks: Arc<RwLock<Vec<AtomicU32>>>, // frontier idx
}

// struct Inner {
//     // TODO: refactor Arcs
// }

impl BlockAllocator {
    pub async fn new<P: AsRef<Path>>(path: P, block_size: u64) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .await?,
            block_size,
            free: Arc::new(SegQueue::new()),
            chunks: Arc::new(RwLock::new(Vec::new())),
        })
    }

    pub async fn init() {
        // TODO: read contents, populate free and chunks
    }

    pub async fn alloc(&self) -> Result<u64> {
        let idx = self.alloc_idx().await;
        panic!("TODO: for now, commit allocation to disk");
        Ok(idx)
    }

    async fn alloc_idx(&self) -> u64 {
        loop {
            if let Some(idx) = self.free.pop() {
                return idx;
            } else {
                let len = {
                    let chunks = self.chunks.read().await;
                    for (i, chunk) in (*chunks).iter().enumerate() {
                        if let Some(idx) = try_increment(chunk, self.block_size as u32) {
                            return (i as u64 * (self.block_size + 1)) + idx as u64;
                        }
                    }
                    chunks.len()
                };
                {
                    let mut chunks = self.chunks.write().await;
                    if chunks.len() == len {
                        // TODO: commit new chuck size to disk, meta block probably
                        chunks.push(AtomicU32::new(1));
                        return len as u64 * (self.block_size + 1);
                    }   
                }
            }
        }
    }

    pub async fn free(&self, idx: u64) {
        // TODO: commit to disk
        self.free.push(idx);
    }

    pub async fn clone(&self) -> Result<Self> {
        Ok(Self {
            path: self.path.clone(),
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&self.path)
                .await?,
            block_size: self.block_size,
            free: self.free.clone(),
            chunks: self.chunks.clone(),
        })
    }
}

fn try_increment(counter: &AtomicU32, max: u32) -> Option<u32> {
    loop {
        let cur = counter.load(Ordering::Relaxed);
        if cur >= max {
            return None;
        }
        if counter.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return Some(cur);
        }
    }
}