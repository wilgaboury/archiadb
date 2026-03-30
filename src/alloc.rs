use tokio_uring::fs::File;

struct Inner {

}

struct BlockAllocator {
    file: File,
    block_size: u64,
}

impl BlockAllocator {
    pub async fn alloc(&self) -> u64 {
        0
    }

    pub async fn free(&self, offset: u64) {

    }
}