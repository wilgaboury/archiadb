use std::sync::Arc;

use crossbeam::queue::ArrayQueue;

struct Buffers {
    inner: Arc<Inner>,
}

struct Inner {
    size: u64,
    block_size: u64,
    bufs: Vec<Vec<u8>>,
    free: ArrayQueue<usize>,
    // waiters
}

impl Buffers {
    pub fn new(size: u64, block_size: u64) -> Self {
        let bufs = (0..size).map(|_| vec![0u8; block_size as usize]).collect();
        let free = ArrayQueue::new(size as usize);
        for i in 0..size {
            let _ = free.push(i as usize);
        }
        Self {
            inner: Arc::new(Inner {
                size,
                block_size,
                bufs,
                free,
            }),
        }
    }
}

struct BufRef {
    idx: usize,
    inner: Arc<Inner>, // TODO: use weak pointers to avoid keeping buffers alive
}

impl BufRef {
    pub fn get(&self) -> &[u8] {
        &self.inner.bufs[self.idx]
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        let ptr = Arc::as_ptr(&mut self.inner) as *mut Inner;
        unsafe { &mut (&mut (*ptr).bufs)[self.idx] }
    }
}

impl Drop for BufRef {
    fn drop(&mut self) {
        let _ = self.inner.free.push(self.idx);
    }
}
