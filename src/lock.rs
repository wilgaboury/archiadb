use std::{
    cell::UnsafeCell,
    pin::Pin,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use crossbeam::utils::Backoff;

struct SpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

impl<T> SpinLock<T> {
    fn new(data: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    fn lock(&self) -> SpinLockGaurd<'_, T> {
        let backoff = Backoff::new();
        loop {
            let lock = self.locked.load(Ordering::Acquire);
            if !lock
                && self
                    .locked
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                break;
            } else {
                backoff.snooze();
            }
        }

        SpinLockGaurd { lock: self }
    }
}

struct SpinLockGaurd<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Drop for SpinLockGaurd<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> std::ops::Deref for SpinLockGaurd<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> std::ops::DerefMut for SpinLockGaurd<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
enum LockType {
    Read = 0,       // acquire node for reading
    ReadChildWrite, // acquire node for reading, intent to modify skip level child nodes
    ReadRecursive, // acquire exclusively for reading and prevent any write or child write aquisitions
    Write,         // acquire node for writing
}

impl LockType {
    pub fn is_compatible(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Read, Self::Read) => Some(Self::Read),
            (Self::Read, Self::ReadChildWrite) => Some(Self::ReadChildWrite),
            (Self::Read, Self::ReadRecursive) => Some(Self::ReadRecursive),
            (Self::Read, Self::Write) => None,
            (Self::ReadChildWrite, Self::Read) => Some(Self::ReadChildWrite),
            (Self::ReadChildWrite, Self::ReadChildWrite) => Some(Self::ReadChildWrite),
            (Self::ReadChildWrite, Self::ReadRecursive) => None,
            (Self::ReadChildWrite, Self::Write) => None,
            (Self::ReadRecursive, Self::Read) => Some(Self::ReadRecursive),
            (Self::ReadRecursive, Self::ReadChildWrite) => None,
            (Self::ReadRecursive, Self::ReadRecursive) => Some(Self::ReadRecursive),
            (Self::ReadRecursive, Self::Write) => None,
            (Self::Write, Self::Read) => None,
            (Self::Write, Self::ReadChildWrite) => None,
            (Self::Write, Self::ReadRecursive) => None,
            (Self::Write, Self::Write) => None,
        }
    }
}

struct SimpleQueue<T> {
    head: *mut SimpleQueueNode<T>,
    tail: *mut SimpleQueueNode<T>,
}

impl<T> SimpleQueue<T> {
    pub fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head.is_null()
    }

    pub fn push(&mut self, data: T) {
        let new_node = Box::into_raw(Box::new(SimpleQueueNode {
            data,
            next: ptr::null_mut(),
        }));

        unsafe {
            if self.head.is_null() {
                self.head = new_node;
            } else {
                (*self.tail).next = new_node;
            }
            self.tail = new_node;
        }
    }

    pub fn peek(&self) -> Option<&T> {
        unsafe {
            if self.head.is_null() {
                None
            } else {
                Some(&(*self.head).data)
            }
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.head.is_null() {
            return None;
        }

        unsafe {
            let node = Box::from_raw(self.head);
            self.head = node.next;

            if self.head.is_null() {
                self.tail = ptr::null_mut();
            }

            Some(node.data)
        }
    }
}

struct SimpleQueueNode<T> {
    data: T,
    next: *mut SimpleQueueNode<T>,
}

#[derive(Clone)]
struct Lock {
    ptr: Arc<SpinLock<LockInternal>>,
}

unsafe impl Send for Lock {}
unsafe impl Sync for Lock {}

impl Lock {
    pub fn new() -> Self {
        Self {
            ptr: Arc::new(SpinLock::new(LockInternal::new())),
        }
    }

    pub fn acquire(&self, lock_type: LockType) -> LockFuture<'_> {
        LockFuture {
            lock: &self,
            lock_type,
            was_queued: false,
            is_aquired: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct LockGuard<'a> {
    lock: &'a Lock,
    is_aquired: Arc<AtomicBool>,
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        let mut gaurd = self.lock.ptr.lock();
        if self.is_aquired.load(Ordering::Acquire) {
            self.is_aquired.store(false, Ordering::Release);
            gaurd.release();
        }
    }
}

struct LockFuture<'a> {
    lock: &'a Lock,
    lock_type: LockType,
    was_queued: bool,
    is_aquired: Arc<AtomicBool>, //TODO: atomic is not neccessary since protected by spin lock
}

impl<'a> Future for LockFuture<'a> {
    type Output = LockGuard<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.was_queued {
            Poll::Ready(LockGuard {
                lock: &self.lock,
                is_aquired: self.is_aquired.clone(),
            })
        } else {
            let mut gaurd = self.lock.ptr.lock();
            if gaurd.acquire(self.lock_type, &cx.waker(), &self.is_aquired) {
                self.is_aquired.store(true, Ordering::Release);
                Poll::Ready(LockGuard {
                    lock: &self.lock,
                    is_aquired: self.is_aquired.clone(),
                })
            } else {
                self.was_queued = true;
                Poll::Pending
            }
        }
    }
}

impl<'a> Drop for LockFuture<'a> {
    fn drop(&mut self) {
        let mut gaurd = self.lock.ptr.lock();
        if self.is_aquired.load(Ordering::Acquire) {
            self.is_aquired.store(false, Ordering::Release);
            gaurd.release();
        }
    }
}

struct LockInternal {
    lock_type: LockType,
    locked: u32,
    queue: SimpleQueue<(LockType, Waker, Arc<AtomicBool>)>,
}

impl LockInternal {
    fn new() -> Self {
        Self {
            lock_type: LockType::Read,
            locked: 0,
            queue: SimpleQueue::new(),
        }
    }

    fn acquire(
        &mut self,
        acq_lock_type: LockType,
        waker: &Waker,
        is_aquired: &Arc<AtomicBool>,
    ) -> bool {
        if !self.queue.is_empty() {
            self.queue
                .push((acq_lock_type, waker.clone(), is_aquired.clone()));
            false
        } else {
            if self.locked == 0 {
                self.lock_type = acq_lock_type;
                self.locked += 1;
                true
            } else if let Some(lock_type) = self.lock_type.is_compatible(&acq_lock_type) {
                self.lock_type = lock_type;
                self.locked += 1;
                true
            } else {
                self.queue
                    .push((acq_lock_type, waker.clone(), is_aquired.clone()));
                false
            }
        }
    }

    fn release(&mut self) {
        self.locked -= 1;

        if !self.queue.is_empty() {
            self.lock_type = if self.locked == 0 {
                self.locked += 1;
                let (lock_type, waker, is_acquired) = self.queue.pop().unwrap();
                is_acquired.store(true, Ordering::Release);
                waker.wake();
                lock_type
            } else {
                self.lock_type.clone()
            };

            while !self.queue.is_empty() {
                if let Some(lock_type) = self.lock_type.is_compatible(&self.queue.peek().unwrap().0)
                {
                    self.lock_type = lock_type;
                    self.locked += 1
                } else {
                    break;
                }
            }
        }
        // else if self.locked == 0 {
        //     // TODO: remove from lock cache
        // }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Result;
    use tokio::{spawn, sync::Barrier, time::timeout};

    use crate::lock;

    use super::*;

    #[tokio::test]
    async fn just_call_aquire() {
        let lock = Lock::new();
        {
            let _gaurd = lock.acquire(LockType::Read).await;
        }
    }

    #[tokio::test]
    async fn check_multiple_reads_dont_block() -> Result<()> {
        let lock1 = Lock::new();
        let lock2 = lock1.clone();
        let barrier1 = Arc::new(Barrier::new(2));
        let barrier2 = barrier1.clone();

        let t1 = spawn(timeout(Duration::from_secs(1), async move {
            let _gaurd = lock1.acquire(LockType::Read).await;
            barrier1.wait().await;
        }));

        let t2 = spawn(timeout(Duration::from_secs(1), async move {
            let _gaurd = lock2.acquire(LockType::Read).await;
            barrier2.wait().await;
        }));

        t1.await??;
        t2.await??;

        Ok(())
    }

    #[tokio::test]
    async fn check_that_read_blocks_write() -> Result<()> {
        let lock1 = Lock::new();
        let lock2 = lock1.clone();
        let lock3 = lock1.clone();
        let read_barrier1 = Arc::new(Barrier::new(2));
        let read_barrier2 = read_barrier1.clone();
        let read2_barrier1 = Arc::new(Barrier::new(2));
        let read2_barrier2 = read2_barrier1.clone();
        let write_barrier_before1 = Arc::new(Barrier::new(2));
        let write_barrier_before2 = write_barrier_before1.clone();
        let write_barrier_after1 = Arc::new(Barrier::new(2));
        let write_barrier_after2 = write_barrier_after1.clone();

        let t1 = spawn(timeout(Duration::from_secs(1), async move {
            let _gaurd = lock1.acquire(LockType::Read).await;
            read_barrier1.wait().await;
        }));

        let t2 = spawn(timeout(Duration::from_secs(1), async move {
            write_barrier_before1.wait().await;
            let _gaurd: LockGuard<'_> = lock2.acquire(LockType::Write).await;
            write_barrier_after1.wait().await;
        }));

        let t3 = spawn(timeout(Duration::from_secs(1), async move {
            read2_barrier1.wait().await;
            let _gaurd: LockGuard<'_> = lock3.acquire(LockType::Read).await;
        }));

        write_barrier_before2.wait().await;
        read_barrier2.wait().await;
        write_barrier_after2.wait().await;
        read2_barrier2.wait().await;

        t1.await??;
        t2.await??;
        t3.await??;

        Ok(())
    }
}
