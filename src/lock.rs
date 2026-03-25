use std::{
    cell::UnsafeCell,
    marker::PhantomData,
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

    pub fn acquire(&self, lock_type: LockType) -> LockFuture {
        LockFuture {
            lock: self.clone(),
            lock_type,
            acquire_state: Arc::new(UnsafeCell::new(AcquireState::Init)),
        }
    }
}

enum AcquireState {
    Init,
    Queued,
    Acquired,
    Gaurded,
    Dropped,
}

struct LockFuture {
    lock: Lock,
    lock_type: LockType,
    acquire_state: Arc<UnsafeCell<AcquireState>>, // protected by spin lock
}

unsafe impl Send for LockFuture {}
unsafe impl Sync for LockFuture {}

impl<'a> Future for LockFuture {
    type Output = LockGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut gaurd = self.lock.ptr.lock();
        let acquire_state = unsafe { &mut *self.acquire_state.get() };
        match acquire_state {
            AcquireState::Init => {
                *acquire_state = gaurd.acquire(self.lock_type, &cx.waker(), &self.acquire_state);
                if let AcquireState::Acquired = *acquire_state {
                    *acquire_state = AcquireState::Gaurded;
                    Poll::Ready(LockGuard {
                        lock: self.lock.clone(),
                        acquire_state: self.acquire_state.clone(),
                    })
                } else {
                    Poll::Pending
                }
            }
            AcquireState::Acquired => {
                *acquire_state = AcquireState::Gaurded;
                Poll::Ready(LockGuard {
                    lock: self.lock.clone(),
                    acquire_state: self.acquire_state.clone(),
                })
            },
            AcquireState::Queued | AcquireState::Gaurded | AcquireState::Dropped => {
                // this should never actually be reached
                Poll::Pending
            }
        }
    }
}


impl Drop for LockFuture {
    fn drop(&mut self) {
        // println!("future dropped");
        let mut gaurd = self.lock.ptr.lock();
        let acquire_state = unsafe { &mut *self.acquire_state.get() };
        match acquire_state {
            AcquireState::Init | AcquireState::Queued => {
                // println!("is this happening");
                *acquire_state = AcquireState::Dropped
            }
            AcquireState::Acquired => {
                *acquire_state = AcquireState::Dropped;
                gaurd.release();
            }
            AcquireState::Gaurded | AcquireState::Dropped => {
                // no_op
            }
        }
    }
}

struct LockGuard {
    lock: Lock,
    acquire_state: Arc<UnsafeCell<AcquireState>>, // TODO: this field can probably be removed
}

unsafe impl Send for LockGuard {}
unsafe impl Sync for LockGuard {}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let mut gaurd = self.lock.ptr.lock();
        let acquire_state = unsafe { &mut *self.acquire_state.get() };
        if !matches!(*acquire_state, AcquireState::Gaurded) {
            panic!("should be gaurded");
        }
        // state should always be gaurded
        *acquire_state = AcquireState::Dropped;
        // println!("lock gaurd releasing");
        gaurd.release();
    }
}
struct LockInternal {
    lock_type: LockType,
    locked: u32,
    queue: SimpleQueue<(LockType, Waker, Arc<UnsafeCell<AcquireState>>)>,
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
        acquire_state: &Arc<UnsafeCell<AcquireState>>,
    ) -> AcquireState {
        if !self.queue.is_empty() {
            self.queue
                .push((acq_lock_type, waker.clone(), acquire_state.clone()));
            // println!("queued gotta wait");
            AcquireState::Queued
        } else {
            if self.locked == 0 {
                // println!("acquired immediately, locked = 0");
                self.lock_type = acq_lock_type;
                self.locked += 1;
                // println!("locked: {}", self.locked);
                AcquireState::Acquired
            } else if let Some(lock_type) = self.lock_type.is_compatible(&acq_lock_type) {
                // println!("acquired immediately, compatible");
                self.lock_type = lock_type;
                self.locked += 1;
                // println!("locked: {}", self.locked);
                AcquireState::Acquired
            } else {
                // println!("queued not compat");
                self.queue
                    .push((acq_lock_type, waker.clone(), acquire_state.clone()));
                AcquireState::Queued
            }
        }
    }

    fn remove_non_queued(&mut self) {
        while !self.queue.is_empty() {
            let acquire_state = unsafe { &*self.queue.peek().unwrap().2.get() };
            if matches!(acquire_state, AcquireState::Queued) {
                break;
            } else {
                self.queue.pop();
            }
        }
    }

    fn wake_next(&mut self) -> LockType {
        let (lock_type, waker, acquire_state) = self.queue.pop().unwrap();
        let acquire_state = unsafe { &mut *acquire_state.get() };
        *acquire_state = AcquireState::Acquired;
        self.locked += 1;
        waker.wake();
        lock_type
    }

    fn release(&mut self) {
        // println!("releasing: {}", self.locked);
        self.locked -= 1;

        self.remove_non_queued();

        if !self.queue.is_empty() {
            self.lock_type = if self.locked == 0 {
                // println!("waking next");
                self.wake_next()
            } else {
                self.lock_type.clone()
            };

            while !self.queue.is_empty() {
                if let Some(lock_type) = self.lock_type.is_compatible(&self.queue.peek().unwrap().0)
                {
                    // println!("should never wake next here");
                    self.wake_next();
                    self.lock_type = lock_type;
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

    use anyhow::{Context, Result, anyhow};
    use tokio::{
        spawn,
        sync::Barrier,
        time::{sleep, timeout},
    };

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
            let _gaurd = lock2.acquire(LockType::Write).await;
            write_barrier_after1.wait().await;
        }));

        let t3 = spawn(timeout(Duration::from_secs(1), async move {
            read2_barrier1.wait().await;
            let _gaurd = lock3.acquire(LockType::Read).await;
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

    #[test]
    fn check_for_spin_lock_overlap() -> Result<()> {
        let lock = Arc::new(SpinLock::new(Box::new(0)));
        let mut threads = Vec::new();

        const THREAD_COUNT: i32 = 50;
        const INC_COUNT: i32 = 1000;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            threads.push(std::thread::spawn(move || {
                let mut gaurd = lock_clone.lock();
                for i in 0..INC_COUNT {
                    let val = **gaurd;
                    std::thread::sleep(Duration::from_nanos(10));
                    **gaurd = val + 1;
                }
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|e| anyhow!("Thread panicked: {:?}", e))?;
        }

        let gaurd = lock.lock();
        assert_eq!(THREAD_COUNT * INC_COUNT, **gaurd);

        Ok(())
    }

    #[tokio::test]
    async fn check_for_concurrent_overlap() -> Result<()> {
        let ptr_usize = Box::into_raw(Box::new(0)) as usize;
        let lock = Lock::new();
        let mut futures = Vec::new();

        const THREAD_COUNT: i32 = 50;
        const INC_COUNT: i32 = 1000;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            futures.push(spawn(timeout(Duration::from_secs(100), async move {
                let _gaurd = lock_clone.acquire(LockType::Write).await;
                for i in 0..INC_COUNT {
                    // println!("{}", i);
                    unsafe {
                        let ptr = ptr_usize as *mut i32;
                        let val = *ptr;
                        sleep(Duration::from_nanos(1)).await;
                        let ptr = ptr_usize as *mut i32;
                        *ptr = val + 1;
                    }
                }
            })));
        }

        for future in futures {
            future.await??;
        }

        unsafe {
            let ptr = ptr_usize as *mut i32;
            assert_eq!(THREAD_COUNT * INC_COUNT, *ptr);
            drop(Box::from_raw(ptr));
        }

        Ok(())
    }
}
