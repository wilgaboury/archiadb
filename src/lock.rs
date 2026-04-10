use std::{
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use crate::spin::SpinLock;

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
enum LockType {
    Read = 0,       // acquire node for reading
    ReadChildWrite, // acquire node for reading with intent to modify child/descendant nodes
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

#[derive(Clone)]
struct Lock {
    inner: Arc<SpinLock<LockInner>>,
}

unsafe impl Send for Lock {}
unsafe impl Sync for Lock {}

impl Lock {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SpinLock::new(LockInner::new())),
        }
    }

    pub fn acquire(&self, lock_type: LockType) -> LockFuture {
        LockFuture::new(self.clone(), lock_type)
    }
}

enum LockFutureState {
    Init,
    Queued(Waker),
    Acquired,
    Gaurded,
}

struct LockFuture {
    lock: Lock,
    lock_type: LockType,

    // protected by spin lock
    state: LockFutureState,
    prev: Option<NonNull<LockFuture>>,
    next: Option<NonNull<LockFuture>>,

    // because this uses intrusive lists, this data cannot be moved
    _pin: PhantomPinned,
}

unsafe impl Send for LockFuture {}
unsafe impl Sync for LockFuture {}

impl LockFuture {
    fn new(lock: Lock, lock_type: LockType) -> Self {
        Self {
            lock,
            lock_type,
            state: LockFutureState::Init,
            prev: None,
            next: None,
            _pin: PhantomPinned,
        }
    }
}

impl<'a> Future for LockFuture {
    type Output = LockGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this_ptr = unsafe { self.get_unchecked_mut() } as *mut Self;
        let mut inner = unsafe { &*this_ptr }.lock.inner.lock();
        let this = unsafe { &mut *this_ptr };
        match &this.state {
            LockFutureState::Init => {
                if !inner.head.is_none() {
                    this.prev = inner.tail;
                    inner.tail = Some(unsafe { NonNull::new_unchecked(this_ptr) });
                    if let Some(mut prev) = this.prev {
                        unsafe { prev.as_mut() }.next = inner.tail;
                    }
                    this.state = LockFutureState::Queued(cx.waker().clone());
                    Poll::Pending
                } else if inner.locked == 0 {
                    inner.locked += 1;
                    this.state = LockFutureState::Gaurded;
                    Poll::Ready(LockGuard {
                        lock: this.lock.clone(),
                    })
                } else if let Some(lock_type) = this.lock_type.is_compatible(&this.lock_type) {
                    inner.locked += 1;
                    inner.lock_type = lock_type;
                    this.state = LockFutureState::Gaurded;
                    Poll::Ready(LockGuard {
                        lock: this.lock.clone(),
                    })
                } else {
                    this.prev = inner.tail;
                    inner.tail = Some(unsafe { NonNull::new_unchecked(this_ptr) });
                    if inner.head.is_none() {
                        inner.head = inner.tail;
                    }
                    this.state = LockFutureState::Queued(cx.waker().clone());
                    Poll::Pending
                }
            }
            LockFutureState::Queued(_) => Poll::Pending,
            LockFutureState::Acquired => {
                this.state = LockFutureState::Gaurded;
                Poll::Ready(LockGuard {
                    lock: this.lock.clone(),
                })
            }
            LockFutureState::Gaurded => panic!("LockFuture polled after returning ready"),
        }
    }
}

impl Drop for LockFuture {
    fn drop(&mut self) {
        let ptr = Some(unsafe { NonNull::new_unchecked(self as *mut Self) });
        let mut inner = unsafe { &*(self as *mut Self) }.lock.inner.lock();
        if let Some(mut prev) = self.prev {
            unsafe { prev.as_mut() }.next = self.next;
        }
        if let Some(mut next) = self.next {
            unsafe { next.as_mut() }.prev = self.prev;
        }
        if inner.head == ptr {
            inner.head = self.next;
        }
        if inner.tail == ptr {
            inner.tail = self.prev;
        }
        if matches!(self.state, LockFutureState::Acquired) {
            // lock was acquired but future was dropped before being polled
            inner.locked -= 1;
        }
    }
}

struct LockGuard {
    lock: Lock,
}

unsafe impl Send for LockGuard {}
unsafe impl Sync for LockGuard {}

impl Drop for LockGuard {
    fn drop(&mut self) {
        {
            // quickly decrement lock count
            let mut inner = self.lock.inner.lock();
            inner.locked -= 1;
        }

        // in a loop, first acquire spin lock then check if next future in queue can aquire lock
        loop {
            let waker: Waker = {
                let mut inner = self.lock.inner.lock();
                if let Some(mut head) = inner.head {
                    let head = unsafe { head.as_mut() };
                    let next_lock_type = if inner.locked == 0 {
                        Some(head.lock_type)
                    } else {
                        inner.lock_type.is_compatible(&head.lock_type)
                    };

                    if let Some(lock_type) = next_lock_type {
                        let waker = if let LockFutureState::Queued(waker) =
                            std::mem::replace(&mut head.state, LockFutureState::Acquired)
                        {
                            waker
                        } else {
                            panic!(
                                "a queued lock future should always be in queue state and contain a waker"
                            )
                        };
                        inner.locked += 1;
                        inner.lock_type = lock_type;
                        inner.head = head.next;
                        if let Some(mut next) = inner.head {
                            unsafe { next.as_mut() }.prev = None;
                        }
                        waker
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            };

            // wakers may be slow so perform wake outside of critical section
            waker.wake();
        }
    }
}

struct LockInner {
    lock_type: LockType,
    locked: u32,

    head: Option<NonNull<LockFuture>>,
    tail: Option<NonNull<LockFuture>>,
}

impl LockInner {
    fn new() -> Self {
        Self {
            lock_type: LockType::Read,
            locked: 0,
            head: None,
            tail: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Mutex, atomic::{AtomicU32, Ordering}},
        time::{Duration, Instant},
    };

    use anyhow::{Result, anyhow};
    use tokio::{spawn, sync::Barrier, time::timeout};

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
        let lock = Arc::new(SpinLock::new(0));
        let mut threads = Vec::new();

        const THREAD_COUNT: i32 = 10;
        const INC_COUNT: i32 = 100;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            threads.push(std::thread::spawn(move || {
                let mut gaurd = lock_clone.lock();
                for _ in 0..INC_COUNT {
                    let val = *gaurd;
                    std::thread::yield_now();
                    *gaurd = val + 1;
                }
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|e| anyhow!("Thread panicked: {:?}", e))?;
        }

        let gaurd = lock.lock();
        assert_eq!(THREAD_COUNT * INC_COUNT, *gaurd);

        Ok(())
    }

    // #[test]
    fn contentious_mutex_test() -> Result<()> {
        let lock = Arc::new(Mutex::new(0));
        let mut threads = Vec::new();

        const THREAD_COUNT: i32 = 100;
        const INC_COUNT: i32 = 1000;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            threads.push(std::thread::spawn(move || {
                let mut gaurd = lock_clone.lock().unwrap();
                for _ in 0..INC_COUNT {
                    let val = *gaurd;
                    std::thread::yield_now();
                    *gaurd = val + 1;
                }
            }));
        }

        for thread in threads {
            thread
                .join()
                .map_err(|e| anyhow!("Thread panicked: {:?}", e))?;
        }

        let gaurd = lock.lock().unwrap();
        assert_eq!(THREAD_COUNT * INC_COUNT, *gaurd);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn check_for_concurrent_overlap() -> Result<()> {
        let ptr_usize = Box::into_raw(Box::new(0)) as usize;
        let lock = Lock::new();
        let mut futures = Vec::new();

        const THREAD_COUNT: i32 = 1000;
        const INC_COUNT: i32 = 10;

        let total_acquire_time: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        for _ in 0..THREAD_COUNT {
            let lock = lock.clone();
            let total_acquire_time = total_acquire_time.clone();
            futures.push(spawn(timeout(Duration::from_secs(10), async move {
                let guard_start = Instant::now();
                let _gaurd = lock.acquire(LockType::Write).await;
                total_acquire_time
                    .fetch_add(guard_start.elapsed().as_nanos() as u32, Ordering::SeqCst);

                for _ in 0..INC_COUNT {
                    unsafe {
                        let ptr = ptr_usize as *mut i32;
                        let val = *ptr;
                        tokio::task::yield_now().await;
                        let ptr = ptr_usize as *mut i32;
                        *ptr = val + 1;
                    }
                }
            })));
        }

        for future in futures {
            future.await??;
        }

        // 68305.2564 ns, 10000 tasks, 100 increments each
        // 5 runs: 288961.9898 ns, 117933.4105 ns, 314133.4715 ns, 172989.3006 ns, 12738.2065 ns
        println!(
            "Average acquire time: {} ns",
            total_acquire_time.load(Ordering::SeqCst) as f64 / THREAD_COUNT as f64
        );

        unsafe {
            let ptr = ptr_usize as *mut i32;
            assert_eq!(THREAD_COUNT * INC_COUNT, *ptr);
            drop(Box::from_raw(ptr));
        }

        Ok(())
    }

    // #[tokio::test(flavor = "multi_thread")]
    async fn check_for_concurrent_overlap_tokio_mutex() -> Result<()> {
        let ptr_usize = Box::into_raw(Box::new(0)) as usize;
        let lock = Arc::new(tokio::sync::Mutex::new(0));
        let mut futures = Vec::new();

        const THREAD_COUNT: i32 = 1000;
        const INC_COUNT: i32 = 10;

        let total_acquire_time: Arc<AtomicU32> = Arc::new(AtomicU32::new(0));

        for _ in 0..THREAD_COUNT {
            let lock = lock.clone();
            let total_acquire_time = total_acquire_time.clone();
            futures.push(spawn(timeout(Duration::from_secs(10), async move {
                let guard_start = Instant::now();
                let _gaurd = lock.lock().await;
                total_acquire_time
                    .fetch_add(guard_start.elapsed().as_nanos() as u32, Ordering::SeqCst);

                for _ in 0..INC_COUNT {
                    unsafe {
                        let ptr = ptr_usize as *mut i32;
                        let val = *ptr;
                        tokio::task::yield_now().await;
                        let ptr = ptr_usize as *mut i32;
                        *ptr = val + 1;
                    }
                }
            })));
        }

        for future in futures {
            future.await??;
        }

        // 10000 tasks, 100 increments each
        // 5 runs: 13552.1244 ns, 277410.2668 ns, 287992.2409 ns, 13891.906 ns, 107075.1494 ns
        println!(
            "Average acquire time: {} ns",
            total_acquire_time.load(Ordering::SeqCst) as f64 / THREAD_COUNT as f64
        );

        unsafe {
            let ptr = ptr_usize as *mut i32;
            assert_eq!(THREAD_COUNT * INC_COUNT, *ptr);
            drop(Box::from_raw(ptr));
        }

        Ok(())
    }
}
