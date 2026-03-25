use std::{
    cell::UnsafeCell, collections::VecDeque, future::Ready, marker::PhantomData, pin::Pin, ptr::{self, NonNull}, sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    }, task::{Context, Poll, Waker}
};

pub struct SpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Sync for SpinLock<T> {}
unsafe impl<T: Send> Send for SpinLock<T> {}

pub struct SpinGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> SpinLock<T> {
    pub const fn new(val: T) -> Self {
        Self { locked: AtomicBool::new(false), data: UnsafeCell::new(val) }
    }

    pub fn lock(&self) -> SpinGuard<'_, T> {
        while self.locked.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            while self.locked.load(Ordering::Relaxed) {
                core::hint::spin_loop();
            }
        }
        SpinGuard { lock: self }
    }
}

impl<T> core::ops::Deref for SpinGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T { unsafe { &*self.lock.data.get() } }
}

impl<T> core::ops::DerefMut for SpinGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T { unsafe { &mut *self.lock.data.get() } }
}

impl<T> Drop for SpinGuard<'_, T> {
    fn drop(&mut self) { self.lock.locked.store(false, Ordering::Release); }
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
        LockFuture {
            lock: self.clone(),
            lock_type,
            
            state: LockFutureState::Init,
            prev: None,
            next: None
        }
    }
}

enum LockFutureState {
     Init,
     Queued(Waker),
     Acquired,
     Gaurded
}

struct LockFuture {
    lock: Lock,
    lock_type: LockType,

    // protected by spin lock
    state: LockFutureState,
    prev: Option<NonNull<LockFuture>>,
    next: Option<NonNull<LockFuture>>
}

unsafe impl Send for LockFuture {}
unsafe impl Sync for LockFuture {}

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
                } else if !inner.locked {
                    inner.locked = true;
                    this.state = LockFutureState::Gaurded;
                    Poll::Ready(LockGuard { lock: this.lock.clone() })
                } else if let Some(lock_type) = this.lock_type.is_compatible(&this.lock_type) {
                    inner.locked = true;
                    inner.lock_type = lock_type;
                    this.state = LockFutureState::Gaurded;
                    Poll::Ready(LockGuard { lock: this.lock.clone() })
                } else {
                    this.prev = inner.tail;
                    inner.tail = Some(unsafe { NonNull::new_unchecked(this_ptr) });
                    if let Some(mut prev) = this.prev {
                        unsafe { prev.as_mut() }.next = inner.tail;
                    }
                    this.state = LockFutureState::Queued(cx.waker().clone());
                    Poll::Pending
                }
            },
            LockFutureState::Queued(_) => Poll::Pending,
            LockFutureState::Acquired => {
                inner.locked = true;
                this.state = LockFutureState::Gaurded;
                Poll::Ready(LockGuard { lock: this.lock.clone() })
            },
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
    }
}

struct LockGuard {
    lock: Lock
}

unsafe impl Send for LockGuard {}
unsafe impl Sync for LockGuard {}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // TODO!
        panic!("implement")
    }
}

struct LockInner {
    lock_type: LockType,
    locked: bool,
    
    head: Option<NonNull<LockFuture>>,
    tail: Option<NonNull<LockFuture>>
}

impl LockInner {
    fn new() -> Self {
        Self {
            lock_type: LockType::Read,
            locked: false,
            head: None,
            tail: None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Mutex, time::{Duration, Instant}};

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
        let lock = Arc::new(SpinLock::new(0));
        let mut threads = Vec::new();

        const THREAD_COUNT: i32 = 50;
        const INC_COUNT: i32 = 1000;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            threads.push(std::thread::spawn(move || {
                let mut gaurd = lock_clone.lock();
                for _ in 0..INC_COUNT {
                    let val = *gaurd;
                    std::thread::sleep(Duration::from_nanos(10));
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

    #[test]
    fn contentious_mutex_test() -> Result<()> {
        let lock = Arc::new(Mutex::new(0));
        let mut threads = Vec::new();

        const THREAD_COUNT: i32 = 100;
        const INC_COUNT: i32 = 10000;

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

        const THREAD_COUNT: i32 = 100;
        const INC_COUNT: i32 = 10000;

        for _ in 0..THREAD_COUNT {
            let lock_clone = lock.clone();
            futures.push(spawn(timeout(Duration::from_secs(100), async move {
                // let guard_start = Instant::now();
                let _gaurd = lock_clone.acquire(LockType::Write).await;
                // let acquire_time = guard_start.elapsed();
                // println!("Acquire took: {:?}", acquire_time);

                // let loop_start = Instant::now();
                for i in 0..INC_COUNT {
                    // println!("{}", i);
                    unsafe {
                        let ptr = ptr_usize as *mut i32;
                        let val = *ptr;
                        // sleep(Duration::from_nanos(1)).await; // cannot handle short sleeps minimum of 1ms
                        tokio::task::yield_now().await;
                        let ptr = ptr_usize as *mut i32;
                        *ptr = val + 1;
                    }
                }
                // let loop_time = loop_start.elapsed();
                // println!("Loop took: {:?}", loop_time);
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
