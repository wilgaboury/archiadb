use std::{
    cell::UnsafeCell, pin::Pin, ptr, sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    }, task::{Context, Poll, Waker}
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
    Read = 0,
    ReadChildWrite,
    ReadRecursive,
    Write,
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

impl Lock {
    fn acquire(&self, lock_type: LockType) -> LockFuture<'_> {
        LockFuture { lock: &self, lock_type }
    }
}

struct LockGuard<'a> {
    lock: &'a Lock
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        let mut gaurd = self.lock.ptr.lock();
        gaurd.release();
    }
}

struct LockFuture<'a> {
    lock: &'a Lock,
    lock_type: LockType
}

impl <'a> Future for LockFuture<'a> {
    type Output = LockGuard<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut gaurd = self.lock.ptr.lock();
        if gaurd.acquire(self.lock_type, &cx.waker()) {
            Poll::Ready(LockGuard { lock: &self.lock })
        } else {
            Poll::Pending
        }
    }
}

struct LockInternal {
    lock_type: LockType,
    locked: u32,
    queue: SimpleQueue<(LockType, Waker)>,
}

impl LockInternal {
    fn acquire(&mut self, acq_lock_type: LockType, waker: &Waker) -> bool {
        if !self.queue.is_empty() {
            self.queue.push((acq_lock_type, waker.clone()));
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
                self.queue.push((acq_lock_type, waker.clone()));
                false
            }
        }
    }

    fn release(&mut self) {
        self.locked -= 1;

        if !self.queue.is_empty() {
            self.lock_type = if self.locked == 0 {
                self.locked += 1;
                let (lock_type, waker) = self.queue.pop().unwrap();
                waker.wake();
                lock_type
            } else {
                self.lock_type.clone()
            };

            while !self.queue.is_empty() {
                if let Some(lock_type) = self.lock_type.is_compatible(&self.queue.peek().unwrap().0) {
                    self.lock_type = lock_type;
                    self.locked += 1
                } else {
                    break;
                }
            }
        } else if self.locked == 0 {
            // TODO: remove from lock cache
        }
    }
}
