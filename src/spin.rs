use std::{
    cell::UnsafeCell,
    sync::atomic::{AtomicBool, Ordering},
};

#[deprecated]
pub struct SpinLock<T> {
    locked: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Sync for SpinLock<T> {}
unsafe impl<T: Send> Send for SpinLock<T> {}

pub struct SpinLockGaurd<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> SpinLock<T> {
    pub fn new(val: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            data: UnsafeCell::new(val),
        }
    }

    #[must_use]
    pub fn lock(&self) -> SpinLockGaurd<'_, T> {
        while self
            .locked
            .compare_exchange_weak(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            // perform relaxed read in loop to avoid thrashing cache lines with compare_exchange
            while self.locked.load(Ordering::Relaxed) {
                core::hint::spin_loop();
            }
        }
        SpinLockGaurd { lock: self }
    }
}

impl<T> core::ops::Deref for SpinLockGaurd<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> core::ops::DerefMut for SpinLockGaurd<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for SpinLockGaurd<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

// #[test]
// fn check_for_spin_lock_overlap() -> Result<()> {
//     let lock = Arc::new(Mutex::new(0));
//     let mut threads = Vec::new();

//     const THREAD_COUNT: i32 = 10;
//     const INC_COUNT: i32 = 100;

//     for _ in 0..THREAD_COUNT {
//         let lock_clone = lock.clone();
//         threads.push(std::thread::spawn(move || {
//             let mut gaurd = lock_clone.lock();
//             for _ in 0..INC_COUNT {
//                 let val = *gaurd;
//                 std::thread::yield_now();
//                 *gaurd = val + 1;
//             }
//         }));
//     }

//     for thread in threads {
//         thread
//             .join()
//             .map_err(|e| anyhow!("Thread panicked: {:?}", e))?;
//     }

//     let gaurd = lock.lock();
//     assert_eq!(THREAD_COUNT * INC_COUNT, *gaurd);

//     Ok(())
// }
