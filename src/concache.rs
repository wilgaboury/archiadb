use std::{
    ops::Deref,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use dashmap::DashMap;

struct ConCache<K, V, F>
where
    F: Fn() -> V,
{
    inner: Arc<Inner<K, V, F>>,
}

struct Inner<K, V, F>
where
    F: Fn() -> V,
{
    cache: DashMap<K, V>,
    create: F,
}

pub struct Carc<K, V, F>
where
    F: Fn() -> V,
{
    inner: NonNull<CarcInner<K, V, F>>,
}

struct CarcInner<K, V, F>
where
    F: Fn() -> V,
{
    ref_count: AtomicUsize,
    data: V,
    inner: Arc<Inner<K, V, F>>,
}

impl<K, V, F> Carc<K, V, F>
where
    F: Fn() -> V,
{
    fn new(data: V, inner: Arc<Inner<K, V, F>>) -> Self {
        let cinner = Box::new(CarcInner {
            ref_count: AtomicUsize::new(1),
            data,
            inner,
        });

        Carc {
            inner: NonNull::from(Box::leak(cinner)),
        }
    }
}

impl<K, V, F> Deref for Carc<K, V, F>
where
    F: Fn() -> V,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        let inner = unsafe { self.inner.as_ref() };
        &inner.data
    }
}

impl<K, V, F> Drop for Carc<K, V, F>
where
    F: Fn() -> V,
{
    fn drop(&mut self) {
        let inner = unsafe { self.inner.as_ref() };

        if inner.ref_count.fetch_sub(1, Ordering::Release) == 1 {
            std::sync::atomic::fence(Ordering::Acquire);
            let _ = unsafe { Box::from_raw(self.inner.as_ptr()) };
        }
    }
}
