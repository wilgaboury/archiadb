use std::{hash::Hash, ops::Deref, ptr::NonNull, sync::Arc};

use dashmap::DashMap;

/// Can be thought of like a keyed Arc. Values are retained in cache so long as there is
/// at least one reference to it, and removed when no longer referenced. The effect is that
/// for a given key, there is always exactly one value that all concurrent operation reference.
struct ConCache<K, V, F>
where
    K: Hash + Eq + Clone,
    F: Fn() -> V,
{
    create: F,
    maps: Arc<Maps<K, V>>,
}

struct Maps<K, V>
where
    K: Hash + Eq + Clone,
{
    counts: DashMap<K, u64>,
    values: DashMap<K, Carc<K, V>>,
}

impl<K, V, F> ConCache<K, V, F>
where
    K: Hash + Eq + Clone,
    F: Fn() -> V,
{
    pub fn new(create: F) -> Self {
        Self {
            create,
            maps: Arc::new(Maps {
                counts: DashMap::new(),
                values: DashMap::new(),
            }),
        }
    }

    pub fn get(&self, key: K) -> Carc<K, V>
    where
        K: Hash + Eq + Clone,
    {
        self.maps
            .counts
            .entry(key.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);
        self.maps
            .values
            .entry(key.clone())
            .or_insert_with(|| {
                let value = (self.create)();
                Carc {
                    inner: NonNull::new(Box::into_raw(Box::new(CarcInner {
                        key: key,
                        data: value,
                        maps: self.maps.clone(),
                    })))
                    .unwrap(),
                }
            })
            .raw_clone()
    }
}

pub struct Carc<K, V>
where
    K: Hash + Eq + Clone,
{
    inner: NonNull<CarcInner<K, V>>,
}

impl<K, V> Carc<K, V>
where
    K: Hash + Eq + Clone,
{
    // private clone function which is just a raw pointer copy
    fn raw_clone(&self) -> Self {
        Self { inner: self.inner }
    }
}

impl<K, V> Clone for Carc<K, V>
where
    K: Hash + Eq + Clone,
{
    fn clone(&self) -> Self {
        let inner = unsafe { self.inner.as_ref() };
        inner
            .maps
            .counts
            .entry(inner.key.clone())
            .and_modify(|c| *c += 1);
        self.raw_clone()
    }
}

struct CarcInner<K, V>
where
    K: Hash + Eq + Clone,
{
    key: K,
    data: V,
    maps: Arc<Maps<K, V>>,
}

impl<K, V> Deref for Carc<K, V>
where
    K: Hash + Eq + Clone,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        let inner = unsafe { self.inner.as_ref() };
        &inner.data
    }
}

impl<K, V> Drop for Carc<K, V>
where
    K: Hash + Eq + Clone,
{
    fn drop(&mut self) {
        let inner = unsafe { self.inner.as_ref() };

        let removed = inner
            .maps
            .values
            .remove_if(&inner.key, |k, _| {
                inner
                    .maps
                    .counts
                    .get(&k)
                    .map(|r| *r.value() == 1)
                    .unwrap_or(false)
            })
            .is_some();

        if removed {
            inner.maps.counts.remove_if(&inner.key, |_, v| *v == 1);
            let owned = unsafe { Box::from_raw(self.inner.as_ptr()) };
            drop(owned);
        }
    }
}
