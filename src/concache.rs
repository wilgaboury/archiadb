use std::{hash::Hash, ops::Deref, ptr::NonNull, sync::Arc};

use dashmap::DashMap;

/// Can be thought of like a keyed Arc. Values are retained in cache so long as there is
/// at least one reference to it, and removed when no longer referenced. The effect is that
/// for a given key, there is always exactly one value that all concurrent operation reference.
pub struct ConCache<K, V>
where
    K: Hash + Eq + Clone,
{
    create: Box<dyn Fn() -> V>,
    maps: Arc<Maps<K, V>>,
}

struct Maps<K, V>
where
    K: Hash + Eq + Clone,
{
    counts: DashMap<K, u64>,
    values: DashMap<K, Carc<K, V>>,
}

impl<K, V> ConCache<K, V>
where
    K: Hash + Eq + Clone,
{
    pub fn new(create: Box<dyn Fn() -> V>) -> Self {
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
        inner.maps.counts.alter(&inner.key, |_, c| c + 1);
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
                if let Some(mut v) = inner.maps.counts.get_mut(&k) {
                    *v -= 1;
                    *v == 0
                } else {
                    eprintln!("Warning: ConCache count missing for during drop");
                    false
                }
            })
            .is_some();

        if removed {
            inner.maps.counts.remove_if(&inner.key, |_, v| *v == 0);
            let owned = unsafe { Box::from_raw(self.inner.as_ptr()) };
            drop(owned);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn single_key_lifecycle() {
        let create_count = Arc::new(AtomicUsize::new(0));
        let cache = {
            let create_count = create_count.clone();
            ConCache::new(Box::new(move || {
                create_count.fetch_add(1, Ordering::SeqCst);
                100
            }))
        };

        let key = "test";

        let h1 = cache.get(key);
        assert_eq!(*h1, 100);
        assert_eq!(create_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(1));

        let h2 = cache.get(key);
        assert_eq!(*h2, 100);
        assert_eq!(create_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(2));

        let h3 = h2.clone();
        assert_eq!(*h3, 100);
        assert_eq!(create_count.load(Ordering::SeqCst), 1);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(3));

        drop(h1);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(2));
        drop(h2);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(1));
        drop(h3);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), None);

        let h4 = cache.get(key);
        assert_eq!(*h4, 100);
        assert_eq!(create_count.load(Ordering::SeqCst), 2);
        assert_eq!(cache.maps.counts.get(&key).map(|v| *v), Some(1));
    }

    // #[test]
    // fn two_keys_independent() {
    //     let create_count = AtomicUsize::new(0);
    //     let cache = ConCache::new(|| {
    //         create_count.fetch_add(1, Ordering::SeqCst);
    //         String::from("value")
    //     });

    //     let key_a = "a";
    //     let key_b = "b";

    //     // Get two different keys
    //     let a1 = cache.get(key_a);
    //     let b1 = cache.get(key_b);
    //     assert_eq!(*a1, "value");
    //     assert_eq!(*b1, "value");
    //     assert_eq!(create_count.load(Ordering::SeqCst), 2);
    //     assert_ne!(&*a1 as *const String, &*b1 as *const String);

    //     // Clone handle for key A
    //     let a2 = a1.clone();
    //     // Drop both handles for key A
    //     drop(a1);
    //     drop(a2);
    //     // Key B should still be alive and untouched
    //     let b2 = cache.get(key_b);
    //     assert_eq!(*b2, "value");
    //     assert_eq!(create_count.load(Ordering::SeqCst), 2); // no new creation

    //     // Get key A again -> must recreate its value
    //     let a3 = cache.get(key_a);
    //     assert_eq!(*a3, "value");
    //     assert_eq!(create_count.load(Ordering::SeqCst), 3);

    //     // Also verify that after last drop of key A, its entry is gone (new pointer)
    //     let ptr_a_old = &*a3 as *const String;
    //     // drop a3 to force cleanup, then get again for fresh pointer
    //     drop(a3);
    //     let a4 = cache.get(key_a);
    //     assert_eq!(create_count.load(Ordering::SeqCst), 4);
    //     assert_ne!(&*a4 as *const String, ptr_a_old);
    // }
}
