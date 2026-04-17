// TODO: this was good brainstorming but bad for my use case of reducing dynamic allocations
// for shared reference counted objects

use std::{cell::RefCell, sync::Arc};

struct LocalPool<T> {
    objects: RefCell<Vec<T>>,
}

impl<T> LocalPool<T> {
    pub fn new(size: usize) -> Self {
        Self {
            objects: RefCell::new(Vec::with_capacity(size)),
        }
    }

    pub fn get(&self) -> Option<T> {
        (*self.objects.borrow_mut()).pop()
    }

    pub fn add(&self, object: T) {
        let objects = &mut *self.objects.borrow_mut();
        if objects.len() < objects.capacity() {
            objects.push(object);
        }
    }
}

#[derive(Clone)]
struct LocalPoolForSomeNums {
    data: Arc<u64>,
}

impl Drop for LocalPoolForSomeNums {
    fn drop(&mut self) {
        // it may be possible that arcs are occationally leaked, but it is unlikely and doesn't really matter as long as it is not common
        if Arc::strong_count(&self.data) == 1 {
            SOME_NUMS.with(|nums| nums.add(self.data.clone()));
        }
    }
}

thread_local! {
    static SOME_NUMS: LocalPool<Arc<u64>> = LocalPool::new(64);
}
