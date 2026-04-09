// Utility trait for unsafe intrusive lists. This is a perfect data structure for future waker queues, because they have
// constant time insertion/deletion and do not require any additional dynamic allocations. Modification operations are
// just a bit of pointer manipulation.

// TODO: finish this jawn

use std::ptr;

pub(crate) trait IntrusiveList {
    type Node: IntrusiveListNode;

    fn head(&self) -> &mut *mut Self::Node;
    fn tail(&self) -> &mut *mut Self::Node;

    fn peek(&self) -> Option<*mut Self::Node> {
        if self.head().is_null() {
            None
        } else {
            Some(*self.head())
        }
    }

    fn pop(&self) -> Option<*mut Self::Node> {
        if self.head().is_null() {
            None
        } else {
            let ret = *self.head();

            *self.head() = *(unsafe { &*ret }.next());
            if (*self.head()).is_null() {
                *self.tail() = ptr::null_mut();
            } else {
                *(unsafe { &*(*self.head()) }.prev()) = ptr::null_mut();
            }

            Some(ret)
        }
    }
}

pub(crate) trait IntrusiveListNode: Sized {
    type List: IntrusiveList<Node = Self>;

    fn prev(&self) -> &mut *mut Self;
    fn next(&self) -> &mut *mut Self;
    fn list(&self) -> Self::List;

    fn push(&self) {
        let tail = *self.list().tail();
        *self.list().tail() = self as *const Self as *mut Self;
        if tail.is_null() {
            *self.list().head() = *self.list().tail();
        } else {
            *(unsafe { &*tail }.next()) = *self.list().tail();
        }
    }

    fn remove(&self) {}
}
