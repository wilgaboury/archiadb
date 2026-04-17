// intrusive list lock futures with spin lock to protect list is honestly a load of crap
// A better scheme is to use crossbeam SegQueue in combination with a lock per shared state

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

    fn push(&self, list: &Self::List) {
        let tail = *list.tail();
        *self.prev() = tail;
        *list.tail() = self as *const Self as *mut Self;
        if tail.is_null() {
            *list.head() = *list.tail();
        } else {
            *(unsafe { &*tail }.next()) = *list.tail();
        }
    }

    fn remove(&self, list: &Self::List) {
        if !self.prev().is_null() {
            *(unsafe { (**self.prev()).next() }) = *self.next();
        }
        if !self.next().is_null() {
            *(unsafe { (**self.next()).prev() }) = *self.prev();
        }

        let this = self as *const Self as *mut Self;
        if *list.head() == this {
            *list.head() = *self.next();
        }
        if *list.tail() == this {
            *list.tail() = *self.prev();
        }

        *self.prev() = ptr::null_mut();
        *self.next() = ptr::null_mut();
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;

    #[derive(Debug)]
    struct TestList {
        head: *mut TestNode,
        tail: *mut TestNode,
    }

    impl IntrusiveList for TestList {
        type Node = TestNode;

        #[allow(invalid_reference_casting)]
        fn head(&self) -> &mut *mut Self::Node {
            let this = unsafe { &mut *(self as *const Self as *mut Self) };
            &mut this.head
        }

        #[allow(invalid_reference_casting)]
        fn tail(&self) -> &mut *mut Self::Node {
            let this = unsafe { &mut *(self as *const Self as *mut Self) };
            &mut this.tail
        }
    }

    #[derive(Debug)]
    struct TestNode {
        num: u64,
        prev: *mut TestNode,
        next: *mut TestNode,
        list: Rc<TestList>,
    }

    impl IntrusiveListNode for TestNode {
        type List = TestList;

        #[allow(invalid_reference_casting)]
        fn prev(&self) -> &mut *mut Self {
            let this = unsafe { &mut *(self as *const Self as *mut Self) };
            &mut this.prev
        }

        #[allow(invalid_reference_casting)]
        fn next(&self) -> &mut *mut Self {
            let this = unsafe { &mut *(self as *const Self as *mut Self) };
            &mut this.next
        }
    }

    #[test]
    pub fn test_operations() {
        let list = Rc::new(TestList {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
        });
        let n1 = TestNode {
            num: 1,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            list: list.clone(),
        };
        n1.push(&list);
        assert!(ptr::eq(list.head, &n1));
        assert!(ptr::eq(list.tail, &n1));

        let n2 = TestNode {
            num: 2,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            list: list.clone(),
        };
        n2.push(&list);

        assert!(ptr::eq(list.head, &n1));
        assert!(ptr::eq(list.tail, &n2));

        assert!(n1.prev.is_null());
        assert!(ptr::eq(n1.next, &n2));
        assert!(ptr::eq(n2.prev, &n1));
        assert!(n2.next.is_null());

        let n3 = TestNode {
            num: 3,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            list: list.clone(),
        };
        n3.push(&list);

        assert!(ptr::eq(list.head, &n1));
        assert!(ptr::eq(list.tail, &n3));

        assert!(n1.prev.is_null());
        assert!(ptr::eq(n1.next, &n2));
        assert!(ptr::eq(n2.prev, &n1));
        assert!(ptr::eq(n2.next, &n3));
        assert!(ptr::eq(n3.prev, &n2));
        assert!(n3.next.is_null());

        n2.remove(&list);

        assert!(ptr::eq(list.head, &n1));
        assert!(ptr::eq(list.tail, &n3));

        assert!(n1.prev.is_null());
        assert!(ptr::eq(n1.next, &n3));
        assert!(ptr::eq(n3.prev, &n1));
        assert!(n3.next.is_null());

        let n4 = TestNode {
            num: 4,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            list: list.clone(),
        };

        n4.push(&list);

        let n5 = TestNode {
            num: 5,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            list: list.clone(),
        };

        n5.push(&list);

        n1.remove(&list);

        assert!(ptr::eq(list.head, &n3));
        assert!(ptr::eq(list.tail, &n5));

        assert!(n3.prev.is_null());
        assert!(ptr::eq(n3.next, &n4));
        assert!(ptr::eq(n4.prev, &n3));
        assert!(ptr::eq(n4.next, &n5));
        assert!(ptr::eq(n5.prev, &n4));
        assert!(n5.next.is_null());

        n5.remove(&list);

        assert!(ptr::eq(list.head, &n3));
        assert!(ptr::eq(list.tail, &n4));

        assert!(n3.prev.is_null());
        assert!(ptr::eq(n3.next, &n4));
        assert!(ptr::eq(n4.prev, &n3));
        assert!(n4.next.is_null());

        let pop1 = list.pop();

        assert!(matches!(pop1, Some(_)));
        assert!(ptr::eq(pop1.unwrap(), &n3));

        assert!(ptr::eq(list.head, &n4));
        assert!(ptr::eq(list.tail, &n4));

        assert!(n4.prev.is_null());
        assert!(n4.next.is_null());

        let pop2 = list.pop();

        assert!(matches!(pop2, Some(_)));
        assert!(ptr::eq(pop2.unwrap(), &n4));

        assert!(list.head.is_null());
        assert!(list.tail.is_null());
    }
}
