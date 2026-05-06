use crate::{alloc::PageAllocator, fio::Fio};

struct Db {
    alloc: PageAllocator,
    fio: Fio,
}
