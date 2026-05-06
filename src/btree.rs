use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    native_endian::{U32, U64},
};

use crate::{const_assert, fio::MIN_PAGE_SIZE};

#[derive(FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Unaligned, Clone)]
#[repr(C)]
struct BtreeRootHeader {
    version: U64,
    parent1: U64,
    parent2: U64,
    len: U32,
}

const_assert!(size_of::<BtreeRootHeader>() + size_of::<u32>() < MIN_PAGE_SIZE as usize);
