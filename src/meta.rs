use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    native_endian::{U64, U128},
};

use crate::{const_assert, fio::MIN_PAGE_SIZE};

const MAGIC: u128 = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
const VERSION: u64 = 0;
const NUM_HEADER_PAGES: u64 = 1;

// static information at the beginning of the file
#[derive(FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Unaligned, Clone)]
#[repr(C)]
pub struct Meta {
    magic: U128,
    version: U64,
    page_size: U64,
    root1: U64,
    root2: U64,
}

const_assert!(size_of::<Meta>() + size_of::<u32>() < MIN_PAGE_SIZE as usize);
