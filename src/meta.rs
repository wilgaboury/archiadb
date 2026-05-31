use std::path::Path;

use anyhow::Result;
use tokio::sync::Mutex;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    native_endian::{U64, U128},
};

use crate::{const_assert, fio::MIN_PAGE_SIZE, util::CHECKSUM_SIZE};

const MAGIC: u128 = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
const FMT_VERSION: u64 = 0;
const NUM_HEADER_PAGES: u64 = 2;

#[derive(FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Unaligned, Clone)]
#[repr(C)]
pub struct Meta {
    magic: U128,

    fmt_version: U64,
    page_size: U64,
    root1: U64,
    root2: U64,

    version: U64,
    open: u8,
    len: U64,
}

const_assert!(size_of::<Meta>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

pub struct MetaHandler {
    fmt_version: U64,
    page_size: U64,
    root1: U64,
    root2: U64,

    inner: Mutex<Inner>,
}

struct Inner {
    version: U64,
    idx: usize,
    front: Vec<u8>,
    back: Vec<u8>,
}

impl MetaHandler {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // read from disk or init
        todo!()
    }
}
