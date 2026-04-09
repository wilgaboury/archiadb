use anyhow::Result;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    native_endian::{U32, U64, U128},
};

const MAGIC: u128 = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
const VERSION: u64 = 0;

#[derive(FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Unaligned, Clone)]
#[repr(C)]
pub struct Meta {
    magic: U128,
    version: U64,
    block_size: U64,
    root1: U64,
    root2: U64,
    checksum: U32,
}

impl Meta {
    pub fn new(block_size: u64) -> Self {
        let mut res = Self {
            magic: MAGIC.into(),
            version: VERSION.into(),
            block_size: block_size.into(),
            root1: 0.into(),
            root2: 1.into(),
            checksum: 0.into(),
        };
        res.update_checksum();
        res
    }

    pub fn update_checksum(&mut self) {
        let bytes = self.as_bytes();
        self.checksum = crc32c::crc32c(&bytes[..bytes.len() - size_of::<u32>()]).into();
    }

    fn validate_checksum(&self) -> bool {
        let bytes = self.as_bytes();
        self.checksum.get() == crc32c::crc32c(&bytes[..bytes.len() - size_of::<u32>()])
    }
}
