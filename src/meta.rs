use anyhow::{Context, Result};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    native_endian::{U32, U64, U128},
};

use crate::swapper::ByteSliceConvert;

const MAGIC: u128 = 0xa90e3b4b1b0833499933888e3933af0d; // Random GUID
const VERSION: u64 = 0;

#[derive(FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Unaligned, Clone)]
#[repr(C)]
struct Meta {
    magic: U128,
    version: U64,
    block_size: U64,
    generation: U64,
    root: U64,
    checksum: U32,
}

impl Meta {
    fn new_with_block_size(block_size: u64) -> Self {
        let mut res = Self {
            magic: MAGIC.into(),
            version: VERSION.into(),
            block_size: block_size.into(),
            generation: 0.into(),
            root: 0.into(),
            checksum: 0.into(),
        };
        res.update_checksum();
        res
    }

    fn update_checksum(&mut self) {
        let bytes = self.as_bytes();
        self.checksum = crc32c::crc32c(&bytes[..bytes.len() - size_of::<u32>()]).into();
    }

    fn validate_checksum(&self) -> bool {
        let bytes = self.as_bytes();
        self.checksum.get() == crc32c::crc32c(&bytes[..bytes.len() - size_of::<u32>()])
    }
}

impl ByteSliceConvert for Meta {
    fn try_from_bytes(bytes: &[u8]) -> Result<Self> {
        Meta::read_from_bytes(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to read meta from bytes: {:?}", e))
    }

    fn try_into_bytes(&self) -> Result<&[u8]> {
        Ok(self.as_bytes())
    }
}
