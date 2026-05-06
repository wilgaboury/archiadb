use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    native_endian::{U32, U64},
};

use crate::{const_assert, fio::MIN_PAGE_SIZE, util::CHECKSUM_SIZE};

const MAX_KEY_SIZE: usize = 256;

#[derive(TryFromBytes, Unaligned, IntoBytes, Debug, KnownLayout, Immutable, Clone)]
#[repr(u8)]
enum PageKind {
    BtreeRoot,
    BtreeInner,
    BtreeLeaf,
}

#[derive(TryFromBytes, Unaligned, IntoBytes, Debug, KnownLayout, Immutable, Clone)]
#[repr(u8)]
enum DataKind {
    Btree,
    ValueEmbedded,
    ValueLinkedList,
}

#[derive(Unaligned, FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Clone)]
#[repr(C, packed)]
struct BtreeRootHeader {
    version: U64,
    parent: U64,
    sibling: U64,
    len: U32,
}

/// Inner node layout:
/// - header
/// - slots:[u32; len], key_len is derivable
/// - data: [u8], grows backward, interleaved child pointer and key
/// - checksum: u32

#[derive(Unaligned, FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Clone)]
#[repr(C, packed)]
struct BtreeHeader {
    parent: U64,
    len: U32,
}

const_assert!(size_of::<BtreeRootHeader>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

/// Leaf node layout:
/// - header
/// - slots: [u32; len]
/// - data: [u8], grows backward, interleaved (key_len, key, value)
/// - checksum: u32

#[derive(Unaligned, FromBytes, IntoBytes, Debug, KnownLayout, Immutable, Clone)]
#[repr(C, packed)]
struct BtreeLeafSlot {
    loc: U32,
    key_len: U32,
    data_len: U32,
}

#[test]
fn test_access() {
    let buffer = vec![0u8; 4096];
    let slice = &buffer[1..];
    let (header, _) = BtreeRootHeader::read_from_prefix(slice).unwrap();
    assert_eq!(header.version, 0);
    assert_eq!(header.parent, 0);
    assert_eq!(header.sibling, 0);
    assert_eq!(header.len, 0);
}
