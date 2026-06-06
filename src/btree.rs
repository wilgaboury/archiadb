use crate::{const_assert, fio::MIN_PAGE_SIZE, util::CHECKSUM_SIZE};

const MAX_KEY_SIZE: usize = 256;

#[repr(u8)]
enum PageKind {
    BtreeRoot,
    BtreeInner,
    BtreeLeaf,
}

#[repr(u8)]
enum DataKind {
    Btree,
    ValueEmbedded,
    ValueLinkedList,
}

#[repr(C, packed)]
struct BtreeRootHeader {
    version: u64,
    parent: u64,
    sibling: u64,
    len: u32,
}

impl BtreeRootHeader {
    fn read_from_buf(buf: &[u8]) -> &BtreeRootHeader {
        unsafe { &*(buf.as_ptr() as *const BtreeRootHeader) }
    }
}

/// Inner node layout:
/// - header
/// - slots:[u32; len], key_len is derivable
/// - data: [u8], grows backward, interleaved child pointer and key
/// - checksum: u32

#[repr(C, packed)]
struct BtreeHeader {
    parent: u64,
    len: u32,
}

const_assert!(size_of::<BtreeRootHeader>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

/// Leaf node layout:
/// - header
/// - slots: [u32; len]
/// - data: [u8], grows backward, interleaved key and value
/// - checksum: u32

#[repr(C, packed)]
struct BtreeLeafSlot {
    loc: u32,
    key_len: u32,
    data_len: u32,
}

#[test]
fn test_access() {
    let buffer = vec![0u8; 4096];
    let slice = &buffer[1..];
    let header = BtreeRootHeader::read_from_buf(slice);
    assert_eq!({ header.version }, 0);
    assert_eq!({ header.parent }, 0);
    assert_eq!({ header.sibling }, 0);
    assert_eq!({ header.len }, 0);
}
