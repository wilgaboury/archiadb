use crate::{
    const_assert,
    fio::MIN_PAGE_SIZE,
    util::{CHECKSUM_SIZE, from_bytes},
};

const MAX_KEY_SIZE: usize = 256;

/// Inner node layout:
/// - header
/// - slots:[u32; len], key_len is derivable
/// - data: [u8], grows backward, interleaved child pointer and key
/// - checksum: u32
///
/// Leaf node layout:
/// - header
/// - slots: [u32; len]
/// - data: [u8], grows backward, interleaved key and value
/// - checksum: u32

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone)]
enum NodeKind {
    BTreeRoot = 0,
    BTreeInner,
    BTreeLeaf,
}

impl From<u8> for NodeKind {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeKind::BTreeRoot,
            1 => NodeKind::BTreeInner,
            2 => NodeKind::BTreeLeaf,
            _ => panic!("invalid discriminant"),
        }
    }
}

impl NodeKind {
    pub fn header_size(&self) -> u32 {
        match self {
            NodeKind::BTreeRoot => size_of::<BTreeRootHeader>() as u32,
            NodeKind::BTreeInner | NodeKind::BTreeLeaf => size_of::<BTreeHeader>() as u32,
        }
    }
}

#[repr(u8)]
#[derive(PartialEq)]
enum DataKind {
    Btree,
    ValueEmbedded,
    ValueLinkedList,
}

#[repr(C, packed)]
struct BTreeHeader {
    kind: NodeKind,
    parent: u64,
    len: u32,
}

#[repr(C, packed)]
struct BTreeRootHeader {
    header: BTreeHeader,
    version: u64,
    sibling: u64,
}

const_assert!(size_of::<BTreeRootHeader>() + CHECKSUM_SIZE < MIN_PAGE_SIZE as usize);

#[repr(C, packed)]
struct BTreeLeafSlot {
    loc: u32,
    key_len: u32,
    data_len: u32,
}

fn min_one_or_zero(val: u32) -> u32 {
    if val > 0 { val - 1 } else { 0 }
}

fn available(buf: &[u8]) -> u32 {
    let header = from_bytes::<BTreeHeader>(buf);
    (buf.len() as u32) - header.kind.header_size()
}

fn remaining(buf: &[u8]) -> u32 {
    let header = from_bytes::<BTreeHeader>(buf);
    (buf.len() as u32)
        - (header.kind.header_size()
            + min_one_or_zero(header.len) * (size_of::<u32>() as u32)
            + (header.len * (size_of::<u64>() as u32)))
}

// only for root and inner
fn insert_at(buf: &[u8], key: &[u8], idx: u32) {
    let header = from_bytes::<BTreeHeader>(buf);
    let slots_idx = header.kind.header_size();
}

#[test]
fn test_access() {
    let buffer = vec![0u8; 4096];
    let slice = &buffer[1..];
    let header = from_bytes::<BTreeRootHeader>(slice);
    assert_eq!({ header.header.kind.clone() }, NodeKind::BTreeRoot);
    assert_eq!({ header.version }, 0);
    assert_eq!({ header.header.parent }, 0);
    assert_eq!({ header.sibling }, 0);
    assert_eq!({ header.header.len }, 0);
}
