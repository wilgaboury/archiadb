use crate::{
    btree::SearchResult::Exact,
    const_assert,
    fio::MIN_PAGE_SIZE,
    key,
    util::{CHECKSUM_SIZE, from_bytes, from_bytes_mut},
};

const MAX_KEY_SIZE: usize = 256;
const PAGE_PTR_SIZE: usize = size_of::<u64>();

/// Inner node layout:
/// - header
/// - slots:[u32; max(0,len-1)], key_len is derivable
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

fn insert_init_inner(buf: &mut [u8], ptr: u64) {
    let header = from_bytes_mut::<BTreeHeader>(buf);
    header.len += 1;
    let end = buf.len() - CHECKSUM_SIZE;
    let start = end - PAGE_PTR_SIZE;
    buf[start..end].copy_from_slice(&ptr.to_ne_bytes());
}

/// will unconditionally copy the key into the node without checking if there is space, assumes len > 1
fn insert_at_inner(buf: &[u8], key: &[u8], ptr: u64, idx: u32) {
    let header = from_bytes::<BTreeHeader>(buf);
    let slots_idx = header.kind.header_size();
    let slots_len = min_one_or_zero(header.len);
}

fn read_slot(buf: &[u8], idx: u32) -> u32 {
    let header = from_bytes::<BTreeHeader>(buf);
    let slots_idx = header.kind.header_size();
    const SLOT_SIZE: u32 = size_of::<u32>() as u32;
    let start = (slots_idx + SLOT_SIZE * idx) as usize;
    let end = start + SLOT_SIZE as usize;
    let mut u32_buf = [0u8; 4];
    u32_buf.copy_from_slice(&buf[start..end]);
    u32::from_ne_bytes(u32_buf)
}

fn get_key_inner(buf: &[u8], idx: u32) -> &[u8] {
    let key_idx = read_slot(buf, idx) as usize;
    let key_len = if idx == 0 {
        key_idx - PAGE_PTR_SIZE - CHECKSUM_SIZE
    } else {
        key_idx - read_slot(buf, idx - 1) as usize - PAGE_PTR_SIZE
    } as usize;

    &buf[key_idx..(key_idx + key_len)]
}

enum SearchResult {
    Exact(u32),
    Insert(u32),
}

fn search_inner(buf: &[u8], target: &[u8]) -> SearchResult {
    let header = from_bytes::<BTreeHeader>(buf);

    if header.len == 0 {
        return SearchResult::Insert(0);
    } else if header.len == 1 {
        return SearchResult::Insert(1);
    }

    let mut left = 0;
    let mut right = min_one_or_zero(header.len);

    while left < right {
        let mid = left + (left + right) / 2;
        let key = get_key_inner(buf, mid);
        match key.cmp(target) {
            std::cmp::Ordering::Equal => return SearchResult::Exact(mid),
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
        }
    }

    SearchResult::Exact(left)
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
