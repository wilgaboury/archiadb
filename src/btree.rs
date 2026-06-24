use crate::{
    const_assert,
    fio::MIN_PAGE_SIZE,
    util::{CHECKSUM_SIZE, from_bytes, from_bytes_mut},
};

type Slot = u32;
type PagePtr = u64;

const MAX_KEY_SIZE: usize = 256;
const SLOT_SIZE: usize = size_of::<Slot>();
const PAGE_PTR_SIZE: usize = size_of::<PagePtr>();

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
    pub fn header_size(&self) -> usize {
        match self {
            NodeKind::BTreeRoot => size_of::<BTreeRootHeader>(),
            NodeKind::BTreeInner | NodeKind::BTreeLeaf => size_of::<BTreeHeader>(),
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

impl BTreeHeader {
    pub fn init_inner(&mut self) {
        self.kind = NodeKind::BTreeInner.into();
        self.parent = 0;
        self.len = 0;
    }
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

fn min_one_or_zero(val: usize) -> usize {
    if val > 0 { val - 1 } else { 0 }
}

fn available(buf: &[u8]) -> usize {
    let header = from_bytes::<BTreeHeader>(buf);
    buf.len() - header.kind.header_size()
}

fn remaining(buf: &[u8]) -> usize {
    let header = from_bytes::<BTreeHeader>(buf);
    let slot_len = min_one_or_zero(header.len as usize);
    let tail_size = if slot_len == 0 {
        CHECKSUM_SIZE
    } else if slot_len == 1 {
        CHECKSUM_SIZE + PAGE_PTR_SIZE
    } else {
        buf.len() - (PAGE_PTR_SIZE + read_slot(buf, slot_len - 1))
    };
    buf.len() - (header.kind.header_size() + slot_len * SLOT_SIZE + tail_size)
}

fn insert_init_inner(buf: &mut [u8], ptr: u64) {
    let header = from_bytes_mut::<BTreeHeader>(buf);
    header.len += 1;
    let end = buf.len() - CHECKSUM_SIZE;
    let start = end - PAGE_PTR_SIZE;
    buf[start..end].copy_from_slice(&ptr.to_ne_bytes());
}

/// will unconditionally copy the key into the node without checking if there is space, always inserts as ptr|key
fn insert_at_inner(buf: &mut [u8], idx: usize, ptr: u64, key: &[u8]) {
    {
        let header = from_bytes::<BTreeHeader>(buf);
        let slots_idx = header.kind.header_size();
        let slots_len = min_one_or_zero(header.len as usize);
        let slots_insert_idx = slots_idx + SLOT_SIZE * idx;
        let slots_end_idx = slots_idx + SLOT_SIZE * slots_len;

        let key_and_ptr_len = key.len() + PAGE_PTR_SIZE;
        let key_and_ptr_end = if idx == 0 {
            buf.len() - CHECKSUM_SIZE - PAGE_PTR_SIZE
        } else {
            read_slot(buf, idx - 1) - PAGE_PTR_SIZE
        };
        let key_and_ptr_start = key_and_ptr_end - key_and_ptr_len;
        let key_start = key_and_ptr_start + PAGE_PTR_SIZE;
        let all_key_and_ptr_start = if slots_len == 0 {
            buf.len() - CHECKSUM_SIZE - PAGE_PTR_SIZE
        } else {
            read_slot(buf, slots_len - 1) - PAGE_PTR_SIZE
        };

        buf.copy_within(
            all_key_and_ptr_start..key_and_ptr_end,
            all_key_and_ptr_start - key_and_ptr_len,
        );
        buf[key_and_ptr_start..key_start].copy_from_slice(&(ptr as PagePtr).to_ne_bytes());
        buf[key_start..key_and_ptr_end].copy_from_slice(key);

        for i in idx..slots_len {
            let slot_value = read_slot(buf, i);
            write_slot(buf, i, slot_value - key_and_ptr_len);
        }

        buf.copy_within(
            slots_insert_idx..slots_end_idx,
            slots_insert_idx + SLOT_SIZE,
        );
        buf[slots_insert_idx..slots_insert_idx + SLOT_SIZE]
            .copy_from_slice(&(key_start as u32).to_ne_bytes());
    }

    {
        let header = from_bytes_mut::<BTreeHeader>(buf);
        header.len += 1;
    }
}

fn read_slot(buf: &[u8], idx: usize) -> usize {
    let header = from_bytes::<BTreeHeader>(buf);
    let slots_idx = header.kind.header_size();
    let start = slots_idx + SLOT_SIZE * idx;
    let end = start + SLOT_SIZE;
    let mut u32_buf = [0u8; 4];
    u32_buf.copy_from_slice(&buf[start..end]);
    u32::from_ne_bytes(u32_buf) as usize
}

fn write_slot(buf: &mut [u8], idx: usize, value: usize) {
    let header = from_bytes::<BTreeHeader>(buf);
    let slots_idx = header.kind.header_size();
    let start = slots_idx + SLOT_SIZE * idx;
    let end = start + SLOT_SIZE;
    buf[start..end].copy_from_slice(&(value as Slot).to_ne_bytes());
}

fn get_key_inner(buf: &[u8], idx: usize) -> &[u8] {
    let key_idx = read_slot(buf, idx) as usize;
    let key_len = if idx == 0 {
        key_idx - PAGE_PTR_SIZE - CHECKSUM_SIZE
    } else {
        key_idx - read_slot(buf, idx - 1) as usize - PAGE_PTR_SIZE
    } as usize;

    &buf[key_idx..(key_idx + key_len)]
}

enum SearchResult {
    Exact(usize),
    Insert(usize),
}

fn search_inner(buf: &[u8], target: &[u8]) -> SearchResult {
    let header = from_bytes::<BTreeHeader>(buf);

    if header.len <= 1 {
        return SearchResult::Insert(0);
    }

    let mut left = 0;
    let mut right = min_one_or_zero(header.len as usize);

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

#[cfg(test)]
mod tests {
    use crate::{
        btree::{BTreeHeader, insert_at_inner, insert_init_inner},
        util::from_bytes_mut,
    };

    #[test]
    fn inner_node_insert_test_1() {
        let mut node = [0u8; 64];
        {
            let header = from_bytes_mut::<BTreeHeader>(&mut node);
            header.init_inner();
        }

        insert_init_inner(&mut node, 1);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 1, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 1, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        insert_at_inner(&mut node, 0, 2, b"a");

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 2, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 1 */ 1, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        insert_at_inner(&mut node, 0, 3, b"b");

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'a',
                /* ptr 1 */ 3, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'b', /* ptr 2 */ 1,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn inner_node_insert_test_2() {
        let mut node = [0u8; 64];
        {
            let header = from_bytes_mut::<BTreeHeader>(&mut node);
            header.init_inner();
        }

        insert_init_inner(&mut node, 1);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 1, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 1, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        insert_at_inner(&mut node, 0, 2, b"a");

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 2, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 1 */ 1, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        insert_at_inner(&mut node, 1, 3, b"b");

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 0 */ 3, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'b',
                /* ptr 1 */ 2, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'a', /* ptr 2 */ 1,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }
}
