use anyhow::Result;

use crate::{
    bio::MIN_PAGE_SIZE,
    block::BlockIo,
    btree::BtreeGetResult,
    const_assert,
    db::Txn,
    flux::FluxBuf,
    lalloc::Arena,
    uint::{InPgIdx, InPgIdxDisk, PgIdx, PgIdxDisk, U24, U64},
    util::{ChecksumDisk, from_bytes, from_bytes_mut},
};

type Slot = InPgIdx;
pub(crate) type SlotDisk = InPgIdxDisk;

type LinkedListLen = PgIdx;
type LinkedListLenDisk = PgIdxDisk;

const MAX_KEY_SIZE: usize = 256;

/// Inner node layout:
/// - header
/// - slots:[u24; max(0,len-1)], key_len is derivable, pointer to beginning of key
/// - data: [u8], grows backward, interleaved child pointer and key
/// - checksum: u64
///
/// Leaf node layout:
/// - header
/// - slots: [u24; len], key_len is derivable, pointer to beginning of (value,key)
/// - data: [u8], grows backward, interleaved value and key
/// - checksum: u64

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum BNodeKind {
    Root = 0,
    Inner,
    Leaf,
}

impl From<u8> for BNodeKind {
    fn from(value: u8) -> Self {
        match value {
            0 => BNodeKind::Root,
            1 => BNodeKind::Inner,
            2 => BNodeKind::Leaf,
            _ => panic!("invalid discriminant"),
        }
    }
}

impl BNodeKind {
    pub fn header_size(&self) -> InPgIdx {
        match self {
            BNodeKind::Root => size_of::<BNodeRootHeader>() as InPgIdx,
            BNodeKind::Inner | BNodeKind::Leaf => size_of::<BNodeHeader>() as InPgIdx,
        }
    }
}

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone)]
pub(crate) enum LeafValueKind {
    Btree = 0,
    ValueEmbedded, // len, value
    ValueLinkedList,
}

#[repr(C, packed)]
pub(crate) struct BNodeHeader {
    pub(crate) kind: BNodeKind,
    pub(crate) len: InPgIdxDisk,
}

impl BNodeHeader {
    pub(crate) fn init(&mut self, kind: BNodeKind) {
        self.set_kind(kind);
        self.set_len(0);
    }

    pub(crate) fn kind(&self) -> BNodeKind {
        self.kind
    }

    pub(crate) fn set_kind(&mut self, kind: BNodeKind) {
        self.kind = kind;
    }

    pub(crate) fn len(&self) -> u64 {
        self.len.get()
    }

    pub(crate) fn set_len(&mut self, len: u64) {
        self.len.set(len);
    }
}

#[repr(C, packed)]
pub(crate) struct BNodeRootHeader {
    pub(crate) header: BNodeHeader,
    pub(crate) version: U64,
    pub(crate) free: PgIdxDisk,
    pub(crate) arena: ArenaDisk,
}

impl BNodeRootHeader {
    pub(crate) fn init(&mut self) {
        self.header.init(BNodeKind::Root);
        self.version.set(0);
        self.free.set(0);
        self.arena.init();
    }
}

const_assert!(size_of::<BNodeRootHeader>() + size_of::<ChecksumDisk>() < MIN_PAGE_SIZE as usize);

#[repr(C, packed)]
pub(crate) struct ArenaDisk {
    pub(crate) start: PgIdxDisk,
    pub(crate) len: U24,
    pub(crate) next: U24,
}

impl ArenaDisk {
    pub(crate) fn init(&mut self) {
        self.start.set(0);
        self.len.set(0);
        self.next.set(0);
    }

    pub(crate) fn invalidate(&mut self) {
        self.start.set(0);
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.start.get() != 0
    }

    pub(crate) fn to_mem(&self) -> Arena {
        Arena {
            start: self.start.get(),
            len: self.len.get(),
            next: self.next.get(),
        }
    }
}

pub(crate) trait BNodeBuf {
    fn header(&self) -> &BNodeHeader;
    fn header_mut(&mut self) -> &mut BNodeHeader;
}

impl BNodeBuf for [u8] {
    fn header(&self) -> &BNodeHeader {
        from_bytes::<BNodeHeader>(self)
    }

    fn header_mut(&mut self) -> &mut BNodeHeader {
        from_bytes_mut::<BNodeHeader>(self)
    }
}

pub(crate) const fn in_pg_size_of<T>() -> InPgIdx {
    size_of::<T>() as InPgIdx
}

pub(crate) const fn in_pg_len(buf: &[u8]) -> InPgIdx {
    buf.len() as InPgIdx
}

pub(crate) fn slots_len(buf: &[u8]) -> InPgIdx {
    let header = from_bytes::<BNodeHeader>(buf);
    match header.kind {
        BNodeKind::Root | BNodeKind::Inner => {
            if header.len() > 0 {
                header.len() - 1
            } else {
                0
            }
        }
        BNodeKind::Leaf => header.len(),
    }
}

pub(crate) fn remaining_bytes(buf: &[u8]) -> InPgIdx {
    let header = from_bytes::<BNodeHeader>(buf);
    let slots_len = slots_len(buf);
    let tail_size = match header.kind {
        BNodeKind::Root | BNodeKind::Inner => {
            if slots_len == 0 {
                in_pg_size_of::<ChecksumDisk>()
            } else if slots_len == 1 {
                in_pg_size_of::<ChecksumDisk>() + in_pg_size_of::<PgIdxDisk>()
            } else {
                in_pg_len(buf) - (in_pg_size_of::<PgIdxDisk>() + read_slot(buf, slots_len - 1))
            }
        }
        BNodeKind::Leaf => {
            if slots_len == 0 {
                in_pg_size_of::<ChecksumDisk>()
            } else {
                in_pg_len(buf) - read_slot(buf, slots_len - 1)
            }
        }
    };

    (buf.len() as InPgIdx)
        - (header.kind.header_size() + slots_len * (size_of::<SlotDisk>() as InPgIdx) + tail_size)
}

pub(crate) fn root_to_inner(buf: &mut [u8]) {
    let slots_start = in_pg_size_of::<BNodeRootHeader>();
    let slots_end = slots_start + slots_len(buf) * in_pg_size_of::<SlotDisk>();
    let dest = in_pg_size_of::<BNodeHeader>();
    buf.copy_within(slots_start as usize..slots_end as usize, dest as usize);
    buf.header_mut().set_kind(BNodeKind::Inner);
}

pub(crate) fn insert_init_inner(buf: &mut [u8], ptr: u64) {
    let header = buf.header_mut();
    header.set_len(header.len() + 1);
    let end = in_pg_len(buf) - in_pg_size_of::<ChecksumDisk>();
    let start = end - in_pg_size_of::<PgIdxDisk>();
    from_bytes_mut::<PgIdxDisk>(&mut buf[start as usize..end as usize]).set(ptr);
}

/// will unconditionally copy the key into the node without checking if there is space, always inserts as ptr|key
pub(crate) fn insert_at_inner(buf: &mut [u8], idx: InPgIdx, left: PgIdx, key: &[u8], right: PgIdx) {
    {
        let header = buf.header();
        let slots_idx = header.kind.header_size();
        let slots_len = slots_len(buf);
        let slots_insert_idx = slots_idx + in_pg_size_of::<SlotDisk>() * idx;
        let slots_end_idx = slots_idx + in_pg_size_of::<SlotDisk>() * slots_len;

        let key_and_ptr_len = in_pg_len(key) + in_pg_size_of::<PgIdxDisk>();
        let key_and_ptr_end = if idx == 0 {
            in_pg_len(buf) - in_pg_size_of::<ChecksumDisk>() - in_pg_size_of::<PgIdxDisk>()
        } else {
            read_slot(buf, idx - 1) - in_pg_size_of::<PgIdxDisk>()
        };
        let key_and_ptr_start = key_and_ptr_end - key_and_ptr_len;
        let key_start = key_and_ptr_start + in_pg_size_of::<PgIdxDisk>();
        let all_key_and_ptr_start = if slots_len == 0 {
            in_pg_len(buf) - in_pg_size_of::<ChecksumDisk>() - in_pg_size_of::<PgIdxDisk>()
        } else {
            read_slot(buf, slots_len - 1) - in_pg_size_of::<PgIdxDisk>()
        };

        {
            let src_start = all_key_and_ptr_start as usize;
            let src_end = key_and_ptr_end as usize;
            let dst_start = (all_key_and_ptr_start - key_and_ptr_len) as usize;
            buf.copy_within(src_start..src_end, dst_start);
        }
        from_bytes_mut::<PgIdxDisk>(&mut buf[key_and_ptr_start as usize..key_start as usize])
            .set(right);
        buf[key_start as usize..key_and_ptr_end as usize].copy_from_slice(key);
        from_bytes_mut::<PgIdxDisk>(
            &mut buf[key_and_ptr_end as usize
                ..(key_and_ptr_end + in_pg_size_of::<PgIdxDisk>()) as usize],
        )
        .set(left);

        for i in idx..slots_len {
            let slot_value = read_slot(buf, i);
            write_slot(buf, i, slot_value - key_and_ptr_len);
        }

        buf.copy_within(
            slots_insert_idx as usize..slots_end_idx as usize,
            (slots_insert_idx + in_pg_size_of::<SlotDisk>()) as usize,
        );
        write_slot(buf, idx, key_start);
    }

    {
        let header = buf.header_mut();
        header.set_len(header.len() + 1);
    }
}

pub(crate) fn insert_at_leaf(buf: &mut [u8], idx: InPgIdx, key: &[u8], value: &LeafValueEncoded) {
    let header = buf.header();
    let slots_idx = header.kind.header_size();
    let slots_len = slots_len(buf);
    let slots_insert_idx = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * idx;
    let slots_end_idx = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * slots_len;

    let value_key_len = (value.len() as InPgIdx) + (key.len() as InPgIdx);
    let value_key_end = if idx == 0 {
        (buf.len() as InPgIdx) - (size_of::<ChecksumDisk>() as InPgIdx)
    } else {
        read_slot(buf, idx - 1)
    };
    let value_key_start = value_key_end - value_key_len;
    let all_value_key_start = if slots_len == 0 {
        (buf.len() as InPgIdx) - (size_of::<ChecksumDisk>() as InPgIdx)
    } else {
        read_slot(buf, slots_len - 1)
    };

    {
        let src_start = all_value_key_start as usize;
        let src_end = value_key_end as usize;
        let dst_start = (all_value_key_start - value_key_len) as usize;
        buf.copy_within(src_start..src_end, dst_start);
    }

    value.write_to_buf(&mut buf[value_key_start as usize..]);
    buf[(value_key_start + value.len()) as usize..value_key_end as usize].copy_from_slice(key);

    for i in idx..slots_len {
        let slot_value = read_slot(buf, i);
        write_slot(buf, i, slot_value - value_key_len);
    }

    {
        let src_start = (slots_insert_idx) as usize;
        let src_end = (slots_end_idx) as usize;
        let dst_start = (slots_insert_idx + (size_of::<SlotDisk>() as InPgIdx)) as usize;
        buf.copy_within(src_start..src_end, dst_start);
    }

    write_slot(buf, idx, value_key_start);

    {
        let header = buf.header_mut();
        header.set_len(header.len() + 1);
    }
}

pub(crate) fn remove_at_leaf(buf: &mut [u8], idx: InPgIdx) {
    let header = buf.header();
    let slots_idx = header.kind.header_size();
    let slots_len = slots_len(buf);
    let slots_remove_idx = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * idx;
    let slots_end_idx = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * slots_len;

    let value_key_end = if idx == 0 {
        (buf.len() as InPgIdx) - (size_of::<ChecksumDisk>() as InPgIdx)
    } else {
        read_slot(buf, idx - 1)
    };
    let value_key_start = read_slot(buf, idx);
    let value_key_len = value_key_end - value_key_start;

    let all_value_key_start = if slots_len == 0 {
        (buf.len() as InPgIdx) - (size_of::<ChecksumDisk>() as InPgIdx)
    } else {
        read_slot(buf, slots_len - 1)
    };

    {
        let src_start = all_value_key_start as usize;
        let src_end = value_key_start as usize;
        let dst_start = (all_value_key_start + value_key_len) as usize;
        buf.copy_within(src_start..src_end, dst_start);
    }

    for i in (idx + 1)..slots_len {
        let slot_value = read_slot(buf, i);
        write_slot(buf, i, slot_value + value_key_len);
    }

    {
        let src_start = (slots_remove_idx + (size_of::<SlotDisk>() as InPgIdx)) as usize;
        let src_end = slots_end_idx as usize;
        let dst_start = slots_remove_idx as usize;
        buf.copy_within(src_start..src_end, dst_start);
    }

    {
        let header = buf.header_mut();
        header.set_len(header.len() - 1);
    }
}

pub(crate) fn read_slot(buf: &[u8], idx: InPgIdx) -> InPgIdx {
    let header = from_bytes::<BNodeHeader>(buf);
    let slots_idx = header.kind.header_size();
    let start = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * idx;
    from_bytes::<SlotDisk>(&buf[start as usize..]).get()
}

pub(crate) fn write_slot(buf: &mut [u8], idx: InPgIdx, value: InPgIdx) {
    let header = from_bytes::<BNodeHeader>(buf);
    let slots_idx = header.kind.header_size();
    let start = slots_idx + (size_of::<SlotDisk>() as InPgIdx) * idx;
    from_bytes_mut::<SlotDisk>(&mut buf[start as usize..]).set(value as u64);
}

pub(crate) fn get_page_ptr(buf: &[u8], idx: InPgIdx) -> PgIdx {
    let loc = if idx == 0 {
        (buf.len() as InPgIdx)
            - (size_of::<ChecksumDisk>() as InPgIdx)
            - (size_of::<PgIdxDisk>() as InPgIdx)
    } else {
        read_slot(buf, idx - 1) - (size_of::<PgIdxDisk>() as InPgIdx)
    };

    let start = loc as usize;
    let end = (loc + size_of::<PgIdxDisk>() as InPgIdx) as usize;
    from_bytes::<PgIdxDisk>(&buf[start..end]).get()
}

pub(crate) fn write_page_ptr(buf: &mut [u8], idx: InPgIdx, value: u64) {
    let loc = if idx == 0 {
        (buf.len() as InPgIdx)
            - (size_of::<ChecksumDisk>() as InPgIdx)
            - (size_of::<PgIdxDisk>() as InPgIdx)
    } else {
        read_slot(buf, idx - 1) - (size_of::<PgIdxDisk>() as InPgIdx)
    };
    from_bytes_mut::<PgIdxDisk>(&mut buf[loc as usize..]).set(value);
}

pub(crate) fn get_key_inner(buf: &[u8], idx: InPgIdx) -> &[u8] {
    let key_idx = read_slot(buf, idx);
    let key_len = if idx == 0 {
        (buf.len() as InPgIdx
            - (size_of::<PgIdxDisk>() as InPgIdx)
            - (size_of::<ChecksumDisk>() as InPgIdx))
            - key_idx
    } else {
        let _test = read_slot(buf, idx - 1);
        let test2 = (read_slot(buf, idx - 1) - (size_of::<PgIdxDisk>() as InPgIdx)) - key_idx;
        test2
    };

    let start = key_idx as usize;
    let end = (key_idx + key_len) as usize;
    &buf[start..end]
}

pub(crate) fn get_key_leaf(buf: &[u8], idx: InPgIdx) -> &[u8] {
    let val_key_idx = read_slot(buf, idx);
    let val_len = 1 + match LeafValueKind::from(buf[val_key_idx as usize]) {
        LeafValueKind::Btree => 2 * (size_of::<PgIdxDisk>() as InPgIdx),
        LeafValueKind::ValueEmbedded => 1 + (buf[(val_key_idx + 1) as usize] as InPgIdx),
        LeafValueKind::ValueLinkedList => {
            (size_of::<PgIdxDisk>() as InPgIdx) + (size_of::<LinkedListLenDisk>() as InPgIdx)
        }
    };
    let key_idx = val_key_idx + val_len;
    let key_len = if idx == 0 {
        buf.len() as InPgIdx - (size_of::<ChecksumDisk>() as InPgIdx) - key_idx
    } else {
        read_slot(buf, idx - 1) - key_idx
    };

    let start = key_idx as usize;
    let end = (key_idx + key_len) as usize;
    &buf[start..end]
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LeafValueEncoded<'a> {
    Btree { pg_idx_1: PgIdx, pg_idx_2: PgIdx },
    ValueEmbedded(&'a [u8]),
    ValueLinkedList { pg_idx: PgIdx, len: PgIdx },
}

#[derive(Debug)]
pub(crate) enum LeafValueGetResult {
    Btree { pg_idx_1: PgIdx, pg_idx_2: PgIdx },
    ValueEmbedded { loc: usize, len: usize },
    ValueLinkedList { pg_idx: PgIdx, len: PgIdx },
}

pub(crate) enum LeafValue<'a> {
    Btree { pg_idx_1: PgIdx, pg_idx_2: PgIdx },
    Value(&'a [u8]),
}

impl<'a> LeafValue<'a> {
    pub async fn encode<B: BlockIo>(self, txn: &mut Txn<'_, B>) -> Result<LeafValueEncoded<'a>> {
        Ok(match self {
            LeafValue::Btree { pg_idx_1, pg_idx_2 } => {
                LeafValueEncoded::Btree { pg_idx_1, pg_idx_2 }
            }
            LeafValue::Value(v) => {
                if v.len() < 256 {
                    LeafValueEncoded::ValueEmbedded(v)
                } else {
                    let pg_idx = txn.create_value_linked_list(v).await?;
                    LeafValueEncoded::ValueLinkedList {
                        pg_idx,
                        len: v.len() as u64,
                    }
                }
            }
        })
    }
}

impl LeafValueEncoded<'_> {
    pub(crate) fn kind(&self) -> LeafValueKind {
        match self {
            LeafValueEncoded::Btree { .. } => LeafValueKind::Btree,
            LeafValueEncoded::ValueEmbedded(_) => LeafValueKind::ValueEmbedded,
            LeafValueEncoded::ValueLinkedList { .. } => LeafValueKind::ValueLinkedList,
        }
    }

    pub(crate) fn len(&self) -> InPgIdx {
        1 + match self {
            LeafValueEncoded::Btree { .. } => 2 * in_pg_size_of::<PgIdxDisk>(),
            LeafValueEncoded::ValueEmbedded(v) => 1 + in_pg_len(&v),
            LeafValueEncoded::ValueLinkedList { .. } => {
                in_pg_size_of::<PgIdxDisk>() + in_pg_size_of::<LinkedListLenDisk>()
            }
        }
    }

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        buf[0] = self.kind() as u8;
        match self {
            LeafValueEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                from_bytes_mut::<PgIdxDisk>(&mut buf[1..]).set(*pg_idx_1);
                from_bytes_mut::<PgIdxDisk>(&mut buf[1 + size_of::<PgIdxDisk>()..]).set(*pg_idx_2);
            }
            LeafValueEncoded::ValueEmbedded(v) => {
                buf[1] = v.len() as u8;
                buf[2..2 + v.len()].copy_from_slice(v);
            }
            LeafValueEncoded::ValueLinkedList { pg_idx, len } => {
                from_bytes_mut::<PgIdxDisk>(&mut buf[1..]).set(*pg_idx);
                from_bytes_mut::<LinkedListLenDisk>(&mut buf[1 + size_of::<PgIdxDisk>()..])
                    .set(*len);
            }
        }
    }
}

impl From<u8> for LeafValueKind {
    fn from(value: u8) -> Self {
        match value {
            0 => LeafValueKind::Btree,
            1 => LeafValueKind::ValueEmbedded,
            2 => LeafValueKind::ValueLinkedList,
            _ => panic!("invalid discriminant"),
        }
    }
}

impl LeafValueGetResult {
    pub(crate) fn encode<'a>(&self, buf: &'a [u8]) -> LeafValueEncoded<'a> {
        match self {
            LeafValueGetResult::Btree { pg_idx_1, pg_idx_2 } => LeafValueEncoded::Btree {
                pg_idx_1: *pg_idx_1,
                pg_idx_2: *pg_idx_2,
            },
            LeafValueGetResult::ValueEmbedded { loc, len } => {
                LeafValueEncoded::ValueEmbedded(&buf[*loc..*loc + *len])
            }
            LeafValueGetResult::ValueLinkedList { pg_idx, len } => {
                LeafValueEncoded::ValueLinkedList {
                    pg_idx: *pg_idx,
                    len: *len,
                }
            }
        }
    }

    pub(crate) fn with_page<B: BlockIo>(&self, buf: FluxBuf<B>) -> BtreeGetResult<B> {
        match self {
            LeafValueGetResult::Btree { pg_idx_1, pg_idx_2 } => BtreeGetResult::Btree {
                pg_idx_1: *pg_idx_1,
                pg_idx_2: *pg_idx_2,
            },
            LeafValueGetResult::ValueEmbedded { loc, len } => BtreeGetResult::ValueEmbedded {
                buf,
                loc: *loc,
                len: *len,
            },
            LeafValueGetResult::ValueLinkedList { pg_idx, len } => {
                BtreeGetResult::ValueLinkedList {
                    pg_idx: *pg_idx,
                    len: *len,
                }
            }
        }
    }
}

pub(crate) fn get_value_leaf(buf: &[u8], idx: InPgIdx) -> LeafValueGetResult {
    let val_key_idx = read_slot(buf, idx);
    match LeafValueKind::from(buf[val_key_idx as usize]) {
        LeafValueKind::Btree => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + in_pg_size_of::<PgIdxDisk>();
            let pg_idx_1 = from_bytes::<PgIdxDisk>(&buf[b_idx_1 as usize..]).get();
            let pg_idx_2 = from_bytes::<PgIdxDisk>(&buf[b_idx_2 as usize..]).get();
            LeafValueGetResult::Btree { pg_idx_1, pg_idx_2 }
        }
        LeafValueKind::ValueEmbedded => {
            let len_idx = val_key_idx + 1;
            let len = buf[(val_key_idx + 1) as usize] as usize;
            let value_idx = (len_idx + 1) as usize;
            LeafValueGetResult::ValueEmbedded {
                loc: value_idx,
                len,
            }
        }
        LeafValueKind::ValueLinkedList => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + in_pg_size_of::<PgIdxDisk>();
            let pg_idx = from_bytes::<PgIdxDisk>(&buf[b_idx_1 as usize..]).get();
            let len = from_bytes::<LinkedListLenDisk>(&buf[b_idx_2 as usize..]).get();
            LeafValueGetResult::ValueLinkedList { pg_idx, len }
        }
    }
}

pub(crate) enum SearchResult {
    Exact(InPgIdx),
    Insert(InPgIdx),
}

impl SearchResult {
    pub fn idx(&self) -> InPgIdx {
        match self {
            SearchResult::Exact(idx) => *idx,
            SearchResult::Insert(idx) => *idx,
        }
    }
}

pub(crate) fn search_inner(buf: &[u8], target: &[u8]) -> SearchResult {
    let header = from_bytes::<BNodeHeader>(buf);

    if header.len() <= 1 {
        return SearchResult::Insert(0);
    }

    let mut left = 0;
    let mut right = slots_len(buf);

    while left < right {
        let mid = left + (right - left) / 2;
        let key = get_key_inner(buf, mid);
        match key.cmp(target) {
            std::cmp::Ordering::Equal => return SearchResult::Exact(mid),
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
        }
    }

    SearchResult::Insert(left)
}

pub(crate) fn search_leaf(buf: &[u8], target: &[u8]) -> SearchResult {
    let mut left: InPgIdx = 0;
    let mut right = slots_len(buf);

    while left < right {
        let mid = left + (right - left) / 2;
        let key = get_key_leaf(buf, mid);
        match key.cmp(target) {
            std::cmp::Ordering::Equal => return SearchResult::Exact(mid),
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
        }
    }

    SearchResult::Insert(left)
}

#[coverage(off)]
#[cfg(test)]
mod tests {

    use anyhow::Result;
    use function_name::named;

    use crate::{
        bnode::{
            BNodeBuf, BNodeHeader, BNodeKind, BNodeRootHeader, LeafValueEncoded, get_key_inner,
            get_key_leaf, get_page_ptr, get_value_leaf, insert_at_inner, insert_at_leaf,
            insert_init_inner, remove_at_leaf,
        },
        test::TmpDir,
        util::{from_bytes, from_bytes_mut},
    };

    #[test]
    fn test_access() {
        let buffer = vec![0u8; 4096];
        let slice = &buffer[1..];
        let header = from_bytes::<BNodeRootHeader>(slice);
        assert_eq!({ header.header.kind.clone() }, BNodeKind::Root);
        assert_eq!(header.version.get(), 0);
        assert_eq!(header.header.len(), 0);
    }

    #[test]
    fn inner_node_insert_test_1() {
        let mut node = [0u8; 64];
        {
            let header = from_bytes_mut::<BNodeHeader>(&mut node);
            header.init(BNodeKind::Inner);
        }

        insert_init_inner(&mut node, 1);

        assert_eq!(1, get_page_ptr(&node, 0));

        insert_at_inner(&mut node, 0, 2, b"a", 3);

        assert_eq!(2, get_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(3, get_page_ptr(&node, 1));

        insert_at_inner(&mut node, 0, 4, b"b", 5);

        assert_eq!(4, get_page_ptr(&node, 0));
        assert_eq!(b"b", get_key_inner(&node, 0));
        assert_eq!(5, get_page_ptr(&node, 1));
        assert_eq!(b"a", get_key_inner(&node, 1));
        assert_eq!(3, get_page_ptr(&node, 2));
    }

    #[test]
    fn inner_node_insert_test_2() {
        let mut node = [0u8; 64];
        {
            let header = from_bytes_mut::<BNodeHeader>(&mut node);
            header.init(BNodeKind::Inner);
        }

        insert_init_inner(&mut node, 1);

        assert_eq!(1, get_page_ptr(&node, 0));

        insert_at_inner(&mut node, 0, 2, b"a", 3);

        assert_eq!(2, get_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(3, get_page_ptr(&node, 1));

        insert_at_inner(&mut node, 1, 4, b"b", 5);

        assert_eq!(2, get_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(4, get_page_ptr(&node, 1));
        assert_eq!(b"b", get_key_inner(&node, 1));
        assert_eq!(5, get_page_ptr(&node, 2));
    }

    #[named]
    #[tokio::test]
    async fn test_linked_list() -> Result<()> {
        let dir = TmpDir::new(function_name!())?;
        let db = dir.db().await?;
        let mut txn = db.txn().begin().await;
        let value_len = txn.db.meta.page_size() as usize * 2.5 as usize;
        let value = vec![1u8; value_len];
        let mut value_test = vec![0u8; value_len];
        let pg_idx = txn.create_value_linked_list(&value).await?;
        txn.read_value_linked_list(pg_idx, &mut value_test).await?;
        assert_eq!(value, value_test);
        Ok(())
    }

    #[tokio::test]
    async fn test_single_inner_insert() -> Result<()> {
        let mut page = vec![0u8; 4096];

        {
            page.header_mut().init(BNodeKind::Inner);
        }
        assert_eq!(0, page.header().len());

        insert_init_inner(&mut page, 1);
        assert_eq!(1, get_page_ptr(&page, 0));
        assert_eq!(1, page.header().len());

        insert_at_inner(&mut page, 0, 2, b"AAA", 3);
        assert_eq!(2, get_page_ptr(&page, 0));
        assert_eq!(b"AAA", get_key_inner(&page, 0));
        assert_eq!(3, get_page_ptr(&page, 1));
        assert_eq!(2, page.header().len());

        insert_at_inner(&mut page, 1, 3, b"BBB", 4);
        assert_eq!(2, get_page_ptr(&page, 0));
        assert_eq!(b"AAA", get_key_inner(&page, 0));
        assert_eq!(3, get_page_ptr(&page, 1));
        assert_eq!(b"BBB", get_key_inner(&page, 1));
        assert_eq!(4, get_page_ptr(&page, 2));
        assert_eq!(3, page.header().len());

        insert_at_inner(&mut page, 0, 1, b"CCC", 2);
        assert_eq!(1, get_page_ptr(&page, 0));
        assert_eq!(b"CCC", get_key_inner(&page, 0));
        assert_eq!(2, get_page_ptr(&page, 1));
        assert_eq!(b"AAA", get_key_inner(&page, 1));
        assert_eq!(3, get_page_ptr(&page, 2));
        assert_eq!(b"BBB", get_key_inner(&page, 2));
        assert_eq!(4, get_page_ptr(&page, 3));
        assert_eq!(4, page.header().len());

        insert_at_inner(&mut page, 2, 5, b"DDD", 6);
        assert_eq!(1, get_page_ptr(&page, 0));
        assert_eq!(b"CCC", get_key_inner(&page, 0));
        assert_eq!(2, get_page_ptr(&page, 1));
        assert_eq!(b"AAA", get_key_inner(&page, 1));
        assert_eq!(5, get_page_ptr(&page, 2));
        assert_eq!(b"DDD", get_key_inner(&page, 2));
        assert_eq!(6, get_page_ptr(&page, 3));
        assert_eq!(b"BBB", get_key_inner(&page, 3));
        assert_eq!(4, get_page_ptr(&page, 4));
        assert_eq!(5, page.header().len());

        Ok(())
    }

    #[tokio::test]
    async fn test_single_leaf_insert() -> Result<()> {
        let mut page = vec![0u8; 4096];

        {
            page.header_mut().init(BNodeKind::Leaf);
        }

        assert_eq!(0, page.header().len());

        insert_at_leaf(
            &mut page,
            0,
            b"K_AAA",
            &LeafValueEncoded::ValueEmbedded(b"V_AAA"),
        );
        assert_eq!(b"K_AAA", get_key_leaf(&page, 0));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 0).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(1, page.header().len());

        insert_at_leaf(
            &mut page,
            1,
            b"K_BBB",
            &LeafValueEncoded::ValueEmbedded(b"V_BBB"),
        );
        assert_eq!(b"K_AAA", get_key_leaf(&page, 0));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 0).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 1).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(2, page.header().len());

        insert_at_leaf(
            &mut page,
            0,
            b"K_CCC",
            &LeafValueEncoded::Btree {
                pg_idx_1: 0x0000007c2ea46f6e,
                pg_idx_2: 0x0000009571ec6979,
            },
        );
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0).encode(&page) {
            LeafValueEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x0000007c2ea46f6e);
                assert_eq!(pg_idx_2, 0x0000009571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 2));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 2).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(3, page.header().len());

        insert_at_leaf(
            &mut page,
            2,
            b"K_DDD",
            &LeafValueEncoded::ValueLinkedList {
                pg_idx: 0x000000542265332b,
                len: 0x000000bd13ba0a79,
            },
        );
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0).encode(&page) {
            LeafValueEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x0000007c2ea46f6e);
                assert_eq!(pg_idx_2, 0x0000009571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_DDD", get_key_leaf(&page, 2));
        match get_value_leaf(&page, 2).encode(&page) {
            LeafValueEncoded::ValueLinkedList { pg_idx, len } => {
                assert_eq!(pg_idx, 0x000000542265332b);
                assert_eq!(len, 0x000000bd13ba0a79);
            }
            _ => panic!("expected embedded value"),
        };

        assert_eq!(b"K_BBB", get_key_leaf(&page, 3));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 3).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(4, page.header().len());

        remove_at_leaf(&mut page, 2);
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0).encode(&page) {
            LeafValueEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x0000007c2ea46f6e);
                assert_eq!(pg_idx_2, 0x0000009571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 2));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 2).encode(&page) {
                LeafValueEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(3, page.header().len());

        Ok(())
    }
}
