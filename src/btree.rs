use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::{
    const_assert,
    db::{Db, Txn},
    fio::{Fio, MIN_PAGE_SIZE, PageBuf},
    key::KeyPath,
    util::{
        CHECKSUM_SIZE, from_bytes, from_bytes_mut, has_valid_checksum, update_checksum,
        validate_checksum,
    },
};

type Slot = u32;
type PagePtr = u64;

const MAX_KEY_SIZE: usize = 256;
const SLOT_SIZE: usize = size_of::<Slot>();
const PAGE_PTR_SIZE: usize = size_of::<PagePtr>();
const LINKED_LIST_VALUE_LEN_SIZE: usize = size_of::<u64>();

/// Inner node layout:
/// - header
/// - slots:[u32; max(0,len-1)], key_len is derivable, pointer to beginning of key
/// - data: [u8], grows backward, interleaved child pointer and key
/// - checksum: u32
///
/// Leaf node layout:
/// - header
/// - slots: [u32; len], key_len is derivable, pointer to beginning of (value,key)
/// - data: [u8], grows backward, interleaved value and key
/// - checksum: u32

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone)]
enum BTreeNodeKind {
    Root = 0,
    Inner,
    Leaf,
}

impl From<u8> for BTreeNodeKind {
    fn from(value: u8) -> Self {
        match value {
            0 => BTreeNodeKind::Root,
            1 => BTreeNodeKind::Inner,
            2 => BTreeNodeKind::Leaf,
            _ => panic!("invalid discriminant"),
        }
    }
}

impl BTreeNodeKind {
    pub fn header_size(&self) -> usize {
        match self {
            BTreeNodeKind::Root => size_of::<BTreeRootHeader>(),
            BTreeNodeKind::Inner | BTreeNodeKind::Leaf => size_of::<BTreeHeader>(),
        }
    }
}

#[repr(u8)]
#[derive(PartialEq, Eq, Debug, Clone)]
enum LeafDataKind {
    Btree = 0,
    ValueEmbedded, // len, value
    ValueLinkedList,
}

enum LeafDataEncoded<'a> {
    Btree { pg_idx_1: u64, pg_idx_2: u64 },
    ValueEmbedded(&'a [u8]),
    ValueLinkedList { pg_idx: u64, len: u64 },
}

enum LeafData<'a> {
    Btree { pg_idx_1: u64, pg_idx_2: u64 },
    Value(&'a [u8]),
}

impl<'a> LeafData<'a> {
    pub async fn encode(self, txn: &mut Txn) -> Result<LeafDataEncoded<'a>> {
        Ok(match self {
            LeafData::Btree { pg_idx_1, pg_idx_2 } => LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 },
            LeafData::Value(v) => {
                if v.len() < 256 {
                    LeafDataEncoded::ValueEmbedded(v)
                } else {
                    let pg_idx = txn.create_value_linked_list(v).await?;
                    LeafDataEncoded::ValueLinkedList {
                        pg_idx,
                        len: v.len() as u64,
                    }
                }
            }
        })
    }
}

impl LeafDataEncoded<'_> {
    pub fn kind(&self) -> LeafDataKind {
        match self {
            LeafDataEncoded::Btree { .. } => LeafDataKind::Btree,
            LeafDataEncoded::ValueEmbedded(_) => LeafDataKind::ValueEmbedded,
            LeafDataEncoded::ValueLinkedList { .. } => LeafDataKind::ValueLinkedList,
        }
    }

    pub fn len(&self) -> usize {
        1 + match self {
            LeafDataEncoded::Btree { .. } => 2 * PAGE_PTR_SIZE,
            LeafDataEncoded::ValueEmbedded(v) => 1 + v.len(),
            LeafDataEncoded::ValueLinkedList { .. } => PAGE_PTR_SIZE + LINKED_LIST_VALUE_LEN_SIZE,
        }
    }

    pub fn write_to_buf(&self, buf: &mut [u8]) {
        buf[0] = self.kind() as u8;
        match self {
            LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                buf[1..1 + PAGE_PTR_SIZE].copy_from_slice(&pg_idx_1.to_ne_bytes());
                buf[1 + PAGE_PTR_SIZE..1 + 2 * PAGE_PTR_SIZE]
                    .copy_from_slice(&pg_idx_2.to_ne_bytes());
            }
            LeafDataEncoded::ValueEmbedded(v) => {
                buf[1] = v.len() as u8;
                buf[2..2 + v.len()].copy_from_slice(v);
            }
            LeafDataEncoded::ValueLinkedList { pg_idx, len } => {
                buf[1..1 + PAGE_PTR_SIZE].copy_from_slice(&pg_idx.to_ne_bytes());
                buf[1 + PAGE_PTR_SIZE..1 + PAGE_PTR_SIZE + LINKED_LIST_VALUE_LEN_SIZE]
                    .copy_from_slice(&len.to_ne_bytes());
            }
        }
    }
}

impl From<u8> for LeafDataKind {
    fn from(value: u8) -> Self {
        match value {
            0 => LeafDataKind::Btree,
            1 => LeafDataKind::ValueEmbedded,
            2 => LeafDataKind::ValueLinkedList,
            _ => panic!("invalid discriminant"),
        }
    }
}

#[repr(C, packed)]
struct BTreeHeader {
    kind: BTreeNodeKind,
    parent: u64,
    len: u32,
}

impl BTreeHeader {
    pub fn init_inner(&mut self) {
        self.kind = BTreeNodeKind::Inner.into();
        self.parent = 0;
        self.len = 0;
    }

    pub fn init_leaf(&mut self, parent: u64) {
        self.kind = BTreeNodeKind::Leaf.into();
        self.parent = parent;
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

trait BTreeNodeBuf {
    fn header(&self) -> &BTreeHeader;
    fn header_mut(&mut self) -> &mut BTreeHeader;
    fn root_header(&self) -> &BTreeRootHeader;
    fn root_header_mut(&mut self) -> &mut BTreeRootHeader;
    fn slots_len(&self) -> usize;
    fn remaining(&self) -> usize;
    fn available(&self) -> usize;
}

impl BTreeNodeBuf for [u8] {
    fn header(&self) -> &BTreeHeader {
        from_bytes::<BTreeHeader>(self)
    }

    fn header_mut(&mut self) -> &mut BTreeHeader {
        from_bytes_mut::<BTreeHeader>(self)
    }

    fn root_header(&self) -> &BTreeRootHeader {
        from_bytes::<BTreeRootHeader>(self)
    }

    fn root_header_mut(&mut self) -> &mut BTreeRootHeader {
        from_bytes_mut::<BTreeRootHeader>(self)
    }

    fn slots_len(&self) -> usize {
        let header = self.header();
        match header.kind {
            BTreeNodeKind::Root | BTreeNodeKind::Inner => {
                (if header.len > 0 { header.len - 1 } else { 0 }) as usize
            }
            BTreeNodeKind::Leaf => header.len as usize,
        }
    }

    fn remaining(&self) -> usize {
        let slots_len = self.slots_len();
        let tail_size = match self.header().kind {
            BTreeNodeKind::Root | BTreeNodeKind::Inner => {
                if slots_len == 0 {
                    CHECKSUM_SIZE
                } else if slots_len == 1 {
                    CHECKSUM_SIZE + PAGE_PTR_SIZE
                } else {
                    self.len() - (PAGE_PTR_SIZE + read_slot(self, slots_len - 1))
                }
            }
            BTreeNodeKind::Leaf => {
                if slots_len == 0 {
                    CHECKSUM_SIZE
                } else {
                    self.len() - read_slot(self, slots_len - 1)
                }
            }
        };

        self.len() - (self.header().kind.header_size() + slots_len * SLOT_SIZE + tail_size)
    }

    fn available(&self) -> usize {
        self.len() - self.header().kind.header_size()
    }
}

fn insert_init_inner(buf: &mut [u8], ptr: u64) {
    let header = buf.header_mut();
    header.len += 1;
    let end = buf.len() - CHECKSUM_SIZE;
    let start = end - PAGE_PTR_SIZE;
    buf[start..end].copy_from_slice(&ptr.to_ne_bytes());
}

/// will unconditionally copy the key into the node without checking if there is space, always inserts as ptr|key
fn insert_at_inner(buf: &mut [u8], idx: usize, left: PagePtr, key: &[u8], right: PagePtr) {
    {
        let header = buf.header();
        let slots_idx = header.kind.header_size();
        let slots_len = buf.slots_len();
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
        buf[key_and_ptr_start..key_start].copy_from_slice(&right.to_ne_bytes());
        buf[key_start..key_and_ptr_end].copy_from_slice(key);
        buf[key_and_ptr_end..key_and_ptr_end + PAGE_PTR_SIZE].copy_from_slice(&left.to_ne_bytes());

        for i in idx..slots_len {
            let slot_value = read_slot(buf, i);
            write_slot(buf, i, slot_value - key_and_ptr_len);
        }

        buf.copy_within(
            slots_insert_idx..slots_end_idx,
            slots_insert_idx + SLOT_SIZE,
        );
        write_slot(buf, idx, key_start);
    }

    {
        let header = buf.header_mut();
        header.len += 1;
    }
}

fn insert_at_leaf(buf: &mut [u8], idx: usize, key: &[u8], value: &LeafDataEncoded) {
    let header = buf.header();
    let slots_idx = header.kind.header_size();
    let slots_len = buf.slots_len();
    let slots_insert_idx = slots_idx + SLOT_SIZE * idx;
    let slots_end_idx = slots_idx + SLOT_SIZE * slots_len;

    let value_key_len = value.len() + key.len();
    let value_key_end = if idx == 0 {
        buf.len() - CHECKSUM_SIZE
    } else {
        read_slot(buf, idx - 1)
    };
    let value_key_start = value_key_end - value_key_len;
    let all_value_key_start = if slots_len == 0 {
        buf.len() - CHECKSUM_SIZE
    } else {
        read_slot(buf, slots_len - 1)
    };

    buf.copy_within(
        all_value_key_start..value_key_end,
        all_value_key_start - value_key_len,
    );
    value.write_to_buf(buf[value_key_start..value_key_start + value.len()].as_mut());
    buf[value_key_start + value.len()..value_key_end].copy_from_slice(key);

    for i in idx..slots_len {
        let slot_value = read_slot(buf, i);
        write_slot(buf, i, slot_value - value_key_len);
    }

    buf.copy_within(
        slots_insert_idx..slots_end_idx,
        slots_insert_idx + SLOT_SIZE,
    );
    write_slot(buf, idx, value_key_start);

    {
        let header = buf.header_mut();
        header.len += 1;
    }
}

fn remove_at_leaf(buf: &mut [u8], idx: usize) {
    let header = buf.header();
    let slots_idx = header.kind.header_size();
    let slots_len = buf.slots_len();
    let slots_remove_idx = slots_idx + SLOT_SIZE * idx;
    let slots_end_idx = slots_idx + SLOT_SIZE * slots_len;

    let value_key_end = if idx == 0 {
        buf.len() - CHECKSUM_SIZE
    } else {
        read_slot(buf, idx - 1)
    };
    let value_key_start = read_slot(buf, idx);
    let value_key_len = value_key_end - value_key_start;

    let all_value_key_start = if slots_len == 0 {
        buf.len() - CHECKSUM_SIZE
    } else {
        read_slot(buf, slots_len - 1)
    };

    buf.copy_within(
        all_value_key_start..value_key_start,
        all_value_key_start + value_key_len,
    );

    for i in (idx + 1)..slots_len {
        let slot_value = read_slot(buf, i);
        write_slot(buf, i, slot_value + value_key_len);
    }

    buf.copy_within(
        (slots_remove_idx + SLOT_SIZE)..slots_end_idx,
        slots_remove_idx,
    );

    {
        let header = buf.header_mut();
        header.len -= 1;
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

fn read_page_ptr(buf: &[u8], idx: usize) -> u64 {
    let mut u64_buf = [0u8; 8];
    let loc = if idx == 0 {
        buf.len() - CHECKSUM_SIZE - PAGE_PTR_SIZE
    } else {
        read_slot(buf, idx - 1) - PAGE_PTR_SIZE
    };
    u64_buf.copy_from_slice(&buf[loc..loc + PAGE_PTR_SIZE]);
    u64::from_ne_bytes(u64_buf)
}

fn read_key(buf: &[u8], idx: usize) -> &[u8] {
    let end = if idx == 0 {
        buf.len() - CHECKSUM_SIZE - PAGE_PTR_SIZE
    } else {
        read_slot(buf, idx - 1) - PAGE_PTR_SIZE
    };
    let start = read_slot(buf, idx);
    &buf[start..end]
}

fn get_key_inner(buf: &[u8], idx: usize) -> &[u8] {
    let key_idx = read_slot(buf, idx);
    let key_len = if idx == 0 {
        (buf.len() - PAGE_PTR_SIZE - CHECKSUM_SIZE) - key_idx
    } else {
        (read_slot(buf, idx - 1) - PAGE_PTR_SIZE) - key_idx
    } as usize;
    &buf[key_idx..(key_idx + key_len)]
}

fn get_key_leaf(buf: &[u8], idx: usize) -> &[u8] {
    let val_key_idx = read_slot(buf, idx);
    let val_len = 1 + match LeafDataKind::from(buf[val_key_idx]) {
        LeafDataKind::Btree => 2 * PAGE_PTR_SIZE,
        LeafDataKind::ValueEmbedded => 1 + buf[val_key_idx + 1] as usize,
        LeafDataKind::ValueLinkedList => PAGE_PTR_SIZE + LINKED_LIST_VALUE_LEN_SIZE,
    };
    let key_idx = val_key_idx + val_len;
    let key_len = if idx == 0 {
        buf.len() - CHECKSUM_SIZE - key_idx
    } else {
        read_slot(buf, idx - 1) - key_idx
    } as usize;
    &buf[key_idx..(key_idx + key_len)]
}

fn read_u64_at(buf: &[u8], idx: usize) -> u64 {
    let mut u64_buf = [0u8; 8];
    u64_buf.copy_from_slice(&buf[idx..idx + 8]);
    u64::from_ne_bytes(u64_buf)
}

fn get_value_leaf(buf: &[u8], idx: usize) -> LeafDataEncoded<'_> {
    let val_key_idx = read_slot(buf, idx);
    match LeafDataKind::from(buf[val_key_idx]) {
        LeafDataKind::Btree => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + PAGE_PTR_SIZE;
            let pg_idx_1 = read_u64_at(buf, b_idx_1);
            let pg_idx_2 = read_u64_at(buf, b_idx_2);
            LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 }
        }
        LeafDataKind::ValueEmbedded => {
            let len_idx = val_key_idx + 1;
            let len = buf[val_key_idx + 1] as usize;
            let value_idx = len_idx + 1;
            let value = &buf[value_idx..value_idx + len];
            LeafDataEncoded::ValueEmbedded(value)
        }
        LeafDataKind::ValueLinkedList => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + PAGE_PTR_SIZE;
            let pg_idx = read_u64_at(buf, b_idx_1);
            let len = read_u64_at(buf, b_idx_2);
            LeafDataEncoded::ValueLinkedList { pg_idx, len }
        }
    }
}

enum SearchResult {
    Exact(usize),
    Insert(usize),
}

impl SearchResult {
    pub fn idx(&self) -> usize {
        match self {
            SearchResult::Exact(idx) => *idx,
            SearchResult::Insert(idx) => *idx,
        }
    }
}

fn search_inner(buf: &[u8], target: &[u8]) -> SearchResult {
    let header = from_bytes::<BTreeHeader>(buf);

    if header.len <= 1 {
        return SearchResult::Insert(0);
    }

    let mut left = 0;
    let mut right = buf.slots_len();

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

fn search_leaf(buf: &[u8], target: &[u8]) -> SearchResult {
    let mut left = 0;
    let mut right = buf.slots_len();

    while left < right {
        let mid = left + (left + right) / 2;
        let key = get_key_leaf(buf, mid);
        match key.cmp(target) {
            std::cmp::Ordering::Equal => return SearchResult::Exact(mid),
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
        }
    }

    SearchResult::Exact(left)
}

struct RootDoublePageBuf {
    front_pg_idx: u64,
    front: PageBuf,
    back_pg_idx: u64,
    back: PageBuf,
}

impl RootDoublePageBuf {
    pub async fn new_no_retry(fio: &Fio, pg_idx1: u64, pg_idx2: u64) -> Result<Self> {
        let pg1 = fio.read(pg_idx1).await?;
        let pg2 = fio.read(pg_idx2).await?;
        let ((front_pg_idx, front), (back_pg_idx, back)) =
            Self::order_front_back(pg_idx1, pg1, pg_idx2, pg2)?;
        Ok(Self {
            front_pg_idx,
            front,
            back_pg_idx,
            back,
        })
    }

    /// For read-only transactions, it's possible that page reads both fail checksum so as
    /// a last ditch it will acquire a write lock to do a consistent read.
    pub async fn new_retry(
        db: &Db,
        key: &KeyPath,
        pg_idx1: u64,
        pg_idx2: u64,
        timeout: Duration,
    ) -> Result<Self> {
        let start = Instant::now();
        while Instant::now().duration_since(start) < timeout {
            let pg1 = db.inner.fio.read(pg_idx1).await?;
            let pg2 = db.inner.fio.read(pg_idx2).await?;
            if let Ok(((front_pg_idx, front), (back_pg_idx, back))) =
                Self::order_front_back(pg_idx1, pg1, pg_idx2, pg2)
            {
                return Ok(Self {
                    front_pg_idx,
                    front,
                    back_pg_idx,
                    back,
                });
            }
        }

        let carc = db.inner.write_locks.get(key.to_owned());
        let _gaurd = carc.lock().await;
        Self::new_no_retry(&db.inner.fio, pg_idx1, pg_idx2).await
    }

    fn order_front_back(
        pg_idx1: u64,
        pg1: PageBuf,
        pg_idx2: u64,
        pg2: PageBuf,
    ) -> Result<((u64, PageBuf), (u64, PageBuf))> {
        let pg1_checksum_valid = has_valid_checksum(&pg1.get());
        let pg2_checksum_valid = has_valid_checksum(&pg2.get());

        if !pg1_checksum_valid && !pg2_checksum_valid {
            bail!("both invalid")
        } else if pg1_checksum_valid && !pg2_checksum_valid {
            Ok(((pg_idx1, pg1), (pg_idx2, pg2)))
        } else if !pg1_checksum_valid && pg2_checksum_valid {
            Ok(((pg_idx2, pg2), (pg_idx1, pg1)))
        } else {
            // Both checksums are valid, choose the one with higher version
            let keep_order = {
                let root1 = from_bytes::<BTreeRootHeader>(&pg1.get());
                let root2 = from_bytes::<BTreeRootHeader>(&pg2.get());
                root1.version >= root2.version
            };
            if keep_order {
                Ok(((pg_idx1, pg1), (pg_idx2, pg2)))
            } else {
                Ok(((pg_idx2, pg2), (pg_idx1, pg1)))
            }
        }
    }
}

enum InsertResult {
    Single(u64),
    Split(u64, Box<[u8]>, u64),
}

impl Txn {
    async fn insert(&mut self, key: &[u8], value: &[u8], root: RootDoublePageBuf) {}

    async fn insert_help(
        &mut self,
        key: &[u8],
        value: LeafData<'_>,
        pg_idx: u64,
        pg: PageBuf,
    ) -> Result<()> {
        let db = &self.db.inner;
        let header = from_bytes::<BTreeHeader>(pg.get());
        if header.kind == BTreeNodeKind::Leaf {
            let ret = self.upsert_leaf(key, value, pg).await?;
            self.free.push(pg_idx);
        }
        Ok(())
    }

    async fn upsert_leaf(
        &mut self,
        key: &[u8],
        value: LeafData<'_>,
        mut pg: PageBuf,
    ) -> Result<InsertResult> {
        let encoded_value = value.encode(self).await?;

        let can_insert = pg.get().remaining() > key.len() + encoded_value.len() + SLOT_SIZE;
        if can_insert {
            let search = search_leaf(pg.get(), key);
            if let SearchResult::Exact(idx) = search {
                remove_at_leaf(pg.get_mut(), idx);
            }
            insert_at_leaf(pg.get_mut(), search.idx(), key, &encoded_value);
            let pg_idx = self.alloc().await?;
            self.db.inner.fio.write(pg_idx, pg).await?;
            Ok(InsertResult::Single(pg_idx))
        } else {
            let left_idx = self.alloc().await?;
            let right_idx = self.alloc().await?;

            todo!("split leaf node and insert")
            // Ok(InsertResult::Split(left, key, right))
        }
    }

    async fn create_value_linked_list(&mut self, value: &[u8]) -> Result<u64> {
        let chunk_size = self.db.inner.meta.page_size() as usize - PAGE_PTR_SIZE - CHECKSUM_SIZE;
        let mut prev_pg_idx = 0u64;
        for chunk in value.chunks(chunk_size).rev() {
            let pg_idx = self.alloc().await?;
            let mut buf = self.db.inner.fio.get_buf();
            let b = buf.get_mut();
            let len = chunk.len();
            b[..len].copy_from_slice(chunk);
            b[len..len + PAGE_PTR_SIZE].copy_from_slice(&prev_pg_idx.to_ne_bytes());
            update_checksum(b);
            prev_pg_idx = pg_idx;
            self.db.inner.fio.write(pg_idx, buf).await?;
        }
        Ok(prev_pg_idx)
    }

    async fn read_value_linked_list(&mut self, mut pg_idx: u64, buf: &mut [u8]) -> Result<()> {
        let mut empty = buf.len();
        while empty > 0 {
            let page = self.db.inner.fio.read(pg_idx).await?;
            let b = page.get();
            validate_checksum(b)?;
            let len = b.len() - PAGE_PTR_SIZE - CHECKSUM_SIZE;
            let cp_len = std::cmp::min(len, empty);
            let cp_start = buf.len() - empty;
            buf[cp_start..cp_start + cp_len].copy_from_slice(&b[0..cp_len]);
            empty -= cp_len;
            pg_idx = b[len..len + PAGE_PTR_SIZE]
                .try_into()
                .map(u64::from_ne_bytes)
                .context("buffer cannot fit page pointer")?;
        }
        Ok(())
    }

    async fn alloc(&mut self) -> Result<u64> {
        self.db
            .inner
            .alloc
            .alloc(&self.db.inner.meta, &mut self.allocs)
            .await
    }
}

#[test]
fn test_access() {
    let buffer = vec![0u8; 4096];
    let slice = &buffer[1..];
    let header = from_bytes::<BTreeRootHeader>(slice);
    assert_eq!({ header.header.kind.clone() }, BTreeNodeKind::Root);
    assert_eq!({ header.version }, 0);
    assert_eq!({ header.header.parent }, 0);
    assert_eq!({ header.sibling }, 0);
    assert_eq!({ header.header.len }, 0);
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use function_name::named;

    use crate::{
        btree::{
            BTreeHeader, BTreeNodeBuf, BTreeRootHeader, LeafDataEncoded, get_key_inner,
            get_key_leaf, get_value_leaf, insert_at_inner, insert_at_leaf, insert_init_inner,
            read_page_ptr, remove_at_leaf,
        },
        test_util::TempDir,
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
        assert_eq!(1, read_page_ptr(&node, 0));

        insert_at_inner(&mut node, 0, 2, b"a", 3);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 2, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, /* ptr 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(2, read_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(3, read_page_ptr(&node, 1));

        insert_at_inner(&mut node, 0, 4, b"b", 5);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 2 */ 3, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'a',
                /* ptr 1 */ 5, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'b', /* ptr 0 */ 4,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(4, read_page_ptr(&node, 0));
        assert_eq!(b"b", get_key_inner(&node, 0));
        assert_eq!(5, read_page_ptr(&node, 1));
        assert_eq!(b"a", get_key_inner(&node, 1));
        assert_eq!(3, read_page_ptr(&node, 2));
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

        assert_eq!(1, read_page_ptr(&node, 0));

        insert_at_inner(&mut node, 0, 2, b"a", 3);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 2, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, /* ptr 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        assert_eq!(2, read_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(3, read_page_ptr(&node, 1));

        insert_at_inner(&mut node, 1, 4, b"b", 5);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 2 */ 5, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'b',
                /* ptr 1 */ 4, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'a', /* ptr 0 */ 2,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        assert_eq!(2, read_page_ptr(&node, 0));
        assert_eq!(b"a", get_key_inner(&node, 0));
        assert_eq!(4, read_page_ptr(&node, 1));
        assert_eq!(b"b", get_key_inner(&node, 1));
        assert_eq!(5, read_page_ptr(&node, 2));
    }

    #[named]
    #[tokio::test]
    async fn test_linked_list() -> Result<()> {
        let dir = TempDir::new(function_name!())?;
        let db = dir.db("db").await?;
        let mut txn = db.txn().begin().await;
        let value_len = txn.db.inner.meta.page_size() as usize * 2.5 as usize;
        let value = vec![1u8; value_len];
        let mut value_test = vec![0u8; value_len];
        let pg_idx = txn.create_value_linked_list(&value).await?;
        txn.read_value_linked_list(pg_idx, &mut value_test).await?;
        assert_eq!(value, value_test);
        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_inner_insert() -> Result<()> {
        let mut page = vec![0u8; 4096];

        {
            page.header_mut().init_inner();
        }
        assert_eq!(0, { page.header().len });

        insert_init_inner(&mut page, 1);
        assert_eq!(1, read_page_ptr(&page, 0));
        assert_eq!(1, { page.header().len });

        insert_at_inner(&mut page, 0, 2, b"AAA", 3);
        assert_eq!(2, read_page_ptr(&page, 0));
        assert_eq!(b"AAA", get_key_inner(&page, 0));
        assert_eq!(3, read_page_ptr(&page, 1));
        assert_eq!(2, { page.header().len });

        insert_at_inner(&mut page, 1, 3, b"BBB", 4);
        assert_eq!(2, read_page_ptr(&page, 0));
        assert_eq!(b"AAA", get_key_inner(&page, 0));
        assert_eq!(3, read_page_ptr(&page, 1));
        assert_eq!(b"BBB", get_key_inner(&page, 1));
        assert_eq!(4, read_page_ptr(&page, 2));
        assert_eq!(3, { page.header().len });

        insert_at_inner(&mut page, 0, 1, b"CCC", 2);
        assert_eq!(1, read_page_ptr(&page, 0));
        assert_eq!(b"CCC", get_key_inner(&page, 0));
        assert_eq!(2, read_page_ptr(&page, 1));
        assert_eq!(b"AAA", get_key_inner(&page, 1));
        assert_eq!(3, read_page_ptr(&page, 2));
        assert_eq!(b"BBB", get_key_inner(&page, 2));
        assert_eq!(4, read_page_ptr(&page, 3));
        assert_eq!(4, { page.header().len });

        insert_at_inner(&mut page, 2, 5, b"DDD", 6);
        assert_eq!(1, read_page_ptr(&page, 0));
        assert_eq!(b"CCC", get_key_inner(&page, 0));
        assert_eq!(2, read_page_ptr(&page, 1));
        assert_eq!(b"AAA", get_key_inner(&page, 1));
        assert_eq!(5, read_page_ptr(&page, 2));
        assert_eq!(b"DDD", get_key_inner(&page, 2));
        assert_eq!(6, read_page_ptr(&page, 3));
        assert_eq!(b"BBB", get_key_inner(&page, 3));
        assert_eq!(4, read_page_ptr(&page, 4));
        assert_eq!(5, { page.header().len });

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn test_single_leaf_insert() -> Result<()> {
        let mut page = vec![0u8; 4096];

        {
            page.header_mut().init_leaf(0);
        }

        assert_eq!(0, { page.header().len });

        insert_at_leaf(
            &mut page,
            0,
            b"K_AAA",
            &LeafDataEncoded::ValueEmbedded(b"V_AAA"),
        );
        assert_eq!(b"K_AAA", get_key_leaf(&page, 0));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 0) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(1, { page.header().len });

        insert_at_leaf(
            &mut page,
            1,
            b"K_BBB",
            &LeafDataEncoded::ValueEmbedded(b"V_BBB"),
        );
        assert_eq!(b"K_AAA", get_key_leaf(&page, 0));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 0) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 1) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(2, { page.header().len });

        insert_at_leaf(
            &mut page,
            0,
            b"K_CCC",
            &LeafDataEncoded::Btree {
                pg_idx_1: 0x6b2a2e7c2ea46f6e,
                pg_idx_2: 0x68d67d9571ec6979,
            },
        );
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0) {
            LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x6b2a2e7c2ea46f6e);
                assert_eq!(pg_idx_2, 0x68d67d9571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 2));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 2) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(3, { page.header().len });

        insert_at_leaf(
            &mut page,
            2,
            b"K_DDD",
            &LeafDataEncoded::ValueLinkedList {
                pg_idx: 0x623c99542265332b,
                len: 0x15c12bbd13ba0a79,
            },
        );
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0) {
            LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x6b2a2e7c2ea46f6e);
                assert_eq!(pg_idx_2, 0x68d67d9571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_DDD", get_key_leaf(&page, 2));
        match get_value_leaf(&page, 2) {
            LeafDataEncoded::ValueLinkedList { pg_idx, len } => {
                assert_eq!(pg_idx, 0x623c99542265332b);
                assert_eq!(len, 0x15c12bbd13ba0a79);
            }
            _ => panic!("expected embedded value"),
        };

        assert_eq!(b"K_BBB", get_key_leaf(&page, 3));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 3) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(4, { page.header().len });

        remove_at_leaf(&mut page, 2);
        assert_eq!(b"K_CCC", get_key_leaf(&page, 0));
        match get_value_leaf(&page, 0) {
            LeafDataEncoded::Btree { pg_idx_1, pg_idx_2 } => {
                assert_eq!(pg_idx_1, 0x6b2a2e7c2ea46f6e);
                assert_eq!(pg_idx_2, 0x68d67d9571ec6979);
            }
            _ => panic!("expected embedded value"),
        };
        assert_eq!(b"K_AAA", get_key_leaf(&page, 1));
        assert_eq!(
            b"V_AAA",
            match get_value_leaf(&page, 1) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(b"K_BBB", get_key_leaf(&page, 2));
        assert_eq!(
            b"V_BBB",
            match get_value_leaf(&page, 2) {
                LeafDataEncoded::ValueEmbedded(v) => v,
                _ => panic!("expected embedded value"),
            }
        );
        assert_eq!(3, { page.header().len });

        Ok(())
    }
}
