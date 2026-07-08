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

enum LeafData<'a> {
    Btree { pg_idx_1: u64, pg_idx_2: u64 },
    ValueEmbedded(&'a [u8]),
    ValueLinkedList { pg_idx: u64, len: u64 },
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
        buf[key_and_ptr_start..key_start].copy_from_slice(&left.to_ne_bytes());
        buf[key_start..key_and_ptr_end].copy_from_slice(key);
        buf[key_and_ptr_end..key_and_ptr_end + PAGE_PTR_SIZE].copy_from_slice(&right.to_ne_bytes());

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
        let header = buf.header_mut();
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
        key_idx - PAGE_PTR_SIZE - CHECKSUM_SIZE
    } else {
        key_idx - read_slot(buf, idx - 1) - PAGE_PTR_SIZE
    } as usize;

    &buf[key_idx..(key_idx + key_len)]
}

fn get_key_leaf(buf: &[u8], idx: usize) -> &[u8] {
    let val_key_idx = read_slot(buf, idx);
    let val_len = 1 + match LeafDataKind::from(buf[val_key_idx]) {
        LeafDataKind::Btree => 2 * PAGE_PTR_SIZE,
        LeafDataKind::ValueEmbedded => buf[val_key_idx + 1] as usize,
        LeafDataKind::ValueLinkedList => PAGE_PTR_SIZE + LINKED_LIST_VALUE_LEN_SIZE,
    };
    let key_idx = val_key_idx + val_len;
    let key_len = if idx == 0 {
        key_idx - CHECKSUM_SIZE
    } else {
        key_idx - read_slot(buf, idx - 1)
    } as usize;

    &buf[key_idx..(key_idx + key_len)]
}

fn get_value_leaf(buf: &[u8], idx: usize) -> LeafData<'_> {
    let val_key_idx = read_slot(buf, idx);
    match LeafDataKind::from(buf[val_key_idx]) {
        LeafDataKind::Btree => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + PAGE_PTR_SIZE;
            let pg_idx_1 = buf[b_idx_1..b_idx_2 + PAGE_PTR_SIZE]
                .try_into()
                .map(PagePtr::from_ne_bytes)
                .expect("buffer cannot fit page pointer");
            let pg_idx_2 = buf[b_idx_2..b_idx_2 + PAGE_PTR_SIZE]
                .try_into()
                .map(PagePtr::from_ne_bytes)
                .expect("buffer cannot fit page pointer");
            LeafData::Btree { pg_idx_1, pg_idx_2 }
        }
        LeafDataKind::ValueEmbedded => {
            let len_idx = val_key_idx + 1;
            let len = buf[val_key_idx + 1] as usize;
            let value_idx = len_idx + 1;
            let value = &buf[value_idx..value_idx + len];
            LeafData::ValueEmbedded(value)
        }
        LeafDataKind::ValueLinkedList => {
            let b_idx_1 = val_key_idx + 1;
            let b_idx_2 = b_idx_1 + PAGE_PTR_SIZE;
            let pg_idx = buf[b_idx_1..b_idx_2 + PAGE_PTR_SIZE]
                .try_into()
                .map(PagePtr::from_ne_bytes)
                .expect("buffer cannot fit page pointer");
            let len = buf[b_idx_2..b_idx_2 + size_of::<u64>()]
                .try_into()
                .map(u64::from_ne_bytes)
                .expect("buffer cannot fit u64");
            LeafData::ValueLinkedList { pg_idx, len }
        }
    }
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
    let header = from_bytes::<BTreeHeader>(buf);

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
        value: &[u8],
        pg_idx: u64,
        pg: PageBuf,
    ) -> Result<()> {
        let db = &self.db.inner;
        let header = from_bytes::<BTreeHeader>(pg.get());
        if header.kind == BTreeNodeKind::Leaf {
            let ret = self.insert_leaf(key, value, pg).await?;
            self.free.push(pg_idx);
        }
        Ok(())
    }

    async fn insert_leaf(
        &mut self,
        key: &[u8],
        value: &[u8],
        mut pg: PageBuf,
    ) -> Result<InsertResult> {
        // let new_pg_idx = self.db.alloc.alloc(&db.meta, &mut self.allocs).await?;

        let header = pg.get_mut().header_mut();

        Ok(InsertResult::Single(0))
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
        btree::{BTreeHeader, insert_at_inner, insert_init_inner, read_page_ptr},
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
                0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(3, read_page_ptr(&node, 0));
        assert_eq!(2, read_page_ptr(&node, 1));

        insert_at_inner(&mut node, 0, 4, b"b", 5);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'a',
                /* ptr 1 */ 4, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'b', /* ptr 2 */ 5,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(5, read_page_ptr(&node, 0));
        assert_eq!(4, read_page_ptr(&node, 1));
        assert_eq!(2, read_page_ptr(&node, 2));
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

        insert_at_inner(&mut node, 0, 2, b"a", 3);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 2, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, /* ptr 0 */ 2, 0, 0, 0, 0, 0, 0, 0,
                /* key 0 */ b'a', /* ptr 1 */ 3, 0, 0, 0, 0, 0, 0, 0,
                /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        insert_at_inner(&mut node, 1, 4, b"b", 5);

        assert_eq!(
            node,
            [
                /* kind */ 1u8, /* parent */ 0, 0, 0, 0, 0, 0, 0, 0, /* len */ 3, 0,
                0, 0, /* slot 0 */ 47, 0, 0, 0, /* slot 1 */ 38, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, /* ptr 0 */ 4, 0, 0, 0, 0, 0, 0, 0, /* key 0 */ b'b',
                /* ptr 1 */ 5, 0, 0, 0, 0, 0, 0, 0, /* key 1 */ b'a', /* ptr 2 */ 3,
                0, 0, 0, 0, 0, 0, 0, /* checksum */ 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
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
}
