use std::{collections::BTreeMap, fs::OpenOptions, path::Path};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    bio::BlockIo,
    bnode::{
        self, BNodeKind, BNodeRootHeader, get_key_inner, get_key_leaf, get_page_ptr,
        get_value_leaf, in_pg_len, in_pg_size_of, read_slot, slots_len,
    },
    flux::Flux,
    lalloc::Arena,
    meta::{FmtVer, MagicType},
    uint::{InPgIdx, InPgIdxDisk, PgIdx, PgIdxDisk},
    util::from_bytes,
};

#[derive(Debug, Clone, Serialize, Deserialize)]

enum BlockKind {
    Meta(Meta),
    Root(BNodeRoot),
    Inner(BNodeInner),
    Leaf(BNodeLeaf),
    Free(Free),
    Value(Value),
}

impl BlockKind {
    pub(crate) fn bnode_from(buf: &[u8]) -> Self {
        let kind = from_bytes::<bnode::BNodeKind>(buf);
        match *kind {
            BNodeKind::Root => BlockKind::Root(BNodeRoot::from(buf)),
            BNodeKind::Inner => BlockKind::Inner(BNodeInner::from(buf)),
            BNodeKind::Leaf => BlockKind::Leaf(BNodeLeaf::from(buf)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Meta {
    pub(crate) magic: MagicType,

    pub(crate) fmt_ver: FmtVer,
    pub(crate) pg_size: InPgIdx,
    pub(crate) root1: PgIdx,
    pub(crate) root2: PgIdx,

    pub(crate) version: u64,
    pub(crate) open: u8,
    pub(crate) len: PgIdx,

    // global alloc data
    pub(crate) galloc_fidx: PgIdx,
    pub(crate) galloc_bidx: PgIdx,
    pub(crate) galloc_len: PgIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BNodeHeader {
    pub(crate) kind: BNodeKind,
    pub(crate) len: InPgIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BNodeRoot {
    header: BNodeHeader,
    version: u64,
    free: PgIdx,
    arena: Arena,
    content: BNodeInnerContent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BNodeInner {
    pub(crate) header: BNodeHeader,
    pub(crate) content: BNodeInnerContent,
}

impl BNodeInner {
    pub(crate) fn remove_at(&mut self, idx: InPgIdx) {
        if self.header.len == 0 {
            return;
        } else if idx == self.header.len - 1 {
            self.header.len -= 1;
            self.content.slots.pop();
            self.content.keys.pop();
            self.content.pgs.pop();
            return;
        } else if idx >= self.header.len {
            panic!("cannot remove past end");
        }

        self.header.len -= 1;
        self.content.slots.remove(idx as usize);
        let removed_key = self.content.keys.remove(idx as usize);
        self.content.pgs.remove(idx as usize);

        for i in idx as usize..self.content.slots.len() {
            self.content.slots[i] += in_pg_len(&removed_key) + in_pg_size_of::<PgIdxDisk>();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BNodeInnerContent {
    pub(crate) slots: Vec<InPgIdx>,
    pub(crate) keys: Vec<Box<[u8]>>,
    pub(crate) pgs: Vec<InPgIdx>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BNodeLeaf {
    header: BNodeHeader,
    slots: Vec<InPgIdx>,
    kvs: Vec<(Box<[u8]>, LeafValue)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum LeafValue {
    Btree {
        pg_idx_1: PgIdx,
        pg_idx_2: PgIdx,
    },
    Embedded {
        loc: usize,
        len: usize,
        value: Box<[u8]>,
    },
    LinkedList {
        pg_idx: PgIdx,
        len: PgIdx,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Free {
    next: PgIdx,
    idxs: Vec<PgIdx>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Value {
    data: Box<[u8]>,
    next: PgIdx,
}

impl From<&[u8]> for BNodeHeader {
    fn from(buf: &[u8]) -> Self {
        let buf_head = from_bytes::<bnode::BNodeHeader>(buf);
        Self {
            kind: buf_head.kind,
            len: buf_head.len(),
        }
    }
}

impl From<&[u8]> for BNodeRoot {
    fn from(buf: &[u8]) -> Self {
        let buf_head = from_bytes::<BNodeRootHeader>(buf);
        let header = BNodeHeader::from(buf);
        let content = BNodeInnerContent::from(buf);

        Self {
            header,
            version: buf_head.version.get(),
            free: buf_head.free.get(),
            arena: buf_head.arena.to_mem(),
            content,
        }
    }
}

impl From<&[u8]> for BNodeInner {
    fn from(buf: &[u8]) -> Self {
        let header = BNodeHeader::from(buf);
        let content = BNodeInnerContent::from(buf);

        Self { header, content }
    }
}

impl From<&[u8]> for BNodeInnerContent {
    fn from(buf: &[u8]) -> Self {
        let header = from_bytes::<bnode::BNodeHeader>(buf);

        let slots_len = slots_len(buf);
        let mut slots = Vec::with_capacity(slots_len as usize);
        for i in 0..slots_len {
            slots.push(read_slot(buf, i));
        }

        let mut keys = Vec::with_capacity(slots_len as usize);
        for i in 0..slots_len {
            keys.push(get_key_inner(buf, i).to_vec().into_boxed_slice());
        }

        let mut pgs = Vec::with_capacity(header.len.get() as usize);
        for i in 0..header.len.get() {
            pgs.push(get_page_ptr(buf, i));
        }

        Self { slots, keys, pgs }
    }
}

impl From<&[u8]> for BNodeLeaf {
    fn from(buf: &[u8]) -> Self {
        let header = BNodeHeader::from(buf);

        let mut slots = Vec::with_capacity(header.len as usize);
        for i in 0..header.len {
            slots.push(read_slot(buf, i));
        }

        let mut kvs = Vec::with_capacity(header.len as usize);
        for i in 0..header.len {
            let buf_val = get_value_leaf(buf, i);
            let val = match buf_val {
                bnode::LeafValueGetResult::Btree { pg_idx_1, pg_idx_2 } => {
                    LeafValue::Btree { pg_idx_1, pg_idx_2 }
                }
                bnode::LeafValueGetResult::ValueEmbedded { loc, len } => LeafValue::Embedded {
                    loc,
                    len,
                    value: buf[loc..loc + len].to_vec().into_boxed_slice(),
                },
                bnode::LeafValueGetResult::ValueLinkedList { pg_idx, len } => {
                    LeafValue::LinkedList { pg_idx, len }
                }
            };

            kvs.push((get_key_leaf(buf, i).to_vec().into_boxed_slice(), val));
        }

        Self { header, slots, kvs }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BTreeResolved {
    root: BNodeRoot,
    blocks: BTreeMap<PgIdx, BlockKind>,
}

impl BTreeResolved {
    pub(crate) async fn create<B: BlockIo>(root_buf: &[u8], flux: &mut Flux<B>) -> Result<Self> {
        let root = BNodeRoot::from(root_buf);
        let mut blocks = BTreeMap::new();

        for idx in root.content.pgs.iter() {
            Self::resolve_recurse(*idx, flux, &mut blocks).await?;
        }

        Ok(Self { root, blocks })
    }

    pub(crate) async fn resolve_recurse<B: BlockIo>(
        idx: PgIdx,
        flux: &mut Flux<B>,
        blocks: &mut BTreeMap<PgIdx, BlockKind>,
    ) -> Result<()> {
        let buf = flux.read(idx).await?;
        let kind = BlockKind::bnode_from(buf.as_ref());
        blocks.insert(idx, kind.clone());

        let pgs = match &kind {
            BlockKind::Root(bnode_root) => &bnode_root.content.pgs,
            BlockKind::Inner(bnode_inner) => &bnode_inner.content.pgs,
            _ => return Ok(()),
        };
        for pg in pgs.iter() {
            Box::pin(Self::resolve_recurse(*pg, flux, blocks)).await?;
        }

        Ok(())
    }
}

pub(crate) fn dumb_log<P: AsRef<Path>>(path: P, str: &str) {
    use std::io::Write;

    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{}", str);
    }
}

pub(crate) async fn dumb_log_btree<P: AsRef<Path>, B: BlockIo>(
    path: P,
    root: &[u8],
    flux: &mut Flux<B>,
) {
    let tree = BTreeResolved::create(root, flux).await.unwrap();
    let string = serde_json::to_string(&tree).unwrap();
    dumb_log(path, &string);
}
