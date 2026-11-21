use serde::{Deserialize, Serialize};

use crate::{
    bnode::BNodeKind,
    lalloc::Arena,
    meta::{FmtVer, MagicType},
    uint::{InPgIdx, PgIdx},
};

#[derive(Debug, Serialize, Deserialize)]

enum BlockKind {
    Meta(Meta),
    Root(BNodeRoot),
    Inner(BNodeInner),
    Leaf(BNodeLeaf),
    Free(Free),
    Value(Value),
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BNodeHeader {
    kind: BNodeKind,
    len: InPgIdx,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BNodeRoot {
    header: BNodeHeader,
    version: u64,
    free: PgIdx,
    arena: Arena,
}

#[derive(Debug, Serialize, Deserialize)]
struct BNodeInner {
    header: BNodeHeader,
    slots: Vec<InPgIdx>,
    keys: Vec<Box<[u8]>>,
    pgs: Vec<InPgIdx>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BNodeLeaf {
    header: BNodeHeader,
    slots: Vec<InPgIdx>,
    kvs: Vec<(Box<[u8]>, LeafValue)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LeafValue {
    Btree { pg_idx_1: PgIdx, pg_idx_2: PgIdx },
    ValueEmbedded { loc: usize, len: usize },
    ValueLinkedList { pg_idx: PgIdx, len: PgIdx },
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Free {
    next: PgIdx,
    idxs: Vec<PgIdx>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Value {
    data: Box<[u8]>,
    next: PgIdx,
}
