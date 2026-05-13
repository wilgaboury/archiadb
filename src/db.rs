use std::{collections::BTreeMap, path::Path, sync::Arc};

use anyhow::{Context, Result};
use bon::bon;

use crate::{
    alloc::PageAllocator,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    key::KeyPath,
    lock::LockType,
};

#[derive(Clone)]
struct Db {
    inner: Arc<Inner>,
}

struct Inner {
    alloc: PageAllocator,
    fio: Fio,
}

#[bon]
impl Db {
    #[builder]
    pub async fn new<P: AsRef<Path>>(
        path: P,
        #[builder(default = DEFAULT_SQ_SIZE)] sq: usize,
        #[builder(default = DEFAULT_CQ_SIZE)] cq: usize,
        page_buf_pool: Option<usize>,
        generic_op_state_pool: Option<usize>,
    ) -> Result<Self> {
        let fio = Fio::new(path, sq, cq, page_buf_pool, generic_op_state_pool)?;
        let alloc = PageAllocator::new(fio.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner { alloc, fio }),
        })
    }

    pub fn txn(&self) -> TxnBuilder {
        TxnBuilder {
            db: self.clone(),
            ops: TxnKeyTrie::Leaf,
        }
    }
}

struct TxnBuilder {
    db: Db,
    ops: TxnKeyTrie,
}

impl TxnBuilder {
    pub fn read(mut self, path: &KeyPath) -> Result<Self> {
        self.ops.insert(path, LockType::Read, LockType::Read);
        Ok(self)
    }

    pub fn write(mut self, path: &KeyPath) -> Result<Self> {
        self.ops
            .insert(path, LockType::ReadChildWrite, LockType::Write);
        Ok(self)
    }

    pub fn read_recur(mut self, path: &KeyPath) -> Self {
        self.ops
            .insert(path, LockType::Read, LockType::ReadRecursive);
        self
    }
}

// TODO: implement an iterator

/// Benefit of tries is that it automatically merges common prefixes, detects conflicts, and is correctly sorted for locking.
enum TxnKeyTrie {
    Inner(TxnKeyTrieInner),
    Leaf,
}

struct TxnKeyTrieInner {
    children: BTreeMap<Vec<u8>, (LockType, TxnKeyTrie)>,
}

impl TxnKeyTrie {
    fn insert(&mut self, path: &KeyPath, inner_lock_type: LockType, leaf_lock_type: LockType) {
        let mut node = self;
        let mut iter = path.into_iter().peekable();
        while let Some(key) = iter.next() {
            let lock_type = if iter.peek().is_none() {
                leaf_lock_type
            } else {
                inner_lock_type
            };

            let content: &mut TxnKeyTrieInner = match node {
                TxnKeyTrie::Inner(content) => content,
                TxnKeyTrie::Leaf => {
                    *node = TxnKeyTrie::Inner(TxnKeyTrieInner {
                        children: BTreeMap::new(),
                    });
                    match node {
                        TxnKeyTrie::Inner(content) => content,
                        TxnKeyTrie::Leaf => unreachable!(),
                    }
                }
            };

            if content.children.contains_key(key) {
                let (existing_lock_type, child) = content.children.get_mut(key).unwrap();
                // TODO: add full key path to error message
                *existing_lock_type = existing_lock_type
                    .is_compatible(&lock_type)
                    .context(format!(
                        "Lock type conflict for key {:?}: existing {:?}, new {:?}",
                        key, existing_lock_type, lock_type
                    ))
                    .unwrap();
                node = child;
            } else {
                content
                    .children
                    .insert(key.to_owned(), (lock_type, TxnKeyTrie::Leaf));
                node = &mut content.children.get_mut(key).unwrap().1;
            }
        }
    }
}
