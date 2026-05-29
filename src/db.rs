use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use bon::bon;

use crate::{
    alloc::PageAllocator,
    fio::{DEFAULT_CQ_SIZE, DEFAULT_SQ_SIZE, Fio},
    key::{KeyPath, KeyPathBuf},
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
            ops: TxnKeyTrie::new(),
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

struct TxnKeyTrieNode {
    lock_type: LockType,
    children: BTreeMap<Vec<u8>, TxnKeyTrieNode>,
}

/// Benefit of tries is that it automatically merges common prefixes, detects conflicts, and is sorted for locking.
struct TxnKeyTrie {
    root: Option<TxnKeyTrieNode>,
}

impl TxnKeyTrie {
    fn new() -> Self {
        Self { root: None }
    }

    // fn level_order_iter(&self) -> TxnKeyTrieLevelIterator<'_> {
    //     TxnKeyTrieLevelIterator::new(self)
    // }
}

struct TxnKeyTrieInner {
    children: BTreeMap<Vec<u8>, TxnKeyTrie>,
}

impl TxnKeyTrieInner {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }
}

impl TxnKeyTrie {
    fn insert(
        &mut self,
        path: &KeyPath,
        inner_lock_type: LockType,
        leaf_lock_type: LockType,
    ) -> Result<()> {
        let next_lock_type = if path.len() == 0 {
            leaf_lock_type
        } else {
            inner_lock_type
        };

        if self.root.is_some() {
            let node = self.root.as_mut().unwrap();
            node.lock_type = node
                .lock_type
                .is_compatible(&next_lock_type)
                .context("incompatible lock types")?;
        } else {
            self.root = Some(TxnKeyTrieNode {
                lock_type: next_lock_type,
                children: BTreeMap::new(),
            })
        }
        let mut node = self.root.as_mut().unwrap();

        let mut iter = path.into_iter().peekable();
        while let Some(key) = iter.next() {
            let next_lock_type = if iter.peek().is_none() {
                leaf_lock_type
            } else {
                inner_lock_type
            };

            node = if node.children.contains_key(key) {
                let next = node.children.get_mut(key).unwrap();
                node.lock_type = node
                    .lock_type
                    .is_compatible(&next_lock_type)
                    .context("incompatible lock types")?;
                next
            } else {
                node.children.insert(
                    key.to_vec(),
                    TxnKeyTrieNode {
                        lock_type: next_lock_type,
                        children: BTreeMap::new(),
                    },
                );
                node.children.get_mut(key).unwrap()
            }
        }

        Ok(())
    }
}

// struct TxnKeyTrieLevelIterator<'a> {
//     queue: VecDeque<(KeyPathBuf, &'a TxnKeyTrie)>,
// }

// impl<'a> TxnKeyTrieLevelIterator<'a> {
//     fn new(root: &'a TxnKeyTrie) -> Self {
//         let mut queue = VecDeque::new();
//         queue.push_back((KeyPathBuf::new(), root));
//         Self { queue }
//     }
// }

// impl<'a> Iterator for TxnKeyTrieLevelIterator<'a> {
//     type Item = (KeyPathBuf, LockType);

//     fn next(&mut self) -> Option<Self::Item> {
//         while let Some((path, node)) = self.queue.pop_front() {
//             match node {
//                 TxnKeyTrie::Node(lock_type, inner) => {
//                     let result = Some((path.clone(), *lock_type));

//                     for (key_segment, child_node) in &inner.children {
//                         let mut child_path = path.clone();
//                         child_path.append(key_segment);
//                         self.queue.push_back((child_path, child_node));
//                     }

//                     return result;
//                 }
//                 TxnKeyTrie::None => {
//                     // no-op
//                 }
//             }
//         }
//         None
//     }
// }

#[cfg(test)]
mod tests {
    use crate::key_path;

    use super::*;

    #[test]
    fn test_level_order_iterator_with_empty_path() {
        let mut trie = TxnKeyTrie::new();

        // Insert some paths
        trie.insert(
            key_path![b"a", b"b"],
            LockType::ReadChildWrite,
            LockType::Write,
        )
        .unwrap();
        trie.insert(
            key_path![b"a", b"c"],
            LockType::ReadChildWrite,
            LockType::Write,
        )
        .unwrap();
        trie.insert(key_path![b"b"], LockType::Read, LockType::Read)
            .unwrap();
        trie.insert(
            key_path![b"a", b"b", b"d"],
            LockType::ReadChildWrite,
            LockType::Write,
        )
        .unwrap();

        // let results: Vec<(Vec<Vec<u8>>, LockType)> = trie
        //     .level_order_iter()
        //     .map(|(path, lock_type)| {
        //         let segments: Vec<Vec<u8>> = path.into_iter().map(|s| s.to_vec()).collect();
        //         (segments, lock_type)
        //     })
        //     .collect();

        // Level order should be:
        // Level 0: empty path (LockType::Read from root)
        // Level 1: [b"b"]
        // Level 2: [b"a", b"b"], [b"a", b"c"]
        // Level 3: [b"a", b"b", b"d"]
        // assert_eq!(results.len(), 5);

        // // Check empty path first
        // assert_eq!(results[0].0, Vec::<Vec<u8>>::new());

        // // Check first level (b)
        // assert_eq!(results[1].0, vec![vec![b'b']]);

        // // Check second level (a/b and a/c) - order depends on BTreeMap key ordering
        // let second_level_paths: Vec<_> = results[2..4].iter().map(|(p, _)| p.clone()).collect();
        // assert!(second_level_paths.contains(&vec![vec![b'a'], vec![b'b']]));
        // assert!(second_level_paths.contains(&vec![vec![b'a'], vec![b'c']]));

        // // Check third level
        // assert_eq!(results[4].0, vec![vec![b'a'], vec![b'b'], vec![b'd']]);
    }

    // #[test]
    // fn test_empty_trie_with_root() {
    //     let trie = TxnKeyTrie::new();
    //     let results: Vec<_> = trie.level_order_iter().collect();

    //     // Should include the root empty path
    //     assert_eq!(results.len(), 1);
    //     assert_eq!(results[0].0.len(), 0); // empty path
    //     assert_eq!(results[0].1, LockType::Read);
    // }

    // #[test]
    // fn test_single_node() {
    //     let mut trie = TxnKeyTrie::new();
    //     trie.insert(key_path![b"x"], LockType::Read, LockType::Write)
    //         .unwrap();

    //     let results: Vec<_> = trie
    //         .level_order_iter()
    //         .map(|(path, lock_type)| {
    //             (
    //                 path.into_iter().map(|s| s.to_vec()).collect::<Vec<_>>(),
    //                 lock_type,
    //             )
    //         })
    //         .collect();

    //     assert_eq!(results.len(), 2);
    //     assert_eq!(results[0].0, Vec::<Vec<u8>>::new()); // empty path root
    //     assert_eq!(results[0].1, LockType::Read);
    //     assert_eq!(results[1].0, vec![vec![b'x']]);
    //     assert_eq!(results[1].1, LockType::Write);
    // }

    // #[test]
    // fn test_root_lock_type_updates() {
    //     let mut trie = TxnKeyTrie::new();

    //     // Insert a path that should update the root lock type
    //     trie.insert(key_path![b"a"], LockType::Write, LockType::Write)
    //         .unwrap();

    //     let results: Vec<(Vec<Vec<u8>>, LockType)> = trie
    //         .level_order_iter()
    //         .map(|(path, lock_type)| {
    //             let segments: Vec<Vec<u8>> = path.into_iter().map(|s| s.to_vec()).collect();
    //             (segments, lock_type)
    //         })
    //         .collect();

    //     // Root should have been upgraded to Write
    //     assert_eq!(results[0].0, Vec::<Vec<u8>>::new());
    //     assert_eq!(results[0].1, LockType::Write);
    //     assert_eq!(results[1].0, vec![vec![b'a']]);
    //     assert_eq!(results[1].1, LockType::Write);
    // }
}
