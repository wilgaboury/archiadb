use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result, bail};

use crate::{
    fio::PageBuf,
    key::{KeyPath, KeyPathBuf},
    lock::LockType,
};

/// Benefit of tries is that it automatically merges common prefixes, detects conflicts, and is sorted for locking.
pub(crate) struct TxnKeyTrie {
    root: Option<TxnKeyTrieNode>,
}

struct TxnKeyTrieNode {
    lock_type: LockType,
    dirty: Option<PageBuf>,
    children: BTreeMap<Vec<u8>, TxnKeyTrieNode>,
}

impl TxnKeyTrie {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn level_order_iter(&self) -> TxnKeyTrieLevelIterator<'_> {
        TxnKeyTrieLevelIterator::new(self)
    }

    pub fn insert(&mut self, path: &KeyPath, lock_type: LockType) -> Result<()> {
        let next_lock_type = if path.len() == 0 {
            lock_type
        } else {
            lock_type.inner_node_type()
        };

        if self.root.is_some() {
            let node = self.root.as_mut().unwrap();
            node.lock_type = node
                .lock_type
                .is_compatible(&next_lock_type)
                .context("incompatible lock types")?;
        } else {
            self.root = Some(TxnKeyTrieNode::new(next_lock_type))
        }
        let mut node = self.root.as_mut().unwrap();

        let mut iter = path.into_iter().peekable();
        while let Some(key) = iter.next() {
            let next_lock_type = if iter.peek().is_none() {
                lock_type
            } else {
                lock_type.inner_node_type()
            };

            node = if node.children.contains_key(key) {
                let next = node.children.get_mut(key).unwrap();
                next.lock_type = next
                    .lock_type
                    .is_compatible(&next_lock_type)
                    .context("incompatible lock types")?;
                next
            } else {
                node.children
                    .insert(key.to_vec(), TxnKeyTrieNode::new(next_lock_type));
                node.children.get_mut(key).unwrap()
            }
        }

        Ok(())
    }

    fn validate_read(&self, key_path: &KeyPath) -> Result<()> {
        let mut node = match self.root.as_ref() {
            Some(node) => node,
            None => bail!("Cannot read node outside transaction bounds"),
        };

        for key in key_path.into_iter() {
            if node.lock_type == LockType::ReadRecursive || node.lock_type == LockType::Write {
                return Ok(());
            } else if let Some(next_node) = node.children.get(key) {
                node = next_node;
            } else {
                bail!("Cannot read node outside transaction bounds");
            }
        }

        Ok(())
    }

    fn validate_write(&self, key_path: &KeyPath) -> Result<()> {
        let mut node = match self.root.as_ref() {
            Some(node) => node,
            None => bail!("Cannot read node outside transaction bounds"),
        };

        for key in key_path.into_iter() {
            if node.lock_type == LockType::Write {
                return Ok(());
            } else if node.lock_type != LockType::ReadChildWrite {
                bail!("Cannot write read nodes");
            } else if let Some(next_node) = node.children.get(key) {
                node = next_node;
            } else {
                bail!("Cannot read node outside transaction bounds");
            }
        }

        if node.lock_type != LockType::Write {
            bail!("Cannot write read nodes");
        }

        Ok(())
    }

    fn get(&mut self, key_path: &KeyPath) -> Option<&TxnKeyTrieNode> {
        let mut node = match self.root.as_ref() {
            Some(node) => node,
            None => return None,
        };

        for key in key_path.into_iter() {
            if let Some(next_node) = node.children.get(key) {
                node = next_node;
            } else {
                return None;
            }
        }

        Some(node)
    }

    fn get_mut(&mut self, key_path: &KeyPath) -> Option<&mut TxnKeyTrieNode> {
        let mut node = match self.root.as_mut() {
            Some(node) => node,
            None => return None,
        };

        for key in key_path.into_iter() {
            if let Some(next_node) = node.children.get_mut(key) {
                node = next_node;
            } else {
                return None;
            }
        }

        Some(node)
    }

    /// Lowest common ancestor of dirty nodes
    pub fn dirty_lca(&self) -> Option<KeyPathBuf> {
        let mut first: Option<KeyPathBuf> = None;
        let mut last: Option<KeyPathBuf> = None;
        if let Some(root) = self.root.as_ref() {
            root.find_dfs_dirty_first_last(&mut KeyPathBuf::new(), &mut first, &mut last);
        }

        match (first, last) {
            (None, None) => None,
            (Some(first), None) => Some(first),
            (None, Some(last)) => Some(last),
            (Some(first), Some(last)) => Some(
                first
                    .into_iter()
                    .zip(last.into_iter())
                    .take_while(|(fi, la)| fi == la)
                    .map(|(fi, _)| fi)
                    .fold(KeyPathBuf::new(), |mut buf, k| {
                        buf.push(k);
                        buf
                    }),
            ),
        }
    }
}

impl TxnKeyTrieNode {
    pub fn new(lock_type: LockType) -> Self {
        Self {
            lock_type,
            dirty: None,
            children: BTreeMap::new(),
        }
    }

    fn find_dfs_dirty_first_last(
        &self,
        stack: &mut KeyPathBuf,
        first: &mut Option<KeyPathBuf>,
        last: &mut Option<KeyPathBuf>,
    ) {
        if matches!(self.dirty, Some(_)) {
            if matches!(first, None) {
                *first = Some(stack.clone())
            }
            *last = Some(stack.clone())
        }
        for (key, child) in self.children.iter() {
            stack.push(&key);
            child.find_dfs_dirty_first_last(stack, first, last);
            stack.pop();
        }
    }
}

pub(crate) struct TxnKeyTrieLevelIterator<'a> {
    queue: VecDeque<(KeyPathBuf, &'a TxnKeyTrieNode)>,
}

impl<'a> TxnKeyTrieLevelIterator<'a> {
    pub fn new(trie: &'a TxnKeyTrie) -> Self {
        let mut queue = VecDeque::new();
        if let Some(root) = trie.root.as_ref() {
            queue.push_back((KeyPathBuf::new(), root));
        }
        Self { queue }
    }
}

impl<'a> Iterator for TxnKeyTrieLevelIterator<'a> {
    type Item = (KeyPathBuf, LockType);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((path, node)) = self.queue.pop_front() {
            let result = Some((path.clone(), node.lock_type));

            for (key_segment, child_node) in &node.children {
                let mut child_path = path.clone();
                child_path.push(key_segment);
                self.queue.push_back((child_path, child_node));
            }

            return result;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::key_path;

    use super::*;

    #[test]
    fn test_level_order_iterator_with_empty_path() {
        let mut trie = TxnKeyTrie::new();

        trie.insert(key_path![], LockType::Read).unwrap();
        trie.insert(key_path![b"a", b"b"], LockType::Write).unwrap();
        trie.insert(key_path![b"a", b"c"], LockType::Write).unwrap();
        trie.insert(key_path![b"b"], LockType::Read).unwrap();

        assert!(matches!(
            trie.insert(key_path![b"a", b"b", b"d"], LockType::Write,),
            Err(_)
        ));

        let results: Vec<(KeyPathBuf, LockType)> = trie.level_order_iter().collect();
        let expected = [
            (key_path![].to_owned(), LockType::ReadChildWrite),
            (key_path![b"a"].to_owned(), LockType::ReadChildWrite),
            (key_path![b"b"].to_owned(), LockType::Read),
            (key_path![b"a", b"b"].to_owned(), LockType::Write),
            (key_path![b"a", b"c"].to_owned(), LockType::Write),
        ];

        assert_eq!(expected.to_vec(), results);

        let expected_keys = [
            key_path![].to_owned(),
            key_path![b"a"].to_owned(),
            key_path![b"b"].to_owned(),
            key_path![b"a", b"b"].to_owned(),
            key_path![b"a", b"c"].to_owned(),
        ];
        let mut keys: Vec<KeyPathBuf> = results.iter().map(|(key, _)| key.clone()).collect();
        keys.sort();
        assert_eq!(expected_keys.to_vec(), keys);
    }

    #[test]
    fn test_read_write_validation() {
        let mut trie = TxnKeyTrie::new();

        trie.insert(key_path![b"read", b"read_recur"], LockType::ReadRecursive)
            .unwrap();
        trie.insert(key_path![b"read", b"write"], LockType::Write)
            .unwrap();
        trie.insert(key_path![b"read", b"read"], LockType::Read)
            .unwrap();

        {
            let n = trie.get(key_path![b"read"]).unwrap();
            assert_eq!(n.lock_type, LockType::ReadChildWrite);
            let n = trie.get(key_path![b"read", b"read_recur"]).unwrap();
            assert_eq!(n.lock_type, LockType::ReadRecursive)
        }

        {
            let n = trie.get_mut(key_path![b"read"]).unwrap();
            assert_eq!(n.lock_type, LockType::ReadChildWrite);
            let n = trie.get_mut(key_path![b"read", b"read_recur"]).unwrap();
            assert_eq!(n.lock_type, LockType::ReadRecursive)
        }

        assert!(trie.validate_read(&key_path![b"read"]).is_ok());
        assert!(
            trie.validate_read(&key_path![b"read", b"read_recur"])
                .is_ok()
        );
        assert!(trie.validate_write(&key_path![b"read"]).is_err());
        assert!(
            trie.validate_write(&key_path![b"read", b"read_recur"])
                .is_err()
        );
        assert!(
            trie.validate_write(&key_path![b"read", b"read_recur", b"subkey"])
                .is_err()
        );
        assert!(
            trie.validate_read(&key_path![b"read", b"read_recur", b"subkey"])
                .is_ok()
        );
        assert!(trie.validate_read(&key_path![b"read", b"write"]).is_ok());
        assert!(
            trie.validate_read(&key_path![b"read", b"write", b"subkey"])
                .is_ok()
        );
        assert!(
            trie.validate_write(&key_path![b"read", b"write", b"subkey"])
                .is_ok()
        );

        assert!(trie.validate_read(&key_path![]).is_ok());
        assert!(trie.validate_write(&key_path![]).is_err());

        assert!(trie.validate_read(&key_path![b"rand"]).is_err());
        assert!(trie.validate_write(&key_path![b"rand"]).is_err());
    }

    #[test]
    fn test_dirty_lca() {
        // todo!("test")
    }
}
