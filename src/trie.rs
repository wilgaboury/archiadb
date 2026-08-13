use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    marker::PhantomData,
};

use anyhow::{Context, Result, bail};

use crate::{
    key::{KeyPath, KeyPathBuf},
    lock::LockType,
};

/// Benefit of tries is that it automatically merges common prefixes, detects conflicts, and is sorted for locking.
pub(crate) struct KeyTrie<T> {
    root: Option<KeyTrieNode<T>>,
}

pub(crate) struct KeyTrieNode<T> {
    value: T,
    children: BTreeMap<Vec<u8>, KeyTrieNode<T>>,
}

impl<T> KeyTrie<T> {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn upsert<F: Fn() -> T>(&mut self, path: &KeyPath, value_inter: F, value: T) -> Result<()> {
        let mut node = match self.root.as_mut() {
            Some(node) => node,
            None => {
                if path.len() > 0 {
                    self.root = Some(KeyTrieNode::new(value_inter()));
                    self.root.as_mut().unwrap()
                } else {
                    self.root = Some(KeyTrieNode::new(value));
                    return Ok(());
                }
            }
        };

        let mut iter = path.into_iter().peekable();
        while let Some(key) = iter.next() {
            if iter.peek().is_some() {
                if !node.children.contains_key(key) {
                    node.children
                        .insert(key.to_vec(), KeyTrieNode::new(value_inter()));
                }
            } else {
                if node.children.contains_key(key) {
                    node.children.get_mut(key).unwrap().value = value;
                } else {
                    node.children.insert(key.to_vec(), KeyTrieNode::new(value));
                }
                break;
            }
            node = node.children.get_mut(key).unwrap()
        }

        Ok(())
    }

    fn get(&mut self, key_path: &KeyPath) -> Option<&T> {
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

        Some(&node.value)
    }

    fn get_mut(&mut self, key_path: &KeyPath) -> Option<&mut T> {
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

        Some(&mut node.value)
    }

    /// Lowest common ancestor of dirty nodes
    pub fn lca<F: Fn(&T) -> bool>(&self, cond: F) -> Option<KeyPathBuf> {
        let mut first: Option<KeyPathBuf> = None;
        let mut last: Option<KeyPathBuf> = None;
        if let Some(root) = self.root.as_ref() {
            root.find_dfs_dirty_first_last(cond, &mut KeyPathBuf::new(), &mut first, &mut last);
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

    pub fn bfs_iter(&self) -> KeyTrieBfsIter<'_, T> {
        KeyTrieBfsIter::new(self)
    }

    pub fn bfs_iter_mut(&mut self) -> KeyTrieBfsIterMut<'_, T> {
        KeyTrieBfsIterMut::new(self)
    }

    pub fn dfs_iter(&self) -> KeyTrieDfsIter<'_, T> {
        KeyTrieDfsIter::new(self)
    }

    pub fn dfs_iter_mut(&mut self) -> KeyTrieDfsIterMut<'_, T> {
        KeyTrieDfsIterMut::new(self)
    }
}

impl KeyTrie<LockType> {
    pub fn insert_lock(&mut self, path: &KeyPath, lock_type: LockType) -> Result<()> {
        let next_lock_type = if path.len() == 0 {
            lock_type
        } else {
            lock_type.inner_node_type()
        };

        if self.root.is_some() {
            let node = self.root.as_mut().unwrap();
            node.value = node
                .value
                .is_compatible(&next_lock_type)
                .context("incompatible lock types")?;
        } else {
            self.root = Some(KeyTrieNode::new(next_lock_type))
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
                next.value = next
                    .value
                    .is_compatible(&next_lock_type)
                    .context("incompatible lock types")?;
                next
            } else {
                node.children
                    .insert(key.to_vec(), KeyTrieNode::new(next_lock_type));
                node.children.get_mut(key).unwrap()
            }
        }

        Ok(())
    }

    pub(crate) fn validate_read(&self, key_path: &KeyPath) -> Result<()> {
        let mut node = match self.root.as_ref() {
            Some(node) => node,
            None => bail!("Cannot read node outside transaction bounds"),
        };

        for key in key_path.into_iter() {
            if node.value == LockType::ReadRecursive || node.value == LockType::Write {
                return Ok(());
            } else if let Some(next_node) = node.children.get(key) {
                node = next_node;
            } else {
                bail!("Cannot read node outside transaction bounds");
            }
        }

        Ok(())
    }

    pub(crate) fn validate_write(&self, key_path: &KeyPath) -> Result<()> {
        let mut node = match self.root.as_ref() {
            Some(node) => node,
            None => bail!("Cannot read node outside transaction bounds"),
        };

        for key in key_path.into_iter() {
            if node.value == LockType::Write {
                return Ok(());
            } else if node.value != LockType::ReadChildWrite {
                bail!("Cannot write read nodes");
            } else if let Some(next_node) = node.children.get(key) {
                node = next_node;
            } else {
                bail!("Cannot read node outside transaction bounds");
            }
        }

        if node.value != LockType::Write {
            bail!("Cannot write read nodes");
        }

        Ok(())
    }
}

impl<T> KeyTrieNode<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            children: BTreeMap::new(),
        }
    }

    fn find_dfs_dirty_first_last<F: Fn(&T) -> bool>(
        &self,
        mut cond: F,
        stack: &mut KeyPathBuf,
        first: &mut Option<KeyPathBuf>,
        last: &mut Option<KeyPathBuf>,
    ) -> F {
        if cond(&self.value) {
            if matches!(first, None) {
                *first = Some(stack.clone())
            }
            *last = Some(stack.clone())
        }
        for (key, child) in self.children.iter() {
            stack.push(&key);
            cond = child.find_dfs_dirty_first_last(cond, stack, first, last);
            stack.pop();
        }
        cond
    }
}

pub(crate) struct KeyTrieDfsIter<'a, T> {
    stack: Vec<(KeyPathBuf, *const KeyTrieNode<T>)>,
    visited: HashSet<*const KeyTrieNode<T>>,
    _phantom: PhantomData<&'a KeyTrie<T>>,
}

impl<'a, T> KeyTrieDfsIter<'a, T> {
    pub fn new(trie: &KeyTrie<T>) -> Self {
        let mut ret = Self {
            stack: Vec::new(),
            visited: HashSet::new(),
            _phantom: PhantomData::default(),
        };

        dfs_iter_init(trie, &mut ret.stack, &mut ret.visited);

        ret
    }
}

impl<'a, T> Iterator for KeyTrieDfsIter<'a, T> {
    type Item = (KeyPathBuf, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, cur) = dfs_step(&mut self.stack, &mut self.visited)?;
        Some((path, unsafe { &(*cur).value }))
    }
}

pub(crate) struct KeyTrieDfsIterMut<'a, T> {
    stack: Vec<(KeyPathBuf, *const KeyTrieNode<T>)>,
    visited: HashSet<*const KeyTrieNode<T>>,
    _phantom: PhantomData<&'a KeyTrie<T>>,
}

impl<'a, T> KeyTrieDfsIterMut<'a, T> {
    pub fn new(trie: &mut KeyTrie<T>) -> Self {
        let mut ret = Self {
            stack: Vec::new(),
            visited: HashSet::new(),
            _phantom: PhantomData::default(),
        };

        dfs_iter_init(trie, &mut ret.stack, &mut ret.visited);

        ret
    }
}

impl<'a, T> Iterator for KeyTrieDfsIterMut<'a, T> {
    type Item = (KeyPathBuf, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, cur) = dfs_step(&mut self.stack, &mut self.visited)?;
        Some((path, unsafe { &mut (*(cur as *mut KeyTrieNode<T>)).value }))
    }
}

fn dfs_iter_init<T>(
    trie: &KeyTrie<T>,
    stack: &mut Vec<(KeyPathBuf, *const KeyTrieNode<T>)>,
    visited: &mut HashSet<*const KeyTrieNode<T>>,
) {
    if let Some(root) = trie.root.as_ref() {
        let root = root as *const _;
        stack.push((KeyPathBuf::new(), root));
        visited.insert(root);
    }
}

fn dfs_step<T>(
    stack: &mut Vec<(KeyPathBuf, *const KeyTrieNode<T>)>,
    visited: &mut HashSet<*const KeyTrieNode<T>>,
) -> Option<(KeyPathBuf, *const KeyTrieNode<T>)> {
    let (path, cur) = stack.pop()?;
    let cur = unsafe { &mut *(cur as *mut KeyTrieNode<T>) };

    for (key, child) in cur.children.iter().rev() {
        let child = child as *const _;
        if !visited.contains(&child) {
            let mut child_path = path.clone();
            child_path.push(key);
            stack.push((child_path, child));
            visited.insert(child);
        }
    }

    Some((path, cur))
}

pub(crate) struct KeyTrieBfsIter<'a, T> {
    queue: VecDeque<(KeyPathBuf, *const KeyTrieNode<T>)>,
    _phantom: PhantomData<&'a KeyTrie<T>>,
}

impl<'a, T> KeyTrieBfsIter<'a, T> {
    pub fn new(trie: &'a KeyTrie<T>) -> Self {
        let mut ret = Self {
            queue: VecDeque::new(),
            _phantom: PhantomData::default(),
        };

        bfs_iter_init(trie, &mut ret.queue);

        ret
    }
}

impl<'a, T> Iterator for KeyTrieBfsIter<'a, T> {
    type Item = (KeyPathBuf, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, node) = bfs_step(&mut self.queue)?;
        Some((path, unsafe { &(*node).value }))
    }
}

pub(crate) struct KeyTrieBfsIterMut<'a, T> {
    queue: VecDeque<(KeyPathBuf, *const KeyTrieNode<T>)>,
    _phantom: PhantomData<&'a KeyTrie<T>>,
}

impl<'a, T> KeyTrieBfsIterMut<'a, T> {
    pub fn new(trie: &'a mut KeyTrie<T>) -> Self {
        let mut ret = Self {
            queue: VecDeque::new(),
            _phantom: PhantomData::default(),
        };

        bfs_iter_init(trie, &mut ret.queue);

        ret
    }
}

impl<'a, T> Iterator for KeyTrieBfsIterMut<'a, T> {
    type Item = (KeyPathBuf, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, node) = bfs_step(&mut self.queue)?;
        Some((path, unsafe { &mut (*(node as *mut KeyTrieNode<T>)).value }))
    }
}

fn bfs_iter_init<T>(trie: &KeyTrie<T>, queue: &mut VecDeque<(KeyPathBuf, *const KeyTrieNode<T>)>) {
    if let Some(root) = trie.root.as_ref() {
        queue.push_back((KeyPathBuf::new(), root as *const _));
    }
}

fn bfs_step<T>(
    queue: &mut VecDeque<(KeyPathBuf, *const KeyTrieNode<T>)>,
) -> Option<(KeyPathBuf, *const KeyTrieNode<T>)> {
    while let Some((path, node)) = queue.pop_front() {
        let result = Some((path.clone(), node));

        for (key_segment, child_node) in unsafe { &*node }.children.iter() {
            let mut child_path = path.clone();
            child_path.push(key_segment);
            queue.push_back((child_path, child_node as *const _));
        }

        return result;
    }
    None
}

#[cfg(test)]
mod tests {
    use crate::key_path;

    use super::*;

    #[test]
    fn test_level_order_iterator_with_empty_path() {
        let mut trie = KeyTrie::new();

        trie.insert_lock(key_path![], LockType::Read).unwrap();
        trie.insert_lock(key_path![b"a", b"b"], LockType::Write)
            .unwrap();
        trie.insert_lock(key_path![b"a", b"c"], LockType::Write)
            .unwrap();
        trie.insert_lock(key_path![b"b"], LockType::Read).unwrap();

        assert!(matches!(
            trie.insert_lock(key_path![b"a", b"b", b"d"], LockType::Write,),
            Err(_)
        ));

        let results: Vec<(KeyPathBuf, LockType)> = trie
            .bfs_iter()
            .map(|(path, lock_type)| (path, *lock_type))
            .collect();
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
        let mut trie = KeyTrie::new();

        trie.insert_lock(key_path![b"read", b"read_recur"], LockType::ReadRecursive)
            .unwrap();
        trie.insert_lock(key_path![b"read", b"write"], LockType::Write)
            .unwrap();
        trie.insert_lock(key_path![b"read", b"read"], LockType::Read)
            .unwrap();

        {
            let lock_type = trie.get(key_path![b"read"]).unwrap();
            assert_eq!(*lock_type, LockType::ReadChildWrite);
            let lock_type = trie.get(key_path![b"read", b"read_recur"]).unwrap();
            assert_eq!(*lock_type, LockType::ReadRecursive)
        }

        {
            let lock_type = trie.get_mut(key_path![b"read"]).unwrap();
            assert_eq!(*lock_type, LockType::ReadChildWrite);
            let lock_type = trie.get_mut(key_path![b"read", b"read_recur"]).unwrap();
            assert_eq!(*lock_type, LockType::ReadRecursive)
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
    fn test_lca() -> Result<()> {
        let mut trie = KeyTrie::new();

        trie.upsert(key_path![b"1", b"2", b"4"], || false, false)?;
        trie.upsert(key_path![b"1", b"2", b"5"], || false, false)?;
        trie.upsert(key_path![b"1", b"3", b"6"], || false, false)?;
        trie.upsert(key_path![b"1", b"3", b"7"], || false, false)?;

        *trie.get_mut(key_path![b"1", b"2", b"4"]).unwrap() = true;

        assert_eq!(
            key_path![b"1", b"2", b"4"],
            trie.lca(|v| *v).unwrap().as_path()
        );

        *trie.get_mut(key_path![b"1", b"2", b"5"]).unwrap() = true;

        assert_eq!(key_path![b"1", b"2"], trie.lca(|v| *v).unwrap().as_path());

        *trie.get_mut(key_path![b"1", b"3", b"7"]).unwrap() = true;

        assert_eq!(key_path![b"1"], trie.lca(|v| *v).unwrap().as_path());

        *trie.get_mut(key_path![b"1", b"3", b"6"]).unwrap() = true;

        assert_eq!(key_path![b"1"], trie.lca(|v| *v).unwrap().as_path());

        trie.upsert(key_path![b"8"], || false, false)?;
        *trie.get_mut(key_path![b"8"]).unwrap() = true;

        assert_eq!(key_path![], trie.lca(|v| *v).unwrap().as_path());

        for (_, v) in trie.dfs_iter_mut() {
            *v = false;
        }

        assert!(trie.lca(|v| *v).is_none());

        Ok(())
    }

    #[test]
    fn test_iter() -> Result<()> {
        let mut trie = KeyTrie::new();

        trie.insert_lock(key_path![b"1", b"2"], LockType::Write)?;
        trie.insert_lock(key_path![b"3", b"4"], LockType::Read)?;

        let mut iter = trie.dfs_iter().map(|(buf, lock_type)| (buf, *lock_type));
        assert_eq!(
            iter.next().unwrap(),
            (key_path![].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1"].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1", b"2"].to_owned(), LockType::Write)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3"].to_owned(), LockType::Read)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3", b"4"].to_owned(), LockType::Read)
        );
        assert!(iter.next().is_none());

        let mut iter = trie
            .dfs_iter_mut()
            .map(|(buf, lock_type)| (buf, *lock_type));
        assert_eq!(
            iter.next().unwrap(),
            (key_path![].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1"].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1", b"2"].to_owned(), LockType::Write)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3"].to_owned(), LockType::Read)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3", b"4"].to_owned(), LockType::Read)
        );
        assert!(iter.next().is_none());

        let mut iter = trie.bfs_iter().map(|(buf, lock_type)| (buf, *lock_type));
        assert_eq!(
            iter.next().unwrap(),
            (key_path![].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1"].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3"].to_owned(), LockType::Read)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1", b"2"].to_owned(), LockType::Write)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3", b"4"].to_owned(), LockType::Read)
        );
        assert!(iter.next().is_none());

        let mut iter = trie
            .bfs_iter_mut()
            .map(|(buf, lock_type)| (buf, *lock_type));
        assert_eq!(
            iter.next().unwrap(),
            (key_path![].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1"].to_owned(), LockType::ReadChildWrite)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3"].to_owned(), LockType::Read)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"1", b"2"].to_owned(), LockType::Write)
        );
        assert_eq!(
            iter.next().unwrap(),
            (key_path![b"3", b"4"].to_owned(), LockType::Read)
        );
        assert!(iter.next().is_none());

        Ok(())
    }

    #[test]
    fn try_all_lock_compatability() {
        let mut trie = KeyTrie::new();
        trie.insert_lock(key_path![], LockType::Read).unwrap();
        trie.insert_lock(key_path![], LockType::Read).unwrap();
        trie.insert_lock(key_path![], LockType::ReadRecursive)
            .unwrap();
        let mut trie = KeyTrie::new();
        trie.insert_lock(key_path![], LockType::Read).unwrap();
        assert!(trie.insert_lock(key_path![], LockType::Write).is_err());

        let mut trie = KeyTrie::new();
        trie.insert_lock(key_path![b"write1"], LockType::Write)
            .unwrap();
        trie.insert_lock(key_path![], LockType::Read).unwrap();
        trie.insert_lock(key_path![b"write2"], LockType::Write)
            .unwrap();
        assert!(
            trie.insert_lock(key_path![], LockType::ReadRecursive)
                .is_err()
        );
        assert!(trie.insert_lock(key_path![], LockType::Write).is_err());

        let mut trie = KeyTrie::new();
        trie.insert_lock(key_path![], LockType::ReadRecursive)
            .unwrap();
        trie.insert_lock(key_path![], LockType::Read).unwrap();
        assert!(
            trie.insert_lock(key_path![b"write"], LockType::Write)
                .is_err()
        );
        trie.insert_lock(key_path![], LockType::ReadRecursive)
            .unwrap();
        assert!(trie.insert_lock(key_path![], LockType::Write).is_err());

        let mut trie = KeyTrie::new();
        trie.insert_lock(key_path![], LockType::Write).unwrap();
        assert!(trie.insert_lock(key_path![], LockType::Read).is_err());
        assert!(
            trie.insert_lock(key_path![b"write"], LockType::Write)
                .is_err()
        );
        assert!(
            trie.insert_lock(key_path![], LockType::ReadRecursive)
                .is_err()
        );
        assert!(trie.insert_lock(key_path![], LockType::Write).is_err());
    }
}
