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
pub(crate) struct TxnKeyTrie<T> {
    root: Option<TxnKeyTrieNode<T>>,
}

pub(crate) struct TxnKeyTrieNode<T> {
    value: T,
    children: BTreeMap<Vec<u8>, TxnKeyTrieNode<T>>,
}

impl<T> TxnKeyTrie<T> {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn upsert<F: Fn() -> T>(&mut self, path: &KeyPath, value_inter: F, value: T) -> Result<()> {
        match self.root.as_mut() {
            Some(mut node) => {
                let mut iter = path.into_iter().peekable();
                while let Some(key) = iter.next() {
                    if iter.peek().is_some() {
                        if node.children.contains_key(key) {
                            node.children.get_mut(key).unwrap().value = value_inter();
                        } else {
                            node.children
                                .insert(key.to_vec(), TxnKeyTrieNode::new(value_inter()));
                        }
                    } else {
                        if node.children.contains_key(key) {
                            node.children.get_mut(key).unwrap().value = value;
                        } else {
                            node.children
                                .insert(key.to_vec(), TxnKeyTrieNode::new(value));
                        }
                        break;
                    }
                    node = node.children.get_mut(key).unwrap()
                }
            }
            None => self.root = Some(TxnKeyTrieNode::new(value)),
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

    pub fn clear_dirty(&mut self) {
        // TODO: this does a lot of uneccesary copying
        for (_, node) in self.dfs_iter_mut() {
            node.dirty = None
        }
    }

    pub fn bfs_iter(&self) -> TxnKeyTrieBfsIter<'_> {
        TxnKeyTrieBfsIter::new(self)
    }

    pub fn bfs_iter_mut(&mut self) -> TxnKeyTrieBfsIterMut<'_> {
        TxnKeyTrieBfsIterMut::new(self)
    }

    pub fn dfs_iter(&self) -> TxnKeyTrieDfsIter<'_> {
        TxnKeyTrieDfsIter::new(self)
    }

    pub fn dfs_iter_mut(&mut self) -> TxnKeyTrieDfsIterMut<'_> {
        TxnKeyTrieDfsIterMut::new(self)
    }
}

impl<T> TxnKeyTrieNode<T> {
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

pub(crate) struct TxnKeyTrieDfsIter<'a, T> {
    stack: Vec<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    visited: HashSet<*const TxnKeyTrieNode<T>>,
    _phantom: PhantomData<&'a TxnKeyTrie<T>>,
}

impl<'a, T> TxnKeyTrieDfsIter<'a, T> {
    pub fn new(trie: &TxnKeyTrie<T>) -> Self {
        let mut ret = Self {
            stack: Vec::new(),
            visited: HashSet::new(),
            _phantom: PhantomData::default(),
        };

        dfs_iter_init(trie, &mut ret.stack, &mut ret.visited);

        ret
    }
}

impl<'a, T> Iterator for TxnKeyTrieDfsIter<'a, T> {
    type Item = (KeyPathBuf, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, cur) = dfs_step(&mut self.stack, &mut self.visited)?;
        Some((path, unsafe { &*cur }))
    }
}

pub(crate) struct TxnKeyTrieDfsIterMut<'a, T> {
    stack: Vec<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    visited: HashSet<*const TxnKeyTrieNode<T>>,
    _phantom: PhantomData<&'a TxnKeyTrie<T>>,
}

impl<'a, T> TxnKeyTrieDfsIterMut<'a, T> {
    pub fn new(trie: &mut TxnKeyTrie<T>) -> Self {
        let mut ret = Self {
            stack: Vec::new(),
            visited: HashSet::new(),
            _phantom: PhantomData::default(),
        };

        dfs_iter_init(trie, &mut ret.stack, &mut ret.visited);

        ret
    }
}

impl<'a, T> Iterator for TxnKeyTrieDfsIterMut<'a, T> {
    type Item = (KeyPathBuf, &'a mut TxnKeyTrieNode<T>);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, cur) = dfs_step(&mut self.stack, &mut self.visited)?;
        Some((path, unsafe { &mut *(cur as *mut _) }))
    }
}

fn dfs_iter_init<T>(
    trie: &TxnKeyTrie<T>,
    stack: &mut Vec<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    visited: &mut HashSet<*const TxnKeyTrieNode<T>>,
) {
    if let Some(root) = trie.root.as_ref() {
        let root = root as *const _;
        stack.push((KeyPathBuf::new(), root));
        visited.insert(root);
    }
}

fn dfs_step<T>(
    stack: &mut Vec<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    visited: &mut HashSet<*const TxnKeyTrieNode<T>>,
) -> Option<(KeyPathBuf, *const TxnKeyTrieNode<T>)> {
    let (path, cur) = stack.pop()?;
    let cur = unsafe { &mut *(cur as *mut _) };

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

pub(crate) struct TxnKeyTrieBfsIter<'a, T> {
    queue: VecDeque<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    _phantom: PhantomData<&'a TxnKeyTrie<T>>,
}

impl<'a, T> TxnKeyTrieBfsIter<'a, T> {
    pub fn new(trie: &'a TxnKeyTrie<T>) -> Self {
        let mut ret = Self {
            queue: VecDeque::new(),
            _phantom: PhantomData::default(),
        };

        bfs_iter_init(trie, &mut ret.queue);

        ret
    }
}

impl<'a, T> Iterator for TxnKeyTrieBfsIter<'a, T> {
    type Item = (KeyPathBuf, &'a TxnKeyTrieNode<T>);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, node) = bfs_step(&mut self.queue)?;
        Some((path, unsafe { &*node }))
    }
}

pub(crate) struct TxnKeyTrieBfsIterMut<'a, T> {
    queue: VecDeque<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
    _phantom: PhantomData<&'a TxnKeyTrie<T>>,
}

impl<'a, T> TxnKeyTrieBfsIterMut<'a, T> {
    pub fn new(trie: &'a mut TxnKeyTrie<T>) -> Self {
        let mut ret = Self {
            queue: VecDeque::new(),
            _phantom: PhantomData::default(),
        };

        bfs_iter_init(trie, &mut ret.queue);

        ret
    }
}

impl<'a, T> Iterator for TxnKeyTrieBfsIterMut<'a, T> {
    type Item = (KeyPathBuf, &'a mut TxnKeyTrieNode<T>);

    fn next(&mut self) -> Option<Self::Item> {
        let (path, node) = bfs_step(&mut self.queue)?;
        Some((path, unsafe { &mut *(node as *mut _) }))
    }
}

fn bfs_iter_init<T>(
    trie: &TxnKeyTrie<T>,
    queue: &mut VecDeque<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
) {
    if let Some(root) = trie.root.as_ref() {
        queue.push_back((KeyPathBuf::new(), root as *const _));
    }
}

fn bfs_step<T>(
    queue: &mut VecDeque<(KeyPathBuf, *const TxnKeyTrieNode<T>)>,
) -> Option<(KeyPathBuf, *const TxnKeyTrieNode<T>)> {
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
    use function_name::named;

    use crate::{key_path, test_util::TempDir};

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

        let results: Vec<(KeyPathBuf, LockType)> = trie
            .bfs_iter()
            .map(|(path, node)| (path, node.lock_type))
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
    #[named]
    fn test_dirty_lca() -> Result<()> {
        let temp_dir = TempDir::new(function_name!())?;
        let (fio, _) = temp_dir.fio("db")?;
        let mut trie = TxnKeyTrie::new();

        trie.insert(key_path![b"1", b"2", b"4"], LockType::Read)?;
        trie.insert(key_path![b"1", b"2", b"5"], LockType::Read)?;
        trie.insert(key_path![b"1", b"3", b"6"], LockType::Read)?;
        trie.insert(key_path![b"1", b"3", b"7"], LockType::Read)?;

        trie.get_mut(key_path![b"1", b"2", b"4"]).unwrap().dirty = Some(fio.get_buf());

        assert_eq!(
            key_path![b"1", b"2", b"4"],
            trie.dirty_lca().unwrap().as_path()
        );

        trie.get_mut(key_path![b"1", b"2", b"5"]).unwrap().dirty = Some(fio.get_buf());

        assert_eq!(key_path![b"1", b"2"], trie.dirty_lca().unwrap().as_path());

        trie.get_mut(key_path![b"1", b"3", b"7"]).unwrap().dirty = Some(fio.get_buf());

        assert_eq!(key_path![b"1"], trie.dirty_lca().unwrap().as_path());

        trie.get_mut(key_path![b"1", b"3", b"6"]).unwrap().dirty = Some(fio.get_buf());

        assert_eq!(key_path![b"1"], trie.dirty_lca().unwrap().as_path());

        trie.insert(key_path![b"8"], LockType::Read)?;
        trie.get_mut(key_path![b"8"]).unwrap().dirty = Some(fio.get_buf());

        assert_eq!(key_path![], trie.dirty_lca().unwrap().as_path());

        trie.clear_dirty();

        assert!(trie.dirty_lca().is_none());

        Ok(())
    }

    #[test]
    fn test_iter() -> Result<()> {
        let mut trie = TxnKeyTrie::new();

        trie.insert(key_path![b"1", b"2"], LockType::Write)?;
        trie.insert(key_path![b"3", b"4"], LockType::Read)?;

        let mut iter = trie.dfs_iter().map(|(buf, node)| (buf, node.lock_type));
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

        let mut iter = trie.dfs_iter_mut().map(|(buf, node)| (buf, node.lock_type));
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

        let mut iter = trie.bfs_iter().map(|(buf, node)| (buf, node.lock_type));
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

        let mut iter = trie.bfs_iter_mut().map(|(buf, node)| (buf, node.lock_type));
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
}
