use std::borrow::Borrow;

use crate::util::{MAX_KEY_PATH_LEN, MAX_KEY_SIZE};

/// Simple encoding of a key path as a sequence of length-prefixed byte slices
///
/// first byte is length
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct KeyPathBuf {
    data: Vec<u8>,
}

impl KeyPathBuf {
    pub fn new() -> Self {
        Self { data: vec![0] }
    }

    pub fn push(&mut self, slice: &[u8]) {
        let part_len = slice.len();
        if self.len() == MAX_KEY_PATH_LEN {
            eprintln!("Key path is at limit of length")
        } else if part_len > MAX_KEY_SIZE {
            eprintln!("Cannot add slice that is larger than key limit to buffer");
        } else {
            self.data[0] += 1;
            self.data.push(part_len as u8);
            self.data.extend_from_slice(slice);
        }
    }

    pub fn pop(&mut self) {
        if self.len() == 0 {
            eprintln!("Cannot pop from empty key path buf")
        } else {
            self.data[0] -= 1;
            let mut i = 1;
            let mut len = 1;
            while (len + 1 + self.data[i] as usize) < self.data.len() {
                len += 1 + self.data[i] as usize;
                i += 1 + self.data[i] as usize;
            }
            self.data.truncate(len);
        }
    }

    pub fn as_path(&self) -> &KeyPath {
        unsafe { &*(self.data.as_slice() as *const [u8] as *const KeyPath) }
    }

    pub fn len(&self) -> usize {
        self.data[0] as usize
    }
}

impl Ord for KeyPathBuf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_path().cmp(other.as_path())
    }
}

impl PartialOrd for KeyPathBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<KeyPath> for KeyPathBuf {
    fn as_ref(&self) -> &KeyPath {
        self.as_path()
    }
}

impl Borrow<KeyPath> for KeyPathBuf {
    fn borrow(&self) -> &KeyPath {
        self.as_path()
    }
}

impl Default for KeyPathBuf {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct KeyPath {
    data: [u8],
}

impl KeyPath {
    pub fn len(&self) -> usize {
        self.data[0] as usize
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl Ord for KeyPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len()
            .cmp(&other.len())
            .then_with(|| self.into_iter().cmp(other.into_iter()))
    }
}

impl PartialOrd for KeyPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AsRef<KeyPath> for KeyPath {
    fn as_ref(&self) -> &KeyPath {
        self
    }
}

impl ToOwned for KeyPath {
    type Owned = KeyPathBuf;

    fn to_owned(&self) -> Self::Owned {
        KeyPathBuf {
            data: self.data.to_vec(),
        }
    }
}

pub struct KeyPathIter<'a> {
    data: &'a [u8],
    index: usize,
}

impl<'a> Iterator for KeyPathIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.data.len() {
            return None;
        }

        let len = self.data[self.index] as usize;
        self.index += 1;

        if self.index + len > self.data.len() {
            eprintln!("buffer is corrupted: length byte exceeds remaining data");
            return None;
        }

        let key_slice = &self.data[self.index..self.index + len];
        self.index += len;
        Some(key_slice)
    }
}

impl<'a> IntoIterator for &'a KeyPathBuf {
    type Item = &'a [u8];
    type IntoIter = KeyPathIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        KeyPathIter {
            data: &self.data,
            index: 1,
        }
    }
}

impl<'a> IntoIterator for &'a KeyPath {
    type Item = &'a [u8];
    type IntoIter = KeyPathIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        KeyPathIter {
            data: &self.data,
            index: 1,
        }
    }
}

/// Creates a `&'static KeyPath` from a list of byte slices.
///
/// ```
/// # use archiadb::key_path;
/// # use archiadb::key::KeyPath;
/// let path = key_path![b"hello", b"world", b"!"];
/// ```
///
/// ```compile_fail
/// # use archiadb::key_path;
/// # use archiadb::key::KeyPath;
/// // This should fail because the slice is longer than MAX_KEY_SIZE
/// const LONG: &[u8] = &[0u8; 256];
/// let path = key_path![LONG];
/// ```
#[macro_export]
macro_rules! key_path {
    ($($slice:expr),* $(,)?) => {{
        const INPUTS: &[&[u8]] = &[$($slice),*];
        const TOTAL_LEN: usize = {
            let mut sum = 0;
            let mut i = 0;
            while i < INPUTS.len() {
                let len = INPUTS[i].len();
                assert!(len <= $crate::util::MAX_KEY_SIZE);
                sum += 1 + len;
                i += 1;
            }
            sum + 1
        };
        const PACKED: [u8; TOTAL_LEN] = {
            let mut arr = [0u8; TOTAL_LEN];
            arr[0] = INPUTS.len() as u8;

            let mut pos = 1;
            let mut i = 0;
            while i < INPUTS.len() {
                let slice = INPUTS[i];
                arr[pos] = slice.len() as u8;
                pos += 1;
                let mut j = 0;
                while j < slice.len() {
                    arr[pos + j] = slice[j];
                    j += 1;
                }
                pos += slice.len();
                i += 1;
            }
            arr
        };
        unsafe { &*(&PACKED as *const [u8] as *const KeyPath) }
    }};
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;

    #[test]
    fn test_happy_path_and_asref() {
        let mut buf = KeyPathBuf::new();
        assert_eq!(0, buf.len());
        assert_eq!(1, buf.data.len());

        buf.push(b"hello");
        buf.push(b"world");
        buf.push(b"rust");

        let slices: Vec<&[u8]> = buf.into_iter().collect();
        assert_eq!(slices, vec![b"hello" as &[u8], b"world", b"rust"]);

        let slices: Vec<&[u8]> = buf.as_path().into_iter().collect();
        assert_eq!(slices, vec![b"hello" as &[u8], b"world", b"rust"]);

        let path_ref: &KeyPath = buf.as_ref();
        let same_path_ref: &KeyPath = path_ref.as_ref();
        assert_eq!(path_ref as *const _, same_path_ref as *const _);

        let default_buf = KeyPathBuf::default();
        assert_eq!(0, default_buf.len());
        assert_eq!(1, default_buf.data.len());

        buf.pop();
        assert_eq!(buf.as_path(), key_path![b"hello", b"world"]);
        buf.pop();
        assert_eq!(buf.as_path(), key_path![b"hello"]);
        buf.pop();
        assert_eq!(buf.as_path(), key_path![]);
    }

    #[test]
    fn test_errors_and_corruption() {
        let mut buf = KeyPathBuf::new();

        let large_slice = vec![0u8; 300];
        buf.push(&large_slice);
        assert_eq!(0, buf.len());
        assert_eq!(1, buf.data.len());

        buf.push(b"ok");
        assert_eq!(buf.data, vec![1, 2, b'o', b'k']);

        // Corrupt by changing the first length byte to 100
        buf.data[1] = 100;

        let iter = buf.into_iter();
        // The iterator should detect corruption and return None (and print a warning).
        assert!(iter.collect::<Vec<&[u8]>>().is_empty());

        // Empty buffer iteration
        let empty_buf = KeyPathBuf::new();
        assert!(empty_buf.into_iter().next().is_none());
    }

    #[test]
    fn test_key_path_macro() {
        const PATH: &KeyPath = key_path![b"hello", b"world", b"!"];

        // Expected packed format:
        // path len=3
        // len=5, b'h','e','l','l','o'
        // len=5, b'w','o','r','l','d'
        // len=1, b'!'
        let expected_bytes: &[u8] = &[
            3, 5, b'h', b'e', b'l', b'l', b'o', 5, b'w', b'o', b'r', b'l', b'd', 1, b'!',
        ];

        assert_eq!(PATH.as_bytes(), expected_bytes);
    }

    #[test]
    fn test_to_owned() {
        let static_path = key_path![b"foo", b"bar"];
        let owned: KeyPathBuf = static_path.to_owned();

        // The owned path should contain the same packed data
        assert_eq!(owned.data, static_path.as_bytes());

        // Iteration over both should yield the same segments
        let static_segments: Vec<&[u8]> = static_path.into_iter().collect();
        let owned_segments: Vec<&[u8]> = owned.into_iter().collect();
        assert_eq!(static_segments, owned_segments);
    }

    #[test]
    fn test_key_path_ordering_comprehensive() {
        // Create various paths for testing
        let path_a1 = key_path![b"a"];
        let path_a2 = key_path![b"a"];
        let path_b = key_path![b"b"];
        let path_ab = key_path![b"a", b"b"];
        let path_ac = key_path![b"a", b"c"];
        let path_aba = key_path![b"a", b"b", b"a"];
        let path_abb = key_path![b"a", b"b", b"b"];

        // Test KeyPath ordering
        assert_eq!(path_a1.cmp(path_a2), Ordering::Equal);
        assert_eq!(path_a1.cmp(path_b), Ordering::Less);
        assert_eq!(path_b.cmp(path_a1), Ordering::Greater);
        assert_eq!(path_a1.cmp(path_ab), Ordering::Less);
        assert_eq!(path_ab.cmp(path_a1), Ordering::Greater);
        assert_eq!(path_ab.cmp(path_ac), Ordering::Less);
        assert_eq!(path_ab.cmp(path_aba), Ordering::Less);
        assert_eq!(path_aba.cmp(path_abb), Ordering::Less);

        // Test KeyPathBuf ordering (delegates to KeyPath)
        let mut buf_a1 = KeyPathBuf::new();
        let mut buf_a2 = KeyPathBuf::new();
        let mut buf_b = KeyPathBuf::new();
        let mut buf_ab = KeyPathBuf::new();

        buf_a1.push(b"a");
        buf_a2.push(b"a");
        buf_b.push(b"b");
        buf_ab.push(b"a");
        buf_ab.push(b"b");

        assert_eq!(buf_a1.cmp(&buf_a2), Ordering::Equal);
        assert_eq!(buf_a1.cmp(&buf_b), Ordering::Less);
        assert_eq!(buf_a1.cmp(&buf_ab), Ordering::Less);
        assert_eq!(buf_ab.cmp(&buf_a1), Ordering::Greater);

        // Test PartialOrd
        assert_eq!(path_a1.partial_cmp(path_a2), Some(Ordering::Equal));
        assert_eq!(buf_a1.partial_cmp(&buf_a2), Some(Ordering::Equal));
    }
}
