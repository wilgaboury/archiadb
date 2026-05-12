use crate::util::MAX_KEY_SIZE;

#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
struct KeyPathBuf {
    data: Vec<u8>,
}

impl KeyPathBuf {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn append(&mut self, slice: &[u8]) {
        let len = slice.len();
        if len > MAX_KEY_SIZE {
            eprintln!("Cannot add slice that is larger than key simit to buffer");
        } else {
            self.data.push(len as u8);
            self.data.extend_from_slice(slice);
        }
    }

    pub fn as_path(&self) -> &KeyPath {
        unsafe { &*(self.data.as_slice() as *const [u8] as *const KeyPath) }
    }
}

impl AsRef<KeyPath> for KeyPath {
    fn as_ref(&self) -> &KeyPath {
        self
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
struct KeyPath {
    data: [u8],
}

impl Default for KeyPathBuf {
    fn default() -> Self {
        Self::new()
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
            index: 0,
        }
    }
}

impl AsRef<KeyPath> for KeyPathBuf {
    fn as_ref(&self) -> &KeyPath {
        self.as_path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_happy_path_and_asref() {
        let mut buf = KeyPathBuf::new();
        assert!(buf.data.is_empty());

        buf.append(b"hello");
        buf.append(b"world");
        buf.append(b"rust");

        let slices: Vec<&[u8]> = buf.into_iter().collect();
        assert_eq!(slices, vec![b"hello" as &[u8], b"world", b"rust"]);

        let path_ref: &KeyPath = buf.as_ref();
        let same_path_ref: &KeyPath = path_ref.as_ref();
        assert_eq!(path_ref as *const _, same_path_ref as *const _);

        let default_buf = KeyPathBuf::default();
        assert!(default_buf.data.is_empty());
    }

    #[test]
    fn test_errors_and_corruption() {
        let mut buf = KeyPathBuf::new();

        let large_slice = vec![0u8; 300];
        buf.append(&large_slice);
        assert!(buf.data.is_empty());

        buf.append(b"ok");
        assert_eq!(buf.data, vec![2, b'o', b'k']);

        // Corrupt by changing the first length byte to 100
        buf.data[0] = 100;

        let iter = buf.into_iter();
        // The iterator should detect corruption and return None (and print a warning).
        assert!(iter.collect::<Vec<&[u8]>>().is_empty());

        // Empty buffer iteration
        let empty_buf = KeyPathBuf::new();
        assert!(empty_buf.into_iter().next().is_none());
    }
}
