use tokio_uring::fs::File;

struct BTree {
    file: File,
    root_offset: u64,
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

struct InternalNode {
    parent: u64,
    keys: [[u8; 256]; 8],
}

struct LeafNode {
    parent: u64,
    keys: [[u8; 256]; 8],
}
