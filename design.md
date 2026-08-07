# Design Doc (__WIP__)

## Introduction

ArchiaDB is a hiercharchial, embedded, transactional database. This document provides a broad overview of how it is implemented and reasoning behind certain design decisions.

## Hierarchical Modeling

Fundamentally, Archia is a nestable key-value store. The following is a in-memory representation that is effectively equivilent:

```rust
struct DB(Map);
type Map = BTreeMap<Key, Value>;
type Key = Box<[u8]>;
enum Value {
    Data(Box<[u8]>),
    Map(Map)
}
```

For this document, it suffices to say that hierarchical modelling is one style among other alternatives like relational, document, or graph. An opinionated, persuasive treatment of it's benefits can be found here (TODO).

## COW B+trees

B+trees are a ubiquitous data structure for on-disk database formats. In brief, for those unfamiliar, they are a form of self-balancing sorted tree map where there is a high branching factor and values are stored only in the leaves. What makes them especially suitable for databases is that they can be designed such that each node makes efficient use of a fixed amount of space, as most underlying storage devices are designed to work best with fixed size blocks (commonly 4kb, for instance).

Before discussing the COW variant, it is pertinent for sake of comparison to discuss the in-place version used by many database systems. In such systems, modifying operations
 are carefully programmed to be idempotent; a record describing each operation is appended to a write ahead log (WAL) before it is actually executed. After an unexpected shutdown, operations from the WAL may be safely replayed, which will clean up any partially applied modifications to the B+tree, ultimately ensuring on-disk data integrity.

COW, on the other hand, maintains integrity by not modifying B+tree nodes once they are written; though, there is one exception to this in ArchiaDB which will be covered shortly.

### Path Copying

This is a ubiquitous technique used by persistent immutable data structures; however, Archia uses it instead to keep its on-disk format crash safe. For just about any tree structure, which consists of nodes and pointers, instead of modifying nodes directly, mutated nodes are first copied. This is done recursively until reaching the root, where path copying creates a new root node. This results in two roots, one pointing to the unmodified previous version, and one pointing to a new structure, containing the modification, composed of both new and old nodes. This is exactly what ArchiaDB does when modifying B+trees, the only difference being that nodes are stored on disk.

![path-copying](https://upload.wikimedia.org/wikipedia/commons/5/56/Purely_functional_tree_after.svg?utm_source=en.wikipedia.org&utm_campaign=index&utm_content=original)

### Double Buffered Root Node

The issue is that path copying cannot regress infinitely, if the goal is to replace the old version; at some point, it must terminate with a simple in-place modification. In-memory data structures can simply update a pointer to the root, but databases do not get that luxury. To solve this ArchiaDB adopts a clever technique from LMDB.

Each root node comprises two pages (or buffers) and each contains two important fields: a version, which is a monotonically increasing integer, and a checksum, which is stored at the end of the page. The canonical root data is determined by choosing the page with the higher version and a correct checksum. To update the root, data containing an incremented version and calculated checksum is written to the old, non-canonical page. If a crash occurs during this process, a torn (partial) write can be detected by checksum, and the tree can be restored from the prior version.

In order to atomically commit transactions, which may touch multiple B+trees, ArchiaDB finds the least common ancestor (LCA) of all the dirty trees and applies path copying all the way up to the LCA B+tree’s root, where it then performs the double buffer root procedure.

## Transactions

### Background

Transactions in ArchiaDB use conservative two phase locking (2PL). In the Archia API, this translates to declaring the read/write set before beginning the transaction. This makes difficult transactions that do not know their full read/write set ahead of time. However, this issue can be mitigated by utilizing non-blocking snapshot isolation read transactions combined with optimistic concurrency control, or by acquiring more coarse grain locks higher up in the node tree.

One of the main benefits of conservative 2PL is that it can entirely avoid deadlocks and livelocks. Many traditional DBMS's perform the first phase dynamically; locks are acquired as the transaction makes progress based on what data it attempts to read or write. For those unfamiliar with this problem space, when two concurrent processes compete for mutual exclusion on two resources but in opposite order, it's possible for each one to lock the resource the other one needs, causing both processes to enter a stuck state. Traditional DBMS's get around this using complex deadlock detection systems, which monitor locks, identify deadlocked processes, and force cancellation so that at least one of them can make progress. However, on high throughput systems, complex transactions may end up livelocked. When more data is touched, there is a higher probability of cancellation, creating the possibility that a transaction is stuck retrying and never completes. The simple solution to deadlocks when using conservative 2PL is to give locks a global ordering, such that each process always acquires/releases them in the same order.

### Top Down Locking

Top-down locks refer to the locks acquired at the very beginning of a transaction and released at the very end. The database utilizes a unique scheme in order to best take advantage of its hierarchical structure. Each node in the B+tree hierarchy has effectively a read/write lock following the golden rule of single writer XOR multiple readers. Locks for the node tree are acquired in BFS order and lexicographically across siblings. Each one uses a fair, FIFO wait queue, to make sure that transactions, regardless of complexity, are always eventually processed.

Users may declare one of three transaction operations on a given node: read, write, and read recursive. The first two are self explanatory, the last one effectively acquires a read lock on a given node and all of its descendants. There is an additional hidden lock type, not directly exposed to the user called read-child-write. This lock is acquired on each ancestor on the path to a write lock and ensures that a read recursive lock can never acquire a sub-tree with an ongoing write and vice versa.

### Bottom Up Locking

These refer to locks that are acquired/released during the commit procedure, which can be triggered by the user any number of times during a transaction. The main issue addressed by this algorithm is that the least common ancestor (LCA) of the dirty nodes may not be in the write lock set of a given transaction, so doing an atomic double-buffered root page write is not trivial to do safely. Consider the case of three nodes: A, B, C, where A is the parent of B and C, and we have a transaction which has acquired top-down write locks on B and C. To properly do path-copying and double-buffered atomic commit, we must modify A. Concurrent runs of this transaction could conflict, so the database acquires "bottom-up" write locks on the dirty node's ancestors up to the LCA. These are locked in reverse BFS order, and the commit algorithm roughly follows this procedure:

1. write dirty transaction pages
2. acquire bottom-up locks
3. write pages on path to LCA
4. fsync
5. double-buffered atomic LCA write 
6. fsync
7. release bottom-up locks

### Read-Only

## IO Layer

ArchiaDB's file IO layer, referred to as FIO, is specifically designed around the io_uring Linux API. Fundamentally, it is responsible for opening a single file and implementing a few notable operations: get page buffer, read page, write page, and commit changes. As may be already obvious, all IO is done via fixed sized blocks which are referred to as pages. ArchiaDB attempts to choose a page size equal to the filesystem block size, but in some cases may choose a multiple of it; regardless, page size does not change for a database file after creation.

In order to stay agnostic of async runtime, FIO runs its own background thread for request processing. Each FIO request (read, write, commit) creates a custom future object, then submits the future's waker and operation specific data to a unbounded lock-free queue. The background thread polls this queue, automatically sleeping when there is no work to do, and translates each operation into io_uring requests that are batched, submitted, and reaped, finally the waker is called to notify the async runtime when a given operation is finished.

One notable optimization is that commit operations, which are translated to fsync requests, are temporally batched such that there is only ever one inflight fsync at any given time. This means that IO code throughout the database can make liberal use of commit while only having to be concerned with latency but not magnitude of calls.

Another optimization is use of DMA and pools for zero-copy buffers. FIO registers a fixed number of DMA buffers with io_uring on initialization. A portion of these buffers, equal to the size of the completion queue, are reserved for heap buffer copying, and the rest are used as a zero-copy pool. Page buffers are needed whenever client code requests one for use in a write request or when the background thread needs one for returning read results. First, FIO attempts to hand out what is effectively a pointer to a buffer in the zero-copy pool. However, because client code can hold onto these buffers for indeterminate amounts of time, FIO will fallback to a regular heap allocated buffer when none are available. These heap buffers then have their contents copied into/out of the reserved buffer range. This system ensures that the kernel is always operating on a fixed set of registered buffers, uses zero-copy when possible, and also never blocks CPU-bound work.

## Page Allocation

### Global Allocation

### Local Allocation

## File Format

### Meta Pages

The first two pages of each database file contain metadata, information like: file size, page size, is currently open, etc. Just like B+tree roots, metadata is double buffered.

### Chunks

### B+Tree Nodes
