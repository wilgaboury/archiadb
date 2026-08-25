# Design Doc (WIP)

## Introduction

ArchiaDB is a hierarchical, embedded, transactional database. This document provides a broad overview of its implementation and design decisions.

## Primary Motivations

- Async rust and io_uring - These technologies make it possible to significantly reduce kernel space context switching. I wanted to explore this design space for my own educational purposes, and see if it would have any notable impact on overall performance.
- Strictly serializable by default - The pervasiveness of snapshot serializable isolation or weaker models by default is baffling to me. Even for experienced engineers, it is far too easy to introduce subtle bugs in complex read-write transactions.
- Hierarchical data model - This paradigm struck me as intriguing and fairly underutilized. Combined with strict serializable isolation hierarchical modelling makes implementing multi-index data and complex transactions in application code intuitive, safe, and parallelizable. It also lends itself quite naturally to the COW data structures.
- Non-declarative - While query languages provide a convenient and powerful abstraction for rapid iteration and business applications development, they are also a leaky abstraction that carries overhead. Expressing operations procedurally with a native programming language is fast, clear, and more ergonomic.

## Hierarchical Modeling

Hierarchical modelling is one paradigm among others like relational, document, or graph; providing a higher-level structure to a set of records. It may help to think of it as nothing more than a nestable key-value store. The following in-memory representation is effectively equivalent:

```rust
struct DB(Map);
type Map = BTreeMap<Key, Value>;
type Key = Box<[u8]>;
enum Value {
    Data(Box<[u8]>),
    Map(Map)
}
```

## COW B+trees

B+trees are a ubiquitous structure used in storage engines. For those unfamiliar, they are a self-balancing sorted tree map with a high branching factor and values only stored in leaves.  Most underlying storage devices (HDDs, SSDs) are designed to work best with fixed size blocks (commonly 4kb), so B+trees are suitable because their nodes can make efficient use of fixed size blocks.

In order to understand copy on write (COW) B+trees, it is pertinent to briefly explain the in-place versions used by many databases. In such systems, modifying operations are carefully programmed to be idempotent, and a record describing each operation is appended to a write ahead log (WAL) before it is actually executed. For recovery after an unexpected shutdown, operations from the WAL may be safely replayed, which will clean up any partially applied modifications to the B+tree, ultimately ensuring on-disk data integrity. COW maintains integrity by never overwriting data; instead, nodes are copied, modified and written to new locations in the file.

### Path Copying

This technique is widely employed by persistent immutable data structures. For B+trees, in order to modify leaf nodes which contain all the keys and values, node COW is performed recursively until reaching the root. This results in two roots, one pointing to the previous version and one pointing to a new version that contains the applied modification, but is composed of both new and old nodes.

![path-copying](https://upload.wikimedia.org/wikipedia/commons/5/56/Purely_functional_tree_after.svg?utm_source=en.wikipedia.org&utm_campaign=index&utm_content=original)

### Double Buffered Root

Path copying cannot regress infinitely. If the goal is to replace the old version, at some point it must terminate with an in-place modification. In-memory data structures can simply update a pointer to the root, but databases do not get that luxury. To solve this, ArchiaDB adopts a clever technique from LMDB.

Each root node comprises two pages (or buffers) and each contains two important fields: a version, which is a monotonically increasing integer, and a checksum, which is stored at the end of the page. The canonical root data is determined by choosing the page with the higher version and a correct checksum. To update the root, data containing an incremented version and calculated checksum is written to the old, non-canonical page. If a crash occurs during this process, a torn (partial) write can be detected by checksum, and the tree can be restored from the prior version.

In order to atomically commit transactions, which may touch multiple B+trees, ArchiaDB finds the least common ancestor (LCA) of all the dirty trees and applies path copying all the way up to the LCA B+tree’s root, where it then performs the double buffer root procedure.

## Transactions

### Background

Transactions in ArchiaDB use conservative two phase locking (2PL). In the API, this translates to declaring the entire read/write set upfront. This design makes it difficult to create transactions that do not know their full read/write set ahead of time; consider, for instance, a bank transfer where the two usernames are known but not the account ids. However, this issue can be mitigated by utilizing non-blocking reads combined with optimistic concurrency control, or by pessimistically locking more of the node tree.

One of the main benefits of conservative 2PL is that it can entirely avoid deadlocks and livelocks. Many traditional DBMS's perform the first phase dynamically; locks are acquired as the transaction makes progress based on what data it attempts to read or write. For those unfamiliar with this problem space, when two concurrent processes compete for mutual exclusion on two resources but in opposite order, it's possible for each one to lock the resource the other one needs, causing both processes to enter a stuck state. Traditional DBMS's get around this using complex deadlock detection systems, which monitor locks, identify deadlocked processes, and force cancellation so that at least one of them can make progress. However, on high throughput systems, complex transactions may end up livelocked. When more data is touched, there is a higher probability of cancellation, creating the possibility that a transaction is stuck retrying and never completes. The solution to deadlocks when using conservative 2PL is to give locks a global ordering, such that each process always acquires/releases them in the same order.

### Top-Down Locking

Top-down locks refer to the locks acquired at the very beginning of a transaction and released at the very end. The database utilizes a unique scheme in order to best take advantage of its hierarchical structure. Each node in the B+tree hierarchy has effectively a read/write lock following the golden rule of single writer XOR multiple readers. Locks for the node tree are acquired in BFS order and lexicographically across siblings. Each one uses a fair, FIFO wait queue, to make sure that transactions, regardless of complexity, are always eventually processed.

Users may declare one of three transaction operations on a given node: read, write, and read recursive. The first two are self explanatory, the last one effectively acquires a read lock on a given node and all of its descendants. There is an additional hidden lock type, not directly exposed to the user called read-child-write. This lock is acquired on each ancestor on the path to a write lock and ensures that a read recursive lock can never acquire a sub-tree with an ongoing write and vice versa.

### Bottom-up Locking

These refer to locks that are acquired/released during the commit procedure, which can be triggered by the user any number of times during a transaction. The main issue addressed by this algorithm is that the least common ancestor (LCA) of the dirty nodes may not be in the write lock set of a given transaction, so doing an atomic double-buffered root page write is not trivial to do safely. Consider the case of three nodes: A, B, C, where A is the parent of B and C, and we have a transaction which has acquired top-down write locks on B and C. To properly do path-copying and double-buffered atomic commit, we must modify node A. Concurrent transactions that also have node A as their LCA could conflict, so the database acquires "bottom-up" write locks on the dirty node's ancestors up to the LCA. These are locked in reverse BFS order, and the commit algorithm roughly follows this procedure:

1. write dirty transaction pages
2. acquire bottom-up locks
3. write pages on path to LCA
4. fsync
5. double-buffered atomic LCA write
6. fsync
7. release bottom-up locks

### Read-Only

While typical transactions can be either read or write, they are always strictly serializable and may block on other ongoing transactions. ArchiaDB also offers non-blocking read-only transactions at the lower snapshot serializable isolation level. From an implementation perspective, read-only transactions are trivial to implement due to the database's COW B+tree structure. The main concern is that read-only processes could read “dirty” pages referenced by the non-canonical B+tree root buffer, which have been written over; however, the database contains in-memory bookkeeping mechanisms to ensure that pages are not reused until there are no longer any transactions that could reference them.

## IO Layer

ArchiaDB's file IO layer, referred to as FIO, is specifically designed around the io_uring Linux API. Fundamentally, it is responsible for opening a single file and implementing a few notable operations: get page buffer, read page, write page, and commit changes. As may be already obvious, all IO is done via fixed sized blocks which are referred to as pages. ArchiaDB attempts to choose a page size equal to the filesystem block size, but in some cases may choose a multiple of it; regardless, page size does not change for a database file after creation.

In order to stay agnostic of async runtime, FIO runs its own background thread for request processing. Each FIO request (read, write, commit) creates a custom future object, then submits the future's waker and operation specific data to a unbounded lock-free queue. The background thread polls this queue, automatically sleeping when there is no work to do, and translates each operation into io_uring requests that are batched, submitted, and reaped, finally the waker is called to notify the async runtime when a given operation is finished.

One notable optimization is that commit operations, which are translated to fsync requests, are temporally batched such that there is only ever one inflight fsync at any given time. This means that IO code throughout the database can make liberal use of commit while only having to be concerned with latency but not magnitude of calls.

Another optimization is use of DMA and pools for zero-copy buffers. FIO registers a fixed number of DMA buffers with io_uring on initialization. A portion of these buffers, equal to the size of the completion queue, are reserved for heap buffer copying, and the rest are used as a zero-copy pool. Page buffers are needed whenever client code requests one for use in a write request or when the background thread needs one for returning read results. First, FIO attempts to hand out what is effectively a pointer to a buffer in the zero-copy pool. However, because client code can hold onto these buffers for indeterminate amounts of time, FIO will fallback to a regular heap allocated buffer when none are available. These heap buffers then have their contents copied into/out of the reserved buffer range. This system ensures that the kernel is always operating on a fixed set of registered buffers, uses zero-copy when possible, and also never blocks CPU-bound work.

## Page Allocation

### Local Allocation

Each B+tree in ArchiaDB maintains its own local allocation system that consists of two components: a list of arenas and a list of free pages.

Arenas are simple bump allocators, where each one points to a chunk of pages. The free list is simply a list of free page indexes. The most interesting thing about this system is the data is encoded and stored as an on-disk linked list of pages. This linked list is also COW and a pointer is stored in the double buffered B+tree root header. Modifications always are performed starting from the head, so regardless of free list length, write transaction overhead is proportional to the amount of allocation work done.

### Global Allocation

When new B+tree roots are first created or when one runs out of pages in their local allocator, a new arena is created for it by the global allocator which bumps the size of the file. The procedure works like so:

1. acquire global alloc lock
2. falloc, extend file to create new arena
3. write new file length and B+tree root indexes to meta page
4. fsync
5. write arena information to B+tree root page
6. fsync
7. wipe btree root indexes from meta page
8. fsync
9. release lock

This dance with the last page data is important for crash recovery, since this process is not committed atomically. A crash that occurs after extending the file but before the new arena has been committed to the B+tree is at risk of leaking a chunk of the file. On startup, if a crash is detected, the recovery process reads the last page. If the checksum is valid and a partial global alloc indicated, it checks the root that it points to and checks that it knows about the arena. If not, the root is updated to complete the allocation.

## File Format

### Storage Assumptions

There is really only one main assumption that ArchiaDB makes about the underlying storage device: after an fsync operation, all previously written pages are persisted. Unlike other databases/formats, there is no assumption of properties like atomic single byte write or powersafe overwrite, which makes ArchiaDB a very resilient format.

### Metadata

The first two pages of each database file contain metadata and are double buffered just like B+tree roots.

### B+tree

### Allocator List
