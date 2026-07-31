# Design Doc (__work in progress__)

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

B+trees are a ubiquitous data structure for on-disk database formats. In brief, for those unfamiliar, they are a form of self-balancing sorted tree map where there is a high branching factor and values are stored only in the leaves. What makes them espetially suitable for databases is that they can be designed such that each node makes efficent use of a fixed amount of space, as most underlying storage devices are designed to work best with fixed size blocks (commonly 4kb, for instance).

Before discussing the COW variant, it is pertinent for sake of comparison to discuss the in-place version used by many database systems. In such systems, tree modifying operation are carefully programmed to be idemponent; a record describing said operation is appended to a write ahead log (WAL) before it is actually executed. On an unexpected shutdown, operations from the WAL may be safely replayed, which will clean up any partially applied modifications to the B+tree. Ultimately this ensures on-disk data integrity.

### Path Copying

TODO: explain path copying

### Double Buffered Root Node

TODO: explain double buffered roots

## Transactions

### Top Down Locking

### Bottom Up Locking

### Read-Only

## IO Layer

ArchiaDB's file IO layer, refereed to as FIO, is specifically designed around the io_uring Linux API, a thorough explanation of which is outside the scope of this document, thus what proceeds assumes a basic level of familiarity.

Fundamentally, this layer opens a single file and offers a few notable operations: get page buffer, read page, write page, and commit changes. As may be already obvious, all IO is done via fixed sized blocks which are referred to as pages. FIO attempts to choose a page size equal to the filesystem block size, but in some cases may choose a multiple of it; regardless, page size does not change for a database file after creation.

In order to stay agnostic of async runtime, FIO runs it's own background thread for request processing. Each FIO request (read, write, commit) creates a custom future object, then submits the future's waker and operation specific data to a unbounded lock-free queue. The background thread polls this queue, automatically sleeping when there is no work to do, and translates each operation into io_uring requests that are batched, submitted, and reaped, finally the waker is called to notify the async runtime when a given operation is finished.

One notable optimization is that commit operations, which are translated to fsync requests, are batched such that there is only ever one inflight fsync at any given time. This means that IO code throughout the database can make liberal use of commit while only having to be concerned with latency but not magnitude of calls. 

Another optimization is use of DMA and pools for zero-copy buffers. FIO registers a fixed number of DMA buffers with io_uring on initialization. A portion of these buffers, equal to the size of the completion queue, are reserved for heap buffer copying, and the rest are used as a zero-copy pool. Page buffers are needed whenever client code requests one for use in a write request or when the background thread needs one for returning read results. First, FIO attempts to hand out what is effectively a pointer to a buffer in the zero-copy pool. However, because client code can hold onto these buffers for indeterminate amounts of time, as they may be performing complex serialization, FIO will fallback to a regular heap allocated buffer when none are available. These heap buffers then have their contents copied into/out of the reserved buffer range. This system may seem complex but it ensures that the kernel is always operating on a fixed set of registered buffers, uses zero-copy when possible, but also never blocks CPU-bound serialization work.

## File Format

### Meta Pages

### Chunks

### B+Tree Nodes