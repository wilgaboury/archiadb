# Design Doc

## Core Design Tenets

### Strict Serializability

This database makes use of conservative 2-phase locking which allows for strict serializability and no deadlocks; although, it comes at the cost of transaction throughput and requires better upfront planning around data layout and queries. However, due to the use of COW BTrees, users can opt into snapshot isolation for non-blocking read transactions when needed. Users need to use optimistic concurrency control for implementing certain conditional transactions ory any which require reading data on disk before it can be determined where to perform subsequent reads/writes, as may be the case with references.

### No Caching

There seems to be continuous debate in database engine design between using mmap and letting the OS manage caching via pages or use O_DIRECT with bespoke caching strategies. I'll do one better, and say that application code should be responsible for determining what data is on disk and what is in memory. This database intends to be only a transactional ACID storage engine which uses as little memory as possible without sacrificing speed. This philosophy may look bad on benchmarks, but gives more control to client code and leads to simpler overall systems.

### Minimize System Calls

In modern programming, system calls and context switching are the enemy of speed. This database makes extensive use of async Rust (runtime agnostic) and Linux's io_uring to avoid entering kernel space as much as possible.

### Maximize Simplicity

A wise programmer once said: "If the code is twice as long, it better be 10 times as fast".

## File Format

Like LMDB the file will begin with a single meta block that contains assorted static information like magic number, database version, and block size. The roots of each BTree will contain a pointer to root, generation, and checksum ([crc32c](https://github.com/zowens/crc32c)). Any modification will swap writing between the two. On startup, the one with higher generation and valid checksum will be chosen as canonical. This strategy combined with COW operations removes the need for a WAL.

A block size must be specified up front when creating the database or will be detected automatically using `fstatvfs.f_bsize`. After the meta block, the rest of the file is a series of chunks, where a chunk begins with a free block bit mask, meaning an entire block such that each 0/1 represents whether a subsequent block is free or allocated, and said series of blocks. For a block size of 4kb, which is a common standard, that translates to a chunk size of ~128mb. This scheme allows for a fairly fast scan of the database file on startup for initialization of in-memory block allocator state. The main tradeoff is that database modifications must pre-commit their block allocations to each chunk header, so in the case of a dirty shutdown it's possible to "leak" blocks. That being said, headers can be cleaned up later by a routine that traverses the BTree structure.
