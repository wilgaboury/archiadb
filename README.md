# ArchiaDB

[![Build Status](https://github.com/wilgaboury/sbdb/workflows/build/badge.svg)](https://github.com/wilgaboury/sbdb/actions)
[![codecov](https://codecov.io/gh/wilgaboury/archiadb/graph/badge.svg?token=9WIXY37Q34)](https://codecov.io/gh/wilgaboury/archiadb)
[![Casual Maintenance Intended](https://casuallymaintained.tech/badge.svg)](https://casuallymaintained.tech/)

The 60s called and they want their database technology back. This is my attempt at creating a modern hierarchical OLTP database.

## Design

I intend to first create an embedded database that can be used as a rust library. If this project is seen through, then I'd like to build a server layer on top so it can be used as a full DBMS.

This database will make use of conservative 2-phase locking which allows for strict serializability and no deadlocks, but comes at the cost of transaction throughput and requires better upfront planning around data layout and queries. However, the underlying BTree structure will be similar to what is found in LMDB, so users can opt into snapshot isolation for fast non-blocking read transactions when needed. Users will be encouraged to use optimistic concurrency control for implementing transactions that require reading data before it can be determined where to perform writes, as may be the case with highly referential data.

### File Format

Like LMDB the file will begin with two meta blocks that contain assorted information like database verison and block size. More importantly the meta blocks contain a pointer to root, generation, and checksum. Any modification will be swap writing between the two. On startup, the one with higher generation and valid checksum will be chosen as canonical. This strategy combined with COW operations removes the need for a WAL.

A block size must be specified up front when creating the database or will be detected automatically using statx.stx_atomic_write_unit_max. After the meta blocks, the rest of the file is a series of chunks, where a chuck begins with a free block bit mask, meaning an entire block such that each 0/1 represents whether a subsequent block is free or allocated, and said series of blocks. For a block size of 4kb, which is a common standard, that translates to a chunk size of ~128mb.