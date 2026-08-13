<img align="left" height="60px" hspace="10" src="files/logo.svg"/>

# ArchiaDB

[![Build Status](https://github.com/wilgaboury/sbdb/workflows/build/badge.svg)](https://github.com/wilgaboury/sbdb/actions)
[![codecov](https://codecov.io/gh/wilgaboury/archiadb/graph/badge.svg?token=9WIXY37Q34)](https://codecov.io/gh/wilgaboury/archiadb)
[![Casual Maintenance Intended](https://casuallymaintained.tech/badge.svg)](https://casuallymaintained.tech/)

**NOTE: This is currently a work in progress; nothing here is fully implemented.**

The 60s called and they want their database technology back. ArchiaDB is a modern hierarchical embedded OLTP database.

### AI Disclosure

LLMs were used sparingly, mostly for research, reference pseudo-code, or code review. This project has been my reprieve from extensive agent usage at work, so it is almost entirely hand written.

## What is a heirarchical database?

Like other OLTP data stores, the paradigm is defined by how records are organized. For comparison, document databases use a set of collections, relational databases use a graph of tables, and key-value stores don't have any higher level structure. A hierarchical database organizes records into a canonical tree of one to many relationships.

## Features

- strictly serializable transactions using conservative 2PL
    - concurrent multi-writer when transactions do not conflict
    - transaction locking is guaranteed to succeed
- non-blocking snapshot serializable read transactions
- single file with architecture independent format
- uses COW B+trees, crash-safe without requiring a WAL
- unopinionated about key/value serialization
- relies on async rust, but is fully runtime agnostic
- utilizes io_uring

The internal workings are outlined in more detail here: [Design Doc](design.md)

## Design Intentions

ArchiaDB was designed with an eye towards server-side applications that benefit from dead simple database administration, like those intended for self-hosting, and I believe it offers a compelling feature combination for this use case. However, it is quite simple and immature. For high throughput, high uptime applications, LSM-tree based data stores or traditional OLTP DBMS's are likely worth the increase in complexity.
