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

B+trees are a ubiquitous data structure for on-disk database formats. In brief, for those unfamiliar, they are a form of self-balancing tree where each node has many children and data is stored only in the leaves. What makes them espetially suitable for databases is that they can be designed such that each node makes efficent use of a fixed amount of space, and most underlying storage devices are designed to work best with fixed size blocks (commonly 4kb, for instance).

Before discussing the COW variant, it is pertinent for sake of comparison to discuss the in-place version used by many database systems. Each modifying operation is carefully programmed so that it is idemponent; a record describing said operation is appended to a write ahead log (WAL) before it is actually executed. On an unexpected shutdown, operations from the WAL may be safely replayed, which will clean up any partially applied modifications to the B+tree.

TODO: explain path copying

### Double Buffered Pages

## Transactions

### Top Down Locking

### Bottom Up Locking

### Read-Only

## IO Layer

## File Format

### Meta Pages

### Chunks

### B+Tree Nodes