use std::collections::BTreeMap;

use dashmap::DashSet;
use parking_lot::Mutex;

use crate::{db::Txn, uint::PgIdx};

pub type DeferId = u64;

/// Tracks currently running transactions and ensures that freeing of pages is deferred until no transactions can reference them
#[derive(Debug)]
pub(crate) struct Defer {
    inner: Mutex<Inner>,
    global: DashSet<PgIdx>,
}

#[derive(Debug)]
pub(crate) struct Inner {
    next: DeferId,
    to_free: BTreeMap<DeferId, Vec<u64>>,
}

impl Defer {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next: 0,
                to_free: BTreeMap::new(),
            }),
            global: DashSet::new(),
        }
    }
}

pub(crate) struct DeferGaurd<'a> {
    id: DeferId,
    freed: Vec<PgIdx>,
    defer: &'a Defer,
}

impl Defer {
    pub(crate) fn begin(&self) -> DeferGaurd<'_> {
        let mut inner = self.inner.lock();
        let id = inner.next;
        inner.next += 1;
        inner.to_free.insert(id, Vec::with_capacity(0));
        DeferGaurd {
            id,
            freed: Vec::new(),
            defer: &self,
        }
    }
}

impl<'a> DeferGaurd<'a> {
    pub(crate) fn flush(&mut self) {
        let free = {
            let mut inner = self.defer.inner.lock();
            // Add pages to last transaction, since we can garuntee there will be no references to freed pages after it finishes
            let last = inner.to_free.iter_mut().next_back();
            match last {
                Some((_, to_free)) => to_free.append(&mut self.freed),
                None => {
                    eprintln!(
                        "There should always be at least one entry since this transaction is still active"
                    );
                }
            }

            let maybe_to_free = inner.to_free.remove(&self.id);
            if let Some(mut to_free) = maybe_to_free {
                let next_back = inner.to_free.range_mut(..self.id).next_back();
                match next_back {
                    Some((_, prev_to_free)) => {
                        // Move pages to previous transaction, so they can be freed when it finishes
                        prev_to_free.append(&mut to_free);
                        Vec::new()
                    }
                    None => {
                        // No previous transactions exist which could reference these pages, so we can free them
                        to_free
                    }
                }
            } else {
                eprintln!("Transaction was either already finished or was never added to the map");
                Vec::with_capacity(0)
            }
        };

        // frees do not need to occur inside lock
        for pg in free {
            self.defer.global.remove(&pg);
        }
    }
}

impl<'a> Txn<'a> {
    pub(crate) fn free(&mut self, pg_idx: PgIdx) {
        self.defer_gaurd.freed.push(pg_idx);
    }
}

#[cfg(test)]
mod tests {
    // TODO: redo testing

    // use crate::test_util::TempDir;

    // use super::*;
    // use anyhow::Result;
    // use function_name::named;

    // fn snapshot(map: &TxnFreeDeferMap) -> BTreeMap<u64, Vec<u64>> {
    //     map.inner.lock().map.clone()
    // }

    // #[named]
    // #[tokio::test]
    // async fn freed_pages_moved_to_earlier_txn_and_freed_when_no_older_txns() -> Result<()> {
    //     let dir = TempDir::new(function_name!()).unwrap();
    //     let (alloc, _fio, meta) = dir.alloc("file").await?;
    //     let map = TxnFreeDeferMap::new();

    //     let mut set = AllocationSet::new();
    //     let pg1 = alloc.alloc(&meta, &mut set).await?;
    //     let pg2 = alloc.alloc(&meta, &mut set).await?;
    //     let pg3 = alloc.alloc(&meta, &mut set).await?;
    //     set.flush(&alloc).await?;

    //     map.begin();

    //     map.begin();
    //     map.finish(1, &mut vec![pg2], &alloc);

    //     map.begin();
    //     map.finish(2, &mut vec![pg3], &alloc);

    //     let snap = snapshot(&map);
    //     println!("{:?}", snap);
    //     assert_eq!(snap.get(&0), Some(&vec![pg2, pg3]));
    //     assert!(!snap.contains_key(&2));
    //     assert!(!snap.contains_key(&3));

    //     assert!(!alloc.is_free(pg1));
    //     assert!(!alloc.is_free(pg2));
    //     assert!(!alloc.is_free(pg3));

    //     map.finish(0, &mut vec![pg1], &alloc);

    //     assert!(alloc.is_free(pg1));
    //     assert!(alloc.is_free(pg2));
    //     assert!(alloc.is_free(pg3));

    //     Ok(())
    // }

    // #[named]
    // #[tokio::test]
    // async fn pages_moved_to_last_active_txn() -> Result<()> {
    //     let dir = TempDir::new(function_name!()).unwrap();
    //     let (alloc, _fio, meta) = dir.alloc("file").await?;
    //     let map = TxnFreeDeferMap::new();

    //     let mut set = AllocationSet::new();
    //     let pg1 = alloc.alloc(&meta, &mut set).await?;
    //     let pg2 = alloc.alloc(&meta, &mut set).await?;
    //     let pg3 = alloc.alloc(&meta, &mut set).await?;
    //     set.flush(&alloc).await?;

    //     map.begin();
    //     map.begin();
    //     map.begin();

    //     map.finish(0, &mut vec![pg1], &alloc);

    //     let snap = snapshot(&map);
    //     assert_eq!(snap.get(&2), Some(&vec![pg1]));

    //     map.finish(1, &mut vec![pg2], &alloc);

    //     let snap = snapshot(&map);
    //     assert_eq!(snap.get(&2), Some(&vec![pg1, pg2]));

    //     assert!(!alloc.is_free(pg1));
    //     assert!(!alloc.is_free(pg2));
    //     assert!(!alloc.is_free(pg3));

    //     map.finish(2, &mut vec![pg3], &alloc);

    //     assert!(alloc.is_free(pg1));
    //     assert!(alloc.is_free(pg2));
    //     assert!(alloc.is_free(pg3));

    //     Ok(())
    // }
}
