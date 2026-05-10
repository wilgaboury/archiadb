use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::alloc::PageAllocator;

/// Tracks all currently running transactions and ensures that freeing of pages is deferred until no transactions reference them.
struct TxnMap {
    next: AtomicU64, // TODO: does not need to be an atomic
    defer: Mutex<BTreeMap<u64, Vec<u64>>>,
    alloc: Arc<PageAllocator>,
}

impl TxnMap {
    pub fn new(alloc: Arc<PageAllocator>) -> Self {
        Self {
            next: AtomicU64::new(0),
            defer: Mutex::new(BTreeMap::new()),
            alloc,
        }
    }

    fn begin(&self) -> TxnMapGuard<'_> {
        let mut defer = self.defer.lock().unwrap();
        let id = self.next.fetch_add(1, Ordering::AcqRel);
        defer.insert(id, Vec::with_capacity(0));
        TxnMapGuard {
            id,
            map: self,
            free: Vec::new(),
        }
    }
}

struct TxnMapGuard<'a> {
    id: u64,
    map: &'a TxnMap,
    free: Vec<u64>,
}

impl TxnMapGuard<'_> {
    fn add_free(&mut self, id: u64) {
        self.free.push(id);
    }
}

impl Drop for TxnMapGuard<'_> {
    fn drop(&mut self) {
        let free_pgs = {
            let mut defer_map = self.map.defer.lock().unwrap();
            // Add pages to last transaction, since we can garuntee there will be no references to freed pages after it finishes
            let last_txn = defer_map.iter_mut().next_back();
            match last_txn {
                Some((_, defer)) => defer.append(&mut self.free),
                None => {
                    eprintln!(
                        "There should always be at least one entry since this transaction is still active"
                    );
                }
            }

            let defer = defer_map.remove(&self.id);
            if let Some(mut defer) = defer {
                let next_back = defer_map.range_mut(..self.id).next_back();
                match next_back {
                    Some((_, prev_defer)) => {
                        // Move pages to previous transaction, so they can be freed when it finishes
                        prev_defer.append(&mut defer);
                        None
                    }
                    None => {
                        // No previous transactions exist which could reference these pages, so we can free them
                        Some(defer)
                    }
                }
            } else {
                eprintln!(
                    "This case should never be hit because each transaction adds itself to the map when started"
                );
                None
            }
        };

        // frees do not need to occur inside lock
        if let Some(free_pgs) = free_pgs {
            for pg in free_pgs {
                self.map.alloc.free(pg);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::TempDir;

    use super::*;
    use anyhow::Result;
    use function_name::named;

    fn snapshot(map: &TxnMap) -> BTreeMap<u64, Vec<u64>> {
        map.defer.lock().unwrap().clone()
    }

    #[named]
    #[tokio::test]
    async fn freed_pages_moved_to_earlier_txn_and_freed_when_no_older_txns() -> Result<()> {
        let dir = TempDir::new(function_name!()).unwrap();
        let alloc = Arc::new(dir.alloc("file").await?);
        let map = TxnMap::new(alloc.clone());

        let mut set = alloc.create_set();
        let pg1 = alloc.alloc(&mut set).await?;
        let pg2 = alloc.alloc(&mut set).await?;
        let pg3 = alloc.alloc(&mut set).await?;
        set.flush().await?;

        let mut t1 = map.begin();
        t1.add_free(pg1);

        let mut t2 = map.begin();
        t2.add_free(pg2);
        drop(t2);

        let mut t3 = map.begin();
        t3.add_free(pg3);
        drop(t3);

        let snap = snapshot(&map);
        println!("{:?}", snap);
        assert_eq!(snap.get(&0), Some(&vec![pg2, pg3]));
        assert!(!snap.contains_key(&1));
        assert!(!snap.contains_key(&2));

        assert!(!alloc.is_free(pg1));
        assert!(!alloc.is_free(pg2));
        assert!(!alloc.is_free(pg3));

        drop(t1);

        assert!(alloc.is_free(pg1));
        assert!(alloc.is_free(pg2));
        assert!(alloc.is_free(pg3));

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn pages_moved_to_last_active_txn() -> Result<()> {
        let dir = TempDir::new(function_name!()).unwrap();
        let alloc = Arc::new(dir.alloc("file").await?);
        let map = TxnMap::new(alloc.clone());

        let mut set = alloc.create_set();
        let pg1 = alloc.alloc(&mut set).await?;
        let pg2 = alloc.alloc(&mut set).await?;
        let pg3 = alloc.alloc(&mut set).await?;
        set.flush().await?;

        let mut t1 = map.begin();
        t1.add_free(pg1);

        let mut t2 = map.begin();
        t2.add_free(pg2);

        let mut t3 = map.begin();
        t3.add_free(pg3);

        drop(t1);

        let snap = snapshot(&map);
        assert_eq!(snap.get(&2), Some(&vec![pg1]));

        drop(t2);

        let snap = snapshot(&map);
        assert_eq!(snap.get(&2), Some(&vec![pg1, pg2]));

        assert!(!alloc.is_free(pg1));
        assert!(!alloc.is_free(pg2));
        assert!(!alloc.is_free(pg3));

        drop(t3);

        assert!(alloc.is_free(pg1));
        assert!(alloc.is_free(pg2));
        assert!(alloc.is_free(pg3));

        Ok(())
    }
}
