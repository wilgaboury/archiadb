use std::collections::BTreeMap;

use parking_lot::Mutex;

use crate::alloc::PageAllocator;

/// Tracks currently running transactions and ensures that freeing of pages is deferred until no transactions reference them.
pub(crate) struct TxnFreeDeferMap {
    map: Mutex<BTreeMap<u64, Vec<u64>>>,
}

impl TxnFreeDeferMap {
    pub fn new() -> Self {
        Self {
            map: Mutex::new(BTreeMap::new()),
        }
    }

    fn begin(&self, txn_id: u64) {
        let mut map = self.map.lock();
        map.insert(txn_id, Vec::with_capacity(0));
    }

    fn finish(&self, txn_id: u64, freeable: &mut Vec<u64>, alloc: &PageAllocator) {
        let free_pgs = {
            let mut map = self.map.lock();
            // Add pages to last transaction, since we can garuntee there will be no references to freed pages after it finishes
            let last_txn = map.iter_mut().next_back();
            match last_txn {
                Some((_, defer)) => defer.append(freeable),
                None => {
                    eprintln!(
                        "There should always be at least one entry since this transaction is still active"
                    );
                }
            }

            let defer = map.remove(&txn_id);
            if let Some(mut defer) = defer {
                let next_back = map.range_mut(..txn_id).next_back();
                match next_back {
                    Some((_, prev_defer)) => {
                        // Move pages to previous transaction, so they can be freed when it finishes
                        prev_defer.append(&mut defer);
                        Vec::new()
                    }
                    None => {
                        // No previous transactions exist which could reference these pages, so we can free them
                        defer
                    }
                }
            } else {
                eprintln!("Transaction was either already finished or was never added to the map");
                Vec::new()
            }
        };

        // frees do not need to occur inside lock
        for pg in free_pgs {
            alloc.free(pg);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_util::TempDir;

    use super::*;
    use anyhow::Result;
    use function_name::named;

    fn snapshot(map: &TxnFreeDeferMap) -> BTreeMap<u64, Vec<u64>> {
        map.map.lock().clone()
    }

    #[named]
    #[tokio::test]
    async fn freed_pages_moved_to_earlier_txn_and_freed_when_no_older_txns() -> Result<()> {
        let dir = TempDir::new(function_name!()).unwrap();
        let (alloc, _fio, meta) = dir.alloc("file").await?;
        let map = TxnFreeDeferMap::new();

        let mut set = alloc.create_set();
        let pg1 = alloc.alloc(&meta, &mut set).await?;
        let pg2 = alloc.alloc(&meta, &mut set).await?;
        let pg3 = alloc.alloc(&meta, &mut set).await?;
        set.flush().await?;

        map.begin(1);

        map.begin(2);
        map.finish(2, &mut vec![pg2], &alloc);

        map.begin(3);
        map.finish(3, &mut vec![pg3], &alloc);

        let snap = snapshot(&map);
        println!("{:?}", snap);
        assert_eq!(snap.get(&1), Some(&vec![pg2, pg3]));
        assert!(!snap.contains_key(&2));
        assert!(!snap.contains_key(&3));

        assert!(!alloc.is_free(pg1));
        assert!(!alloc.is_free(pg2));
        assert!(!alloc.is_free(pg3));

        map.finish(1, &mut vec![pg1], &alloc);

        assert!(alloc.is_free(pg1));
        assert!(alloc.is_free(pg2));
        assert!(alloc.is_free(pg3));

        Ok(())
    }

    #[named]
    #[tokio::test]
    async fn pages_moved_to_last_active_txn() -> Result<()> {
        let dir = TempDir::new(function_name!()).unwrap();
        let (alloc, _fio, meta) = dir.alloc("file").await?;
        let map = TxnFreeDeferMap::new();

        let mut set = alloc.create_set();
        let pg1 = alloc.alloc(&meta, &mut set).await?;
        let pg2 = alloc.alloc(&meta, &mut set).await?;
        let pg3 = alloc.alloc(&meta, &mut set).await?;
        set.flush().await?;

        map.begin(1);
        map.begin(2);
        map.begin(3);

        map.finish(1, &mut vec![pg1], &alloc);

        let snap = snapshot(&map);
        assert_eq!(snap.get(&3), Some(&vec![pg1]));

        map.finish(2, &mut vec![pg2], &alloc);

        let snap = snapshot(&map);
        assert_eq!(snap.get(&3), Some(&vec![pg1, pg2]));

        assert!(!alloc.is_free(pg1));
        assert!(!alloc.is_free(pg2));
        assert!(!alloc.is_free(pg3));

        map.finish(3, &mut vec![pg3], &alloc);

        assert!(alloc.is_free(pg1));
        assert!(alloc.is_free(pg2));
        assert!(alloc.is_free(pg3));

        Ok(())
    }
}
