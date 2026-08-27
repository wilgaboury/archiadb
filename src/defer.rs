use std::collections::BTreeMap;

use dashmap::DashSet;
use parking_lot::Mutex;

use crate::uint::PgIdx;

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

    pub fn contains(&self, pg_idx: PgIdx) -> bool {
        self.global.contains(&pg_idx)
    }
}

pub(crate) struct DeferGaurd<'txn> {
    id: DeferId,
    freed: Vec<PgIdx>,
    pub(crate) defer: &'txn Defer,
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
    pub(crate) fn free(&mut self, pg_idx: PgIdx) {
        self.freed.push(pg_idx);
    }

    pub(crate) fn flush(&mut self) {
        for pg in self.freed.iter() {
            self.defer.global.insert(*pg);
        }

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
    }
}

impl<'a> Drop for DeferGaurd<'a> {
    fn drop(&mut self) {
        let free = {
            let mut inner = self.defer.inner.lock();
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

#[coverage(off)]
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    fn snapshot(map: &Defer) -> BTreeMap<u64, Vec<u64>> {
        map.inner.lock().to_free.clone()
    }

    #[tokio::test]
    async fn defer_workflow() -> Result<()> {
        let defer = Defer::new();

        let g1 = defer.begin();
        let g1_id = g1.id;
        let mut g2 = defer.begin();
        let g2_id = g2.id;

        assert!(g1_id < g2_id);

        g2.free(1);
        g2.flush();

        let snap = snapshot(&defer);

        assert_eq!(*snap.get(&g1_id).unwrap(), vec![]);
        assert_eq!(*snap.get(&g2_id).unwrap(), vec![1u64]);

        drop(g2);

        let snap = snapshot(&defer);

        assert_eq!(*snap.get(&g1_id).unwrap(), vec![1u64]);
        assert!(snap.get(&g2_id).is_none());

        let mut g3 = defer.begin();
        let g3_id = g3.id;
        let g4 = defer.begin();
        let g4_id = g4.id;

        assert!(g2_id < g3_id && g3_id < g4_id);

        g3.free(2);
        g3.free(3);
        g3.flush();

        let snap = snapshot(&defer);

        assert_eq!(*snap.get(&g1_id).unwrap(), vec![1u64]);
        assert!(snap.get(&g2_id).is_none());
        assert_eq!(*snap.get(&g3_id).unwrap(), vec![]);
        assert_eq!(*snap.get(&g4_id).unwrap(), vec![2u64, 3u64]);

        drop(g4);

        let snap = snapshot(&defer);

        assert_eq!(*snap.get(&g1_id).unwrap(), vec![1u64]);
        assert!(snap.get(&g2_id).is_none());
        assert_eq!(*snap.get(&g3_id).unwrap(), [2u64, 3u64]);
        assert!(snap.get(&g4_id).is_none());

        drop(g3);

        let snap = snapshot(&defer);

        assert_eq!(*snap.get(&g1_id).unwrap(), vec![1u64, 2u64, 3u64]);
        assert!(snap.get(&g2_id).is_none());
        assert!(snap.get(&g3_id).is_none());
        assert!(snap.get(&g4_id).is_none());

        assert_eq!(3, defer.global.len());

        drop(g1);

        let snap = snapshot(&defer);

        assert!(snap.get(&g2_id).is_none());
        assert!(snap.get(&g2_id).is_none());
        assert!(snap.get(&g3_id).is_none());
        assert!(snap.get(&g4_id).is_none());

        assert_eq!(0, defer.global.len());

        Ok(())
    }
}
