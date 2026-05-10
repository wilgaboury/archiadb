use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::alloc::PageAllocator;

struct TxnMap {
    next: AtomicU64,
    free: Mutex<BTreeMap<u64, Vec<u64>>>,
    alloc: PageAllocator,
}

impl TxnMap {
    fn begin(&self) -> TxnMapGaurd<'_> {
        let id = self.next.fetch_add(1, Ordering::AcqRel);
        TxnMapGaurd {
            id,
            map: self,
            free: Vec::new(),
        }
    }
}

struct TxnMapGaurd<'a> {
    id: u64,
    map: &'a TxnMap,
    free: Vec<u64>,
}

impl TxnMapGaurd<'_> {
    fn add_free(&mut self, id: u64) {
        self.free.push(id);
    }
}

impl Drop for TxnMapGaurd<'_> {
    fn drop(&mut self) {
        let mut free_map = self.map.free.lock().unwrap();
        let cur = self.map.next.load(Ordering::Acquire);
        // Add freed blocks to next transaction, since we can garuntee there will be no references to them after it finishes
        match free_map.get_mut(&cur) {
            Some(ids) => {
                ids.append(&mut self.free);
            }
            None => {
                free_map.insert(cur, std::mem::take(&mut self.free));
            }
        }
        let free = free_map.remove(&self.id);
        if let Some(mut free) = free {
            let next_back = free_map.range_mut(..self.id).next_back();
            match next_back {
                Some((_, free_vec)) => {
                    // Move blocks to previous transaction, so they can be freed when it finishes
                    free_vec.append(&mut free);
                }
                None => {
                    // No previous transactions can reference these blocks, so we can free them
                    for pg_idx in free {
                        self.map.alloc.free(pg_idx);
                    }
                }
            }
        }
    }
}
