use anyhow::Result;
use tokio::sync::Mutex;

use crate::{db::DbInner, fio::PageBuf};

#[derive(Debug)]
pub(crate) struct Galloc {
    lock: Mutex<()>,
}

impl Galloc {
    pub(crate) fn new() -> Self {
        Self {
            lock: Mutex::new(()),
        }
    }
}

impl DbInner {
    pub(crate) async fn galloc(&self, root: PageBuf, root_idx: u64) -> Result<()> {
        let _gaurd = self.galloc.lock.lock().await;

        todo!("implement")
    }
}
