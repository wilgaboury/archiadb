use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::fio::Fio;

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(suffix: &str) -> Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "{}_{}_{}",
            suffix,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn fio<P: AsRef<Path>>(&self, path: P) -> Result<Fio> {
        Fio::builder()
            .sq(2)
            .cq(4)
            .page_buf_pool(2)
            .path(self.path.join(path.as_ref()))
            .build()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
