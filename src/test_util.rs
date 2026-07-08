use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

use crate::{alloc::PageAllocator, db::Db, file::DbFile, fio::Fio, meta::MetaHandler};

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

    pub async fn db<P: AsRef<Path>>(&self, path: P) -> Result<Db> {
        Db::builder()
            .path(self.path.join(path.as_ref()))
            .build()
            .await
    }

    pub fn db_file<P: AsRef<Path>>(&self, path: P) -> Result<DbFile> {
        DbFile::open(self.path.join(path.as_ref()))
    }

    pub fn file<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path.join(path.as_ref()))?)
    }

    pub fn meta<P: AsRef<Path>>(&self, path: P) -> Result<MetaHandler> {
        let file = self.file(path)?;
        MetaHandler::new(&file)
    }

    pub fn fio<P: AsRef<Path>>(&self, path: P) -> Result<(Fio, MetaHandler)> {
        let file = self.db_file(path)?;
        let meta = MetaHandler::new(file.file())?;
        Ok((
            Fio::builder()
                .file(Arc::new(file))
                .meta(&meta)
                .sq(2)
                .cq(4)
                .page_buf_pool(2)
                .build()?,
            meta,
        ))
    }

    pub fn fio_cust<P: AsRef<Path>>(&self, path: P) -> Result<(Arc<DbFile>, MetaHandler)> {
        let file = self.db_file(path)?;
        let meta = MetaHandler::new(file.file())?;
        Ok((Arc::new(file), meta))
    }

    pub async fn alloc<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<(PageAllocator, Fio, MetaHandler)> {
        let (fio, meta) = self.fio(path)?;
        Ok((PageAllocator::new(fio.clone(), &meta).await?, fio, meta))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
