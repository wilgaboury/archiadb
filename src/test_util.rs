use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

use crate::{
    db::Db,
    file::DbFile,
    fio::Fio,
    meta::MetaHandler,
    util::{ChecksumDisk, from_bytes_mut},
};

pub(crate) fn corrupt_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = from_bytes_mut::<ChecksumDisk>(&mut buf[len - size_of::<ChecksumDisk>()..]);
    checksum.set(checksum.get() + 1);
}

pub(crate) fn uncorrput_checksum(buf: &mut [u8]) {
    let len = buf.len();
    let checksum = from_bytes_mut::<ChecksumDisk>(&mut buf[len - size_of::<ChecksumDisk>()..]);
    checksum.set(checksum.get() - 1);
}

pub(crate) struct TempDir {
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

    pub fn root(&self) -> &Path {
        &self.path
    }

    pub fn path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.path.join(path)
    }

    pub async fn db<P: AsRef<Path>>(&self, path: P) -> Result<Db> {
        Db::builder()
            .path(self.path(path))
            .sq(2)
            .cq(4)
            .page_buf_pool(2)
            .build()
            .await
    }

    pub fn file<P: AsRef<Path>>(&self, path: P) -> Result<DbFile> {
        DbFile::open(self.path.join(path.as_ref()))
    }

    pub fn file_raw<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.path.join(path.as_ref()))?)
    }

    pub fn meta<P: AsRef<Path>>(&self, path: P) -> Result<MetaHandler> {
        let file = self.file_raw(path)?;
        MetaHandler::new(&file)
    }

    pub fn fio<P: AsRef<Path>>(&self, path: P) -> Result<(Fio, MetaHandler)> {
        let file = self.file(path)?;
        let meta = MetaHandler::new(file.file())?;
        Ok((
            Fio::builder()
                .file(Arc::new(file))
                .page_size(meta.page_size())
                .sq(2)
                .cq(4)
                .page_buf_pool(2)
                .build()?,
            meta,
        ))
    }

    pub fn fio_cust<P: AsRef<Path>>(&self, path: P) -> Result<(Arc<DbFile>, MetaHandler)> {
        let file = self.file(path)?;
        let meta = MetaHandler::new(file.file())?;
        Ok((Arc::new(file), meta))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
