use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

use anyhow::Result;

pub struct DbFile {
    path: PathBuf,
    file: File,
}

impl DbFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        file.try_lock()?; // prevent multiple processes from operating on the same file

        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file(&self) -> &File {
        &self.file
    }
}

impl Drop for DbFile {
    fn drop(&mut self) {
        self.file.sync_all().expect("Failed to sync file on drop");
    }
}
