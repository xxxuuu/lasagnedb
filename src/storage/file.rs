use crate::storage::storage::Storage;
use anyhow::Result;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileStorageError {}

pub struct FileStorage {
    file: Mutex<File>,
}

impl FileStorage {
    pub fn create(path: impl AsRef<Path>, data: Vec<u8>) -> Result<Self> {
        let mut file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.write_all(&data).unwrap();
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(0))?;
        file.read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }
}

impl Storage for FileStorage {}
