use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};

use anyhow::Result;
use parking_lot::Mutex;
use thiserror::Error;

use crate::storage::storage::Storage;

#[derive(Debug, Error)]
pub enum FileStorageError {}

#[derive(Debug)]
pub struct FileStorage {
    file: Mutex<File>,
    path: PathBuf,
}

impl FileStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(Self {
            file: Mutex::new(file),
            path: PathBuf::from(path.as_ref()),
        })
    }

    pub fn create(path: impl AsRef<Path>, data: Vec<u8>) -> Result<Self> {
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.write_all(&data).unwrap();
        Ok(Self {
            file: Mutex::new(file),
            path: PathBuf::from(path.as_ref()),
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

    pub fn read_to_end(&self, offset: u64) -> Result<Vec<u8>> {
        let mut buf = vec![];
        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(offset))?;
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn write(&self, data: &[u8]) {
        let mut file = self.file.lock();
        file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(data).unwrap();
    }

    pub fn sync(&self) {
        let file = self.file.lock();
        file.sync_all().unwrap();
    }

    pub fn rename(&self, new_path: impl AsRef<Path>) -> anyhow::Result<()> {
        fs::rename(&self.path, &new_path)?;
        Ok(())
    }

    pub fn delete(&self) -> anyhow::Result<()> {
        fs::remove_file(&self.path)?;
        Ok(())
    }

    pub fn size(&self) -> anyhow::Result<u64> {
        let metadata = fs::metadata(&self.path)?;
        Ok(metadata.len())
    }
}

impl Storage for FileStorage {}

#[cfg(test)]
mod tests {
    use crate::storage::file::FileStorage;
    use bytes::Bytes;
    use std::fs;

    #[test]
    fn test_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path()).unwrap();
        let path = dir.path().join("TEST");
        let file = FileStorage::open(path).unwrap();
        file.write(b"123");
        file.sync();

        let content = file.read_to_end(0).unwrap();
        assert_eq!(Bytes::from(content), Bytes::from("123"));
    }
}
