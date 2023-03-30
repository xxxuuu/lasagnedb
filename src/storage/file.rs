use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::ioarc::IoArc;
use anyhow::Result;
use parking_lot::Mutex;
use tracing::instrument;

use crate::storage::storage::Storage;

struct FileStorageInner {
    file: Arc<File>,
    reader: BufReader<IoArc<File>>,
    writer: BufWriter<IoArc<File>>,
}

impl FileStorageInner {
    pub fn new(file: Arc<File>) -> Self {
        Self {
            file: file.clone(),
            reader: BufReader::new(IoArc::from_arc(file.clone())),
            writer: BufWriter::new(IoArc::from_arc(file)),
        }
    }
}

pub struct FileStorage {
    inner: Mutex<FileStorageInner>,
    path: PathBuf,
}

impl FileStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = Arc::new(
            File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?,
        );
        Ok(Self {
            inner: Mutex::new(FileStorageInner::new(file)),
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
            inner: Mutex::new(FileStorageInner::new(Arc::new(file))),
            path: PathBuf::from(path.as_ref()),
        })
    }

    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        
        let mut data = vec![0; len as usize];
        let mut guard = self.inner.lock();
        guard.reader.seek(SeekFrom::Start(offset))?;
        guard.reader.read_exact(&mut data)?;
        Ok(data)
    }

    pub fn read_to_end(&self, offset: u64) -> Result<Vec<u8>> {
        let mut buf = vec![];
        let mut guard = self.inner.lock();
        guard.reader.seek(SeekFrom::Start(offset))?;
        guard.reader.read_to_end(&mut buf)?;
        Ok(buf)
    }

    #[instrument(skip_all)]
    pub fn write(&self, data: &[u8]) {
        let mut guard = self.inner.lock();
        guard.writer.seek(SeekFrom::End(0)).unwrap();
        guard.writer.write_all(data).unwrap();
    }

    #[instrument(skip_all)]
    pub fn sync(&self) {
        self.inner.lock().writer.flush().unwrap();
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

impl Debug for FileStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStorage")
            .field("path", &self.path)
            .finish()
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
