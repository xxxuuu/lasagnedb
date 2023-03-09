use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Deref;

use std::{io, result};

use bytes::{BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::entry::Entry;
use crate::wal::{JournalReader, JournalWriter};

pub type Result<T> = result::Result<T, JournalError>;

#[derive(Error, Debug)]
pub enum JournalError {
    #[error("io error: {0}")]
    IOError(#[from] io::Error),
}

pub struct Journal {
    fd: File,
}

impl Journal {
    pub fn new(f: File) -> Journal {
        Journal { fd: f }
    }

    fn next(&self) -> JournalWriter<File> {
        let mut f = self.fd.try_clone().unwrap();
        f.seek(SeekFrom::End(0)).unwrap();
        JournalWriter::new(f)
    }

    fn iter(&mut self) -> JournalReader<File> {
        let mut f = self.fd.try_clone().unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        JournalReader::new(f)
    }

    pub fn rotate() {
        todo!()
    }

    pub fn write(&self, seq_number: u64, batches: Vec<Entry>) -> Result<()> {
        let mut single_writer = self.next();
        let journal_item = JournalItem::new(seq_number, batches);
        single_writer.write(&journal_item[..])?;
        single_writer.flush()?;
        Ok(())
    }
}

/// `JournalItem` is a singe journal written
/// ```text
/// +--------------------------+-----------------------+------------+-----+------------+
/// | sequence number(8 bytes) | entry number(4 bytes) | batch data | ... | batch data |
/// +--------------------------+-----------------------+------------+-----+------------+
/// ```
pub struct JournalItem(Bytes);

impl JournalItem {
    pub fn new(seq_number: u64, batches: Vec<Entry>) -> Self {
        let mut data = BytesMut::new();
        data.put_u64_le(seq_number);
        data.put_u32_le(batches.len() as u32);
        for b in &batches {
            data.put(b.encode());
        }
        JournalItem(data.freeze())
    }

    pub fn with_bytes(data: Bytes) -> Self {
        JournalItem(data)
    }

    pub fn parse() -> (u64, u32, Vec<Entry>) {
        todo!()
    }
}

impl Deref for JournalItem {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::entry::Entry;
    use crate::value::OpType;
    use crate::wal::{Journal, JournalItem};

    fn test_batches() -> Vec<Entry> {
        vec![
            Entry::new(OpType::Get.encode(), Bytes::from("k1"), Bytes::from("v1")),
            Entry::new(OpType::Get.encode(), Bytes::from("k2"), Bytes::from("v2")),
            Entry::new(OpType::Get.encode(), Bytes::from("k3"), Bytes::from("v3")),
        ]
    }

    #[test]
    fn test_journal() {
        let file = tempfile::tempfile().unwrap();
        let mut wal = Journal::new(file.try_clone().unwrap());

        let batches = test_batches();

        wal.write(1, batches).expect("write error");

        let byte = JournalItem::new(1, test_batches()).0;
        let byte2 = wal.iter().next().unwrap().0;
        assert_eq!(byte, byte2);
    }
}
