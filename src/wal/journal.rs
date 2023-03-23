use anyhow::anyhow;

use std::io::{Write};

use std::path::{Path};
use std::sync::Arc;


use bytes::{Buf, Bytes};


use crate::entry::Entry;
use crate::record::{Record, RecordBuilder, RecordItem};
use crate::storage::file::FileStorage;

#[derive(Debug)]
pub struct Journal {
    file: FileStorage,
    records: Vec<Arc<Record<JournalItem>>>,
}

impl Journal {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let file = FileStorage::open(path)?;
        let mut records = vec![];

        let mut buf = Bytes::from(file.read_to_end(0)?);
        while buf.has_remaining() {
            records.push(Arc::new(Record::decode_with_bytes(&mut buf)?));
        }

        Ok(Self { file, records })
    }

    pub fn num_of_records(&self) -> usize {
        self.records.len()
    }

    pub fn rename(&self, new_path: impl AsRef<Path>) -> anyhow::Result<()> {
        self.file.rename(new_path)
    }

    pub fn delete(&self) -> anyhow::Result<()> {
        self.file.delete()
    }

    pub fn write(&self, batches: Vec<Entry>) -> anyhow::Result<()> {
        let mut builder = RecordBuilder::new();
        for i in batches {
            builder.add(JournalItem(i));
        }
        let record = builder.build();
        self.file.write(&record.encode());
        self.file.sync();
        Ok(())
    }

    pub fn read_record(&self, record_idx: usize) -> anyhow::Result<Arc<Record<JournalItem>>> {
        if record_idx >= self.num_of_records() {
            return Err(anyhow!(
                "index out of bound, blocks num: {}, record_idx: {}",
                self.num_of_records(),
                record_idx
            ));
        }

        Ok(self.records[record_idx].clone())
    }
}

#[derive(Debug, Clone)]
pub struct JournalItem(Entry);

impl RecordItem for JournalItem {
    fn encode(&self) -> Bytes {
        self.0.encode()
    }

    fn decode_with_bytes(bytes: &mut Bytes) -> anyhow::Result<Self> {
        Ok(Self(Entry::decode_with_bytes(bytes)))
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

impl AsRef<Entry> for JournalItem {
    fn as_ref(&self) -> &Entry {
        &self.0
    }
}