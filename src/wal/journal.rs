use anyhow::anyhow;
use std::fmt::{Debug, Formatter};

use std::path::Path;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use tracing::instrument;

use crate::entry::Entry;
use crate::record::{Record, RecordBuilder, RecordItem};
use crate::storage::file::FileStorage;

pub struct Journal {
    id: u32,
    file: FileStorage,
    records: Vec<Arc<Record<JournalItem>>>,
}

impl Journal {
    #[instrument]
    pub fn open(id: u32, path: impl AsRef<Path> + Debug) -> anyhow::Result<Self> {
        // TODO 优化
        let file = FileStorage::open(path)?;
        let mut records = vec![];

        let mut buf = Bytes::from(file.read_to_end(0)?);
        while buf.has_remaining() {
            records.push(Arc::new(Record::decode_with_bytes(&mut buf)?));
        }

        Ok(Self { id, file, records })
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn num_of_records(&self) -> usize {
        self.records.len()
    }

    pub fn delete(&self) -> anyhow::Result<()> {
        self.file.delete()
    }

    #[instrument(skip_all)]
    pub fn write(&self, batches: Vec<Entry>) -> anyhow::Result<()> {
        let mut builder = RecordBuilder::with_len(batches.len());
        for i in batches {
            builder.add(JournalItem(i));
        }
        let record = builder.build();
        self.file.write(&record.encode());
        Ok(())
    }

    #[instrument]
    pub fn flush(&self) {
        self.file.sync();
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

impl Debug for Journal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("file", &self.file)
            .field("records len", &self.records.len())
            .finish()
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
