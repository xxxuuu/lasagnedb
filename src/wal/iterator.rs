use crate::record::RecordIterator;
use crate::wal::{Journal, JournalItem};
use std::sync::Arc;
use tracing::instrument;

#[derive(Debug)]
pub struct JournalIterator {
    journal: Arc<Journal>,
    record_iter: RecordIterator<JournalItem>,
    idx: usize,
}

impl JournalIterator {
    #[instrument]
    pub fn create_and_seek_to_first(journal: Arc<Journal>) -> anyhow::Result<Self> {
        Ok(Self {
            journal: journal.clone(),
            record_iter: RecordIterator::create_and_seek_to_first(journal.read_record(0)?)?,
            idx: 0,
        })
    }

    pub fn is_valid(&self) -> bool {
        self.record_iter.is_valid()
    }

    pub fn record_item(&self) -> JournalItem {
        self.record_iter.record_item()
    }

    #[instrument]
    pub fn next(&mut self) -> anyhow::Result<()> {
        self.record_iter.next();
        if !self.record_iter.is_valid() {
            self.idx += 1;
            if self.idx < self.journal.num_of_records() {
                self.record_iter =
                    RecordIterator::create_and_seek_to_first(self.journal.read_record(self.idx)?)?;
            }
        }

        Ok(())
    }
}
