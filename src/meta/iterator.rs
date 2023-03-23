use crate::meta::manifest::{Manifest, ManifestItem};
use crate::record::{RecordIterator};
use std::sync::Arc;

pub struct ManifestIterator {
    manifest: Arc<Manifest>,
    record_iter: RecordIterator<ManifestItem>,
    idx: usize,
}

impl ManifestIterator {
    pub fn create_and_seek_to_first(manifest: Arc<Manifest>) -> anyhow::Result<Self> {
        Ok(Self {
            manifest: manifest.clone(),
            record_iter: RecordIterator::create_and_seek_to_first(manifest.read_record(0)?)?,
            idx: 0,
        })
    }

    pub fn is_valid(&self) -> bool {
        self.record_iter.is_valid()
    }

    pub fn record_item(&self) -> ManifestItem {
        self.record_iter.record_item()
    }

    pub fn next(&mut self) -> anyhow::Result<()> {
        self.record_iter.next();
        if !self.record_iter.is_valid() {
            self.idx += 1;
            if self.idx < self.manifest.num_of_records() {
                self.record_iter =
                    RecordIterator::create_and_seek_to_first(self.manifest.read_record(self.idx)?)?;
            }
        }

        Ok(())
    }
}
