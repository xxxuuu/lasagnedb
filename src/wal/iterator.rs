use crate::iterator::StorageIterator;

pub struct JournalIterator {}

impl StorageIterator for JournalIterator {
    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}
