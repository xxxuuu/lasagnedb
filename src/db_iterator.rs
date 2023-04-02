use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::two_merge_iterator::TwoMergeIterator;
use crate::iterator::StorageIterator;
use crate::memtable::iterator::MemTableIterator;
use crate::sstable::iterator::VSsTableIterator;
use bytes::Bytes;
use std::ops::Bound;

type DbIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<VSsTableIterator>>;

pub struct DbIterator {
    iter: DbIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl DbIterator {
    pub(crate) fn new(iter: DbIteratorInner, end_bound: Bound<Bytes>) -> anyhow::Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            iter,
            end_bound,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> anyhow::Result<()> {
        self.iter.next()?;
        if !self.iter.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.iter.key() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.iter.key() < key.as_ref(),
        }
        Ok(())
    }

    fn move_to_non_delete(&mut self) -> anyhow::Result<()> {
        while self.is_valid() && self.iter.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for DbIterator {
    fn meta(&self) -> &[u8] {
        self.iter.meta()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn meta(&self) -> &[u8] {
        self.iter.meta()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        if self.iter.is_valid() {
            self.iter.next()?;
        }
        Ok(())
    }
}
