pub use crate::block::builder::Block;
use crate::entry::{Entry, EntryBuilder};
use std::sync::Arc;

/// Iterates on a block.
#[derive(Debug)]
pub struct BlockIterator {
    block: Arc<Block>,
    meta: Vec<u8>,
    entry: Entry,
    valid: bool,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            meta: vec![],
            entry: EntryBuilder::empty(),
            valid: false,
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Return the current entry.
    pub fn entry(&self) -> &Entry {
        debug_assert!(self.valid, "invalid iterator");
        &self.entry
    }

    /// Returns meta info of the current entry.
    pub fn meta(&self) -> &[u8] {
        debug_assert!(self.valid, "invalid iterator");
        &self.meta[..]
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        debug_assert!(self.valid, "invalid iterator");
        &self.entry.key[..]
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid, "invalid iterator");
        &self.entry.value[..]
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.valid
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Seeks to the idx-th key in the block.
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.entry = EntryBuilder::empty();
            self.valid = false;
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let entry = Entry::decode(&self.block.data[offset..]);
        self.entry = entry;
        self.meta = self.entry.meta.to_le_bytes().to_vec();
        self.valid = true;
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}
