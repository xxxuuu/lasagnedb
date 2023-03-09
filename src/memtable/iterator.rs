use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Range;

use crate::iterator::StorageIterator;

use crate::Key;

pub struct MemTableIterator<'a> {
    iter: Range<'a, Key, (Bound<Key>, Bound<Key>), Key, Bytes>,
    valid: bool,
    key: Bytes,
    value: Bytes,
}

impl<'a> MemTableIterator<'a> {
    pub fn new(rang: Range<'a, Key, (Bound<Key>, Bound<Key>), Key, Bytes>) -> Self {
        let mut i = MemTableIterator {
            iter: rang,
            valid: true,
            key: Bytes::new(),
            value: Bytes::new(),
        };
        i.next().expect("MemTableIterator create failed");
        i
    }
}

impl StorageIterator for MemTableIterator<'_> {
    fn key(&self) -> &[u8] {
        &self.key[..]
    }

    fn value(&self) -> &[u8] {
        &self.value[..]
    }

    fn is_valid(&self) -> bool {
        self.valid
    }

    fn next(&mut self) -> Result<()> {
        if let Some(e) = self.iter.next() {
            self.valid = true;
            self.key = e.key().encode();
            self.value = e.value().clone();
        } else {
            self.valid = false;
        }
        Ok(())
    }
}
