use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::entry::EntryBuilder;
use bytes::Bytes;

use crossbeam_skiplist::SkipMap;

use crate::memtable::iterator::MemTableIterator;
use crate::sstable::builder::SsTableBuilder;
use crate::Key;
use crate::OpType;

#[derive(Debug)]
pub struct MemTable {
    db: Arc<SkipMap<Key, Bytes>>,
    size: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            db: Arc::new(SkipMap::new()),
            size: AtomicUsize::new(0),
        }
    }

    pub fn put(&self, key: Key, value: Bytes) {
        self.size
            .fetch_add(key.len() + value.len(), Ordering::Release);
        self.db.insert(key, value);
    }

    pub fn get(&self, key: &Key) -> Option<(Key, Bytes)> {
        match self.db.range(key..).next() {
            None => None,
            Some(e) => {
                if e.key().op_type == OpType::Delete {
                    None
                } else if e.key().user_key != key.user_key {
                    None
                } else {
                    Some((e.key().clone(), e.value().clone()))
                }
            }
        }
    }

    pub fn scan(&self, begin: Bound<Key>, end: Bound<Key>) -> MemTableIterator {
        let iter = self.db.range((begin, end));
        MemTableIterator::new(iter)
    }

    pub fn flush(&self, builder: &mut SsTableBuilder) -> anyhow::Result<()> {
        for e in self.db.iter() {
            let key = e.key().clone();
            let user_key = key.user_key.clone();
            let value = e.value().clone();
            let entry = EntryBuilder::new()
                .op_type(key.op_type)
                .key_value(user_key, value)
                .build();

            builder.add(&entry);
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.size.store(0, Ordering::Release);
        self.db.clear();
    }

    pub fn size(&self) -> usize {
        self.size.load(Ordering::Acquire)
    }
}
