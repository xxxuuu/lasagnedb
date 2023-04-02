use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::map::Range;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterator::StorageIterator;

use crate::Key;

#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Key, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: Range<'this, Key, (Bound<Key>, Bound<Key>), Key, Bytes>,
    item: (Bytes, Bytes),
}

impl MemTableIterator {
    pub fn create(map: Arc<SkipMap<Key, Bytes>>, lower: Bound<Key>, upper: Bound<Key>) -> Self {
        let mut iter = MemTableIteratorBuilder {
            map,
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
        .build();
        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    fn entry_to_item(entry: Option<Entry<'_, Key, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().user_key.clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    fn meta(&self) -> &[u8] {
        unimplemented!()
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
