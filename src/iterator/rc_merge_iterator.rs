use crate::entry::Entry;
use crate::iterator::merge_iterator::MergeIterator;
use crate::sstable::iterator::{SsTableIterator, VSsTableIterator};
use crate::StorageIterator;
use bytes::Buf;
use std::collections::binary_heap::PeekMut;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct RcMergeIterator<I: StorageIterator> {
    iter: MergeIterator<I>,
    vsst_rc_delta: HashMap<u32, i32>,
}

impl<I: StorageIterator> RcMergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        Self {
            iter: MergeIterator::create(iters),
            vsst_rc_delta: HashMap::default(),
        }
    }

    pub fn vsst_rc_delta(self) -> HashMap<u32, i32> {
        self.vsst_rc_delta
    }
}

impl<I: StorageIterator> StorageIterator for RcMergeIterator<I> {
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
        let current = unsafe { self.iter.current.as_mut().unwrap_unchecked() };
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iter.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );
            if inner_iter.1.key() == current.1.key() {
                // 当前项被忽略，如果是分离的话就减少对应 VSST 引用计数
                if Entry::is_separate(inner_iter.1.meta()) {
                    let vsst_id = inner_iter.1.value().get_u32_le();
                    self.vsst_rc_delta
                        .insert(vsst_id, self.vsst_rc_delta.get(&vsst_id).unwrap_or(&0) - 1);
                }

                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iter.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut inner_iter) = self.iter.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }
}
