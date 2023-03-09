use crate::block::iterator::BlockIterator;
use crate::iterator::StorageIterator;
use crate::sstable::builder::SsTable;
use anyhow::Result;
use std::sync::Arc;

pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(table.read_block(0)?),
        ))
    }

    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            block_iter,
            table,
            block_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&self.table)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter = BlockIterator::create_and_seek_to_key(table.read_block(blk_idx)?, key);
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter = BlockIterator::create_and_seek_to_first(table.read_block(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            block_iter,
            table,
            block_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.block_iter = block_iter;
        self.block_idx = block_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iter =
                    BlockIterator::create_and_seek_to_first(self.table.read_block(self.block_idx)?);
            }
        }
        Ok(())
    }
}
