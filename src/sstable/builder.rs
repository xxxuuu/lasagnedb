use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::block::builder::{Block, BlockBuilder};
use crate::cache::BlockCache;
use crate::entry::Entry;
use crate::sstable::meta::MetaBlock;
use crate::storage::file::FileStorage;

pub struct SsTable {
    id: usize,
    file: FileStorage,
    metas: Vec<MetaBlock>,
    meta_offset: usize,
    cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    pub fn open(
        _id: usize,
        _block_cache: Option<Arc<BlockCache>>,
        _file: FileStorage,
    ) -> Result<Self> {
        unimplemented!()
    }

    pub fn num_of_blocks(&self) -> usize {
        self.metas.len()
    }

    fn read_block_with_disk(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.metas[block_idx].offset;
        let offset_end = self
            .metas
            .get(block_idx + 1)
            .map_or(self.meta_offset, |x| x.offset as usize);
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset as usize) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || {
                    self.read_block_with_disk(block_idx)
                })
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block_with_disk(block_idx)
        }
    }

    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.metas
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }
}

pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    meta: Vec<MetaBlock>,
    data: Vec<u8>,
}

impl SsTableBuilder {
    pub fn new() -> SsTableBuilder {
        SsTableBuilder {
            builder: BlockBuilder::new(),
            first_key: Vec::new(),
            meta: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn add(&mut self, e: &Entry) {
        if self.first_key.is_empty() {
            self.first_key = e.key.to_vec();
        }

        if self.builder.add(e) {
            return;
        }

        self.finish_block();

        assert!(self.builder.add(e));
        self.first_key = e.key.to_vec();
    }

    fn finish_block(&mut self) {
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new());
        let encoded_block = old_builder.build().encode();
        self.meta.push(MetaBlock {
            offset: self.data.len() as u16,
            first_key: std::mem::take(&mut self.first_key).into(),
        });
        self.data.extend(encoded_block);
    }

    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();

        let meta_offset = self.data.len();
        self.meta
            .iter()
            .for_each(|meta_block| self.data.extend(&meta_block.encode()));
        let file = FileStorage::create(path, self.data.clone())?;
        Ok(SsTable {
            id,
            file,
            metas: self.meta,
            meta_offset,
            cache: block_cache,
        })
    }
}
