use crate::block::builder::Block;
use std::sync::Arc;

// (sst id, block id)
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;
