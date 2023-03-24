use crate::block::builder::Block;
use std::sync::Arc;

// (sst id, block id)
pub type BlockCache = moka::sync::Cache<(u32, usize), Arc<Block>>;
