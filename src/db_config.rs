pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

pub const MEMTABLE_SIZE_LIMIT: usize = 4 * MB;
pub const BLOCK_CACHE_SIZE: u64 = 4 * GB as u64;
pub const MIN_VSST_SIZE: u64 = 4 * KB as u64;
pub const SST_LEVEL_LIMIT: u32 = 6;

pub const L0_SST_NUM_LIMIT: usize = 4;