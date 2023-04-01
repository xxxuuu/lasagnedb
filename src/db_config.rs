pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

pub const BLOCK_SIZE: usize = 4 * KB;
pub const MEMTABLE_SIZE_LIMIT: usize = 4 * MB;
pub const BLOCK_CACHE_SIZE: u64 = 8 * MB as u64;
pub const MIN_VSST_SIZE: u64 = 4 * KB as u64;
pub const SST_LEVEL_LIMIT: u32 = 6;

pub const MAX_SST_SIZE: u64 = 4 * MB as u64;
pub const MAX_LEVEL_SIZE: [u64; SST_LEVEL_LIMIT as usize] = [
    4 * MB as u64,
    10 * MB as u64,
    100 * MB as u64,
    1 * GB as u64,
    10 * GB as u64,
    100 * GB as u64,
];

pub const L0_SST_NUM_LIMIT: usize = 4;
