use crate::entry::Entry;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::mem;

/// `Block` 是持久化存储中的最小读写单元，大小 4KB
///
/// ```text
/// +---------------+--------------------------+------------------+-------------------+
/// | data(entries) | offsets(2byte*entry num) | checksum(4bytes) | entry num(2bytes) |
/// +---------------+--------------------------+------------------+-------------------+
/// ```
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
    pub(crate) checksum: u32,
    pub(crate) entry_num: u16,
}

pub const BLOCK_SIZE: usize = 4 * 1024; // 4KB
const SIZEOF_U16: usize = mem::size_of::<u16>();
const SIZEOF_U32: usize = mem::size_of::<u32>();

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::new();
        b.put(&self.data[..]);
        for offset in &self.offsets {
            b.put_u16_le(*offset);
        }
        b.put_u32_le(self.checksum);
        b.put_u16_le(self.entry_num);
        // TODO snappy 压缩 和 检查校验和
        b.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let entry_num = (&data[data.len() - SIZEOF_U16..]).get_u16_le() as usize;
        let checksum = (&data[data.len() - SIZEOF_U16 - SIZEOF_U32..]).get_u32_le();

        let data_end = data.len() - SIZEOF_U16 - SIZEOF_U32 - entry_num * SIZEOF_U16;

        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16 - SIZEOF_U32];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16_le())
            .collect();

        let data = data[0..data_end].to_vec();

        Self {
            data,
            offsets,
            checksum,
            entry_num: entry_num as u16,
        }
    }
}

pub struct BlockBuilder {
    data: Vec<Entry>,
    offsets: Vec<u16>,
    entry_size: usize,
}

impl BlockBuilder {
    pub fn new() -> BlockBuilder {
        BlockBuilder {
            data: Vec::new(),
            offsets: Vec::new(),
            entry_size: 0,
        }
    }

    pub fn add(&mut self, e: &Entry) -> bool {
        if self.size() + e.size() > BLOCK_SIZE && !self.is_empty() {
            return false;
        }

        self.offsets.push(self.entry_size as u16);
        self.data.push(e.clone());
        self.entry_size += e.size();
        true
    }

    pub fn build(self) -> Block {
        let mut b = BytesMut::new();
        for e in &self.data {
            b.put(e.encode());
        }
        let checksum = crc::crc32::checksum_ieee(&b);
        let entry_num = self.data.len() as u16;

        Block {
            data: b.to_vec(),
            offsets: self.offsets,
            checksum,
            entry_num,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    fn size(&self) -> usize {
        // entries + offsets + checksum(4bytes) + entry num(2bytes)
        self.entry_size + self.offsets.len() * SIZEOF_U16 + SIZEOF_U32 + SIZEOF_U16
    }
}
