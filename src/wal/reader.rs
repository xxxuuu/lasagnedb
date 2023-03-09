use crate::wal::{ChunkType, JournalItem, BLOCK_SIZE};
use bytes::{Buf, BytesMut};

use std::io::Read;

pub struct JournalReader<R> {
    r: R,
}

impl<R: Read> JournalReader<R> {
    pub fn new(reader: R) -> Self {
        JournalReader { r: reader }
    }
}

impl<R: Read> Iterator for JournalReader<R> {
    type Item = JournalItem;

    fn next(&mut self) -> Option<Self::Item> {
        let mut block_buf = BytesMut::zeroed(BLOCK_SIZE);

        let mut buf = BytesMut::new();
        let mut finish = false;

        while !finish {
            // 读一个Block
            match self
                .r
                .by_ref()
                .take(BLOCK_SIZE as u64)
                .read(block_buf.as_mut())
            {
                Ok(0) => break,
                Err(e) => panic!("{}", e),
                _ => {}
            }
            finish = true;
            while block_buf.has_remaining() {
                finish = false;
                // 读 Block 里的每个 Chunk 并校验
                let checksum = block_buf.get_u32_le();
                let length = block_buf.get_u16_le();
                let chunk_type = block_buf.get_u8();
                let data = block_buf.get(..length as usize).unwrap();
                if length == 0 {
                    // block 末尾的填充数据
                    break;
                }
                if checksum == crc::crc32::checksum_ieee(data) {
                    // data 是 journal item 的一部分，加到 buf 里
                    buf.extend(data);
                }
                if chunk_type == ChunkType::Last.value() || chunk_type == ChunkType::Full.value() {
                    finish = true;
                    break;
                }
            }
        }

        Some(JournalItem::with_bytes(buf.freeze()))
    }
}
