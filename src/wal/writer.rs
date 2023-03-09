use std::io;
use std::io::Write;

use bytes::{BufMut, BytesMut};

use crate::wal::writer::ChunkType::{First, Full, Last, Middle};

pub const BLOCK_SIZE: usize = 32 * 1024; // 32KiB
pub const CHUNK_HEADER_SIZE: usize = 7; // 7 bytes

pub struct JournalWriter<W> {
    block_buf: BytesMut, // buffer
    written: usize,      // offset of block
    w: W,
}

impl<W: Write> JournalWriter<W> {
    pub fn new(w: W) -> Self {
        JournalWriter {
            block_buf: BytesMut::with_capacity(BLOCK_SIZE),
            written: 0,
            w,
        }
    }

    fn write_block(&mut self) -> io::Result<()> {
        // 剩余部分全部填充 0，避免一个 Chunk 里出现多条日志的情况，简化设计
        self.block_buf
            .extend(BytesMut::zeroed(BLOCK_SIZE - self.written));

        self.w.write(&self.block_buf)?;
        self.block_buf.clear();
        self.written = 0;
        Ok(())
    }
}

impl<W: Write> Write for JournalWriter<W> {
    fn write(&mut self, journal_item: &[u8]) -> io::Result<usize> {
        let write_len = journal_item.len();

        // Block 可以装得下整条日志，Chunk type 为 Full
        if self.written + CHUNK_HEADER_SIZE + journal_item.len() <= BLOCK_SIZE {
            Chunk::put_chunk(&mut self.block_buf, Full, journal_item);
            self.written += CHUNK_HEADER_SIZE + journal_item.len();
            return Ok(journal_item.len());
        }

        // 要分到多个 Block 中，第一个是 First，最后一个是 Last，中间的是 Middle
        let len = BLOCK_SIZE - self.written - CHUNK_HEADER_SIZE;
        Chunk::put_chunk(&mut self.block_buf, First, &journal_item[..len]);
        let mut offset = len;
        self.written += CHUNK_HEADER_SIZE + len;
        while self.written <= BLOCK_SIZE && offset < journal_item.len() {
            if self.written == BLOCK_SIZE {
                self.write_block()?;
            }

            let (chunk_type, len) = if journal_item.len() - offset + CHUNK_HEADER_SIZE > BLOCK_SIZE
            {
                (Middle, BLOCK_SIZE - CHUNK_HEADER_SIZE)
            } else {
                (Last, journal_item.len() - offset)
            };

            Chunk::put_chunk(
                &mut self.block_buf,
                chunk_type,
                &journal_item[offset..offset + len],
            );
            offset += len;
            self.written += CHUNK_HEADER_SIZE + len;
        }

        Ok(write_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_block()
    }
}

/// `JournalItem` is divided into multiple `Chunks` for storage
/// ```text
/// +-------------------+-----------------+--------------------+------+
/// | checksum(4 bytes) | length(2 bytes) | chunk type(1 byte) | data |
/// +-------------------+-----------------+--------------------+------+
/// ```
struct Chunk;

impl Chunk {
    pub fn put_chunk(buf: &mut BytesMut, chunk_type: ChunkType, data: &[u8]) -> usize {
        let checksum = crc::crc32::checksum_ieee(data);
        buf.put_u32_le(checksum);
        buf.put_u16_le(data.len() as u16);
        buf.put_u8(chunk_type.value());
        buf.put(data);
        4 + 2 + 1 + data.len()
    }
}

pub enum ChunkType {
    Full,
    First,
    Middle,
    Last,
}

impl ChunkType {
    pub fn value(&self) -> u8 {
        match self {
            Full => 0,
            First => 1,
            Middle => 2,
            Last => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::{BufMut, BytesMut};

    use crate::wal::writer::{BLOCK_SIZE, CHUNK_HEADER_SIZE};
    use crate::wal::JournalWriter;

    #[test]
    pub fn test_journal_write_full() {
        let mock_bytes = BytesMut::new();
        let mut mock_writer = mock_bytes.writer();

        let mut w = JournalWriter::new(&mut mock_writer);

        const DATA_LEN: usize = 10;
        let data = BytesMut::zeroed(DATA_LEN);
        w.write(&data[..]).expect("write error");
        w.flush().expect("write error");

        let mock_bytes = mock_writer.into_inner();
        assert_eq!(mock_bytes.len(), BLOCK_SIZE);
    }

    #[test]
    pub fn test_journal_writer_first_last() {
        let mock_bytes = BytesMut::new();
        let mut mock_writer = mock_bytes.writer();

        let mut w = JournalWriter::new(&mut mock_writer);

        const DATA_LEN: usize = BLOCK_SIZE - CHUNK_HEADER_SIZE + 1;
        let data = BytesMut::zeroed(DATA_LEN);
        w.write(&data[..]).expect("write error");
        w.flush().expect("write error");

        let mock_bytes = mock_writer.into_inner();
        assert_eq!(mock_bytes.len(), 2 * BLOCK_SIZE);
    }

    #[test]
    pub fn test_journal_writer_first_middle_last() {
        let mock_bytes = BytesMut::new();
        let mut mock_writer = mock_bytes.writer();

        let mut w = JournalWriter::new(&mut mock_writer);

        const DATA_LEN: usize = 2 * BLOCK_SIZE;
        let data = BytesMut::zeroed(DATA_LEN);
        w.write(&data[..]).expect("write error");
        w.flush().expect("write error");

        let mock_bytes = mock_writer.into_inner();
        assert_eq!(mock_bytes.len(), 3 * BLOCK_SIZE);
    }
}
