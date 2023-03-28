use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::cell::RefCell;
use std::fmt::Debug;

use std::sync::Arc;

/// `Record` 是被写入到 `Manifest` 或 `Journal`/`WAL` 中的一条记录，`Record` 内包含多条 `RecordItem`
/// layout
/// ```text
/// +-------------------+------------------------------+-----------------+
/// | checksum(4 bytes) | record items number(8 bytes) | record items... |
/// +-------------------+------------------------------+-----------------+
/// ```
#[derive(Debug, Clone)]
pub struct Record<T> {
    items: Vec<T>,
}

impl<T: RecordItem + Clone> Record<T> {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // checksum reservation
        buf.put_u64_le(self.items.len() as u64);
        for i in &self.items {
            buf.extend(&i.encode()[..]);
        }
        let data_buf = buf.split_off(4);
        let checksum = crc::crc32::checksum_ieee(&data_buf);
        buf[0] = (checksum & 0xFF) as u8;
        buf[1] = ((checksum >> 8) & 0xFF) as u8;
        buf[2] = ((checksum >> 16) & 0xFF) as u8;
        buf[3] = ((checksum >> 24) & 0xFF) as u8;
        buf.unsplit(data_buf);
        buf.freeze()
    }

    pub fn decode_with_bytes(buf: &mut Bytes) -> anyhow::Result<Self> {
        let mut _buf = buf.clone();
        let expect_checksum = buf.get_u32_le();
        let item_num = buf.get_u64_le();

        let mut items = Vec::with_capacity(item_num as usize);
        let mut data_len: usize = 0;
        for _ in 0..item_num {
            let item = T::decode_with_bytes(buf)?;
            items.push(item.clone());
            data_len += item.size();
        }

        _buf.advance(4);
        let checksum = crc::crc32::checksum_ieee(&_buf[..8 + data_len]);
        if expect_checksum != checksum {
            return Err(anyhow!(
                "verify checksum failed when decode record, expect: {}, but got: {}",
                expect_checksum,
                checksum
            ));
        }

        Ok(Self { items })
    }

    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        let mut buf = Bytes::copy_from_slice(data);
        Self::decode_with_bytes(&mut buf)
    }

    pub fn num_of_items(&self) -> usize {
        self.items.len()
    }

    pub fn item(&self, idx: usize) -> &T {
        &self.items[idx]
    }
}

pub struct RecordBuilder<T> {
    items: Vec<T>,
}

impl<T> RecordBuilder<T> {
    pub fn new() -> Self {
        RecordBuilder { items: vec![] }
    }

    pub fn add(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn build(self) -> Record<T> {
        Record { items: self.items }
    }
}

pub trait RecordItem {
    fn encode(&self) -> Bytes;

    fn decode_with_bytes(bytes: &mut Bytes) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn size(&self) -> usize;
}

#[derive(Debug)]
pub struct RecordIterator<T> {
    record: Arc<Record<T>>,
    item: RefCell<Option<T>>,
    idx: usize,
}

impl<T: RecordItem + Clone> RecordIterator<T> {
    pub fn create_and_seek_to_first(record: Arc<Record<T>>) -> anyhow::Result<Self> {
        Ok(Self {
            record,
            item: RefCell::new(None),
            idx: 0,
        })
    }

    pub fn record_item(&self) -> T {
        if let Some(item) = self.item.borrow().as_ref() {
            return item.clone();
        }
        *self.item.borrow_mut() = Some(self.record.item(self.idx).clone());
        self.record_item()
    }

    pub fn is_valid(&self) -> bool {
        self.idx < self.record.num_of_items()
    }

    pub fn next(&mut self) {
        self.idx += 1;
        *self.item.borrow_mut() = None;
    }
}

#[cfg(test)]
mod tests {
    use crate::record::{Record, RecordBuilder, RecordItem};
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use std::mem;

    #[derive(Clone)]
    struct TestItem(u64);

    impl RecordItem for TestItem {
        fn encode(&self) -> Bytes {
            let mut buf = BytesMut::new();
            buf.put_u64_le(self.0);
            buf.freeze()
        }

        fn decode_with_bytes(bytes: &mut Bytes) -> anyhow::Result<Self>
        where
            Self: Sized,
        {
            Ok(Self(bytes.get_u64_le()))
        }

        fn size(&self) -> usize {
            mem::size_of::<u64>()
        }
    }

    #[test]
    fn test_record_encode() {
        let mut builder = RecordBuilder::new();
        builder.add(TestItem(1));
        let r = builder.build();

        let b = r.encode();
        let r2: Record<TestItem> = Record::decode(&(b.clone())).unwrap();
        assert_eq!(b, r2.encode());
    }
}
