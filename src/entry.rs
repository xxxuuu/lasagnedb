use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::OpType;

/// `Entry` 是一次 KV 写入的打包格式
///
/// layout:
/// ```text
/// +---------------+---------------------+-----+-----------------------+-------+
/// | meta(4 bytes) | key length(8 bytes) | key | value length(8 bytes) | value |
/// +---------------+---------------------+-----+-----------------------+-------+
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Entry {
    pub(crate) meta: u32,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl Entry {
    fn new(meta: u32, key: Bytes, value: Bytes) -> Self {
        Entry { meta, key, value }
    }

    pub fn size(&self) -> usize {
        4 + 8 + 8 + self.key.len() + self.value.len()
    }

    pub fn has_value(&self) -> bool {
        !self.value.is_empty()
    }

    pub fn op_type(&self) -> OpType {
        OpType::from((self.meta & 0xFF) as u8)
    }

    pub fn value_separate(&self) -> bool {
        (self.meta >> 8) | 0x1 == 0x1
    }

    pub fn encode(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(self.size());
        bytes.put_u32_le(self.meta);
        bytes.put_u64_le(self.key.len() as u64);
        bytes.put(&self.key[..]);
        bytes.put_u64_le(self.value.len() as u64);
        bytes.put(&self.value[..]);
        bytes.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let meta = (&data[..]).get_u32_le();
        let key_len = (&data[4..12]).get_u64_le() as usize;
        let key = Bytes::copy_from_slice(&data[12..12 + key_len]);

        let value_off = 12 + key_len;
        let value_len = (&data[value_off..value_off + 8]).get_u64_le() as usize;
        let value = Bytes::copy_from_slice(&data[value_off + 8..value_off + 8 + value_len]);

        Entry { meta, key, value }
    }

    pub fn decode_with_bytes(buf: &mut Bytes) -> Self {
        let e = Self::decode(&buf[..]);
        buf.advance(e.size());
        e
    }
}

#[derive(Default)]
pub struct EntryBuilder {
    meta: u32,
    key: Bytes,
    value: Bytes,
}

impl EntryBuilder {
    pub fn new() -> Self {
        EntryBuilder::default()
    }

    pub fn op_type(&mut self, op_type: OpType) -> &mut Self {
        self.meta |= op_type.encode() as u32;
        self
    }

    pub fn kv_separate(&mut self, separate: bool) -> &mut Self {
        if separate {
            self.meta |= 1 << 8
        } else {
            self.meta &= !(1 << 8);
        }
        self
    }

    pub fn key_value(&mut self, key: Bytes, value: Bytes) -> &mut Self {
        self.key = key;
        self.value = value;
        self
    }

    pub fn build(&self) -> Entry {
        Entry::new(self.meta, self.key.clone(), self.value.clone())
    }

    pub fn empty() -> Entry {
        Entry::new(
            0,
            BytesMut::zeroed(0).freeze(),
            BytesMut::zeroed(0).freeze(),
        )
    }
}

#[cfg(test)]
pub mod tests {

    use bytes::Bytes;
    use rand::distributions::{Alphanumeric, DistString};
    use rand::{thread_rng, Rng};

    use crate::entry::{Entry, EntryBuilder};

    use crate::OpType::Get;

    pub fn rand_gen_entry() -> (Bytes, Bytes, Entry) {
        let rand_str = || -> String {
            Alphanumeric.sample_string(&mut thread_rng(), thread_rng().gen_range(0..100))
        };

        let key = Bytes::from(rand_str());
        let value = Bytes::from(rand_str());

        (
            key.clone(),
            value.clone(),
            EntryBuilder::new()
                .op_type(Get)
                .key_value(key, value)
                .build(),
        )
    }

    #[test]
    fn test_entry_builder() {
        let (key, value, entry) = rand_gen_entry();
        assert_eq!(entry.op_type(), Get);
        assert_eq!(entry.key, key);
        assert_eq!(entry.value, value);
    }

    #[test]
    fn test_entry_encode() {
        let (_key, _value, entry) = rand_gen_entry();
        let encode_entry = entry.encode();
        let entry2 = Entry::decode(&encode_entry[..]);
        assert_eq!(entry, entry2)
    }

    #[test]
    fn test_entry_empty_value() {
        let key = Bytes::from("test_key");
        let value = Bytes::new();
        let b = EntryBuilder::new()
            .op_type(Get)
            .key_value(key, value)
            .build();

        assert_eq!(b.has_value(), false);
    }
}
