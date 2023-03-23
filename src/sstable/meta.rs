use bytes::{Buf, BufMut, Bytes, BytesMut};

///
/// layout
/// ```text
/// +----------------+-----------------+
/// | offset(4bytes) | block first_key |
/// +----------------+-----------------+
/// ```
#[derive(Debug)]
pub struct MetaBlock {
    pub(crate) offset: u32,
    pub(crate) first_key: Bytes,
}

impl MetaBlock {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(4 + self.first_key.len());
        b.put_u32_le(self.offset);
        b.put_u64_le(self.first_key.len() as u64);
        b.put(&self.first_key[..]);
        b.freeze()
    }

    pub fn decode_with_bytes(buf: &mut Bytes) -> MetaBlock {
        let offset = buf.get_u32_le() as usize;
        let first_key_len = buf.get_u64_le() as usize;
        let first_key = buf.copy_to_bytes(first_key_len);
        MetaBlock {
            offset: offset as u32,
            first_key,
        }
    }
}
