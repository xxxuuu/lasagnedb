use bytes::{BufMut, Bytes, BytesMut};

///
/// layout
/// ```text
/// +----------------+-----------------+
/// | offset(2bytes) | block first_key |
/// +----------------+-----------------+
/// ```
pub struct MetaBlock {
    pub(crate) offset: u16,
    pub(crate) first_key: Bytes,
}

impl MetaBlock {
    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::with_capacity(2 + self.first_key.len());
        b.put_u16_le(self.offset);
        b.put(&self.first_key[..]);
        b.freeze()
    }
}
