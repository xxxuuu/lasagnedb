use crate::OpType::{Delete, Get, Put};
use bytes::{Bytes, BytesMut};
use std::cmp::Ordering;

/// Internal key in Db
///
/// layout:
/// ```text
/// +----------+--------------------------+--------------+
/// | user key | sequence number(7 bytes) | type(1 byte) |
/// +----------+--------------------------+--------------+
/// ```
#[derive(Clone, Debug)]
pub struct Key {
    pub user_key: Bytes,
    pub seq_num: u64,
    pub op_type: OpType,
}

impl Key {
    pub fn new(key: Bytes, seq_num: u64, op_type: OpType) -> Self {
        Key {
            user_key: key,
            seq_num,
            op_type,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut b = BytesMut::from(&self.user_key[..]);
        let len = b.len();
        b.extend(self.seq_num.to_le_bytes().iter());
        b[len + 7] = self.op_type.encode();
        b.freeze()
    }

    // pub fn decode(data: &[u8]) -> Self {
    //     let key = Bytes::from(&data[0..data.len()-8]);
    //
    //     let op_type = OpType::from(data[data.len()-1]);
    // }

    pub fn len(&self) -> usize {
        8 + self.user_key.len()
    }
}

impl Eq for Key {}

impl PartialEq<Self> for Key {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key
            && self.seq_num == other.seq_num
            && self.op_type == self.op_type
    }
}

impl PartialOrd<Self> for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_user_key = &self.user_key[..];
        let other_user_key = &other.user_key[..];

        // 优先级：user key -> seq num -> op type
        // 相同 key，让 seq num 大的排在前面（更小），相同 seq num，GET最前，DELETE比PUT更前
        match self_user_key.cmp(other_user_key) {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let self_seq_num = self.seq_num;
                let other_seq_num = other.seq_num;
                match self_seq_num.cmp(&other_seq_num) {
                    Ordering::Less => Ordering::Greater,
                    Ordering::Greater => Ordering::Less,
                    Ordering::Equal => (other.op_type.encode()).cmp(&(self.op_type.encode())),
                }
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum OpType {
    Get = 255,
    Put = 1,
    Delete = 2,
}

impl OpType {
    pub fn from(num: u8) -> OpType {
        match num {
            2 => Delete,
            1 => Put,
            _ => Get,
        }
    }

    pub fn encode(&self) -> u8 {
        *self as u8
    }
}

#[cfg(test)]
mod tests {
    use crate::Key;
    use crate::OpType::{Delete, Get};
    use bytes::Bytes;
    use std::cmp::Ordering;

    #[test]
    fn test_key_order() {
        let k1 = Key::new(Bytes::from("a"), 1, Get);
        let k2 = Key::new(Bytes::from("a"), 1, Delete);
        assert_eq!(k1.cmp(&k2), Ordering::Less);

        let k1 = Key::new(Bytes::from("a"), 1, Get);
        let k2 = Key::new(Bytes::from("a"), 2, Delete);
        assert_eq!(k1.cmp(&k2), Ordering::Greater);

        let k1 = Key::new(Bytes::from("a"), 3, Get);
        let k2 = Key::new(Bytes::from("b"), 2, Delete);
        assert_eq!(k1.cmp(&k2), Ordering::Less);
    }
}
