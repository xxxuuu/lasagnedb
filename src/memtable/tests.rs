use crate::iterator::StorageIterator;
use crate::memtable::MemTable;
use crate::{Key, OpType};
use bytes::Bytes;
use std::collections::Bound;

#[test]
fn test_memtable_rw() {
    let t = MemTable::new();
    let k1 = Key::new(Bytes::from("k1"), 1, OpType::Put);
    let v1 = Bytes::from("v1");
    t.put(k1.clone(), v1.clone());
    assert_eq!(&(t.get(&k1).unwrap().1)[..], &v1[..]);

    let k2 = Key::new(Bytes::from("k2"), 1, OpType::Put);
    let v2 = Bytes::from("v2");
    t.put(k2.clone(), v2.clone());
    assert_eq!(&(t.get(&k2).unwrap().1)[..], &v2[..]);

    let v2 = Bytes::from("v2_2");
    t.put(k2.clone(), v2.clone());
    assert_eq!(&(t.get(&k2).unwrap().1)[..], &v2[..]);

    let k2 = Key::new(Bytes::from("k2"), 2, OpType::Put);
    let v2 = Bytes::from("v2");
    t.put(k2.clone(), v2.clone());
    assert_eq!(&(t.get(&k2).unwrap().1)[..], &v2[..]);
}

#[test]
fn test_memtable_iterator() {
    let t = MemTable::new();

    t.put(
        Key::new(Bytes::from("k1"), 1, OpType::Put),
        Bytes::from("v1"),
    );
    t.put(
        Key::new(Bytes::from("k2"), 2, OpType::Put),
        Bytes::from("v2"),
    );
    t.put(
        Key::new(Bytes::from("k3"), 3, OpType::Put),
        Bytes::from("v3"),
    );
    t.put(Key::new(Bytes::from("k1"), 4, OpType::Delete), Bytes::new());
    t.put(
        Key::new(Bytes::from("k2"), 5, OpType::Put),
        Bytes::from("v2_2"),
    );

    let mut iter = t.scan(Bound::Unbounded, Bound::Unbounded);
    assert_eq!(iter.value(), Bytes::new());
    iter.next().unwrap();
    assert_eq!(iter.value(), Bytes::from("v1"));
    iter.next().unwrap();
    assert_eq!(iter.value(), Bytes::from("v2_2"));
    iter.next().unwrap();
    assert_eq!(iter.value(), Bytes::from("v2"));
    iter.next().unwrap();
    assert_eq!(iter.value(), Bytes::from("v3"));
}
