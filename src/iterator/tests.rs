use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::two_merge_iterator::TwoMergeIterator;
use crate::iterator::StorageIterator;

struct TestIterator {
    data: Vec<(Vec<u8>, Vec<u8>)>,
    idx: usize,
}

impl TestIterator {
    pub fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { data, idx: 0 }
    }
}

impl StorageIterator for TestIterator {
    fn key(&self) -> &[u8] {
        self.data[self.idx].0.as_slice()
    }

    fn value(&self) -> &[u8] {
        self.data[self.idx].1.as_slice()
    }

    fn is_valid(&self) -> bool {
        self.idx < self.data.len()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        self.idx += 1;
        Ok(())
    }
}

#[test]
fn test_merge_iterator() {
    let iter1 = TestIterator::new(vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k3".to_vec(), b"v3".to_vec()),
    ]);
    let iter2 = TestIterator::new(vec![
        (b"k1".to_vec(), b"v1_1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
    ]);

    let mut i = MergeIterator::create(vec![Box::new(iter1), Box::new(iter2)]);
    assert_eq!(i.key(), b"k1");
    assert_eq!(i.value(), b"v1");
    i.next().unwrap();
    assert_eq!(i.key(), b"k2");
    assert_eq!(i.value(), b"v2");
    i.next().unwrap();
    assert_eq!(i.key(), b"k3");
    assert_eq!(i.value(), b"v3");
    i.next().unwrap();
}

#[test]
fn test_two_merge_iterator() {
    let iter1 = TestIterator::new(vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k3".to_vec(), b"v3".to_vec()),
    ]);
    let iter2 = TestIterator::new(vec![
        (b"k1".to_vec(), b"v1_1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
    ]);

    let mut i = TwoMergeIterator::create(iter1, iter2).unwrap();
    assert_eq!(i.key(), b"k1");
    assert_eq!(i.value(), b"v1");
    i.next().unwrap();
    assert_eq!(i.key(), b"k2");
    assert_eq!(i.value(), b"v2");
    i.next().unwrap();
    assert_eq!(i.key(), b"k3");
    assert_eq!(i.value(), b"v3");
    i.next().unwrap();
}
