use crate::entry::{Entry, EntryBuilder};
use crate::value::OpType;
use crate::wal::iterator::JournalIterator;
use crate::wal::Journal;
use bytes::Bytes;
use std::sync::Arc;

fn test_batches() -> Vec<Entry> {
    vec![
        EntryBuilder::new()
            .op_type(OpType::Get)
            .key_value(Bytes::from("k1"), Bytes::from("v1"))
            .build(),
        EntryBuilder::new()
            .op_type(OpType::Get)
            .key_value(Bytes::from("k2"), Bytes::from("v2"))
            .build(),
        EntryBuilder::new()
            .op_type(OpType::Get)
            .key_value(Bytes::from("k3"), Bytes::from("v3"))
            .build(),
    ]
}

#[test]
fn test_journal() {
    let (batch1, batch2) = (test_batches(), test_batches());
    let file_path = tempfile::tempdir().unwrap().into_path().join("LOG");
    {
        let wal = Journal::open(file_path.clone()).unwrap();
        wal.write(batch1.clone()).unwrap();
        wal.write(batch2.clone()).unwrap();
    }

    let wal = Arc::new(Journal::open(file_path).unwrap());
    let mut iter = JournalIterator::create_and_seek_to_first(wal).unwrap();
    let mut batches = batch1;
    batches.extend(batch2);

    batches.iter().for_each(|item| {
        assert!(iter.is_valid());
        assert_eq!(item, iter.record_item().as_ref());
        iter.next().unwrap();
    })
}
