use std::path::Path;
use std::sync::Arc;

use crate::block::tests::rand_gen_entries;

use crate::entry::Entry;
use crate::iterator::StorageIterator;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::SsTableIterator;

fn rand_gen_sst() -> (SsTable, Box<Path>, Vec<Entry>) {
    let mut builder = SsTableBuilder::new();

    let entries = rand_gen_entries(100);

    entries.iter().for_each(|e| builder.add(e));

    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path();
    let path = path.join("1.db");
    let sst = builder.build(1, None, path.clone()).unwrap();
    (sst, path.into(), entries)
}

#[test]
fn test_sst_builder() {
    rand_gen_sst();
}

#[test]
fn test_sst_iterator() {
    let (sst, _, entries) = rand_gen_sst();
    let sst = Arc::new(sst);

    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    entries.iter().for_each(|e| {
        assert_eq!(&e.key[..], iter.key());
        iter.next().unwrap();
    });
}
