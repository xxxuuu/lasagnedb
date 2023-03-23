use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::block::tests::rand_gen_entries;

use crate::entry::Entry;
use crate::iterator::StorageIterator;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::SsTableIterator;
use crate::storage::file::FileStorage;

fn rand_gen_sst(path: impl AsRef<Path>) -> (SsTable, PathBuf, Vec<Entry>) {
    let mut builder = SsTableBuilder::new();

    let entries = rand_gen_entries(100);

    entries.iter().for_each(|e| builder.add(e));

    let path = path.as_ref().join("1.db");
    let sst = builder.build(1, None, path.clone()).unwrap();
    (sst, path, entries)
}

#[test]
fn test_sst_builder() {
    let tmpdir = tempfile::tempdir().unwrap();
    rand_gen_sst(tmpdir.path());
}

#[test]
fn test_open_iter() {
    let tmpdir = tempfile::tempdir().unwrap();
    let (path, entries) = {
        let (_sst, _path, _entries) = rand_gen_sst(tmpdir.path());
        let mut iter = SsTableIterator::create_and_seek_to_first(Arc::new(_sst)).unwrap();
        _entries.iter().for_each(|e| {
            assert_eq!(&e.key[..], iter.key());
            iter.next().unwrap();
        });
        (_path, _entries)
    };

    let file = FileStorage::open(path).unwrap();
    let sst =  Arc::new(SsTable::open(1,None, file).unwrap());
    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    entries.iter().for_each(|e| {
        assert_eq!(&e.key[..], iter.key());
        iter.next().unwrap();
    });
}