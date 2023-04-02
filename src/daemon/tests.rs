use crate::daemon::DbDaemon;
use crate::entry::{Entry, EntryBuilder};
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::SsTableIterator;
use crate::{OpType, StorageIterator};
use bytes::Bytes;
use lazy_static::lazy_static;
use moka::sync::Cache;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::env::join_paths;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn generate_entry(key: Bytes, value: Bytes) -> Entry {
    let mut b = EntryBuilder::new();
    b.op_type(OpType::Put)
        .kv_separate(false)
        .key_value(key, value)
        .build()
}

fn map_to_string(num: u32) -> String {
    let mut result = String::new();
    for i in 0..num {
        result.push(('a' as u8 + (i % 26) as u8) as char);
    }
    result
}

fn generate_rang_sst(path: impl AsRef<Path>, id: u32, from: u32, to: u32) -> Arc<SsTable> {
    let mut b = SsTableBuilder::new();

    for i in from..=to {
        b.add(&generate_entry(Bytes::from(map_to_string(i)), Bytes::new()));
    }

    return Arc::new(
        b.build(id, None, path.as_ref().join(format!("{}.sst", id)))
            .unwrap(),
    );
}

#[test]
fn test_select_overlap_sst() {
    let tempdir = tempfile::tempdir().unwrap();
    let base_path = tempdir.path();

    let mut levels = vec![vec![]; 6];

    levels[0].push(generate_rang_sst(base_path, 1, 2, 100)); // be picked
    levels[0].push(generate_rang_sst(base_path, 2, 15, 70)); // be picked
    levels[0].push(generate_rang_sst(base_path, 3, 1, 50)); // be picked
    levels[0].push(generate_rang_sst(base_path, 4, 101, 150)); // be picked
    levels[0].push(generate_rang_sst(base_path, 5, 201, 300));

    levels[1].push(generate_rang_sst(base_path, 6, 1, 50)); // be picked
    levels[1].push(generate_rang_sst(base_path, 7, 50, 60)); // be picked
    levels[1].push(generate_rang_sst(base_path, 8, 60, 200)); // be picked
    levels[1].push(generate_rang_sst(base_path, 9, 201, 300));

    let res = DbDaemon::select_overlap_sst(&levels, 0, levels[0][0].clone());
    assert_eq!(res.0.len(), 4);
    res.0
        .iter()
        .for_each(|sst| assert!(vec![1, 2, 3, 4].contains(&(sst.id() as i32))));
    assert_eq!(res.1.len(), 3);
    res.1
        .iter()
        .for_each(|sst| assert!(vec![6, 7, 8, 9].contains(&(sst.id() as i32))));
}

#[test]
fn test_merge() {
    let tempdir = tempfile::tempdir().unwrap();
    let base_path = tempdir.path();
    let vsst = Arc::new(RwLock::new(HashMap::new()));

    let mut levels = vec![];
    levels.push(generate_rang_sst(base_path, 1, 2, 5));
    levels.push(generate_rang_sst(base_path, 2, 3, 4));
    levels.push(generate_rang_sst(base_path, 3, 1, 2));

    let temp_cache = Arc::new(Cache::new(0));
    let (mut new_ssts, _, _) = DbDaemon::merge(
        base_path,
        1,
        levels,
        temp_cache.clone(),
        1,
        vsst.clone(),
        temp_cache.clone(),
        Arc::new(RwLock::new(HashMap::default())),
    )
    .unwrap();
    assert_eq!(new_ssts.len(), 1);
    let sst = new_ssts.remove(0);
    let mut iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
    for i in 1..=5 {
        assert_eq!(iter.key(), Bytes::from(map_to_string(i)));
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}
