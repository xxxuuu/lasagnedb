use std::io::Read;
use std::thread;
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use crate::db::Db;
use crate::{KB, MEMTABLE_SIZE_LIMIT, MIN_VSST_SIZE, SST_LEVEL_LIMIT};

impl Db {
    fn print_debug_info(&self) {
        use chrono::Local;

        let _inner = self.inner.read();

        println!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
        println!("memtable: {}KiB", _inner.memtable.size() / 1024);
        dbg!(&_inner.memtable);

        println!("forzen memtable number: {}", _inner.frozen_memtable.len());
        dbg!(&_inner.frozen_memtable);

        println!("l0 sst number: {}", _inner.levels[0].len());
        println!()
    }
}

#[test]
fn test_write_read() {
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let mut db = Db::open_file(data_dir.path()).unwrap();

    let k1 = Bytes::from("k1");
    let v1 = Bytes::from("v1");
    let v1_1 = Bytes::from("v1_1");
    db.put(k1.clone(), v1).unwrap();
    db.put(k1.clone(), v1_1.clone()).unwrap();
    assert_eq!(db.get(&k1).unwrap().unwrap(), &v1_1);

    let k2 = Bytes::from("k2");
    let v2 = Bytes::from("v2");
    db.put(k2.clone(), v2.clone()).unwrap();
    assert_eq!(db.get(&k2).unwrap().unwrap(), &v2);

    let k3 = Bytes::from("k3");
    let v3 = Bytes::from("v3");
    db.put(k3.clone(), v3).unwrap();
    db.delete(k3.clone()).unwrap();
    assert_eq!(db.get(&k3).unwrap(), None);
}

#[test]
fn test_recover() {
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let k1 = Bytes::from("k1");
    let v1 = Bytes::from("v1");
    let big_k1 = Bytes::from("big_ke");
    let big_v1 = BytesMut::zeroed(MIN_VSST_SIZE as usize * 2).freeze();
    let _k1 = Bytes::from("tmp_k1");
    let _v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

    {
        let mut db = Db::open_file(data_dir.path()).unwrap();
        db.put(big_k1.clone(), big_v1.clone()).unwrap();
        for _ in 1..50 {
            db.put(_k1.clone(), _v1.clone()).unwrap();
        }
        db.put(k1.clone(), v1.clone()).unwrap();
    }
    thread::sleep(Duration::from_secs(2));
    {
        let db = Db::open_file(data_dir.path()).unwrap();
        assert_eq!(db.get(&k1).unwrap(), Some(v1));
        assert_eq!(db.get(&_k1).unwrap(), Some(_v1));
        assert_eq!(db.get(&big_k1).unwrap(), Some(big_v1));
    }
}

#[test]
fn test_rotate() {
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let mut db = Db::open_file(data_dir.path()).unwrap();

    for _ in 1..50 {
        let k1 = Bytes::from("k1");
        let v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

        db.put(k1.clone(), v1.clone()).unwrap();
    }

    thread::sleep(Duration::from_secs(2));
    db.print_debug_info();
    assert_eq!(db.inner.read().levels[0].len(), 1);
}
