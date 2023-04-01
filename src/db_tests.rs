use std::io::Read;
use std::ops::Bound::Unbounded;
use std::sync::{Arc, Once};
use std::thread::{self, Thread};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tracing::{debug, info, instrument, span};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use crate::db::Db;
use crate::iterator::StorageIterator;
use crate::{MEMTABLE_SIZE_LIMIT, MIN_VSST_SIZE};

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

static INIT: Once = Once::new();

fn setup() {
    if let Some(jaeger_endpoint) = option_env!("JAEGER_ENDPOINT") {
        println!("JAEGER_ENDPOINT: {}", jaeger_endpoint);
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(jaeger_endpoint)
            .with_service_name("lasagnedb")
            .install_simple()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = Registry::default().with(opentelemetry);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }
}

#[test]
fn test_write_read() {
    INIT.call_once(setup);
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let db = Db::open_file(data_dir.path()).unwrap();

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
    INIT.call_once(setup);
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let k1 = Bytes::from("k1");
    let v1 = Bytes::from("v1");
    let big_k1 = Bytes::from("big_ke");
    let big_v1 = BytesMut::zeroed(MIN_VSST_SIZE as usize * 2).freeze();
    let _k1 = Bytes::from("tmp_k1");
    let _v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

    {
        let db = Db::open_file(data_dir.path()).unwrap();
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
    INIT.call_once(setup);
    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let db = Db::open_file(data_dir.path()).unwrap();

    for _ in 1..50 {
        let k1 = Bytes::from("k1");
        let v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

        db.put(k1.clone(), v1.clone()).unwrap();
    }

    thread::sleep(Duration::from_secs(2));
    db.print_debug_info();
    assert_eq!(db.inner.read().levels[0].len(), 1);
}

#[test]
fn test_background_write() {
    INIT.call_once(setup);
    let data_dir = tempfile::tempdir().unwrap();

    let db = Arc::new(Db::open_file(data_dir.path()).unwrap());
    let _db = db.clone();
    thread::spawn(move || loop {
        let k1 = Bytes::from("k1");
        let v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

        _db.put(k1.clone(), v1.clone()).unwrap();
        thread::sleep(Duration::from_millis(100));
    });

    for i in 1..60 {
        {
            let snapshot = {
                let guard = db.inner.read();
                guard.as_ref().clone()
            };
            let _span = span!(tracing::Level::TRACE, "test background write");
            let _enter = _span.enter();
            debug!("{} sec", i);
            debug!("MEM SIZE: {}KB", snapshot.memtable.size() / 1024);
            debug!("FROZEN MEM: {}", snapshot.frozen_memtable.len());
            debug!("FROZEN LOG: {}", snapshot.frozen_wal.len());
            debug!("L0 SST: {}", snapshot.levels[0].len());
            debug!("L1 SST: {}", snapshot.levels[1].len());
            debug!("VSST: {}", snapshot.vssts.read().len());
            drop(_enter);
        }
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn test_iterator() {
    INIT.call_once(setup);

    let data_dir = tempfile::tempdir().unwrap();
    println!("tempdir: {}", data_dir.path().to_str().unwrap());

    let db = Db::open_file(data_dir.path()).unwrap();
    for i in 1..100 {
        let k1 = Bytes::from(format!("k{:04}", i));
        let v1 = Bytes::from(format!("v{:04}", i));
        db.put(k1.clone(), v1.clone()).unwrap();
    }

    let mut iter = db.scan(Unbounded, Unbounded).unwrap();
    for i in 1..100 {
        let key = String::from_utf8_lossy(iter.key());
        let value = String::from_utf8_lossy(iter.value());
        assert_eq!(key, format!("k{:04}", i));
        assert_eq!(value, format!("v{:04}", i));
        iter.next().unwrap();
    }
    assert!(!iter.is_valid());
}
