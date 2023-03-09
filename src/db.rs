use std::fs::OpenOptions;

use std::ops::Bound;
use std::path::{Path, PathBuf};

use std::sync::atomic::AtomicU64;

use std::sync::{atomic, Arc};
use std::{fs, io, result, thread};

use bytes::Bytes;
use crossbeam::channel;
use parking_lot::RwLock;
use thiserror::Error;

use crate::cache::BlockCache;
use crate::entry::Entry;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::memtable::MemTable;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::SsTableIterator;
use crate::wal::{Journal, JournalError};
use crate::OpType::{Delete, Get, Put};
use crate::{Key, OpType};

pub const MEMTABLE_SIZE_LIMIT: usize = 1024 * 1024 * 4; // 4MiB
pub const BLOCK_CACHE_SIZE: u64 = 1 << 20; // 4GiB

pub type Result<T> = result::Result<T, DBError>;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("journal error: {0}")]
    JournalError(#[from] JournalError),
    #[error("io error: {0}")]
    IOError(#[from] io::Error),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

#[derive(Clone)]
struct DbInner {
    wal: Arc<Journal>,
    frozen_wal: Vec<Arc<Journal>>,
    memtable: Arc<MemTable>,
    frozen_memtable: Vec<Arc<MemTable>>,

    l0_sst: Vec<Arc<SsTable>>,
    levels: Vec<Vec<Arc<SsTable>>>,

    seq_num: Arc<AtomicU64>,
    sst_id: usize,
}

struct DbDaemon {
    inner: Arc<RwLock<Arc<DbInner>>>,
    cache: Arc<BlockCache>,
    path: PathBuf,
}

impl DbDaemon {
    pub fn new(db_inner: Arc<RwLock<Arc<DbInner>>>, cache: Arc<BlockCache>, path: PathBuf) -> Self {
        DbDaemon {
            inner: db_inner,
            cache,
            path,
        }
    }

    pub fn rotate(&self) -> Result<()> {
        let flush_memtable;
        let sst_id;

        // 冻结 memtable 和 wal
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::new()));

            flush_memtable = memtable.clone();
            sst_id = snapshot.sst_id;
            snapshot.frozen_memtable.push(memtable);
            *guard = Arc::new(snapshot);
        }

        // 写入到 L0 SST
        let mut builder = SsTableBuilder::new();
        flush_memtable.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.cache.clone()),
            Db::path_of_sst(&self.path, sst_id),
        )?);

        // 加入 L0 SST 到 list 里
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.frozen_memtable.pop();
            snapshot.l0_sst.push(sst);
            snapshot.sst_id += 1;
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }
}

pub struct Db {
    inner: Arc<RwLock<Arc<DbInner>>>,

    path: PathBuf,
    version: AtomicU64,
    block_cache: Arc<BlockCache>,

    flush_chan: (channel::Sender<()>, channel::Receiver<()>),
    compaction_chan: (channel::Sender<()>, channel::Receiver<()>),
    exit_chan: (channel::Sender<()>, channel::Receiver<()>),
    daemon: Arc<DbDaemon>,
}

pub struct Options {}

impl Db {
    /// open database from file system
    pub fn open_file(path: impl AsRef<Path>) -> Db {
        let path = path.as_ref().to_path_buf();

        fs::create_dir_all(&path).expect("create data dir failed");
        // File::create(path.join(Db::path_of_manifest(&path, 1))).unwrap();
        // let current = path.join("CURRENT");
        // File::create(&current).unwrap();

        // let mut manifest = OpenOptions::new()
        //     .read(true)
        //     .write(true)
        //     .truncate(true)
        //     .create(true)
        //     .open(current)
        //     .expect("create MANIFEST failed");
        // manifest
        //     .write(Db::path_of_manifest(&path, 1).as_os_str())
        //     .unwrap();

        let log = OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(Db::path_of_wal(&path))
            .expect("create LOG failed");

        let (flush_tx, flush_rx) = channel::bounded(1);
        let (compaction_tx, compaction_rx) = channel::unbounded();
        let (exit_tx, exit_rx) = channel::bounded(1);

        let inner = Arc::new(RwLock::new(Arc::new(DbInner {
            wal: Arc::new(Journal::new(log)),
            frozen_wal: vec![],
            memtable: Arc::new(MemTable::new()),
            frozen_memtable: vec![],

            l0_sst: vec![],
            levels: vec![],

            seq_num: Arc::new(AtomicU64::new(1)),
            sst_id: 0,
        })));

        let cache = Arc::new(BlockCache::new(BLOCK_CACHE_SIZE));
        let mut db = Db {
            inner: inner.clone(),
            path: path.clone(),
            version: AtomicU64::new(1),
            block_cache: cache.clone(),

            flush_chan: (flush_tx, flush_rx),
            compaction_chan: (compaction_tx, compaction_rx),
            exit_chan: (exit_tx, exit_rx),
            daemon: Arc::new(DbDaemon::new(inner, cache, path)),
        };

        db.recover();
        db.run_background_tasks();
        db
    }

    fn run_background_tasks(&self) {
        let _flush_rx = self.flush_chan.1.clone();
        let _daemon = self.daemon.clone();
        thread::spawn(move || {
            for _ in _flush_rx {
                _daemon.rotate().expect("TODO: panic message");
            }
        });
    }

    fn path_of_manifest(base_path: &PathBuf, id: usize) -> PathBuf {
        base_path.join(format!("{:05}.MANIFEST", id))
    }

    fn path_of_wal(base_path: &PathBuf) -> PathBuf {
        base_path.join("LOG")
    }

    fn path_of_sst(base_path: &PathBuf, sst_id: usize) -> PathBuf {
        base_path.join(format!("{:05}.SST", sst_id))
    }

    fn path_of_vsst(base_path: &PathBuf, vsst_id: usize) -> PathBuf {
        base_path.join(format!("{:05}.VSST", vsst_id))
    }

    /// recover from wal
    pub fn recover(&mut self) {
        // unimplemented!()
    }

    /// close database connect, that will ensure all committed transactions will be fsync to journal
    pub fn close(&mut self) -> Result<()> {
        unimplemented!()
    }

    fn make_internal_key(&self, seq_num: u64, op_type: OpType, key: &Bytes) -> Key {
        Key::new(key.clone(), seq_num, op_type)
    }

    /// put a key-value pair
    pub fn put(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        self.append(key, Some(value))
    }

    /// delete value by key
    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        self.append(key, None)
    }

    /// get value by key
    pub fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let (snapshot, seq_num) = {
            let guard = self.inner.read();
            let seq_num = guard.seq_num.fetch_add(1, atomic::Ordering::AcqRel);
            (Arc::clone(&guard), seq_num)
        };

        let internal_key = self.make_internal_key(seq_num, Get, key);

        // memtable
        if let Some((_, v)) = snapshot.memtable.get(&internal_key) {
            return Ok(Some(v));
        }

        // frozen memtable
        for memtable in snapshot.frozen_memtable.iter().rev() {
            if let Some((_, v)) = memtable.get(&internal_key) {
                return Ok(Some(v));
            }
        }

        // sst
        let mut iters = Vec::new();
        iters.reserve(snapshot.l0_sst.len());
        for table in snapshot.l0_sst.iter().rev() {
            iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                table.clone(),
                key,
            )?));
        }
        let iter = MergeIterator::create(iters);
        if iter.is_valid() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    pub fn scan(&self, _begin: Bound<Key>, _end: Bound<Key>) {
        unimplemented!()
    }

    fn append(&self, key: Bytes, value: Option<Bytes>) -> Result<()> {
        let (value, op_type) = match value {
            None => (Bytes::new(), Delete),
            Some(v) => (v, Put),
        };
        let entry = Entry::new(op_type.encode(), key.clone(), value.clone());

        let guard = self.inner.read();

        let seq_num = guard.seq_num.fetch_add(1, atomic::Ordering::AcqRel);
        guard.wal.write(seq_num, vec![entry])?;

        let internal_key = self.make_internal_key(seq_num, op_type, &key);
        guard.memtable.put(internal_key, value);

        if guard.memtable.size() > MEMTABLE_SIZE_LIMIT {
            if let Err(e) = self.flush_chan.0.send(()) {
                eprintln!("{}", e);
            }
        }

        Ok(())
    }
}

pub struct DbIterator {}

impl DbIterator {}

impl StorageIterator for DbIterator {
    fn key(&self) -> &[u8] {
        todo!()
    }

    fn value(&self) -> &[u8] {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn next(&mut self) -> anyhow::Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use crate::db::Db;

    #[test]
    fn test_write_read() {
        let data_dir = tempfile::tempdir().unwrap();
        println!("tempdir: {}", data_dir.path().to_str().unwrap());

        let mut db = Db::open_file(data_dir.path().to_str().unwrap());

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
}
