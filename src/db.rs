use std::{fs, thread};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::ops::Bound;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc};
use std::sync::atomic::AtomicU64;

use anyhow::Context;
use bytes::Bytes;


use crossbeam::channel;


use parking_lot::RwLock;


use crate::{Key, OpType};
use crate::cache::BlockCache;

use crate::daemon::DbDaemon;
use crate::entry::Entry;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::memtable::MemTable;
use crate::meta::iterator::ManifestIterator;
use crate::meta::manifest::{Manifest, ManifestItem};
use crate::OpType::{Delete, Get, Put};
use crate::record::RecordBuilder;
use crate::sstable::builder::{SsTable};
use crate::sstable::iterator::SsTableIterator;
use crate::storage::file::FileStorage;
use crate::wal::{Journal};
use crate::wal::iterator::JournalIterator;

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

pub const MEMTABLE_SIZE_LIMIT: usize = 4 * MB;
pub const BLOCK_CACHE_SIZE: u64 = 4 * GB as u64;
pub const SST_LIMIT: usize = 6;

#[derive(Clone, Debug)]
pub(crate) struct DbInner {
    pub(crate) wal: Arc<Journal>,
    pub(crate) frozen_wal: Vec<Arc<Journal>>,
    pub(crate) memtable: Arc<MemTable>,
    pub(crate) frozen_memtable: Vec<Arc<MemTable>>,

    pub(crate) l0_sst: Vec<Arc<SsTable>>,
    pub(crate) levels: Vec<Vec<Arc<SsTable>>>,

    pub(crate) seq_num: u64,
    pub(crate) sst_id: usize,
}

#[derive(Debug)]
pub struct Db {
    inner: Arc<RwLock<Arc<DbInner>>>,

    path: Arc<PathBuf>,
    version: AtomicU64,
    block_cache: Arc<BlockCache>,

    flush_chan: (channel::Sender<()>, channel::Receiver<()>),
    compaction_chan: (channel::Sender<()>, channel::Receiver<()>),
    exit_chan: (channel::Sender<()>, channel::Receiver<()>),
    daemon: Arc<DbDaemon>,
    manifest: Arc<RwLock<Manifest>>,
}

pub struct Options {}

impl Db {
    /// open database from file system
    pub fn open_file(path: impl AsRef<Path>) -> anyhow::Result<Db> {
        fs::create_dir_all(&path).context("create data dir failed")?;
        let mut db = Db::open(&path)?;
        db.run_background_tasks();
        Ok(db)
    }

    fn run_background_tasks(&mut self) {
        let _flush_rx = self.flush_chan.1.clone();
        let _daemon = self.daemon.clone();
        thread::spawn(move || {
            for _ in _flush_rx {
                if let Err(_) = _daemon.rotate() {
                    // TODO
                }
            }
        });
    }

    pub(crate) fn path_of_current(base_path: impl AsRef<Path>) -> PathBuf {
        base_path.as_ref().join("CURRENT")
    }

    pub(crate) fn path_of_manifest(base_path: impl AsRef<Path>, id: usize) -> PathBuf {
        base_path.as_ref().join(format!("{:05}.MANIFEST", id))
    }

    pub(crate) fn path_of_new_wal(base_path: impl AsRef<Path>) -> PathBuf {
        base_path.as_ref().join("NEW_LOG")
    }

    pub(crate) fn path_of_wal(base_path: impl AsRef<Path>) -> PathBuf {
        base_path.as_ref().join("LOG")
    }

    pub(crate) fn path_of_sst(base_path: impl AsRef<Path>, sst_id: usize) -> PathBuf {
        base_path.as_ref().join(format!("{:05}.SST", sst_id))
    }

    pub(crate) fn path_of_vsst(base_path: impl AsRef<Path>, vsst_id: usize) -> PathBuf {
        base_path.as_ref().join(format!("{:05}.VSST", vsst_id))
    }

    pub fn recover(
        path: impl AsRef<Path>, manifest: Arc<Manifest>, cache: Arc<BlockCache>
    ) -> anyhow::Result<(Vec<Vec<Arc<SsTable>>>, Vec<Arc<SsTable>>, usize, Arc<MemTable>, Arc<Journal>)> {
        // 从 MANIFEST 恢复元信息
        let mut iter = ManifestIterator::create_and_seek_to_first(manifest)?;
        let mut now_sst_id = 0;
        let mut sst_map: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        let mut seq_num = 1;
        while iter.is_valid() {
            let record_item = iter.record_item();
            match record_item {
                ManifestItem::NewSst(level, sst_id) => {
                    sst_map.entry(level).or_default().push(sst_id);
                    now_sst_id = if now_sst_id > sst_id { now_sst_id } else { sst_id }
                }
                ManifestItem::DelSst(level, sst_id) => {
                    if let Some(vec) = sst_map.get_mut(&level) {
                        vec.retain(|id| *id != sst_id);
                    }
                }
                ManifestItem::MaxSeqNum(_seq_num) => seq_num = _seq_num,
                ManifestItem::Init(_) => {}
                ManifestItem::RotateWal => {
                    let new_log = Db::path_of_new_wal(&path);
                    if new_log.exists() {
                        let old_log = Db::path_of_wal(&path);
                        old_log.exists().then(|| fs::remove_file(old_log.clone()));
                        fs::rename(new_log, old_log)?;
                    }
                }
            }
            iter.next()?;
        }
        // 恢复 SST
        let mut l0_sst: Vec<Arc<SsTable>> = vec![];
        let mut levels: Vec<Vec<Arc<SsTable>>> = vec![];
        levels.resize(SST_LIMIT, vec![]);
        for level in 0..SST_LIMIT {
            let l: &mut Vec<Arc<SsTable>> = if level == 0 {
                &mut l0_sst
            } else {
                &mut levels[level]
            };
            if let Some(sst_ids) = sst_map.get(&level) {
                for sst_id in sst_ids {
                    let sst = Arc::new(
                        SsTable::open(
                            *sst_id,
                            Some(cache.clone()),
                            FileStorage::open(Db::path_of_sst(&path, *sst_id))?,
                        )?,
                    );
                    l.push(sst);
                }
            }
        }

        // 重新执行 LOG 操作
        let wal = Arc::new(Journal::open(Db::path_of_wal(&path))?);
        let memtable = Arc::new(MemTable::new());
        if wal.num_of_records() > 0 {
            let mut wal_iter = JournalIterator::create_and_seek_to_first(wal.clone())?;
            while wal_iter.is_valid() {
                let wal_item = wal_iter.record_item();
                let entry = wal_item.as_ref();
                let op_code = OpType::from(entry.meta);
                let key = Db::make_internal_key(1, op_code, &entry.key);
                memtable.put(key, entry.value.clone());
                wal_iter.next()?;
            }
        }

        Ok((levels, l0_sst, now_sst_id, memtable, wal))
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let current_path = Db::path_of_current(&path);
        let version = 0;

        let mut levels: Vec<Vec<Arc<SsTable>>> = vec![];
        let mut l0_sst: Vec<Arc<SsTable>> = vec![];
        let mut memtable = Arc::new(MemTable::new());
        let mut wal = None;
        let mut sst_id = 0;
        let cache = Arc::new(BlockCache::new(BLOCK_CACHE_SIZE));
        if current_path.exists() {
            // 从 CURRENT 中获取当前的 MANIFEST 文件
            let current_manifest: anyhow::Result<String> = {
                let mut content = String::new();
                File::open(current_path.as_path())?.read_to_string(&mut content)?;
                Ok(content)
            };
            let manifest = Arc::new(Manifest::open(
                path.as_ref().join(PathBuf::from(current_manifest?)),
            )?);
            // 根据 MANIFEST 恢复数据
            if manifest.num_of_records() > 0 {
                let recover_res = Db::recover(&path, manifest, cache.clone());
                let (_levels, _l0_sst, _sst_id, _memtable, _wal) = recover_res?;
                (levels, l0_sst, sst_id, memtable, wal) = (_levels, _l0_sst, _sst_id, _memtable, Some(_wal));
            }
        }

        // 新建 MANIFEST 和 CURRENT，TODO 删除其它多余 MANIFEST
        let manifest_path = Db::path_of_manifest(&path, version+1);
        let mut manifest = Manifest::open(manifest_path.as_path())?;
        let mut r = RecordBuilder::new();
        r.add(ManifestItem::Init(version as i32 + 1));
        manifest.add(&r.build());
        let manifest = Arc::new(RwLock::new(manifest));
        let mut current = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(current_path)?;
        assert!(manifest_path.is_file());
        current.write(manifest_path.file_name().unwrap().as_bytes())?;

        // 构建Db
        let inner = Arc::new(RwLock::new(Arc::new(DbInner {
            wal: Arc::new(Journal::open(Db::path_of_wal(&path))?),
            frozen_wal: vec![],
            memtable: memtable,
            frozen_memtable: vec![],

            l0_sst,
            levels,

            seq_num: 1,
            sst_id,
        })));

        let path = Arc::new(PathBuf::from(path.as_ref()));
        Ok(Db {
            inner: inner.clone(),
            path: path.clone(),
            version: AtomicU64::new(version as u64),
            block_cache: cache.clone(),

            flush_chan: channel::unbounded(),
            compaction_chan: channel::unbounded(),
            exit_chan: channel::bounded(1),
            daemon: Arc::new(DbDaemon::new(inner, cache, manifest.clone(), path)),
            manifest,
        })
    }

    /// close database connect, that will ensure all committed transactions will be fsync to journal
    pub fn close(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }

    fn make_internal_key(seq_num: u64, op_type: OpType, key: &Bytes) -> Key {
        Key::new(key.clone(), seq_num, op_type)
    }

    /// put a key-value pair
    pub fn put(&mut self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        self.append(key, Some(value))
    }

    /// delete value by key
    pub fn delete(&mut self, key: Bytes) -> anyhow::Result<()> {
        self.append(key, None)
    }

    /// get value by key
    pub fn get(&self, key: &Bytes) -> anyhow::Result<Option<Bytes>> {
        let (snapshot, seq_num) = {
            let guard = self.inner.read();
            (Arc::clone(&guard), guard.seq_num)
        };

        let internal_key = Db::make_internal_key(seq_num, Get, key);

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

    fn append(&self, key: Bytes, value: Option<Bytes>) -> anyhow::Result<()> {
        let (value, op_type) = match value {
            None => (Bytes::new(), Delete),
            Some(v) => (v, Put),
        };
        let entry = Entry::new(op_type.encode(), key.clone(), value.clone());

        let guard = self.inner.read();

        let seq_num = guard.seq_num;
        guard.wal.write(vec![entry])?;

        let internal_key = Db::make_internal_key(seq_num, op_type, &key);
        guard.memtable.put(internal_key, value);

        if guard.memtable.size() > MEMTABLE_SIZE_LIMIT {
            if let Err(e) = self.flush_chan.0.send(()) {
                eprintln!("{}", e);
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn print_debug_info(&self) {
        use chrono::Local;;

        let _inner = self.inner.read();

        println!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
        println!("memtable: {}KiB", _inner.memtable.size() / 1024);
        dbg!(&_inner.memtable);

        println!("forzen memtable number: {}", _inner.frozen_memtable.len());
        dbg!(&_inner.frozen_memtable);

        println!("l0 sst number: {}", _inner.l0_sst.len());
        println!()
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
    use std::io::Read;
    use std::thread;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};

    use crate::{MEMTABLE_SIZE_LIMIT};
    use crate::db::Db;

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
        let _k1 = Bytes::from("tmp_k1");
        let _v1 = BytesMut::zeroed(MEMTABLE_SIZE_LIMIT / 40).freeze();

        {
            let mut db = Db::open_file(data_dir.path()).unwrap();
            for _ in 1..50 {
                db.put(_k1.clone(), _v1.clone()).unwrap();
            }
            db.put(k1.clone(), v1.clone()).unwrap();
        }
        thread::sleep(Duration::from_secs(2));
        {
            let db = Db::open_file(data_dir.path()).unwrap();
            assert_eq!(db.get(&k1).unwrap(), Some(v1.clone()));
            assert_eq!(db.get(&_k1).unwrap(), Some(_v1.clone()));
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
        assert_eq!(db.inner.read().l0_sst.len(), 1);
    }
}
