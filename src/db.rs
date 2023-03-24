use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};

use std::io::{Read, Write};

use std::ops::Bound;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::{fs, thread};

use anyhow::Context;
use bytes::Bytes;

use crossbeam::channel;

use parking_lot::RwLock;

use crate::cache::BlockCache;
use crate::{Key, OpType};

use crate::daemon::DbDaemon;
use crate::entry::EntryBuilder;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::memtable::MemTable;
use crate::meta::iterator::ManifestIterator;
use crate::meta::manifest::{Manifest, ManifestItem};
use crate::record::RecordBuilder;
use crate::sstable::builder::SsTable;
use crate::sstable::iterator::VSsTableIterator;
use crate::storage::file::FileStorage;
use crate::wal::iterator::JournalIterator;
use crate::wal::Journal;
use crate::OpType::{Delete, Get, Put};

pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

pub const MEMTABLE_SIZE_LIMIT: usize = 4 * MB;
pub const BLOCK_CACHE_SIZE: u64 = 4 * GB as u64;
pub const MIN_VSST_SIZE: u64 = 4 * KB as u64;
pub const SST_LEVEL_LIMIT: u32 = 6;

#[derive(Clone, Debug)]
pub(crate) struct DbInner {
    pub(crate) wal: Arc<Journal>,
    pub(crate) frozen_wal: Vec<Arc<Journal>>,
    pub(crate) memtable: Arc<MemTable>,
    pub(crate) frozen_memtable: Vec<Arc<MemTable>>,

    pub(crate) levels: Vec<Vec<Arc<SsTable>>>,
    pub(crate) vssts: Arc<RwLock<HashMap<u32, Arc<SsTable>>>>,

    pub(crate) seq_num: u64,
    pub(crate) sst_id: u32,
    pub(crate) vsst_id: u32,
}

#[derive(Debug)]
pub struct Db {
    pub(crate) inner: Arc<RwLock<Arc<DbInner>>>,

    path: Arc<PathBuf>,
    version: AtomicU64,
    sst_cache: Arc<BlockCache>,
    vsst_cache: Arc<BlockCache>,

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
                if let Err(err) = _daemon.rotate() {
                    eprintln!("rotate failed: {}", err)
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

    pub(crate) fn path_of_sst(base_path: impl AsRef<Path>, sst_id: u32) -> PathBuf {
        base_path.as_ref().join(format!("{:05}.SST", sst_id))
    }

    pub(crate) fn path_of_vsst(base_path: impl AsRef<Path>, vsst_id: u32) -> PathBuf {
        base_path.as_ref().join(format!("{:05}.VSST", vsst_id))
    }

    pub fn recover(
        path: impl AsRef<Path>,
        manifest: Arc<Manifest>,
        sst_cache: Arc<BlockCache>,
        vsst_cache: Arc<BlockCache>,
    ) -> anyhow::Result<(
        Vec<Vec<Arc<SsTable>>>,
        u32,
        HashMap<u32, Arc<SsTable>>,
        u32,
        Arc<MemTable>,
        Arc<Journal>,
    )> {
        // 从 MANIFEST 恢复元信息
        let mut iter = ManifestIterator::create_and_seek_to_first(manifest)?;
        let mut now_sst_id = 0;
        let mut now_vsst_id = 0;
        let mut sst_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut vsst_set: HashSet<u32> = HashSet::new();
        let mut seq_num = 1;
        while iter.is_valid() {
            let record_item = iter.record_item();
            match record_item {
                ManifestItem::NewSst(level, sst_id) => {
                    sst_map.entry(level).or_default().push(sst_id);
                    now_sst_id = if now_sst_id > sst_id {
                        now_sst_id
                    } else {
                        sst_id
                    }
                }
                ManifestItem::DelSst(level, sst_id) => {
                    if let Some(vec) = sst_map.get_mut(&level) {
                        vec.retain(|id| *id != sst_id);
                    }
                }
                ManifestItem::NewVSst(sst_id) => {
                    vsst_set.insert(sst_id);
                    now_vsst_id = if now_vsst_id > sst_id {
                        now_vsst_id
                    } else {
                        sst_id
                    }
                }
                ManifestItem::DelVSst(sst_id) => {
                    vsst_set.remove(&sst_id);
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
        let mut levels: Vec<Vec<Arc<SsTable>>> = vec![];
        levels.resize(SST_LEVEL_LIMIT as usize, vec![]);
        for level in 0..SST_LEVEL_LIMIT {
            let l: &mut Vec<Arc<SsTable>> = &mut levels[level as usize];
            if let Some(sst_ids) = sst_map.get(&level) {
                for sst_id in sst_ids {
                    let sst = Arc::new(SsTable::open(
                        *sst_id,
                        Some(sst_cache.clone()),
                        FileStorage::open(Db::path_of_sst(&path, *sst_id))?,
                    )?);
                    l.push(sst);
                }
            }
        }
        let mut vssts: HashMap<u32, Arc<SsTable>> = HashMap::new();
        for vsst_id in vsst_set {
            vssts.insert(
                vsst_id,
                Arc::new(SsTable::open(
                    vsst_id,
                    Some(vsst_cache.clone()),
                    FileStorage::open(Db::path_of_vsst(&path, vsst_id))?,
                )?),
            );
        }

        // 重新执行 LOG 操作
        let wal = Arc::new(Journal::open(Db::path_of_wal(&path))?);
        let memtable = Arc::new(MemTable::new());
        if wal.num_of_records() > 0 {
            let mut wal_iter = JournalIterator::create_and_seek_to_first(wal.clone())?;
            while wal_iter.is_valid() {
                let wal_item = wal_iter.record_item();
                let entry = wal_item.as_ref();
                let op_code = OpType::from((entry.meta & 0xFF) as u8);
                let key = Db::make_internal_key(1, op_code, &entry.key);
                memtable.put(key, entry.value.clone());
                wal_iter.next()?;
            }
        }

        Ok((levels, now_sst_id, vssts, now_vsst_id, memtable, wal))
    }

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let current_path = Db::path_of_current(&path);
        let version = 0;

        let mut levels: Vec<Vec<Arc<SsTable>>> = vec![];
        levels.resize(SST_LEVEL_LIMIT as usize, vec![]);
        let mut vssts: HashMap<u32, Arc<SsTable>> = HashMap::new();
        let mut memtable = Arc::new(MemTable::new());
        let mut wal = None;
        let mut sst_id = 0;
        let mut vsst_id = 0;
        let sst_cache = Arc::new(BlockCache::new(BLOCK_CACHE_SIZE));
        let vsst_cache = Arc::new(BlockCache::new(BLOCK_CACHE_SIZE));

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
                let recover_res =
                    Db::recover(&path, manifest, sst_cache.clone(), vsst_cache.clone());
                let (_levels, _sst_id, _vssts, _vsst_id, _memtable, _wal) = recover_res?;
                (levels, sst_id, vssts, vsst_id, memtable, wal) =
                    (_levels, _sst_id, _vssts, _vsst_id, _memtable, Some(_wal));
            }
        }

        // 新建 MANIFEST 和 CURRENT，TODO 删除其它多余 MANIFEST
        let manifest_path = Db::path_of_manifest(&path, version + 1);
        let mut manifest = Manifest::open(manifest_path.as_path())?;
        let mut r = RecordBuilder::new();
        r.add(ManifestItem::Init(version as i32 + 1));
        for (_level, _ssts) in levels.iter().enumerate() {
            for sst in _ssts {
                r.add(ManifestItem::NewSst(_level as u32, sst.id()));
            }
        }
        for (_vsst_id, _) in &vssts {
            r.add(ManifestItem::NewVSst(*_vsst_id));
        }
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
            memtable,
            frozen_memtable: vec![],
            levels,
            vssts: Arc::new(RwLock::new(vssts)),
            seq_num: 1,
            sst_id,
            vsst_id,
        })));

        let path = Arc::new(PathBuf::from(path.as_ref()));
        Ok(Db {
            inner: inner.clone(),
            path: path.clone(),
            version: AtomicU64::new(version as u64),
            sst_cache: sst_cache.clone(),
            vsst_cache: vsst_cache.clone(),

            flush_chan: channel::unbounded(),
            compaction_chan: channel::unbounded(),
            exit_chan: channel::bounded(1),
            daemon: Arc::new(DbDaemon::new(
                inner,
                sst_cache,
                vsst_cache,
                manifest.clone(),
                path,
            )),
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
        iters.reserve(snapshot.levels[0].len());
        for table in snapshot.levels[0].iter().rev() {
            iters.push(Box::new(VSsTableIterator::create_and_seek_to_key(
                table.clone(),
                key,
                snapshot.vssts.clone(),
            )?));
        }
        // TODO
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
        let mut entry_builder = EntryBuilder::new();
        entry_builder
            .op_type(op_type)
            .key_value(key.clone(), value.clone());
        let entry = entry_builder.build();

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
