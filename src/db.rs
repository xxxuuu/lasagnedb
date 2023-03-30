use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};

use std::io::{Read, Write};

use std::fmt::Debug;
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

use tracing::{debug, instrument, span, warn};

use crate::cache::BlockCache;
use crate::{Key, OpType, BLOCK_CACHE_SIZE, MEMTABLE_SIZE_LIMIT, SST_LEVEL_LIMIT};

use crate::daemon::DbDaemon;
use crate::db_iterator::{DbIterator, FusedIterator};
use crate::entry::EntryBuilder;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::two_merge_iterator::TwoMergeIterator;
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
    compaction_chan: (channel::Sender<u32>, channel::Receiver<u32>),
    exit_chan: (channel::Sender<()>, channel::Receiver<()>),
    daemon: Arc<DbDaemon>,
    manifest: Arc<RwLock<Manifest>>,
}

pub struct Options {}

impl Db {
    /// open database from file system
    #[instrument]
    pub fn open_file(path: impl AsRef<Path> + Debug) -> anyhow::Result<Db> {
        fs::create_dir_all(&path).context("create data dir failed")?;
        let db = Db::open(&path)?;
        db.run_background_tasks();
        Ok(db)
    }

    fn run_background_tasks(&self) {
        let _flush_rx = self.flush_chan.1.clone();
        let _daemon = self.daemon.clone();
        thread::spawn(move || {
            for _ in _flush_rx {
                if let Err(err) = _daemon.rotate() {
                    warn!("rotate failed: {}", err)
                }
            }
        });
        let _compaction_rx = self.compaction_chan.1.clone();
        let _daemon = self.daemon.clone();
        thread::spawn(move || {
            for level in _compaction_rx {
                if let Err(err) = _daemon.compaction(level) {
                    warn!("compaction failed: {}", err)
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

    #[instrument]
    pub fn recover(
        path: impl AsRef<Path> + Debug,
        manifest: Arc<Manifest>,
        sst_cache: Arc<BlockCache>,
        vsst_cache: Arc<BlockCache>,
    ) -> anyhow::Result<(
        Vec<Vec<Arc<SsTable>>>,
        u32,
        HashMap<u32, Arc<SsTable>>,
        u32,
        Arc<MemTable>,
    )> {
        // 从 MANIFEST 恢复元信息
        let mut iter = ManifestIterator::create_and_seek_to_first(manifest)?;
        let mut now_sst_id = 0;
        let mut now_vsst_id = 0;
        let mut sst_map: HashMap<u32, Vec<u32>> = HashMap::new();
        let mut vsst_set: HashSet<u32> = HashSet::new();
        let mut seq_num = 1;
        let iter_manifest_span = span!(tracing::Level::TRACE, "iterate manifest").entered();
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
        drop(iter_manifest_span);

        // 恢复 SST
        let recover_sst_span = span!(tracing::Level::TRACE, "recover sst info").entered();
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
        drop(recover_sst_span);

        // 重新执行 LOG 操作
        let redo_log_span = span!(tracing::Level::TRACE, "redo log").entered();
        let wal = Arc::new(Journal::open(Db::path_of_wal(&path))?);
        let memtable = Arc::new(MemTable::new());
        if wal.num_of_records() > 0 {
            let mut wal_iter = JournalIterator::create_and_seek_to_first(wal)?;
            while wal_iter.is_valid() {
                let wal_item = wal_iter.record_item();
                let entry = wal_item.as_ref();
                let op_code = OpType::from((entry.meta & 0xFF) as u8);
                let key = Db::make_internal_key(1, op_code, &entry.key);
                memtable.put(key, entry.value.clone());
                wal_iter.next()?;
            }
        }
        drop(redo_log_span);

        Ok((levels, now_sst_id, vssts, now_vsst_id, memtable))
    }

    #[instrument]
    pub fn open(path: impl AsRef<Path> + Debug) -> anyhow::Result<Self> {
        let current_path = Db::path_of_current(&path);
        let version = 0;

        let mut levels: Vec<Vec<Arc<SsTable>>> = vec![];
        levels.resize(SST_LEVEL_LIMIT as usize, vec![]);
        let mut vssts: HashMap<u32, Arc<SsTable>> = HashMap::new();
        let mut memtable = Arc::new(MemTable::new());
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
                    Db::recover(&path, manifest, sst_cache.clone(), vsst_cache.clone())?;
                debug!("recover result: {:?}", recover_res);
                (levels, sst_id, vssts, vsst_id, memtable) = recover_res;
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
        let flush_chan = channel::unbounded();
        let compaction_chan = channel::unbounded();
        let exit_chan = channel::bounded(1);
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

            flush_chan: flush_chan.clone(),
            compaction_chan: compaction_chan.clone(),
            exit_chan: exit_chan.clone(),
            daemon: Arc::new(DbDaemon::new(
                inner,
                sst_cache,
                vsst_cache,
                manifest.clone(),
                path,
                flush_chan,
                compaction_chan,
                exit_chan,
            )),
            manifest,
        })
    }

    /// close database connect, that will ensure all committed transactions will be fsync to journal
    pub fn close(&self) -> anyhow::Result<()> {
        unimplemented!()
    }

    fn make_internal_key(seq_num: u64, op_type: OpType, key: &Bytes) -> Key {
        Key::new(key.clone(), seq_num, op_type)
    }

    /// put a key-value pair
    #[instrument(skip_all)]
    pub fn put(&self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        self.append(key, Some(value))
    }

    /// delete value by key
    #[instrument(skip_all)]
    pub fn delete(&self, key: Bytes) -> anyhow::Result<()> {
        self.append(key, None)
    }

    /// get value by key
    #[instrument(skip_all)]
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
        for level in 0..SST_LEVEL_LIMIT {
            let mut iters = Vec::new();
            iters.reserve(snapshot.levels[level as usize].len());
            for table in snapshot.levels[level as usize].iter().rev() {
                if table.maybe_contains_key(key) {
                    iters.push(Box::new(VSsTableIterator::create_and_seek_to_key(
                        table.clone(),
                        key,
                        snapshot.vssts.clone(),
                    )?));
                }
            }
            let iter = MergeIterator::create(iters);
            if iter.is_valid() && iter.key() == key {
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            }
        }

        Ok(None)
    }

    #[instrument(skip_all)]
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
        guard.wal.flush();

        let internal_key = Db::make_internal_key(seq_num, op_type, &key);
        guard.memtable.put(internal_key, value);

        if guard.memtable.size() > MEMTABLE_SIZE_LIMIT {
            if let Err(e) = self.flush_chan.0.send(()) {
                eprintln!("{}", e);
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    pub fn scan(
        &self,
        lower: Bound<Bytes>,
        upper: Bound<Bytes>,
    ) -> anyhow::Result<FusedIterator<DbIterator>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        };

        let mut mem_iters = Vec::new();
        mem_iters.reserve(snapshot.frozen_memtable.len() + 1);
        mem_iters.push(Box::new(
            snapshot.memtable.scan(lower.clone(), upper.clone()),
        ));
        for _memtable in snapshot.frozen_memtable.iter().rev() {
            let memtable = _memtable.clone();
            mem_iters.push(Box::new(memtable.scan(lower.clone(), upper.clone())));
        }
        let mem_iter = MergeIterator::create(mem_iters);

        let mut sst_iters = Vec::new();
        for level in 0..SST_LEVEL_LIMIT {
            for table in snapshot.levels[level as usize].iter().rev() {
                let iter = match lower.clone() {
                    Bound::Included(key) => VSsTableIterator::create_and_seek_to_key(
                        table.clone(),
                        &key[..],
                        snapshot.vssts.clone(),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = VSsTableIterator::create_and_seek_to_key(
                            table.clone(),
                            &key[..],
                            snapshot.vssts.clone(),
                        )?;
                        if iter.is_valid() && iter.key() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => VSsTableIterator::create_and_seek_to_first(
                        table.clone(),
                        snapshot.vssts.clone(),
                    )?,
                };
                sst_iters.push(Box::new(iter));
            }
        }
        let sst_iter = MergeIterator::create(sst_iters);

        let iter = TwoMergeIterator::create(mem_iter, sst_iter)?;

        Ok(FusedIterator::new(DbIterator::new(iter, upper)?))
    }
}
