use std::path::PathBuf;
use std::sync::Arc;
use crossbeam::channel;
use parking_lot::RwLock;
use crate::cache::BlockCache;
use crate::db::DbInner;
use crate::meta::manifest::Manifest;

mod compaction;
mod rotate;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct DbDaemon {
    inner: Arc<RwLock<Arc<DbInner>>>,
    sst_cache: Arc<BlockCache>,
    vsst_cache: Arc<BlockCache>,
    manifest: Arc<RwLock<Manifest>>,
    path: Arc<PathBuf>,

    flush_chan: (channel::Sender<()>, channel::Receiver<()>),
    compaction_chan: (channel::Sender<u32>, channel::Receiver<u32>),
    exit_chan: (channel::Sender<()>, channel::Receiver<()>),
}

impl DbDaemon {
    pub fn new(
        db_inner: Arc<RwLock<Arc<DbInner>>>,
        sst_cache: Arc<BlockCache>,
        vsst_cache: Arc<BlockCache>,
        manifest: Arc<RwLock<Manifest>>,
        path: Arc<PathBuf>,

        flush_chan: (channel::Sender<()>, channel::Receiver<()>),
        compaction_chan: (channel::Sender<u32>, channel::Receiver<u32>),
        exit_chan: (channel::Sender<()>, channel::Receiver<()>),
    ) -> Self {
        DbDaemon {
            inner: db_inner,
            sst_cache,
            vsst_cache,
            manifest,
            path,

            flush_chan,
            compaction_chan,
            exit_chan,
        }
    }
}