use crate::cache::BlockCache;
use crate::db::DbInner;
use crate::memtable::MemTable;
use crate::meta::manifest::{Manifest, ManifestItem};
use crate::record::RecordBuilder;
use crate::sstable::builder::SsTableBuilder;
use crate::{Db, MEMTABLE_SIZE_LIMIT};
use parking_lot::RwLock;
use std::path::{PathBuf};
use std::sync::Arc;
use crate::wal::Journal;

#[derive(Debug)]
pub(crate) struct DbDaemon {
    inner: Arc<RwLock<Arc<DbInner>>>,
    cache: Arc<BlockCache>,
    manifest: Arc<RwLock<Manifest>>,
    path: Arc<PathBuf>,
}

impl DbDaemon {
    pub fn new(
        db_inner: Arc<RwLock<Arc<DbInner>>>,
        cache: Arc<BlockCache>,
        manifest: Arc<RwLock<Manifest>>,
        path: Arc<PathBuf>,
    ) -> Self {
        DbDaemon {
            inner: db_inner,
            cache,
            manifest,
            path,
        }
    }

    pub fn rotate(&self) -> anyhow::Result<()> {
        let mut rotate = false;
        {
            let guard = self.inner.read();
            if guard.memtable.size() > MEMTABLE_SIZE_LIMIT {
                rotate = true;
            }
        }
        if !rotate {
            return Ok(());
        }

        let flush_memtable;
        let sst_id;

        // 冻结 memtable 和 wal
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(
                &mut snapshot.memtable,
                Arc::new(MemTable::new())
            );
            let old_wal = std::mem::replace(
                &mut snapshot.wal,
                Arc::new(Journal::open(Db::path_of_new_wal(self.path.as_ref()))?)
            );

            flush_memtable = old_memtable.clone();
            sst_id = snapshot.sst_id + 1;
            snapshot.sst_id = sst_id;
            snapshot.frozen_memtable.push(old_memtable);
            snapshot.frozen_wal.push(old_wal);
            *guard = Arc::new(snapshot);
        }

        // TODO KV分离
        // 写入到 L0 SST
        let mut builder = SsTableBuilder::new();
        flush_memtable.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.cache.clone()),
            Db::path_of_sst(self.path.as_ref(), sst_id),
        )?);

        // 加入 L0 SST 到 list 里
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let mut _old_wal = snapshot.frozen_wal.pop();
            snapshot.frozen_memtable.pop();
            snapshot.l0_sst.push(sst);

            // 更新元数据
            let mut manifest = self.manifest.write();
            let mut r = RecordBuilder::new();
            r.add(ManifestItem::NewSst(0, sst_id));
            r.add(ManifestItem::MaxSeqNum(snapshot.seq_num));
            r.add(ManifestItem::RotateWal);
            manifest.add(&r.build());

            if let Some(old_wal) = _old_wal {
                old_wal.delete();
            }
            snapshot.wal.rename(Db::path_of_wal(self.path.as_ref()))?;

            *guard = Arc::new(snapshot);
        }

        Ok(())
    }
}
