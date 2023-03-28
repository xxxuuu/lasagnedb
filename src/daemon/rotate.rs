use crate::daemon::DbDaemon;
use crate::entry::EntryBuilder;
use crate::memtable::MemTable;
use crate::meta::manifest::ManifestItem;
use crate::record::RecordBuilder;
use crate::sstable::builder::SsTableBuilder;
use crate::wal::Journal;
use crate::{Db, L0_SST_NUM_LIMIT, MEMTABLE_SIZE_LIMIT, MIN_VSST_SIZE};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;
use tracing::instrument;

impl DbDaemon {
    #[instrument]
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
        let sst_id: u32;
        let vsst_id: u32;

        // 冻结 memtable 和 wal
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::new()));
            let old_wal = std::mem::replace(
                &mut snapshot.wal,
                Arc::new(Journal::open(Db::path_of_new_wal(self.path.as_ref()))?),
            );

            flush_memtable = old_memtable.clone();
            sst_id = snapshot.sst_id + 1;
            vsst_id = snapshot.vsst_id + 1;
            snapshot.sst_id = sst_id;
            snapshot.vsst_id = vsst_id;
            snapshot.frozen_memtable.push(old_memtable);
            snapshot.frozen_wal.push(old_wal);
            *guard = Arc::new(snapshot);
        }

        // 写入到 L0 SST
        let mut sst_builder = SsTableBuilder::new();
        let mut vsst_builder = SsTableBuilder::new();
        flush_memtable.for_each(|_key, _value| {
            let user_key = _key.user_key.clone();
            let value = _value.clone();
            // KV 分离
            if _value.len() as u64 > MIN_VSST_SIZE {
                let mut _sst_value = BytesMut::new();
                _sst_value.put_u32_le(vsst_id);
                let sst_entry = EntryBuilder::new()
                    .op_type(_key.op_type)
                    .kv_separate(true)
                    .key_value(user_key.clone(), _sst_value.freeze())
                    .build();
                let vsst_entry = EntryBuilder::new()
                    .op_type(_key.op_type)
                    .key_value(user_key, value)
                    .build();
                sst_builder.add(&sst_entry);
                vsst_builder.add(&vsst_entry);
            } else {
                let entry = EntryBuilder::new()
                    .op_type(_key.op_type)
                    .key_value(user_key, value)
                    .build();
                sst_builder.add(&entry);
            }
        });
        let sst = Arc::new(sst_builder.build(
            sst_id,
            Some(self.sst_cache.clone()),
            Db::path_of_sst(self.path.as_ref(), sst_id),
        )?);
        let mut vsst = None;
        let kv_separate = vsst_builder.len() > 0;
        if kv_separate {
            vsst = Some(Arc::new(vsst_builder.build(
                vsst_id,
                Some(self.vsst_cache.clone()),
                Db::path_of_vsst(self.path.as_ref(), vsst_id),
            )?));
        }

        // 更新 SST 信息到 inner 和写入元数据
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            let mut _old_wal = snapshot.frozen_wal.pop();
            snapshot.frozen_memtable.pop();
            snapshot.levels[0].push(sst);
            if let Some(_vsst) = vsst {
                snapshot.vssts.write().insert(vsst_id, _vsst);
            }

            // 更新元数据
            let mut manifest = self.manifest.write();
            let mut r = RecordBuilder::new();
            r.add(ManifestItem::NewSst(0, sst_id));
            if kv_separate {
                r.add(ManifestItem::NewVSst(vsst_id))
            }
            r.add(ManifestItem::MaxSeqNum(snapshot.seq_num));
            r.add(ManifestItem::RotateWal);
            manifest.add(&r.build());

            if let Some(old_wal) = _old_wal {
                old_wal.delete();
            }
            snapshot.wal.rename(Db::path_of_wal(self.path.as_ref()))?;

            let l0_compaction = snapshot.levels[0].len() > L0_SST_NUM_LIMIT;

            *guard = Arc::new(snapshot);

            // L0 SST 数量过多，触发合并
            if l0_compaction {
                self.compaction_chan.0.send(0)?;
            }
        }

        Ok(())
    }
}
