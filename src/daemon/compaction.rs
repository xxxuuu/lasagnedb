use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use bytes::Bytes;
use tracing::instrument;
use crate::daemon::DbDaemon;
use crate::db::DbInner;
use crate::entry::EntryBuilder;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::{MIN_VSST_SIZE, OpType, SST_LEVEL_LIMIT};
use crate::meta::manifest::ManifestItem;
use crate::record::RecordBuilder;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::{SsTableIterator, VSsTableIterator};

impl DbDaemon {
    #[instrument]
    pub fn compaction(&self, level: u32) -> anyhow::Result<()> {
        if level == SST_LEVEL_LIMIT {
            return Ok(())
        }

        let guard = self.inner.write();
        let mut snapshot = guard.as_ref().clone();

        // 选择基准SST
        let _base_sst = Self::pick_base_sst(&snapshot, level);
        if let None = _base_sst {
            println!("l0 sst is empty");
            return Ok(())
        }
        let base_sst = _base_sst.unwrap();
        // 获取有重叠key范围的SST
        let (li_sst, li1_sst) = Self::select_overlap_sst(&snapshot, 0, base_sst.clone());

        let mut ssts = vec![];
        for _sst in &li_sst {
            ssts.push(_sst.clone());
        }
        for _sst in &li1_sst {
            ssts.push(_sst.clone());
        }
        let mut sst_ids = HashSet::new();
        for _sst in &ssts {
            sst_ids.insert(_sst.id());
        }

        // 合并
        if let Ok(mut sst_builder) = Self::merge(&snapshot, ssts) {
            let sst_id = snapshot.sst_id;
            snapshot.sst_id += 1;
            let new_sst = sst_builder.build(
                sst_id,
                Some(self.sst_cache.clone()),
                self.path.as_ref()
            )?;

            // 更新元数据
            let mut manifest = self.manifest.write();
            let mut r = RecordBuilder::new();
            r.add(ManifestItem::NewSst(level+1, sst_id));
            for _sst in li_sst {
                r.add(ManifestItem::DelSst(level, _sst.id()));
                _sst.delete()?;
            }
            for _sst in li1_sst {
                r.add(ManifestItem::DelSst(level+1, _sst.id()));
                _sst.delete()?;
            }
            manifest.add(&r.build());

            // 添加新SST和清理过期SST
            snapshot.levels[level as usize].retain(|_sst| {
                !sst_ids.contains(&_sst.id())
            });
            snapshot.levels[(level+1) as usize].retain(|_sst| {
                !sst_ids.contains(&_sst.id())
            });
            snapshot.levels[(level+1) as usize].push(Arc::new(new_sst));
        }

        Ok(())
    }

    #[instrument]
    fn pick_base_sst(snapshot: &DbInner, level: u32) -> Option<Arc<SsTable>> {
        // TODO 更好的挑选方法
        match snapshot.levels[level as usize].get(0) {
            None => None,
            Some(_sst) => Some(_sst.clone())
        }
    }

    #[instrument]
    fn select_overlap_sst(snapshot: &DbInner,
                          level: u32, base_sst: Arc<SsTable>) -> (Vec<Arc<SsTable>>, Vec<Arc<SsTable>>) {
        let (mut min_key, mut max_key) = base_sst.key_range();
        let mut li_sst_id = HashSet::new();
        li_sst_id.insert(base_sst.id());
        let mut li1_sst_id = HashSet::new();

        // 选Li重叠的
        for _sst in &snapshot.levels[level as usize] {
            if _sst.id() == base_sst.id() {
                continue;
            }
            if base_sst.is_overlap(_sst.clone()) {
                let (_min_key, _max_key) = _sst.key_range();
                if _min_key < min_key {
                    min_key = _min_key;
                }
                if _max_key > max_key {
                    max_key = _max_key;
                }
                li_sst_id.insert(_sst.id());
            }
        }
        // 选Li+1重叠的
        for _sst in &snapshot.levels[(level + 1) as usize] {
            let (_min_key, _max_key) = _sst.key_range();
            if _min_key < min_key {
                min_key = _min_key;
                li1_sst_id.insert(_sst.id());
            } else if _max_key > max_key {
                max_key = _max_key;
                li1_sst_id.insert(_sst.id());
            }
        }
        // 再反过来选Li重叠的
        for _sst in &snapshot.levels[level as usize] {
            let (_min_key, _max_key) = _sst.key_range();
            if _min_key < min_key {
                min_key = _min_key;
                li_sst_id.insert(_sst.id());
            } else if _max_key > max_key {
                max_key = _max_key;
                li_sst_id.insert(_sst.id());
            }
        }

        let (mut li_sst, mut li1_sst) = (vec![], vec![]);
        for _sst in &snapshot.levels[level as usize] {
            if li_sst_id.contains(&_sst.id()) {
                li_sst.push(_sst.clone());
            }
        }
        for _sst in &snapshot.levels[(level + 1) as usize] {
            if li1_sst_id.contains(&_sst.id()) {
                li1_sst.push(_sst.clone());
            }
        }

        return (li_sst, li1_sst)
    }

    #[instrument]
    fn merge(snapshot: &DbInner, ssts: Vec<Arc<SsTable>>) -> anyhow::Result<SsTableBuilder> {
        let mut sst_iters = vec![];
        for _sst in ssts {
            sst_iters.push(Box::new(
                VSsTableIterator::create_and_seek_to_first(_sst, snapshot.vssts.clone())?));
        }

        let mut iter = MergeIterator::create(sst_iters);
        let mut builder = SsTableBuilder::new();

        while iter.is_valid() {
            let mut entry_builder = EntryBuilder::new();
            builder.add(
                &entry_builder
                    .op_type(OpType::Put)
                    .kv_separate(iter.key().len() as u64 > MIN_VSST_SIZE)
                    .key_value(Bytes::copy_from_slice(iter.key()),
                               Bytes::copy_from_slice(iter.value()))
                    .build()
            );
        }

        Ok(builder)
    }
}
