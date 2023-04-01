use crate::daemon::DbDaemon;
use crate::entry::EntryBuilder;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::meta::manifest::ManifestItem;
use crate::record::RecordBuilder;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::VSsTableIterator;
use crate::{Db, OpType, MAX_LEVEL_SIZE, MAX_SST_SIZE, MIN_VSST_SIZE, SST_LEVEL_LIMIT};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, instrument, warn};

impl DbDaemon {
    #[instrument]
    pub fn compaction(&self, level: u32) -> anyhow::Result<()> {
        self.compaction_count.fetch_add(1, Ordering::Release);
        if level == SST_LEVEL_LIMIT {
            return Ok(());
        }

        let mut guard = self.inner.write();
        let mut snapshot = guard.as_ref().clone();

        // 选择基准SST
        let _base_sst = Self::pick_base_sst(&snapshot.levels, level);
        if _base_sst.is_none() {
            println!("l0 sst is empty");
            return Ok(());
        }
        let base_sst = _base_sst.unwrap();
        // 获取有重叠key范围的SST
        let (li_sst, li1_sst) = Self::select_overlap_sst(&snapshot.levels, 0, base_sst);

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
        let sst_builders = Self::merge(ssts, snapshot.vssts.clone())?;
        let mut r = RecordBuilder::new();
        let mut new_ssts = vec![];

        for sst_builder in sst_builders {
            let sst_id = snapshot.sst_id;
            snapshot.sst_id += 1;
            let new_sst = Arc::new(sst_builder.build(
                sst_id,
                Some(self.sst_cache.clone()),
                Db::path_of_sst(self.path.as_ref(), sst_id),
            )?);
            info!("new sst {:?}", new_sst);
            new_ssts.push(new_sst.clone());
            r.add(ManifestItem::NewSst(level + 1, sst_id));
        }

        // 更新元数据
        let mut manifest = self.manifest.write();
        for _sst in li_sst {
            r.add(ManifestItem::DelSst(level, _sst.id()));
            _sst.delete()?;
        }
        for _sst in li1_sst {
            r.add(ManifestItem::DelSst(level + 1, _sst.id()));
            _sst.delete()?;
        }
        manifest.add(&r.build());

        // 添加新SST和清理过期SST
        snapshot.levels[level as usize].retain(|_sst| !sst_ids.contains(&_sst.id()));
        snapshot.levels[(level + 1) as usize].retain(|_sst| !sst_ids.contains(&_sst.id()));
        snapshot.levels[(level + 1) as usize].extend(new_ssts);

        // 检查是否需要触发新的合并
        let mut leveli1_size = 0;
        snapshot.levels[(level + 1) as usize]
            .iter()
            .for_each(|_sst| leveli1_size += _sst.size());
        *guard = Arc::new(snapshot);

        if leveli1_size > MAX_LEVEL_SIZE[(level + 1) as usize] {
            if let Err(e) = self.compaction_chan.0.try_send(level + 1) {
                warn!("send compaction message failed {}", e);
            }
        }

        Ok(())
    }

    pub(crate) fn pick_base_sst(
        levels: &Vec<Vec<Arc<SsTable>>>,
        level: u32,
    ) -> Option<Arc<SsTable>> {
        // TODO 更好的挑选方法
        levels[level as usize].get(0).cloned()
    }

    #[instrument]
    pub(crate) fn select_overlap_sst(
        levels: &Vec<Vec<Arc<SsTable>>>,
        level: u32,
        base_sst: Arc<SsTable>,
    ) -> (Vec<Arc<SsTable>>, Vec<Arc<SsTable>>) {
        let (mut min_key, mut max_key) = base_sst.key_range();
        let mut li_sst_id = HashSet::new();
        li_sst_id.insert(base_sst.id());
        let mut li1_sst_id = HashSet::new();

        // 选Li重叠的
        for _sst in &levels[level as usize] {
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
        for _sst in &levels[(level + 1) as usize] {
            let (_min_key, _max_key) = _sst.key_range();
            if min_key <= _max_key && _min_key <= max_key {
                li1_sst_id.insert(_sst.id());
                if _min_key < min_key {
                    min_key = _min_key;
                } else if _max_key > max_key {
                    max_key = _max_key;
                }
            }
        }
        // 再反过来选Li重叠的
        for _sst in &levels[level as usize] {
            let (_min_key, _max_key) = _sst.key_range();
            if min_key <= _max_key && _min_key <= max_key && !li_sst_id.contains(&_sst.id()) {
                li_sst_id.insert(_sst.id());
                if _min_key < min_key {
                    min_key = _min_key;
                } else if _max_key > max_key {
                    max_key = _max_key;
                }
            }
        }

        let (mut li_sst, mut li1_sst) = (vec![], vec![]);
        for _sst in &levels[level as usize] {
            if li_sst_id.contains(&_sst.id()) {
                li_sst.push(_sst.clone());
            }
        }
        for _sst in &levels[(level + 1) as usize] {
            if li1_sst_id.contains(&_sst.id()) {
                li1_sst.push(_sst.clone());
            }
        }

        (li_sst, li1_sst)
    }

    #[instrument]
    pub(crate) fn merge(
        ssts: Vec<Arc<SsTable>>,
        vssts: Arc<RwLock<HashMap<u32, Arc<SsTable>>>>,
    ) -> anyhow::Result<Vec<SsTableBuilder>> {
        let mut sst_iters = vec![];
        for _sst in ssts {
            sst_iters.push(Box::new(VSsTableIterator::create_and_seek_to_first(
                _sst,
                vssts.clone(),
            )?));
        }

        // 创建多个SST
        let mut iter = MergeIterator::create(sst_iters);
        let mut builders = vec![];
        let mut builder = SsTableBuilder::new();

        while iter.is_valid() {
            let mut entry_builder = EntryBuilder::new();
            let entry = &entry_builder
                .op_type(OpType::Put)
                .kv_separate(iter.value().len() as u64 > MIN_VSST_SIZE)
                .key_value(
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                )
                .build();

            if builder.size() + entry.size() > MAX_SST_SIZE as usize {
                builders.push(builder);
                builder = SsTableBuilder::new();
            }
            builder.add(entry);

            iter.next()?;
        }
        if builder.size() > 0 {
            builders.push(builder);
        }
        Ok(builders)
    }
}
