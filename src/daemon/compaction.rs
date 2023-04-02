use crate::daemon::DbDaemon;
use crate::entry::EntryBuilder;
use crate::iterator::merge_iterator::MergeIterator;
use crate::iterator::StorageIterator;
use crate::meta::manifest::ManifestItem;
use crate::record::RecordBuilder;
use crate::sstable::builder::{SsTable, SsTableBuilder};
use crate::sstable::iterator::{SsTableIterator, VSsTableIterator};
use crate::{
    Db, OpType, MAX_LEVEL_SIZE, MAX_SST_SIZE, MAX_VSST_SPARE_RATIO, MIN_VSST_SIZE, SST_LEVEL_LIMIT,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::Path;
use std::ptr::read;

use crate::cache::BlockCache;
use crate::iterator::rc_merge_iterator::RcMergeIterator;
use parking_lot::RwLock;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, instrument, span, warn};

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
        let (new_ssts, new_vssts, vsst_rc_delta) = Self::merge(
            &self.path.as_path(),
            snapshot.sst_id,
            ssts,
            self.sst_cache.clone(),
            snapshot.vsst_id,
            snapshot.vssts.clone(),
            self.vsst_cache.clone(),
            snapshot.vsst_rc.clone(),
        )?;
        let mut r = RecordBuilder::new();

        // 添加新SST和清理过期SST
        snapshot.levels[level as usize].retain(|_sst| !sst_ids.contains(&_sst.id()));
        snapshot.levels[(level + 1) as usize].retain(|_sst| !sst_ids.contains(&_sst.id()));
        snapshot.levels[(level + 1) as usize].extend(new_ssts);
        for _vsst in new_vssts {
            snapshot.vssts.write().insert(_vsst.id(), _vsst.clone());
        }
        // 处理 VSST 引用计数
        for (_vsst_id, _delta) in vsst_rc_delta.as_ref() {
            let old_rc = snapshot.vsst_rc.read().get(&_vsst_id).unwrap_or(&0).clone();
            let new_rc = old_rc as i32 + _delta;
            if new_rc <= 0 {
                let _span = span!(tracing::Level::INFO, "Delete VSST");
                let _enter = _span.enter();

                info!("DEL {}.VSST", _vsst_id);
                {
                    let reader = snapshot.vssts.read();
                    match reader.get(_vsst_id) {
                        Some(_delete_vsst) => _delete_vsst.delete()?,
                        None => warn!("{}.VSST not existed", _vsst_id),
                    }
                }
                snapshot.vssts.write().remove(_vsst_id);
                snapshot.vsst_rc.write().remove(_vsst_id);
                r.add(ManifestItem::VSstRefCnt(*_vsst_id, 0));
                r.add(ManifestItem::DelVSst(*_vsst_id));
            } else {
                snapshot.vsst_rc.write().insert(*_vsst_id, new_rc as u32);
                r.add(ManifestItem::VSstRefCnt(*_vsst_id, new_rc as u32));
            }
        }

        // 更新元数据
        for _sst in li_sst {
            info!("DEL L{} {}.SST", level, _sst.id());
            r.add(ManifestItem::DelSst(level, _sst.id()));
            _sst.delete()?;
        }
        for _sst in li1_sst {
            info!("DEL L{} {}.SST", level, _sst.id());
            r.add(ManifestItem::DelSst(level + 1, _sst.id()));
            _sst.delete()?;
        }
        {
            let mut manifest = self.manifest.write();
            manifest.add(&r.build());
        }

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
        path: impl AsRef<Path> + Debug,
        now_sst_id: u32,
        ssts: Vec<Arc<SsTable>>,
        sst_cache: Arc<BlockCache>,
        now_vsst_id: u32,
        vssts: Arc<RwLock<HashMap<u32, Arc<SsTable>>>>,
        vsst_cache: Arc<BlockCache>,
        vsst_rc: Arc<RwLock<HashMap<u32, u32>>>,
    ) -> anyhow::Result<(
        Vec<Arc<SsTable>>,      //  new sst
        Vec<Arc<SsTable>>,      // new vsst
        Arc<HashMap<u32, i32>>, // vsst rc delta
    )> {
        let mut sst_iters = vec![];
        for _sst in ssts {
            sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(_sst)?));
        }

        // 创建多个SST
        let mut iter = RcMergeIterator::create(sst_iters);
        let mut new_ssts = vec![];
        let mut builder = SsTableBuilder::new();

        let mut new_vssts = vec![];
        let mut vsst_builder = SsTableBuilder::new();
        let mut vsst_rc_delta: HashMap<u32, i32> = HashMap::new();

        let mut next_sst_id = now_sst_id + 1;
        let mut next_vsst_id = now_vsst_id + 1;

        while iter.is_valid() {
            let is_separate = iter.value().len() as u64 > MIN_VSST_SIZE;

            let mut merge = false;
            let mut vsst_id = 0;
            if is_separate {
                // 若该项 KV 分离，判断对应 VSST 空洞率
                vsst_id = iter.value().get_u32_le();
                if let Some(ref_cnt) = vsst_rc.read().get(&vsst_id) {
                    let tot_cnt = vssts.read().get(&vsst_id).unwrap().num_of_pairs();
                    if *ref_cnt as f32 / tot_cnt as f32 > MAX_VSST_SPARE_RATIO {
                        merge = true;
                    }
                }
            }

            let mut entry_builder = EntryBuilder::new();
            // 如果空洞率超限，迁移到新 VSST
            if merge {
                // 先读原来那个 VSST 的数据（顺便要减引用计数
                let mut _iter = SsTableIterator::create_and_seek_to_key(
                    vssts.read().get(&vsst_id).unwrap().clone(),
                    iter.key(),
                )?;
                let key = Bytes::copy_from_slice(iter.key());
                let value = Bytes::copy_from_slice(_iter.value());
                vsst_rc_delta.insert(vsst_id, vsst_rc_delta.get(&vsst_id).unwrap_or(&0) - 1);

                // 然后写到新 VSST 里（增加引用计数
                vsst_builder.add(&EntryBuilder::new().key_value(key.clone(), value).build());
                vsst_rc_delta.insert(
                    next_vsst_id,
                    vsst_rc_delta.get(&next_vsst_id).unwrap_or(&0) + 1,
                );

                // 最后合并 SST 的时候修改 value 为新 VSST ID
                let mut sst_id_value = BytesMut::new();
                sst_id_value.put_u32_le(next_vsst_id);
                entry_builder
                    .op_type(OpType::Put)
                    .kv_separate(true)
                    .key_value(key, sst_id_value.freeze())
                    .build();
            } else {
                // 常规操作，只合并 SST
                entry_builder
                    .op_type(OpType::Put)
                    .kv_separate(is_separate)
                    .key_value(
                        Bytes::copy_from_slice(iter.key()),
                        Bytes::copy_from_slice(iter.value()),
                    )
                    .build();
            }

            let entry = entry_builder.build();
            if builder.size() + entry.size() > MAX_SST_SIZE as usize {
                builder.build(
                    next_sst_id,
                    Some(sst_cache.clone()),
                    Db::path_of_sst(&path, next_sst_id),
                )?;

                next_sst_id += 1;
                builder = SsTableBuilder::new();
            }
            builder.add(&entry);

            iter.next()?;
        }

        if builder.size() > 0 {
            new_ssts.push(Arc::new(builder.build(
                next_sst_id,
                Some(sst_cache.clone()),
                Db::path_of_sst(&path, next_sst_id),
            )?));
        }
        if vsst_builder.size() > 0 {
            new_vssts.push(Arc::new(vsst_builder.build(
                next_vsst_id,
                Some(vsst_cache.clone()),
                Db::path_of_vsst(&path, next_vsst_id),
            )?));
        }

        let _vsst_rc_delta = iter.vsst_rc_delta();
        for (vsst_id, delta) in _vsst_rc_delta {
            vsst_rc_delta.insert(vsst_id, vsst_rc_delta.get(&vsst_id).unwrap_or(&0) + delta);
        }

        Ok((new_ssts, new_vssts, Arc::new(vsst_rc_delta)))
    }
}
