use std::fmt::Debug;
use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::instrument;

use crate::record::{Record, RecordItem};
use crate::storage::file::FileStorage;

#[derive(Debug)]
pub struct Manifest {
    file: FileStorage,
    records: Vec<Arc<Record<ManifestItem>>>,
}

impl Manifest {
    #[instrument]
    pub fn open(path: impl AsRef<Path> + Debug) -> anyhow::Result<Self> {
        let file = FileStorage::open(path)?;

        let mut records = vec![];
        let mut buf = Bytes::from(file.read_to_end(0)?);
        while !buf.is_empty() {
            records.push(Arc::new(Record::decode_with_bytes(&mut buf)?));
        }

        Ok(Self { file, records })
    }

    pub fn add(&mut self, r: &Record<ManifestItem>) {
        self.records.push(Arc::new(r.clone()));
        self.file.write(&r.encode());
        self.file.sync();
    }

    pub fn num_of_records(&self) -> usize {
        self.records.len()
    }

    pub fn read_record(&self, record_idx: usize) -> anyhow::Result<Arc<Record<ManifestItem>>> {
        if record_idx >= self.num_of_records() {
            return Err(anyhow!(
                "index out of bound, blocks num: {}, record_idx: {}",
                self.num_of_records(),
                record_idx
            ));
        }

        Ok(self.records[record_idx].clone())
    }
}

/// `ManifestItem` 是元数据的一次变更
/// layout
/// ```text
/// +--------------------+------------------+------+
/// | record type(1byte) | data len(4bytes) | data |
/// +--------------------+------------------+------+
/// ```
#[derive(Copy, Clone, Debug)]
pub enum ManifestItem {
    /// 初始化（version)
    Init(i32),
    /// 新增 SST 文件 (level, sst_id)
    NewSst(u32, u32),
    /// 删除 SST 文件 (level, sst_id)
    DelSst(u32, u32),
    /// 新增 vSST 文件 (vsst_id)
    NewVSst(u32),
    /// 删除 vSST 文件 (vsst_id)
    DelVSst(u32),
    /// 更新最大 seq num
    MaxSeqNum(u64),
    /// 新旧 WAL 切换
    RotateWal,
}

impl ManifestItem {
    pub fn decode(data: &[u8]) -> anyhow::Result<Self> {
        let mut bytes = Bytes::copy_from_slice(data);
        Self::decode_with_bytes(&mut bytes)
    }

    #[inline]
    pub fn type_encode(&self) -> u8 {
        match self {
            ManifestItem::Init(_) => 0,
            ManifestItem::NewSst(_, _) => 1,
            ManifestItem::DelSst(_, _) => 2,
            ManifestItem::NewVSst(_) => 3,
            ManifestItem::DelVSst(_) => 4,
            ManifestItem::MaxSeqNum(_) => 5,
            ManifestItem::RotateWal => 6,
        }
    }

    pub fn put_content(&self, buf: &mut BytesMut) {
        match self {
            ManifestItem::NewSst(level, sst_id) => {
                buf.put_u32_le(*level);
                buf.put_u32_le(*sst_id)
            }
            ManifestItem::DelSst(level, sst_id) => {
                buf.put_u32_le(*level);
                buf.put_u32_le(*sst_id)
            }
            ManifestItem::MaxSeqNum(seq_num) => {
                buf.put_u64_le(*seq_num);
            }
            ManifestItem::Init(version) => {
                buf.put_i32_le(*version);
            }
            ManifestItem::RotateWal => {}
            ManifestItem::NewVSst(vsst_id) => buf.put_u32_le(*vsst_id),
            ManifestItem::DelVSst(vsst_id) => buf.put_u32_le(*vsst_id),
        }
    }

    #[inline]
    pub fn content_size(&self) -> usize {
        match self {
            ManifestItem::NewSst(_, _) => mem::size_of::<u32>() * 2,
            ManifestItem::DelSst(_, _) => mem::size_of::<u32>() * 2,
            ManifestItem::NewVSst(_) => mem::size_of::<u32>(),
            ManifestItem::DelVSst(_) => mem::size_of::<u32>(),
            ManifestItem::MaxSeqNum(_) => mem::size_of::<u64>(),
            ManifestItem::Init(_) => mem::size_of::<i32>(),
            ManifestItem::RotateWal => 0,
        }
    }
}

impl RecordItem for ManifestItem {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.type_encode());
        buf.put_u32_le(self.content_size() as u32);
        self.put_content(&mut buf);
        buf.freeze()
    }

    fn decode_with_bytes(bytes: &mut Bytes) -> anyhow::Result<Self> {
        let item_type = bytes.get_u8();
        let _data_len = bytes.get_u32_le();
        match item_type {
            0 => {
                let version = bytes.get_i32_le();
                Ok(ManifestItem::Init(version))
            }
            1 => {
                let level = bytes.get_u32_le();
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::NewSst(level, sst_id))
            }
            2 => {
                let level = bytes.get_u32_le();
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::DelSst(level, sst_id))
            }
            3 => {
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::NewVSst(sst_id))
            }
            4 => {
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::DelVSst(sst_id))
            }
            5 => {
                let seq_num = bytes.get_u64_le();
                Ok(ManifestItem::MaxSeqNum(seq_num))
            }
            6 => Ok(ManifestItem::RotateWal),
            _ => Err(anyhow!("unsupported record item type: {}", item_type)),
        }
    }

    #[inline]
    fn size(&self) -> usize {
        const HEADER_SIZE: usize = 5;
        HEADER_SIZE + self.content_size()
    }
}
