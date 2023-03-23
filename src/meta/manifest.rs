use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};


use crate::record::{Record, RecordItem};
use crate::storage::file::FileStorage;

#[derive(Debug)]
pub struct Manifest {
    file: FileStorage,
    records: Vec<Arc<Record<ManifestItem>>>,
}

impl Manifest {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
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
    NewSst(usize, usize),
    /// 删除 SST 文件 (level, sst_id)
    DelSst(usize, usize),
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
    pub fn content_size(&self) -> usize {
        const HEADER_SIZE: usize = 5;
        match self {
            ManifestItem::NewSst(_, _) => mem::size_of::<u32>() * 2,
            ManifestItem::DelSst(_, _) => mem::size_of::<u32>() * 2,
            ManifestItem::MaxSeqNum(_) => mem::size_of::<u64>(),
            ManifestItem::Init(_) => mem::size_of::<i32>(),
            ManifestItem::RotateWal => 0,
        }
    }
}

impl RecordItem for ManifestItem {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            ManifestItem::NewSst(level, sst_id) => {
                buf.put_u8(0);
                buf.put_u32_le(self.content_size() as u32);
                buf.put_u32_le(*level as u32);
                buf.put_u32_le(*sst_id as u32)
            }
            ManifestItem::DelSst(level, sst_id) => {
                buf.put_u8(1);
                buf.put_u32_le(self.content_size() as u32);
                buf.put_u32_le(*level as u32);
                buf.put_u32_le(*sst_id as u32)
            }
            ManifestItem::MaxSeqNum(seq_num) => {
                buf.put_u8(2);
                buf.put_u32_le(self.content_size() as u32);
                buf.put_u64_le(*seq_num);
            }
            ManifestItem::Init(version) => {
                buf.put_u8(3);
                buf.put_u32_le(self.content_size() as u32);
                buf.put_i32_le(*version);
            }
            ManifestItem::RotateWal => {
                buf.put_u8(4);
                buf.put_u32_le(0);
            }
        }
        buf.freeze()
    }

    fn decode_with_bytes(bytes: &mut Bytes) -> anyhow::Result<Self> {
        let item_type = bytes.get_u8();
        let _data_len = bytes.get_u32_le();
        match item_type {
            0 => {
                let level = bytes.get_u32_le();
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::NewSst(level as usize, sst_id as usize))
            }
            1 => {
                let level = bytes.get_u32_le();
                let sst_id = bytes.get_u32_le();
                Ok(ManifestItem::DelSst(level as usize, sst_id as usize))
            }
            2 => {
                let seq_num = bytes.get_u64_le();
                Ok(ManifestItem::MaxSeqNum(seq_num))
            }
            3 => {
                let version = bytes.get_i32_le();
                Ok(ManifestItem::Init(version))
            }
            4 => {
                Ok(ManifestItem::RotateWal)
            }
            _ => Err(anyhow!("unsupported record item type: {}", item_type)),
        }
    }

    #[inline]
    fn size(&self) -> usize {
        const HEADER_SIZE: usize = 5;
        HEADER_SIZE + self.content_size()
    }
}
