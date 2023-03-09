use crate::block::builder::{Block, BlockBuilder};
use crate::block::iterator::BlockIterator;
use crate::entry::{Entry, EntryBuilder};
use crate::OpType;
use bytes::Bytes;
use rand::distributions::{Alphanumeric, DistString};
use rand::{thread_rng, Rng};
use std::sync::Arc;

pub fn rand_gen_entries(num: usize) -> Vec<Entry> {
    let mut ret = Vec::with_capacity(num);

    let rand_str = || -> String {
        Alphanumeric.sample_string(&mut thread_rng(), thread_rng().gen_range(1..100))
    };

    for _ in 0..num {
        ret.push(
            EntryBuilder::new()
                .op_type(OpType::Get)
                .key_value(Bytes::from(rand_str()), Bytes::from(rand_str()))
                .build(),
        )
    }

    ret
}

fn rand_gen_block() -> (Block, Vec<Entry>) {
    let mut builder = BlockBuilder::new();
    let num = 10;
    let entries = rand_gen_entries(num);

    entries.iter().for_each(|e| assert!(builder.add(e)));
    (builder.build(), entries)
}

#[test]
fn test_block_builder() {
    let (block, entries) = rand_gen_block();

    let mut offsets: Vec<u16> = Vec::new();
    let mut off: u16 = 0;
    for i in &entries {
        offsets.push(off);
        off += i.size() as u16;
    }
    assert_eq!(block.offsets, offsets);
}

#[test]
fn test_block_encode() {
    let (block, _) = rand_gen_block();
    let block_encode = block.encode();
    let block2 = Block::decode(&block_encode[..]);
    assert_eq!(block, block2);
}

#[test]
fn test_block_iterator() {
    let (block, entries) = rand_gen_block();
    let block = Arc::new(block);
    let mut iter = BlockIterator::create_and_seek_to_first(block);

    entries.iter().for_each(|e| {
        assert_eq!(&e.key[..], iter.key());
        iter.next();
    });
}
