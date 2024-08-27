use std::sync::Arc;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum CachedBlock {
    Block(Arc<Block>),
    Index(Arc<SsTableIndexOwned>),
    Filter(Arc<BloomFilter>),
}

pub(crate) type BlockCache = moka::future::Cache<(SsTableId, usize), CachedBlock>;

impl From<CachedBlock> for Option<Arc<Block>> {
    fn from(val: CachedBlock) -> Self {
        match val {
            CachedBlock::Block(block) => Some(block),
            _ => None,
        }
    }
}
