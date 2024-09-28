use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

/// The default max capacity for the cache. (64MB)
pub const DEFAULT_MAX_CAPACITY: u64 = 64 * 1024 * 1024;
/// The default cached block size for the cache. (32 bytes)
pub const DEFAULT_CACHED_BLOCK_SIZE: u32 = 32;

pub mod foyer;
pub mod moka;

/// The cached block types.
#[derive(Clone)]
pub enum CachedBlock {
    Block(Arc<Block>),
    Index(Arc<SsTableIndexOwned>),
    Filter(Arc<BloomFilter>),
}

/// A trait for in-memory caches.
///
/// This trait defines the interface for an in-memory cache,
/// which is used to store and retrieve cached blocks associated with SSTable IDs.
#[async_trait]
pub trait DbCache: Send + Sync + 'static {
    async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry>;
    async fn insert(&self, key: (SsTableId, u64), value: CachedBlock);
    #[allow(dead_code)]
    async fn remove(&self, key: (SsTableId, u64));
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

/// A cached entry from the cache.
#[derive(Clone, Default)]
pub struct CachedEntry {
    block: Option<Arc<Block>>,
    sst_index: Option<Arc<SsTableIndexOwned>>,
    bloom_filter: Option<Arc<BloomFilter>>,
}

impl CachedEntry {
    pub fn block(&self) -> Option<Arc<Block>> {
        self.block.clone()
    }

    pub fn sst_index(&self) -> Option<Arc<SsTableIndexOwned>> {
        self.sst_index.clone()
    }

    pub fn bloom_filter(&self) -> Option<Arc<BloomFilter>> {
        self.bloom_filter.clone()
    }
}
