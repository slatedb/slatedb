use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

/// The default max capacity for the cache. (64MB)
pub const DEFAULT_MAX_CAPACITY: u64 = 64 * 1024 * 1024;
/// The default cached block size for the cache. (32 bytes)
pub const DEFAULT_CACHED_BLOCK_SIZE: u32 = 32;

#[cfg(feature = "foyer")]
pub mod foyer;
#[cfg(feature = "moka")]
pub mod moka;

/// A trait for in-memory caches.
///
/// This trait defines the interface for an in-memory cache,
/// which is used to store and retrieve cached blocks associated with SSTable IDs.
#[async_trait]
pub trait DbCache: Send + Sync {
    async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry>;
    async fn insert(&self, key: (SsTableId, u64), value: CachedEntry);
    #[allow(dead_code)]
    async fn remove(&self, key: (SsTableId, u64));
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

/// A cached entry stored in the cache.
#[derive(Clone, Default)]
pub struct CachedEntry {
    block: Option<Arc<Block>>,
    sst_index: Option<Arc<SsTableIndexOwned>>,
    bloom_filter: Option<Arc<BloomFilter>>,
}

impl CachedEntry {
    /// Create a new `CachedEntry` with the given block.
    pub(crate) fn with_block(self, block: Arc<Block>) -> Self {
        Self {
            block: Some(block),
            ..self
        }
    }

    /// Create a new `CachedEntry` with the given SSTable index.
    pub(crate) fn with_sst_index(self, sst_index: Arc<SsTableIndexOwned>) -> Self {
        Self {
            sst_index: Some(sst_index),
            ..self
        }
    }

    /// Create a new `CachedEntry` with the given bloom filter.
    pub(crate) fn with_bloom_filter(self, bloom_filter: Arc<BloomFilter>) -> Self {
        Self {
            bloom_filter: Some(bloom_filter),
            ..self
        }
    }

    pub(crate) fn block(&self) -> Option<Arc<Block>> {
        self.block.clone()
    }

    pub(crate) fn sst_index(&self) -> Option<Arc<SsTableIndexOwned>> {
        self.sst_index.clone()
    }

    pub(crate) fn bloom_filter(&self) -> Option<Arc<BloomFilter>> {
        self.bloom_filter.clone()
    }
}
