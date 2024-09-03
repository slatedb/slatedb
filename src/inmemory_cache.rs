use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub(crate) enum CachedBlock {
    Block(Arc<Block>),
    #[allow(dead_code)]
    Index(Arc<SsTableIndexOwned>),
    #[allow(dead_code)]
    Filter(Arc<BloomFilter>),
}

#[derive(Clone, Copy)]
pub enum CacheType {
    Moka,
}

#[derive(Clone, Copy)]
pub struct BlockCacheOptions {
    pub max_capacity: u64,
    pub cached_block_size: u32,
    pub time_to_live: Option<Duration>,
    pub time_to_idle: Option<Duration>,
    pub cache_type: CacheType,
}

impl Default for BlockCacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: 64 * 1024 * 1024, // 64MB default max capacity
            cached_block_size: 32,          // 32 bytes default
            time_to_live: None,
            time_to_idle: None,
            cache_type: CacheType::Moka, // Default to Moka cache
        }
    }
}

#[async_trait]
pub(crate) trait BlockCache: Send + Sync + 'static {
    async fn get(&self, key: (SsTableId, usize)) -> CachedBlockOption;
    async fn insert(&self, key: (SsTableId, usize), value: CachedBlock);
    #[allow(dead_code)]
    async fn remove(&self, key: (SsTableId, usize));
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

pub(crate) struct MokaCache {
    inner: moka::future::Cache<(SsTableId, usize), CachedBlock>,
}

impl MokaCache {
    pub fn new(options: BlockCacheOptions) -> Self {
        let mut builder = moka::future::Cache::builder()
            .weigher(move |_, _| return options.cached_block_size)
            .max_capacity(options.max_capacity);

        if let Some(ttl) = options.time_to_live {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = options.time_to_idle {
            builder = builder.time_to_idle(tti);
        }

        let cache = builder.build();

        Self { inner: cache }
    }
}

#[async_trait]
impl BlockCache for MokaCache {
    async fn get(&self, key: (SsTableId, usize)) -> CachedBlockOption {
        CachedBlockOption(self.inner.get(&key).await)
    }

    async fn insert(&self, key: (SsTableId, usize), value: CachedBlock) {
        self.inner.insert(key, value).await;
    }

    async fn remove(&self, key: (SsTableId, usize)) {
        self.inner.remove(&key).await;
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

/// Factory function to create the appropriate cache based on BlockCacheOptions
pub fn create_block_cache(options: Option<BlockCacheOptions>) -> Option<Arc<dyn BlockCache>> {
    if let Some(options) = options {
        match options.cache_type {
            CacheType::Moka => Some(Arc::new(MokaCache::new(options))),
        }
    } else {
        None
    }
}

/// wrapper around Option<CachedBlock> to provide helper functions
pub struct CachedBlockOption(Option<CachedBlock>);

impl CachedBlockOption {
    pub fn block(&self) -> Option<Arc<Block>> {
        match self {
            CachedBlockOption(Some(CachedBlock::Block(block))) => Some(block.clone()),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn sst_index(&self) -> Option<Arc<SsTableIndexOwned>> {
        match self {
            CachedBlockOption(Some(CachedBlock::Index(index))) => Some(index.clone()),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn bloom_filter(&self) -> Option<Arc<BloomFilter>> {
        match self {
            CachedBlockOption(Some(CachedBlock::Filter(filter))) => Some(filter.clone()),
            _ => None,
        }
    }
}
