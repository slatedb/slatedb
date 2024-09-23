use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

#[derive(Clone)]
pub(crate) enum CachedBlock {
    Block(Arc<Block>),
    Index(Arc<SsTableIndexOwned>),
    Filter(Arc<BloomFilter>),
}

#[derive(Clone, Copy)]
pub enum CacheType {
    Moka,
    Foyer,
}

#[derive(Clone, Copy)]
pub struct InMemoryCacheOptions {
    pub max_capacity: u64,
    pub cached_block_size: u32,
    pub time_to_live: Option<Duration>,
    pub time_to_idle: Option<Duration>,
    pub cache_type: CacheType,
}

impl Default for InMemoryCacheOptions {
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
pub(crate) trait InMemoryCache: Send + Sync + 'static {
    async fn get(&self, key: (SsTableId, u64)) -> CachedBlockOption;
    async fn insert(&self, key: (SsTableId, u64), value: CachedBlock);
    #[allow(dead_code)]
    async fn remove(&self, key: (SsTableId, u64));
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

pub(crate) struct MokaCache {
    inner: moka::future::Cache<(SsTableId, u64), CachedBlock>,
}

impl MokaCache {
    pub fn new(options: InMemoryCacheOptions) -> Self {
        let mut builder = moka::future::Cache::builder()
            .weigher(move |_, _| options.cached_block_size)
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
impl InMemoryCache for MokaCache {
    async fn get(&self, key: (SsTableId, u64)) -> CachedBlockOption {
        CachedBlockOption::Moka(self.inner.get(&key).await)
    }

    async fn insert(&self, key: (SsTableId, u64), value: CachedBlock) {
        self.inner.insert(key, value).await;
    }

    async fn remove(&self, key: (SsTableId, u64)) {
        self.inner.remove(&key).await;
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}

pub(crate) struct FoyerCache {
    inner: foyer::Cache<(SsTableId, u64), CachedBlock>,
}

impl FoyerCache {
    pub fn new(options: InMemoryCacheOptions) -> Self {
        let builder = foyer::CacheBuilder::new(options.max_capacity as _)
            .with_weighter(move |_, _| options.cached_block_size as _);

        if options.time_to_live.is_some() {
            unimplemented!("ttl is not supported by foyer yet");
        }

        if options.time_to_idle.is_some() {
            unimplemented!("tti is not supported by foyer yet");
        }

        let cache = builder.build();

        Self { inner: cache }
    }
}

#[async_trait]
impl InMemoryCache for FoyerCache {
    async fn get(&self, key: (SsTableId, u64)) -> CachedBlockOption {
        CachedBlockOption::Foyer(self.inner.get(&key))
    }

    async fn insert(&self, key: (SsTableId, u64), value: CachedBlock) {
        self.inner.insert(key, value);
    }

    async fn remove(&self, key: (SsTableId, u64)) {
        self.inner.remove(&key);
    }

    fn entry_count(&self) -> u64 {
        self.inner.usage() as _
    }
}

/// Factory function to create the appropriate cache based on InMemoryCacheOptions
pub(crate) fn create_block_cache(
    options: Option<InMemoryCacheOptions>,
) -> Option<Arc<dyn InMemoryCache>> {
    if let Some(options) = options {
        match options.cache_type {
            CacheType::Moka => Some(Arc::new(MokaCache::new(options))),
            CacheType::Foyer => Some(Arc::new(FoyerCache::new(options))),
        }
    } else {
        None
    }
}

/// wrapper around Option<CachedBlock> to provide helper functions
pub(crate) enum CachedBlockOption {
    Moka(Option<CachedBlock>),
    Foyer(Option<foyer::CacheEntry<(SsTableId, u64), CachedBlock>>),
}

impl CachedBlockOption {
    pub(crate) fn block(&self) -> Option<Arc<Block>> {
        match self {
            CachedBlockOption::Moka(Some(CachedBlock::Block(block))) => Some(block.clone()),
            CachedBlockOption::Foyer(Some(entry)) => match entry.value() {
                CachedBlock::Block(block) => Some(block.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn sst_index(&self) -> Option<Arc<SsTableIndexOwned>> {
        match self {
            CachedBlockOption::Moka(Some(CachedBlock::Index(index))) => Some(index.clone()),
            CachedBlockOption::Foyer(Some(entry)) => match entry.value() {
                CachedBlock::Index(index) => Some(index.clone()),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn bloom_filter(&self) -> Option<Arc<BloomFilter>> {
        match self {
            CachedBlockOption::Moka(Some(CachedBlock::Filter(filter))) => Some(filter.clone()),
            CachedBlockOption::Foyer(Some(entry)) => match entry.value() {
                CachedBlock::Filter(filter) => Some(filter.clone()),
                _ => None,
            },
            _ => None,
        }
    }
}
