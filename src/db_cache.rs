use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

/// The cached block types.
#[derive(Clone)]
pub enum CachedBlock {
    Block(Arc<Block>),
    Index(Arc<SsTableIndexOwned>),
    Filter(Arc<BloomFilter>),
}

/// The options for the Moka cache.
#[derive(Clone, Copy, Debug)]
pub struct MokaCacheOptions {
    pub max_capacity: u64,
    pub cached_block_size: u32,
    pub time_to_live: Option<Duration>,
    pub time_to_idle: Option<Duration>,
}

impl Default for MokaCacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: 64 * 1024 * 1024, // 64MB default max capacity,
            cached_block_size: 32,          // 32 bytes default
            time_to_live: None,
            time_to_idle: None,
        }
    }
}

/// The options for the Foyer cache.
#[derive(Clone, Copy, Debug)]
pub struct FoyerCacheOptions {
    pub max_capacity: u64,
    pub cached_block_size: u32,
}

impl Default for FoyerCacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: 64 * 1024 * 1024, // 64MB default max capacity,
            cached_block_size: 32,
        }
    }
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

/// A cache implementation using the Moka library.
///
/// This struct wraps a Moka cache, providing an in-memory caching solution
/// for storing and retrieving cached blocks associated with SSTable IDs.
///
/// # Fields
///
/// * `inner` - The underlying Moka cache instance, which maps `(SsTableId, u64)`
///   keys to `CachedBlock` values.
///
/// # Notes
///
/// The cache is configured based on the provided `InMemoryCacheOptions`,
/// including settings for capacity, time-to-live (TTL), and time-to-idle (TTI).
/// It uses a custom weigher to account for the size of cached blocks.
pub struct MokaCache {
    inner: moka::future::Cache<(SsTableId, u64), CachedBlock>,
}

impl MokaCache {
    pub fn new(options: MokaCacheOptions) -> Self {
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
impl DbCache for MokaCache {
    async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry> {
        self.inner
            .get(&key)
            .await
            .map(convert_moka_cache_to_cached_entry)
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

fn convert_moka_cache_to_cached_entry(entry: CachedBlock) -> CachedEntry {
    let mut cached_entry = CachedEntry::default();

    match entry {
        CachedBlock::Block(block) => cached_entry.block = Some(block),
        CachedBlock::Index(index) => cached_entry.sst_index = Some(index),
        CachedBlock::Filter(filter) => cached_entry.bloom_filter = Some(filter),
    }

    cached_entry
}

/// A cache implementation using the Foyer library.
///
/// This struct wraps a Foyer cache, providing an in-memory caching solution
/// for storing and retrieving cached blocks associated with SSTable IDs.
///
/// # Fields
///
/// * `inner` - The underlying Foyer cache instance, which maps `(SsTableId, u64)`
///   keys to `CachedBlock` values.
///
/// # Notes
///
/// The cache is configured based on the provided `InMemoryCacheOptions`,
/// including settings for capacity, time-to-live (TTL), and time-to-idle (TTI).
/// It uses a custom weigher to account for the size of cached blocks.
pub struct FoyerCache {
    inner: foyer::Cache<(SsTableId, u64), CachedBlock>,
}

impl FoyerCache {
    pub fn new(options: FoyerCacheOptions) -> Self {
        let builder = foyer::CacheBuilder::new(options.max_capacity as _)
            .with_weighter(move |_, _| options.cached_block_size as _);

        let cache = builder.build();

        Self { inner: cache }
    }
}

#[async_trait]
impl DbCache for FoyerCache {
    async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry> {
        self.inner
            .get(&key)
            .map(convert_foyer_cache_to_cached_entry)
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

fn convert_foyer_cache_to_cached_entry(
    entry: foyer::CacheEntry<(SsTableId, u64), CachedBlock>,
) -> CachedEntry {
    let mut cached_entry = CachedEntry::default();

    match entry.value() {
        CachedBlock::Block(block) => cached_entry.block = Some(block.clone()),
        CachedBlock::Index(index) => cached_entry.sst_index = Some(index.clone()),
        CachedBlock::Filter(filter) => cached_entry.bloom_filter = Some(filter.clone()),
    }

    cached_entry
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
