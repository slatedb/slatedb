use crate::db_cache::{CachedBlock, CachedEntry, DbCache, SsTableId};
use async_trait::async_trait;
use std::time::Duration;

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
