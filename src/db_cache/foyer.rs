use crate::db_cache::{CachedBlock, CachedEntry, DbCache, SsTableId};
use async_trait::async_trait;

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
