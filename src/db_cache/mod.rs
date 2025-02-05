//! # DB Cache
//!
//! This module provides a pluggable caching solution for storing and retrieving
//! cached blocks, index and bloom filters associated with SSTable IDs.
//!
//! There are currently two built-in cache implementations:
//! - [Foyer](crate::db_cache::foyer::FoyerCache): Requires the `foyer` feature flag.
//! - [Moka](crate::db_cache::moka::MokaCache): Requires the `moka` feature flag. (Enabled by default)
//!
//! ## Usage
//!
//! To use the cache, you need to configure the [DbOptions](crate::config::DbOptions) with the desired cache implementation.

use std::sync::Arc;

use async_trait::async_trait;

use crate::db_cache::stats::DbCacheStats;
use crate::stats::StatRegistry;
use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

#[cfg(feature = "foyer")]
pub mod foyer;
#[cfg(feature = "moka")]
pub mod moka;

/// The default max capacity for the cache. (64MB)
pub const DEFAULT_MAX_CAPACITY: u64 = 64 * 1024 * 1024;

/// A trait for slatedb's block cache.
///
/// This trait defines the interface for a block cache,
/// which is used to store and retrieve cached blocks associated with SSTable IDs.
///
/// Example:
///
/// ```rust,no_run
/// use async_trait::async_trait;
/// use object_store::local::LocalFileSystem;
/// use slatedb::Db;
/// use slatedb::config::DbOptions;
/// use slatedb::db_cache::{DbCache, CachedEntry, CachedKey};
/// use std::collections::HashMap;
/// use std::sync::{Arc, Mutex};
///
/// struct MyCache {
///     inner: Mutex<MyCacheInner>,
/// }
///
/// struct MyCacheInner {
///     data: HashMap<CachedKey, CachedEntry>,
///     usage: u64,
///     capacity: u64
/// }
///
/// impl MyCache {
///     pub fn new(capacity: u64) -> Self {
///         Self {
///             inner: Mutex::new(
///                 MyCacheInner{
///                     data: HashMap::new(),
///                     usage: 0,
///                     capacity,
///                 }
///             )
///         }
///     }
/// }
///
/// #[async_trait]
/// impl DbCache for MyCache {
///     async fn get(&self, key: CachedKey) -> Option<CachedEntry> {
///         let guard = self.inner.lock().unwrap();
///         guard.data.get(&key).cloned()
///     }
///
///     async fn insert(&self, key: CachedKey, value: CachedEntry) {
///         let mut guard = self.inner.lock().unwrap();
///         guard.usage += value.size() as u64;
///         if let Some(v) = guard.data.insert(key, value) {
///             guard.usage -= v.size() as u64;
///         }
///     }
///
///     async fn remove(&self, key: CachedKey) {
///         let mut guard = self.inner.lock().unwrap();
///         if let Some(v) = guard.data.remove(&key) {
///             guard.usage -= v.size() as u64;
///         }
///     }
///
///     fn entry_count(&self) -> u64 {
///         let mut guard = self.inner.lock().unwrap();
///         guard.capacity
///     }
/// }
///
/// #[::tokio::main]
/// async fn main() {
///     use object_store::path::Path;
///     use slatedb::db_cache::DbCacheWrapper;
///     let object_store = Arc::new(LocalFileSystem::new());
///     let cache = Arc::new(MyCache::new(128u64 * 1024 * 1024));
///     let options = DbOptions {
///         block_cache: Some(cache),
///         ..Default::default()
///     };
///     let path = Path::from("/path/to/db");
///     let db = Db::open_with_opts(path, options, object_store).await;
/// }
/// ```
#[async_trait]
pub trait DbCache: Send + Sync {
    async fn get(&self, key: CachedKey) -> Option<CachedEntry>;
    async fn insert(&self, key: CachedKey, value: CachedEntry);
    #[allow(dead_code)]
    async fn remove(&self, key: CachedKey);
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

/// A key used to identify a cached entry.
///
/// The key is a tuple of an SSTable ID and a block ID.
/// The tuple is private to this module, so the implementation details
/// of the cache are not exposed publicly.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CachedKey(SsTableId, u64);

impl From<(SsTableId, u64)> for CachedKey {
    fn from((sst_id, block_id): (SsTableId, u64)) -> Self {
        Self(sst_id, block_id)
    }
}

#[non_exhaustive]
#[derive(Clone)]
enum CachedItem {
    Block(Arc<Block>),
    SsTableIndex(Arc<SsTableIndexOwned>),
    BloomFilter(Arc<BloomFilter>),
}

/// A cached entry stored in the cache.
///
/// The entry stores data in an internal enum that represents the type of cached item.
/// The internal types of the entries that are stored in the cache are private,
/// so the implementation details of the cache are not exposed publicly.
#[derive(Clone)]
pub struct CachedEntry {
    item: CachedItem,
}

impl CachedEntry {
    /// Create a new `CachedEntry` with the given block.
    pub(crate) fn with_block(block: Arc<Block>) -> Self {
        Self {
            item: CachedItem::Block(block),
        }
    }

    /// Create a new `CachedEntry` with the given SSTable index.
    pub(crate) fn with_sst_index(sst_index: Arc<SsTableIndexOwned>) -> Self {
        Self {
            item: CachedItem::SsTableIndex(sst_index),
        }
    }

    /// Create a new `CachedEntry` with the given bloom filter.
    pub(crate) fn with_bloom_filter(bloom_filter: Arc<BloomFilter>) -> Self {
        Self {
            item: CachedItem::BloomFilter(bloom_filter),
        }
    }

    pub(crate) fn block(&self) -> Option<Arc<Block>> {
        match &self.item {
            CachedItem::Block(block) => Some(block.clone()),
            _ => None,
        }
    }

    pub(crate) fn sst_index(&self) -> Option<Arc<SsTableIndexOwned>> {
        match &self.item {
            CachedItem::SsTableIndex(sst_index) => Some(sst_index.clone()),
            _ => None,
        }
    }

    pub(crate) fn bloom_filter(&self) -> Option<Arc<BloomFilter>> {
        match &self.item {
            CachedItem::BloomFilter(bloom_filter) => Some(bloom_filter.clone()),
            _ => None,
        }
    }

    /// Returns the size of the cached entry in bytes.
    ///
    /// This method is public to allow external cache implementations
    /// to use it to implement custom weighers.
    pub fn size(&self) -> usize {
        match &self.item {
            CachedItem::Block(block) => block.size(),
            CachedItem::SsTableIndex(sst_index) => sst_index.size(),
            CachedItem::BloomFilter(bloom_filter) => bloom_filter.size(),
        }
    }
}

pub struct DbCacheWrapper {
    stats: DbCacheStats,
    cache: Arc<dyn DbCache>,
}

impl DbCacheWrapper {
    pub fn new(cache: Arc<dyn DbCache>, stats_registry: &StatRegistry) -> Self {
        Self {
            stats: DbCacheStats::new(stats_registry),
            cache,
        }
    }

    pub(crate) async fn get_filter(&self, key: CachedKey) -> Option<CachedEntry> {
        let result = self.cache.get(key).await;
        if result.is_some() {
            self.stats.filter_hit.inc();
        } else {
            self.stats.filter_miss.inc();
        }
        result
    }

    pub(crate) async fn get_index(&self, key: CachedKey) -> Option<CachedEntry> {
        let result = self.cache.get(key).await;
        if result.is_some() {
            self.stats.index_hit.inc();
        } else {
            self.stats.index_miss.inc();
        }
        result
    }

    pub(crate) async fn get_data_block(&self, key: CachedKey) -> Option<CachedEntry> {
        let result = self.cache.get(key).await;
        if result.is_some() {
            self.stats.data_block_hit.inc();
        } else {
            self.stats.data_block_miss.inc();
        }
        result
    }

    pub(crate) async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.cache.insert(key, value).await
    }

    #[allow(dead_code)]
    pub(crate) async fn remove(&self, key: CachedKey) {
        self.cache.remove(key).await
    }
}

pub mod stats {
    use crate::stats::{Counter, StatRegistry};
    use std::sync::Arc;

    macro_rules! dbcache_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("dbcache", $suffix)
        };
    }

    pub const DB_CACHE_FILTER_HIT: &str = dbcache_stat_name!("filter_hit");
    pub const DB_CACHE_FILTER_MISS: &str = dbcache_stat_name!("filter_miss");
    pub const DB_CACHE_INDEX_HIT: &str = dbcache_stat_name!("index_hit");
    pub const DB_CACHE_INDEX_MISS: &str = dbcache_stat_name!("index_miss");
    pub const DB_CACHE_DATA_BLOCK_HIT: &str = dbcache_stat_name!("data_block_hit");
    pub const DB_CACHE_DATA_BLOCK_MISS: &str = dbcache_stat_name!("data_block_miss");

    pub(super) struct DbCacheStats {
        pub(super) filter_hit: Arc<Counter>,
        pub(super) filter_miss: Arc<Counter>,
        pub(super) index_hit: Arc<Counter>,
        pub(super) index_miss: Arc<Counter>,
        pub(super) data_block_hit: Arc<Counter>,
        pub(super) data_block_miss: Arc<Counter>,
    }

    impl DbCacheStats {
        pub(super) fn new(registry: &StatRegistry) -> Self {
            let stats = Self {
                filter_hit: Arc::new(Counter::default()),
                filter_miss: Arc::new(Counter::default()),
                index_hit: Arc::new(Counter::default()),
                index_miss: Arc::new(Counter::default()),
                data_block_hit: Arc::new(Counter::default()),
                data_block_miss: Arc::new(Counter::default()),
            };
            registry.register(DB_CACHE_FILTER_HIT, stats.filter_hit.clone());
            registry.register(DB_CACHE_FILTER_MISS, stats.filter_miss.clone());
            registry.register(DB_CACHE_INDEX_HIT, stats.index_hit.clone());
            registry.register(DB_CACHE_INDEX_MISS, stats.index_miss.clone());
            registry.register(DB_CACHE_DATA_BLOCK_HIT, stats.data_block_hit.clone());
            registry.register(DB_CACHE_DATA_BLOCK_MISS, stats.data_block_miss.clone());
            stats
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db_cache::{CachedEntry, CachedKey, DbCache, DbCacheWrapper};
    use crate::db_state::SsTableId;
    use crate::sst::SsTableFormat;
    use crate::stats::{ReadableStat, StatRegistry};
    use crate::test_utils::{build_test_sst, SstData};
    use async_trait::async_trait;
    use rstest::{fixture, rstest};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use ulid::Ulid;

    const SST_ID: SsTableId = SsTableId::Compacted(Ulid::from_parts(0u64, 0u128));

    #[rstest]
    #[tokio::test]
    async fn test_should_count_filter_hits(
        cache: DbCacheWrapper,
        sst_format: SsTableFormat,
        sst: SstData,
    ) {
        // given:
        let filter = sst_format
            .read_filter_raw(&sst.info, &sst.data)
            .unwrap()
            .unwrap();
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_bloom_filter(filter))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_filter(key.clone()).await;

            // then:
            assert_eq!(0, cache.stats.filter_miss.get());
            assert_eq!(i, cache.stats.filter_hit.get());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_filter_misses(cache: DbCacheWrapper) {
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_filter(key.clone()).await;

            // then:
            assert_eq!(i, cache.stats.filter_miss.get());
            assert_eq!(0, cache.stats.filter_hit.get());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_index_hits(
        cache: DbCacheWrapper,
        sst_format: SsTableFormat,
        sst: SstData,
    ) {
        // given:
        let index = sst_format.read_index_raw(&sst.info, &sst.data).unwrap();
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_sst_index(Arc::new(index)))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_index(key.clone()).await;

            // then:
            assert_eq!(0, cache.stats.index_miss.get());
            assert_eq!(i, cache.stats.index_hit.get());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_index_misses(cache: DbCacheWrapper) {
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_index(key.clone()).await;

            // then:
            assert_eq!(i, cache.stats.index_miss.get());
            assert_eq!(0, cache.stats.index_hit.get());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_data_block_hits(
        cache: DbCacheWrapper,
        sst_format: SsTableFormat,
        sst: SstData,
    ) {
        // given:
        let index = sst_format.read_index_raw(&sst.info, &sst.data).unwrap();
        let block = sst_format
            .read_block_raw(&sst.info, &index, 0, &sst.data)
            .unwrap();
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_block(Arc::new(block)))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_data_block(key.clone()).await;

            // then:
            assert_eq!(0, cache.stats.data_block_miss.get());
            assert_eq!(i, cache.stats.data_block_hit.get());
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_data_block_misses(cache: DbCacheWrapper) {
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_data_block(key.clone()).await;

            // then:
            assert_eq!(i, cache.stats.data_block_miss.get());
            assert_eq!(0, cache.stats.data_block_hit.get());
        }
    }

    #[fixture]
    fn cache() -> DbCacheWrapper {
        let registry = StatRegistry::new();
        DbCacheWrapper::new(Arc::new(TestCache::new()), &registry)
    }

    #[fixture]
    fn sst_format() -> SsTableFormat {
        SsTableFormat {
            block_size: 128,
            ..SsTableFormat::default()
        }
    }

    #[fixture]
    fn sst(sst_format: SsTableFormat) -> SstData {
        build_test_sst(&sst_format, 1)
    }

    struct TestCache {
        items: Mutex<HashMap<CachedKey, CachedEntry>>,
    }

    impl TestCache {
        fn new() -> Self {
            Self {
                items: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl DbCache for TestCache {
        async fn get(&self, key: CachedKey) -> Option<CachedEntry> {
            let guard = self.items.lock().unwrap();
            guard.get(&key).cloned()
        }

        async fn insert(&self, key: CachedKey, value: CachedEntry) {
            let mut guard = self.items.lock().unwrap();
            guard.insert(key, value);
        }

        async fn remove(&self, key: CachedKey) {
            let mut guard = self.items.lock().unwrap();
            guard.remove(&key);
        }

        fn entry_count(&self) -> u64 {
            let guard = self.items.lock().unwrap();
            guard.iter().count() as u64
        }
    }
}
