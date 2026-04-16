//! # DB Cache
//!
//! This module provides a pluggable caching solution for storing and retrieving
//! cached blocks, index and filters associated with SSTable IDs.
//!
//! There are currently two built-in cache implementations:
//! - [Foyer](crate::db_cache::foyer::FoyerCache): Requires the `foyer` feature flag. (Enabled by default)
//! - [Moka](crate::db_cache::moka::MokaCache): Requires the `moka` feature flag.
//!
//! ## Usage
//!
//! To use the cache, you need to configure the [DbOptions](crate::config::DbOptions) with the desired cache implementation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use log::{debug, error, trace};
use parking_lot::Mutex;

use crate::db_cache::stats::DbCacheStats;
use crate::db_state::SsTableId;
use crate::filter_policy::NamedFilter;
use crate::flatbuffer_types::SsTableIndexOwned;
use crate::format::block::Block;
use crate::sst_stats::SstStats;
use slatedb_common::clock::SystemClock;
use slatedb_common::metrics::MetricsRecorderHelper;

#[cfg(feature = "foyer")]
pub mod foyer;
#[cfg(feature = "foyer")]
pub mod foyer_hybrid;
#[cfg(feature = "moka")]
pub mod moka;
mod serde;

/// The default max capacity for the user default cache. (64MB)
pub const DEFAULT_MAX_CAPACITY: u64 = 64 * 1024 * 1024;
pub const DEFAULT_BLOCK_CACHE_CAPACITY: u64 = 512 * 1024 * 1024;
pub const DEFAULT_META_CACHE_CAPACITY: u64 = 128 * 1024 * 1024;

/// Atomic counter to generate unique scope IDs for `DbCacheWrapper` instances.
static NEXT_CACHE_SCOPE_ID: AtomicU64 = AtomicU64::new(0);

/// A trait for slatedb's in-memory cache.
///
/// This trait defines the interface for an in-memory cache,
/// which is used to store and retrieve cached blocks, indices and filters
/// associated with SSTable IDs.
///
/// Example:
///
/// ```
/// use async_trait::async_trait;
/// use slatedb::{Db, Error};
/// use slatedb::db_cache::{DbCache, CachedEntry, CachedKey};
/// use slatedb::object_store::memory::InMemory;
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
///     async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, Error> {
///         let guard = self.inner.lock().unwrap();
///         Ok(guard.data.get(key).cloned())
///     }
///
///     async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, Error> {
///         let guard = self.inner.lock().unwrap();
///         Ok(guard.data.get(key).cloned())
///     }
///
///     async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, Error> {
///         let guard = self.inner.lock().unwrap();
///         Ok(guard.data.get(key).cloned())
///     }
///
///     async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, Error> {
///         let guard = self.inner.lock().unwrap();
///         Ok(guard.data.get(key).cloned())
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
///     async fn remove(&self, key: &CachedKey) {
///         let mut guard = self.inner.lock().unwrap();
///         if let Some(v) = guard.data.remove(key) {
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
///     let object_store = Arc::new(InMemory::new());
///     let cache = Arc::new(MyCache::new(128u64 * 1024 * 1024));
///     let db = Db::builder("/path/to/db", object_store)
///         .with_db_cache(cache)
///         .build()
///         .await;
/// }
/// ```
#[async_trait]
pub trait DbCache: Send + Sync {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error>;
    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error>;
    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error>;
    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error>;
    async fn insert(&self, key: CachedKey, value: CachedEntry);
    #[allow(dead_code)]
    async fn remove(&self, key: &CachedKey);
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;

    /// Gracefully close the cache, flushing any in-memory state to disk.
    ///
    /// Implementations backed by hybrid (memory + disk) caches should use
    /// this to ensure cached entries survive process restarts. The default
    /// implementation is a no-op.
    async fn close(&self) -> Result<(), crate::Error> {
        Ok(())
    }
}

/// A key used to identify a cached entry.
///
/// The key is composed of a scope ID (set per [`DbCacheWrapper`] instance), an SSTable ID,
/// and a block ID. The fields are private to this module, so the implementation details of the
/// cache are not exposed publicly.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CachedKey {
    /// Scope identifier set per `DbCacheWrapper`. This ensures that multiple `Db` instances
    /// sharing the same underlying cache do not collide on WAL or compacted file entries.
    /// Scope `0` is reserved for legacy keys created before scoping existed; new wrappers
    /// always receive unique scope IDs starting at `1`.
    pub(crate) scope_id: u64,
    pub(crate) sst_id: SsTableId,
    pub(crate) block_id: u64,
}

impl CachedKey {
    fn with_scope(&self, scope_id: u64) -> Self {
        Self {
            scope_id,
            sst_id: self.sst_id,
            block_id: self.block_id,
        }
    }
}

impl From<(SsTableId, u64)> for CachedKey {
    fn from((sst_id, block_id): (SsTableId, u64)) -> Self {
        Self {
            scope_id: 0,
            sst_id,
            block_id,
        }
    }
}

/// A filter cached in its on-disk byte form, paired with the name of the
/// policy that produced it. Only produced by disk-cache deserialization
/// (`db_cache::serde`), which has no access to the configured filter
/// policies; converted to a [`NamedFilter`] by `TableStore::read_filters`
/// on the first hit after deserialization.
#[derive(Clone)]
pub(crate) struct EncodedCachedFilter {
    pub(crate) name: String,
    pub(crate) data: bytes::Bytes,
}

#[non_exhaustive]
#[derive(Clone)]
enum CachedItem {
    Block(Arc<Block>),
    SsTableIndex(Arc<SsTableIndexOwned>),
    Filters(Arc<[NamedFilter]>),
    EncodedFilters(Arc<[EncodedCachedFilter]>),
    SstStats(Arc<SstStats>),
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

    /// Create a new `CachedEntry` with the given decoded filters.
    pub(crate) fn with_filters(filters: Arc<[NamedFilter]>) -> Self {
        Self {
            item: CachedItem::Filters(filters),
        }
    }

    /// Create a new `CachedEntry` with the given SST stats.
    pub(crate) fn with_sst_stats(stats: Arc<SstStats>) -> Self {
        Self {
            item: CachedItem::SstStats(stats),
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

    pub(crate) fn filters(&self) -> Option<Arc<[NamedFilter]>> {
        match &self.item {
            CachedItem::Filters(filters) => Some(filters.clone()),
            _ => None,
        }
    }

    pub(crate) fn encoded_filters(&self) -> Option<Arc<[EncodedCachedFilter]>> {
        match &self.item {
            CachedItem::EncodedFilters(filters) => Some(filters.clone()),
            _ => None,
        }
    }

    pub(crate) fn sst_stats(&self) -> Option<Arc<SstStats>> {
        match &self.item {
            CachedItem::SstStats(stats) => Some(stats.clone()),
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
            CachedItem::Filters(filters) => filters.iter().map(|nf| nf.filter.size()).sum(),
            CachedItem::EncodedFilters(filters) => filters.iter().map(|ef| ef.data.len()).sum(),
            CachedItem::SstStats(stats) => stats.size(),
        }
    }

    pub fn clamp_allocated_size(&self) -> Self {
        match &self.item {
            CachedItem::Block(block) => Self::with_block(Arc::new(block.clamp_allocated_size())),
            CachedItem::SsTableIndex(sst_index) => {
                Self::with_sst_index(Arc::new(sst_index.clamp_allocated_size()))
            }
            CachedItem::Filters(filters) => Self::with_filters(
                filters
                    .iter()
                    .map(|nf| NamedFilter {
                        name: nf.name.clone(),
                        filter: nf.filter.clamp_allocated_size(),
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            CachedItem::EncodedFilters(filters) => Self {
                item: CachedItem::EncodedFilters(filters.clone()),
            },
            CachedItem::SstStats(stats) => {
                Self::with_sst_stats(Arc::new(stats.clamp_allocated_size()))
            }
        }
    }
}

pub struct SplitCache {
    // Cache for block data
    block_cache: Option<Arc<dyn DbCache>>,
    // Cache for indices and filters
    meta_cache: Option<Arc<dyn DbCache>>,
}

impl SplitCache {
    pub fn new() -> Self {
        Self {
            block_cache: None,
            meta_cache: None,
        }
    }

    pub fn with_block_cache(mut self, cache: Option<Arc<dyn DbCache>>) -> Self {
        self.block_cache = cache;
        self
    }

    pub fn with_meta_cache(mut self, cache: Option<Arc<dyn DbCache>>) -> Self {
        self.meta_cache = cache;
        self
    }

    pub fn build(self) -> Self {
        self
    }
}

impl Default for SplitCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DbCache for SplitCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        if let Some(cache) = &self.block_cache {
            cache.get_block(key).await
        } else {
            Ok(None)
        }
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.get_index(key).await
        } else {
            Ok(None)
        }
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.get_filter(key).await
        } else {
            Ok(None)
        }
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.get_stats(key).await
        } else {
            Ok(None)
        }
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        match &value.item {
            CachedItem::Block(_) => {
                if let Some(ref cache) = self.block_cache {
                    cache.insert(key, value.clamp_allocated_size()).await;
                } else {
                    trace!("no block cache available for insertion");
                }
            }
            CachedItem::SsTableIndex(_) | CachedItem::Filters(_) | CachedItem::SstStats(_) => {
                if let Some(ref cache) = self.meta_cache {
                    cache.insert(key, value.clamp_allocated_size()).await;
                } else {
                    trace!("no meta cache available for insertion");
                }
            }
            // EncodedFilters only exist as the transient output of
            // disk-cache deserialization, which happens inside the
            // underlying cache impl (foyer) and never flows back through
            // `SplitCache::insert`. A direct insert of an encoded entry
            // would indicate the invariant was bypassed.
            CachedItem::EncodedFilters(_) => {
                error!(
                    "SplitCache::insert called with EncodedFilters; encoded \
                     entries only exist inside foyer's deserialization path"
                );
                debug_assert!(false, "EncodedFilters in SplitCache::insert");
            }
        }
    }

    #[allow(dead_code)]
    async fn remove(&self, key: &CachedKey) {
        // Because `CachedKey` is uniquely identified by (scope ID, SST ID, offset), given a
        // `CachedKey`, it will only appear in the block cache or meta cache, which is safe and
        // will not cause duplicate deletion.
        if let Some(ref cache) = self.block_cache {
            cache.remove(key).await;
        }
        if let Some(ref cache) = self.meta_cache {
            cache.remove(key).await;
        }
    }

    fn entry_count(&self) -> u64 {
        self.block_cache.as_ref().map_or(0, |c| c.entry_count())
            + self.meta_cache.as_ref().map_or(0, |c| c.entry_count())
    }

    async fn close(&self) -> Result<(), crate::Error> {
        if let Some(ref cache) = self.block_cache {
            cache.close().await?;
        }
        if let Some(ref cache) = self.meta_cache {
            cache.close().await?;
        }
        Ok(())
    }
}

/// Wraps a [`DbCache`] to add statistics, error logging, and cache scoping.
///
/// ## Scoping
/// When multiple `Db` instances share the same underlying cache object, this wrapper assigns a
/// unique `scope_id` so their entries do not collide. All cache operations transparently rewrite
/// keys to include the wrapper's `scope_id`, isolating WAL and compacted SST entries per wrapper.
pub(crate) struct DbCacheWrapper {
    stats: DbCacheStats,
    system_clock: Arc<dyn SystemClock>,
    cache: Arc<dyn DbCache>,
    /// Unique identifier applied to every key passed through this wrapper. This prevents different
    /// `DbCacheWrapper` instances that share the same cache from clobbering each other's entries.
    /// Legacy keys use scope `0`; new wrappers are assigned distinct, non-zero scopes.
    scope_id: u64,
    // Records the last time that the wrapper logged an error from the wrapped cache at error
    // level. Used to ensure we only log at error level once every ERROR_LOG_INTERVAL.
    last_err_log_time: Mutex<Option<DateTime<Utc>>>,
}

impl DbCacheWrapper {
    pub(crate) fn new(
        cache: Arc<dyn DbCache>,
        recorder: &MetricsRecorderHelper,
        system_clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            stats: DbCacheStats::new(recorder),
            cache,
            scope_id: NEXT_CACHE_SCOPE_ID.fetch_add(1, Ordering::Relaxed),
            last_err_log_time: Mutex::new(None),
            system_clock,
        }
    }
}

// The minimum interval between which the wrapper logs cache errors at error level. This is used to
// ensure we don't spam the logs on non-transient errors from the cache.
const ERROR_LOG_INTERVAL: TimeDelta = TimeDelta::seconds(1);

impl DbCacheWrapper {
    fn scoped_key(&self, key: &CachedKey) -> CachedKey {
        key.with_scope(self.scope_id)
    }

    fn record_get_err(&self, block_type: &str, err: &crate::Error) {
        let log_at_err = {
            let mut guard = self.last_err_log_time.lock();
            match *guard {
                None => {
                    *guard = Some(self.system_clock.now());
                    true
                }
                Some(t) if self.system_clock.now() - t > ERROR_LOG_INTERVAL => {
                    *guard = Some(self.system_clock.now());
                    true
                }
                _ => false,
            }
        };
        if log_at_err {
            error!(
                "error getting block from cache [block_type={} error={:?}]",
                block_type, err
            );
        } else {
            debug!(
                "error getting block from cache [block_type={} error={:?}]",
                block_type, err
            );
        }
        self.stats.get_error.increment(1);
    }
}

#[async_trait]
impl DbCache for DbCacheWrapper {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        let scoped_key = self.scoped_key(key);
        let entry = match self.cache.get_block(&scoped_key).await {
            Ok(e) => e,
            Err(err) => {
                self.record_get_err("block", &err);
                return Err(err);
            }
        };
        if entry.is_some() {
            self.stats.data_block_hit.increment(1);
        } else {
            self.stats.data_block_miss.increment(1);
        }
        Ok(entry)
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        let scoped_key = self.scoped_key(key);
        let entry = match self.cache.get_index(&scoped_key).await {
            Ok(e) => e,
            Err(err) => {
                self.record_get_err("index", &err);
                return Err(err);
            }
        };
        if entry.is_some() {
            self.stats.index_hit.increment(1);
        } else {
            self.stats.index_miss.increment(1);
        }
        Ok(entry)
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        let scoped_key = self.scoped_key(key);
        let entry = match self.cache.get_filter(&scoped_key).await {
            Ok(e) => e,
            Err(err) => {
                self.record_get_err("filter", &err);
                return Err(err);
            }
        };
        if entry.is_some() {
            self.stats.filter_hit.increment(1);
        } else {
            self.stats.filter_miss.increment(1);
        }
        Ok(entry)
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        let scoped_key = self.scoped_key(key);
        let entry = match self.cache.get_stats(&scoped_key).await {
            Ok(e) => e,
            Err(err) => {
                self.record_get_err("stats", &err);
                return Err(err);
            }
        };
        if entry.is_some() {
            self.stats.stats_hit.increment(1);
        } else {
            self.stats.stats_miss.increment(1);
        }
        Ok(entry)
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        let scoped_key = self.scoped_key(&key);
        self.cache.insert(scoped_key, value).await
    }

    #[allow(dead_code)]
    async fn remove(&self, key: &CachedKey) {
        let scoped_key = self.scoped_key(key);
        self.cache.remove(&scoped_key).await
    }

    fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    async fn close(&self) -> Result<(), crate::Error> {
        self.cache.close().await
    }
}

pub mod stats {
    use slatedb_common::metrics::{CounterFn, MetricsRecorderHelper};
    use std::sync::Arc;

    macro_rules! dbcache_stat_name {
        ($suffix:expr) => {
            concat!("slatedb.db_cache.", $suffix)
        };
    }

    pub const ACCESS_COUNT: &str = dbcache_stat_name!("access_count");
    pub const ERROR_COUNT: &str = dbcache_stat_name!("error_count");

    pub(super) struct DbCacheStats {
        pub(super) filter_hit: Arc<dyn CounterFn>,
        pub(super) filter_miss: Arc<dyn CounterFn>,
        pub(super) index_hit: Arc<dyn CounterFn>,
        pub(super) index_miss: Arc<dyn CounterFn>,
        pub(super) data_block_hit: Arc<dyn CounterFn>,
        pub(super) data_block_miss: Arc<dyn CounterFn>,
        pub(super) stats_hit: Arc<dyn CounterFn>,
        pub(super) stats_miss: Arc<dyn CounterFn>,
        pub(super) get_error: Arc<dyn CounterFn>,
    }

    impl DbCacheStats {
        pub(super) fn new(recorder: &MetricsRecorderHelper) -> Self {
            Self {
                filter_hit: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "filter"), ("result", "hit")])
                    .register(),
                filter_miss: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "filter"), ("result", "miss")])
                    .register(),
                index_hit: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "index"), ("result", "hit")])
                    .register(),
                index_miss: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "index"), ("result", "miss")])
                    .register(),
                data_block_hit: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "data_block"), ("result", "hit")])
                    .register(),
                data_block_miss: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "data_block"), ("result", "miss")])
                    .register(),
                stats_hit: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "stats"), ("result", "hit")])
                    .register(),
                stats_miss: recorder
                    .counter(ACCESS_COUNT)
                    .labels(&[("entry_kind", "stats"), ("result", "miss")])
                    .register(),
                get_error: recorder.counter(ERROR_COUNT).register(),
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use crate::db_cache::{CachedEntry, CachedKey, DbCache};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// A cache that always returns an error from get operations.
    pub(crate) struct FailingCache;

    #[async_trait]
    impl DbCache for FailingCache {
        async fn get_block(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
        async fn get_index(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
        async fn get_filter(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
        async fn get_stats(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
        async fn insert(&self, _: CachedKey, _: CachedEntry) {}
        async fn remove(&self, _: &CachedKey) {}
        fn entry_count(&self) -> u64 {
            0
        }
    }

    pub(crate) struct TestCache {
        items: Mutex<HashMap<CachedKey, CachedEntry>>,
    }

    impl TestCache {
        pub(crate) fn new() -> Self {
            Self {
                items: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl DbCache for TestCache {
        async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            let guard = self.items.lock().unwrap();
            Ok(guard.get(key).cloned())
        }

        async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            let guard = self.items.lock().unwrap();
            Ok(guard.get(key).cloned())
        }

        async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            let guard = self.items.lock().unwrap();
            Ok(guard.get(key).cloned())
        }

        async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
            let guard = self.items.lock().unwrap();
            Ok(guard.get(key).cloned())
        }

        async fn insert(&self, key: CachedKey, value: CachedEntry) {
            let mut guard = self.items.lock().unwrap();
            guard.insert(key, value);
        }

        async fn remove(&self, key: &CachedKey) {
            let mut guard = self.items.lock().unwrap();
            guard.remove(key);
        }

        fn entry_count(&self) -> u64 {
            let guard = self.items.lock().unwrap();
            guard.iter().count() as u64
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::db_cache::{CachedEntry, CachedKey, DbCache, DbCacheWrapper, SplitCache};
    use crate::db_state::SsTableId;
    use crate::filter_policy::{BloomFilterPolicy, FilterPolicy, NamedFilter};
    use crate::format::sst::BlockBuilder;
    use slatedb_common::clock::DefaultSystemClock;

    use crate::flatbuffer_types::test_utils::assert_index_clamped;

    use crate::db_cache::test_utils::TestCache;
    use crate::format::sst::{EncodedSsTable, SsTableFormat};
    use crate::test_utils::build_test_sst;
    use crate::types::{RowEntry, ValueDeletable};
    use rstest::{fixture, rstest};
    use slatedb_common::metrics::{
        lookup_metric_with_labels, DefaultMetricsRecorder, MetricLevel, MetricsRecorderHelper,
    };
    use std::sync::Arc;
    use ulid::Ulid;

    const SST_ID: SsTableId = SsTableId::Compacted(Ulid::from_parts(0u64, 0u128));

    #[rstest]
    #[tokio::test]
    async fn test_should_count_filter_hits(
        cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>),
        #[future(awt)] sst: EncodedSsTable,
    ) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_filters(sst.filters))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_filter(&key).await;

            // then:
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "filter"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "filter"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_filter_misses(cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>)) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_filter(&key).await;

            // then:
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "filter"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "filter"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_index_hits(
        cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>),
        #[future(awt)] sst: EncodedSsTable,
    ) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(
                key.clone(),
                CachedEntry::with_sst_index(Arc::new(sst.index)),
            )
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_index(&key).await;

            // then:
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "index"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "index"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_clamp_entries_to_cache(
        cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>),
        sst_format: SsTableFormat,
        #[future(awt)] sst: EncodedSsTable,
    ) {
        let (cache, _registry) = cache;
        // given:
        let bytes = sst.remaining_as_bytes();
        let index = Arc::new(sst_format.read_index_raw(&sst.info, &bytes).await.unwrap());
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_sst_index(index.clone()))
            .await;

        // when:
        let cached = cache.get_index(&key).await.unwrap().unwrap();

        // then:
        assert_index_clamped(index.as_ref(), cached.sst_index().unwrap().as_ref());
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_index_misses(cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>)) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_index(&key).await;

            // then:
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "index"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "index"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_data_block_hits(
        cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>),
        sst_format: SsTableFormat,
        #[future(awt)] sst: EncodedSsTable,
    ) {
        let (cache, registry) = cache;
        // given:
        let data = sst.remaining_as_bytes();
        let block = sst_format
            .read_block_raw(&sst.info, &sst.index, 0, &data)
            .await
            .unwrap();
        let key = CachedKey::from((SST_ID, 12345u64));
        cache
            .insert(key.clone(), CachedEntry::with_block(Arc::new(block)))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_block(&key).await;

            // then:
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "data_block"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "data_block"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_data_block_misses(
        cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>),
    ) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_block(&key).await;

            // then:
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "data_block"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "data_block"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_stats_hits(cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>)) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));
        let stats = crate::sst_stats::SstStats::default();
        cache
            .insert(key.clone(), CachedEntry::with_sst_stats(Arc::new(stats)))
            .await;

        for i in 1..4 {
            // when:
            let _ = cache.get_stats(&key).await;

            // then:
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "stats"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "stats"), ("result", "hit")]
                )
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_should_count_stats_misses(cache: (DbCacheWrapper, Arc<DefaultMetricsRecorder>)) {
        let (cache, registry) = cache;
        // given:
        let key = CachedKey::from((SST_ID, 12345u64));

        for i in 1..4 {
            // when:
            let _ = cache.get_stats(&key).await;

            // then:
            assert_eq!(
                Some(i as i64),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "stats"), ("result", "miss")]
                )
            );
            assert_eq!(
                Some(0),
                lookup_metric_with_labels(
                    &registry,
                    super::stats::ACCESS_COUNT,
                    &[("entry_kind", "stats"), ("result", "hit")]
                )
            );
        }
    }

    #[tokio::test]
    async fn test_should_count_get_errors() {
        // given: a cache that always returns errors
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let failing_cache: Arc<dyn super::DbCache> = Arc::new(super::test_utils::FailingCache);
        let cache = super::DbCacheWrapper::new(
            failing_cache,
            &helper,
            Arc::new(slatedb_common::clock::DefaultSystemClock::default()),
        );
        let key = CachedKey::from((SST_ID, 12345u64));

        // when: each get method returns an error
        let _ = cache.get_block(&key).await;
        let _ = cache.get_index(&key).await;
        let _ = cache.get_filter(&key).await;
        let _ = cache.get_stats(&key).await;

        // then:
        assert_eq!(
            slatedb_common::metrics::lookup_metric(&recorder, super::stats::ERROR_COUNT),
            Some(4)
        );
    }

    #[tokio::test]
    async fn test_cache_wrapper_scopes_keys() {
        let recorder_a = MetricsRecorderHelper::noop();
        let recorder_b = MetricsRecorderHelper::noop();
        let system_clock = Arc::new(DefaultSystemClock::default());
        let shared_cache: Arc<dyn DbCache> = Arc::new(TestCache::new());
        let cache_a = DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone());
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock);
        assert_ne!(cache_a.scope_id, cache_b.scope_id);

        let policy = BloomFilterPolicy::new(1);
        let mut builder = policy.builder();
        builder.add_entry(&RowEntry::new(
            bytes::Bytes::from_static(b"a"),
            ValueDeletable::Value(bytes::Bytes::new()),
            0,
            None,
            None,
        ));
        let filter = builder.build();
        let named = NamedFilter {
            name: BloomFilterPolicy::NAME.to_string(),
            filter,
        };
        let key = CachedKey::from((SST_ID, 1u64));

        cache_a
            .insert(
                key.clone(),
                CachedEntry::with_filters(Arc::from([named.clone()])),
            )
            .await;

        assert!(cache_a.get_filter(&key).await.unwrap().is_some());
        assert!(cache_b.get_filter(&key).await.unwrap().is_none());

        cache_b
            .insert(key.clone(), CachedEntry::with_filters(Arc::from([named])))
            .await;

        assert_eq!(2, shared_cache.entry_count());
    }

    #[tokio::test]
    async fn test_cache_wrapper_scopes_index_entries() {
        let recorder_a = MetricsRecorderHelper::noop();
        let recorder_b = MetricsRecorderHelper::noop();
        let system_clock = Arc::new(DefaultSystemClock::default());
        let shared_cache: Arc<dyn DbCache> = Arc::new(TestCache::new());
        let cache_a = DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone());
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock);

        let sst = build_test_sst(&SsTableFormat::default(), 1).await;
        let index = Arc::new(sst.index);
        let key = CachedKey::from((SST_ID, 2u64));

        cache_a
            .insert(key.clone(), CachedEntry::with_sst_index(index.clone()))
            .await;

        assert!(cache_a.get_index(&key).await.unwrap().is_some());
        assert!(cache_b.get_index(&key).await.unwrap().is_none());

        cache_b
            .insert(key.clone(), CachedEntry::with_sst_index(index))
            .await;

        assert_eq!(2, shared_cache.entry_count());
    }

    #[tokio::test]
    async fn test_cache_wrapper_scopes_block_entries() {
        let recorder_a = MetricsRecorderHelper::noop();
        let recorder_b = MetricsRecorderHelper::noop();
        let system_clock = Arc::new(DefaultSystemClock::default());
        let shared_cache: Arc<dyn DbCache> = Arc::new(TestCache::new());
        let cache_a = DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone());
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock);

        let mut builder = BlockBuilder::new_latest(4096);
        assert!(builder.add(RowEntry::new_value(b"k1", b"v1", 0)).unwrap());
        let block = Arc::new(builder.build().unwrap());
        let key = CachedKey::from((SST_ID, 3u64));

        cache_a
            .insert(key.clone(), CachedEntry::with_block(block.clone()))
            .await;

        assert!(cache_a.get_block(&key).await.unwrap().is_some());
        assert!(cache_b.get_block(&key).await.unwrap().is_none());

        cache_b
            .insert(key.clone(), CachedEntry::with_block(block))
            .await;

        assert_eq!(2, shared_cache.entry_count());
    }

    #[fixture]
    fn cache() -> (DbCacheWrapper, Arc<DefaultMetricsRecorder>) {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(recorder.clone(), MetricLevel::default());
        let cache = SplitCache::new()
            .with_block_cache(Some(Arc::new(TestCache::new())))
            .with_meta_cache(Some(Arc::new(TestCache::new())))
            .build();

        let wrapper = DbCacheWrapper::new(
            Arc::new(cache),
            &helper,
            Arc::new(DefaultSystemClock::default()),
        );
        (wrapper, recorder)
    }

    #[fixture]
    fn sst_format() -> SsTableFormat {
        SsTableFormat {
            block_size: 128,
            ..SsTableFormat::default()
        }
    }

    #[fixture]
    async fn sst(sst_format: SsTableFormat) -> EncodedSsTable {
        build_test_sst(&sst_format, 1).await
    }
}
