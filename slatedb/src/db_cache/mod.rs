//! # DB Cache
//!
//! This module provides a pluggable caching solution for storing and retrieving
//! cached blocks, index and filters associated with SSTable IDs.
//!
//! There are currently two built-in cache implementations:
//! - [Foyer](crate::db_cache::foyer::FoyerCache): Requires the `foyer` feature flag. (Enabled by default)
//! - [Moka](crate::db_cache::moka::MokaCache): Requires the `moka` feature flag. (Enabled by default)
//!
//! ## Usage
//!
//! To use the cache, you need to configure the [DbOptions](crate::config::DbOptions) with the desired cache implementation.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeDelta, Utc};
use futures::future::BoxFuture;
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

/// A `FnOnce` returning a future that produces a [`CachedEntry`] on cache miss.
///
/// Used by [`DbCache::fetch_block`] and friends to load an entry into the cache. The closure
/// is invoked at most once per concurrent fetch group; subsequent callers for the same key
/// receive the same result.
pub type CacheLoader =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<CachedEntry, crate::Error>> + Send + 'static>;

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
///         .with_db_cache(cache, 0)
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
    ///
    /// SlateDB only invokes this on caches it created itself (the default
    /// cache built when none is configured). If you pass your own cache to
    /// [`with_db_cache`](crate::db::builder::DbBuilder::with_db_cache), you
    /// own it: close it yourself after closing every `Db` and `DbReader`
    /// that uses it. This allows a single cache to be safely shared across
    /// multiple databases without the first `close()` disabling it for the
    /// others.
    async fn close(&self) -> Result<(), crate::Error> {
        Ok(())
    }

    /// Best-effort: if `key` is resident in a fast (memory) tier, move it to a slower
    /// durable tier (e.g. disk) and drop it from the fast tier. A no-op for a
    /// non-resident key.
    ///
    /// This lets a caller reclaim a hybrid cache's memory footprint for one key
    /// without losing the warmth it provides, by relocating it rather than removing
    /// it outright (contrast with [`remove`](Self::remove), which drops the entry
    /// entirely). Used by [`DbCacheManagerOps::flush_cache_to_disk`](crate::DbCacheManagerOps::flush_cache_to_disk)
    /// to shrink one `Db`'s footprint in a cache shared with other `Db`s, while
    /// keeping the entries recoverable from disk.
    ///
    /// The default implementation is a no-op: caches with no slower tier to spill
    /// to (or backed by a cache library without an explicit "move to disk"
    /// primitive) have nothing useful to do here.
    ///
    /// `Ok(())` means handed off, not confirmed durable — an implementation's
    /// cache library may have its own silent-drop paths under pressure that
    /// don't surface as an error (see `foyer_hybrid`'s implementation).
    ///
    /// Implementations must remove `key` from the fast tier whether or not
    /// this returns `Err` — callers clear touch-tracking after a failed spill
    /// on that assumption.
    async fn spill_and_evict(&self, _key: &CachedKey) -> Result<(), crate::Error> {
        Ok(())
    }

    /// Wait for any disk writes enqueued by [`spill_and_evict`](Self::spill_and_evict)
    /// to complete. The default implementation is a no-op.
    async fn wait_for_spills(&self) -> Result<(), crate::Error> {
        Ok(())
    }

    /// Returns the ids of SSTs this cache currently has a recorded entry for
    /// (each cleared individually via [`Self::clear_touched_if_unchanged`], not
    /// by a bulk operation). Lets a caller bound an evacuation walk to SSTs
    /// that might have something resident, instead of walking every SST it
    /// knows about. Over-approximates in one direction: an id can still appear
    /// here after its entries were already evicted by ordinary cache pressure,
    /// in which case evacuating it is just a fast no-op. The default
    /// implementation returns an empty list.
    fn touched_sst_ids(&self) -> Vec<SsTableId> {
        Vec::new()
    }

    /// Returns `id`'s current touch generation, if tracked. The default
    /// implementation returns `None`. See [`Self::clear_touched_if_unchanged`].
    fn touched_generation(&self, _id: SsTableId) -> Option<u64> {
        None
    }

    /// Clears `id` from [`Self::touched_sst_ids`] only if its generation is
    /// still `observed_generation` — i.e. nothing recorded a fresh entry for
    /// this SST since the caller snapshotted that generation via
    /// [`Self::touched_generation`]. Lets a caller that walks an SST's
    /// entries across multiple `.await` points safely clear its tracking
    /// afterward without racing a concurrent insert for the same SST. The
    /// default implementation is a no-op.
    fn clear_touched_if_unchanged(&self, _id: SsTableId, _observed_generation: Option<u64>) {}

    /// Fetch a data-block entry, invoking `loader` on cache miss.
    ///
    /// Implementations should deduplicate concurrent fetches: if multiple callers request the
    /// same key while it is being loaded, only one should run `loader` and the rest should
    /// share its result. The default implementation does **not** dedup; it simply does a
    /// get-miss-load-insert. Override in implementations whose backing cache exposes a
    /// dedup-aware fetch primitive (e.g. foyer's `Cache::fetch`).
    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(entry) = self.get_block(&key).await? {
            return Ok(entry);
        }
        let entry = loader().await?;
        self.insert(key, entry.clone()).await;
        Ok(entry)
    }

    /// Fetch an index entry, invoking `loader` on cache miss. See [`Self::fetch_block`].
    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(entry) = self.get_index(&key).await? {
            return Ok(entry);
        }
        let entry = loader().await?;
        self.insert(key, entry.clone()).await;
        Ok(entry)
    }

    /// Fetch a filter entry, invoking `loader` on cache miss. See [`Self::fetch_block`].
    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(entry) = self.get_filter(&key).await? {
            return Ok(entry);
        }
        let entry = loader().await?;
        self.insert(key, entry.clone()).await;
        Ok(entry)
    }

    /// Fetch a stats entry, invoking `loader` on cache miss. See [`Self::fetch_block`].
    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(entry) = self.get_stats(&key).await? {
            return Ok(entry);
        }
        let entry = loader().await?;
        self.insert(key, entry.clone()).await;
        Ok(entry)
    }
}

/// An SST component that can be inserted into the block cache, either by
/// warming an existing SST via
/// [`DbCacheManagerOps::warm_sst`](crate::DbCacheManagerOps::warm_sst) or as
/// the SST is written via [`BlockCachePolicy`](crate::BlockCachePolicy).
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CacheTarget {
    /// All filter blocks on the SST, if any exist.
    Filters,
    /// The SST index.
    Index,
    /// The SST stats block, if one exists.
    Stats,
    /// Data blocks whose key span overlaps the supplied key range.
    Data((Bound<Bytes>, Bound<Bytes>)),
}

impl CacheTarget {
    /// Convenience constructor for [`CacheTarget::Data`] that accepts any
    /// [`RangeBounds`], mirroring the `Db::scan` signature. Pass `..` to
    /// select all data blocks.
    pub fn data<K, T>(range: T) -> Self
    where
        K: AsRef<[u8]>,
        T: RangeBounds<K>,
    {
        let start = range
            .start_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        let end = range
            .end_bound()
            .map(|b| Bytes::copy_from_slice(b.as_ref()));
        CacheTarget::Data((start, end))
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
    /// Scope identifier set per `DbCacheWrapper`, so multiple `Db` instances sharing
    /// one cache don't collide on WAL or compacted entries. Caller-supplied (see
    /// `DbBuilder::with_db_cache`), not derived by SlateDB. `0` is a valid choice,
    /// not a reserved/legacy value.
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
    pub(crate) data: Bytes,
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

    async fn spill_and_evict(&self, key: &CachedKey) -> Result<(), crate::Error> {
        // Attempt both sub-caches rather than `?`-chaining, matching the
        // "try everything, surface the first error" pattern used elsewhere in
        // this module (e.g. `TableStore::spill_sst_to_disk`): a given key
        // lives in at most one of the two, but an error from one must not
        // skip the other.
        let block_result = match &self.block_cache {
            Some(cache) => cache.spill_and_evict(key).await,
            None => Ok(()),
        };
        let meta_result = match &self.meta_cache {
            Some(cache) => cache.spill_and_evict(key).await,
            None => Ok(()),
        };
        block_result.and(meta_result)
    }

    async fn wait_for_spills(&self) -> Result<(), crate::Error> {
        let block_result = match &self.block_cache {
            Some(cache) => cache.wait_for_spills().await,
            None => Ok(()),
        };
        let meta_result = match &self.meta_cache {
            Some(cache) => cache.wait_for_spills().await,
            None => Ok(()),
        };
        block_result.and(meta_result)
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(cache) = &self.block_cache {
            cache.fetch_block(key, loader).await
        } else {
            loader().await
        }
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.fetch_index(key, loader).await
        } else {
            loader().await
        }
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.fetch_filter(key, loader).await
        } else {
            loader().await
        }
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        if let Some(cache) = &self.meta_cache {
            cache.fetch_stats(key, loader).await
        } else {
            loader().await
        }
    }
}

/// Wraps a [`DbCache`] to add statistics, error logging, cache scoping, and
/// per-scope touched-SST tracking.
///
/// ## Scoping
/// When multiple `Db`/`DbReader` instances share the same underlying cache object, this
/// wrapper assigns a `scope_id` so their entries do not collide. All cache operations
/// transparently rewrite keys to include the wrapper's `scope_id`, isolating WAL and
/// compacted SST entries per wrapper.
///
/// `scope_id` is supplied by the caller at construction time (see [`Self::new`]),
/// not derived by SlateDB: pass the same id across a legitimate reopen of the same
/// logical database to recover its warm entries, and different ids for logically
/// different databases sharing the cache.
pub(crate) struct DbCacheWrapper {
    stats: DbCacheStats,
    system_clock: Arc<dyn SystemClock>,
    cache: Arc<dyn DbCache>,
    /// Identifier applied to every key passed through this wrapper, supplied by
    /// the caller of [`Self::new`]. This prevents different `DbCacheWrapper`
    /// instances backed by different logical `Db`s from clobbering each other's entries
    /// in a cache they share. `0` is a valid choice when only one instance will ever
    /// use this cache.
    scope_id: u64,
    // Records the last time that the wrapper logged an error from the wrapped cache at error
    // level. Used to ensure we only log at error level once every ERROR_LOG_INTERVAL.
    last_err_log_time: Mutex<Option<DateTime<Utc>>>,
    /// SSTs with at least one recorded cache entry, keyed to the
    /// [`Self::touch_seq`] value at the time of the most recent touch. See
    /// [`DbCache::touched_sst_ids`]. Cleared per-SST only via the opt-in
    /// [`flush_cache_to_disk`](crate::DbCacheManagerOps::flush_cache_to_disk)/
    /// [`evict_cached_sst`](crate::DbCacheManagerOps::evict_cached_sst) APIs;
    /// bounded independently of those by [`evict_oldest_if_over_cap`] on every touch.
    touched_ssts: Mutex<HashMap<SsTableId, u64>>,
    /// Monotonic stamp source for [`Self::touched_ssts`], shared across all
    /// SSTs rather than reset per-SST. A per-SST counter that resets to `0`
    /// on removal is ABA-prone: two independent clearers (e.g.
    /// `evict_cached_sst` racing `flush_cache_to_disk`) can each snapshot `0`,
    /// and a fresh touch landing between their clears would reinsert at `0`
    /// too, matching a stale snapshot. A never-repeating sequence makes that
    /// collision impossible.
    touch_seq: AtomicU64,
}

impl DbCacheWrapper {
    /// See the struct-level "Scoping" section above for what `scope_id` does.
    pub(crate) fn new(
        cache: Arc<dyn DbCache>,
        recorder: &MetricsRecorderHelper,
        system_clock: Arc<dyn SystemClock>,
        scope_id: u64,
    ) -> Self {
        Self {
            stats: DbCacheStats::new(recorder),
            cache,
            scope_id,
            last_err_log_time: Mutex::new(None),
            system_clock,
            touched_ssts: Mutex::new(HashMap::new()),
            touch_seq: AtomicU64::new(0),
        }
    }

    /// Records that `sst_id` may now have a resident entry under this scope.
    /// Excludes `SsTableId::Wal`: WAL entries are never spilled to a hybrid
    /// cache's persistent disk tier by evacuation, so tracking them would
    /// only let a stale, scope-colliding entry survive longer than ordinary
    /// memory LRU pressure would allow.
    fn record_touched(&self, sst_id: SsTableId) {
        if matches!(sst_id, SsTableId::Wal(_)) {
            return;
        }
        let seq = self.touch_seq.fetch_add(1, Ordering::Relaxed);
        let mut touched = self.touched_ssts.lock();
        touched.insert(sst_id, seq);
        evict_oldest_if_over_cap(&mut touched, MAX_TOUCHED_SSTS, TOUCHED_SSTS_EVICT_SLACK);
    }
}

/// Hard-ish cap on [`DbCacheWrapper::touched_ssts`]'s size: comfortably above what any
/// real evacuation workflow needs to track live, while still bounding worst-case memory
/// for a caller that never calls `flush_cache_to_disk`/`evict_cached_sst` at all.
const MAX_TOUCHED_SSTS: usize = 100_000;

/// Batch size for [`evict_oldest_if_over_cap`], so the O(n log n) eviction pass is
/// amortized over this many new distinct SSTs rather than run on every touch once at
/// the cap.
const TOUCHED_SSTS_EVICT_SLACK: usize = 10_000;

/// Evicts the `slack` least-recently-touched entries once `touched` exceeds `cap +
/// slack`, bounding its size without scanning on every insert. Evicting the oldest
/// only risks a missed future flush for an SST that's still resident — not a
/// correctness issue — and is unlikely anyway: low recency also means ordinary LRU
/// pressure has probably already evicted it from the real cache.
fn evict_oldest_if_over_cap(touched: &mut HashMap<SsTableId, u64>, cap: usize, slack: usize) {
    if touched.len() <= cap + slack {
        return;
    }
    let mut by_seq: Vec<(SsTableId, u64)> = touched.iter().map(|(&id, &seq)| (id, seq)).collect();
    by_seq.sort_unstable_by_key(|&(_, seq)| seq);
    for (id, _) in by_seq.into_iter().take(slack) {
        touched.remove(&id);
    }
}

// The minimum interval between which the wrapper logs cache errors at error level. This is used to
// ensure we don't spam the logs on non-transient errors from the cache.
const ERROR_LOG_INTERVAL: TimeDelta = TimeDelta::seconds(1);

impl DbCacheWrapper {
    fn scoped_key(&self, key: &CachedKey) -> CachedKey {
        key.with_scope(self.scope_id)
    }

    fn record_fetch_outcome(
        &self,
        block_type: &str,
        loader_ran: bool,
        result: &Result<CachedEntry, crate::Error>,
    ) {
        match result {
            Ok(_) if loader_ran => self.record_miss(block_type),
            Ok(_) => self.record_hit(block_type),
            Err(err) => self.record_get_err(block_type, err),
        }
    }

    fn record_hit(&self, block_type: &str) {
        match block_type {
            "block" => self.stats.data_block_hit.increment(1),
            "index" => self.stats.index_hit.increment(1),
            "filter" => self.stats.filter_hit.increment(1),
            "stats" => self.stats.stats_hit.increment(1),
            _ => {}
        }
    }

    fn record_miss(&self, block_type: &str) {
        match block_type {
            "block" => self.stats.data_block_miss.increment(1),
            "index" => self.stats.index_miss.increment(1),
            "filter" => self.stats.filter_miss.increment(1),
            "stats" => self.stats.stats_miss.increment(1),
            _ => {}
        }
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
        self.cache.insert(scoped_key, value).await;
        self.record_touched(key.sst_id);
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

    async fn spill_and_evict(&self, key: &CachedKey) -> Result<(), crate::Error> {
        let scoped_key = self.scoped_key(key);
        self.cache.spill_and_evict(&scoped_key).await
    }

    async fn wait_for_spills(&self) -> Result<(), crate::Error> {
        self.cache.wait_for_spills().await
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let scoped_key = self.scoped_key(&key);
        let (loader, loader_ran) = instrumented_loader(loader);
        let result = self.cache.fetch_block(scoped_key, loader).await;
        self.record_fetch_outcome("block", loader_ran.was_called(), &result);
        if result.is_ok() {
            self.record_touched(key.sst_id);
        }
        result
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let scoped_key = self.scoped_key(&key);
        let (loader, loader_ran) = instrumented_loader(loader);
        let result = self.cache.fetch_index(scoped_key, loader).await;
        self.record_fetch_outcome("index", loader_ran.was_called(), &result);
        if result.is_ok() {
            self.record_touched(key.sst_id);
        }
        result
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let scoped_key = self.scoped_key(&key);
        let (loader, loader_ran) = instrumented_loader(loader);
        let result = self.cache.fetch_filter(scoped_key, loader).await;
        self.record_fetch_outcome("filter", loader_ran.was_called(), &result);
        if result.is_ok() {
            self.record_touched(key.sst_id);
        }
        result
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let scoped_key = self.scoped_key(&key);
        let (loader, loader_ran) = instrumented_loader(loader);
        let result = self.cache.fetch_stats(scoped_key, loader).await;
        self.record_fetch_outcome("stats", loader_ran.was_called(), &result);
        if result.is_ok() {
            self.record_touched(key.sst_id);
        }
        result
    }

    fn touched_sst_ids(&self) -> Vec<SsTableId> {
        self.touched_ssts.lock().keys().copied().collect()
    }

    fn touched_generation(&self, id: SsTableId) -> Option<u64> {
        self.touched_ssts.lock().get(&id).copied()
    }

    fn clear_touched_if_unchanged(&self, id: SsTableId, observed_generation: Option<u64>) {
        if let Entry::Occupied(e) = self.touched_ssts.lock().entry(id) {
            if Some(*e.get()) == observed_generation {
                e.remove();
            }
        }
    }
}

/// Wraps a user-provided [`DbCache`] so that [`DbCache::close`] becomes a no-op.
///
/// SlateDB does not own caches passed in via `with_db_cache`: the caller may be sharing one
/// cache across several `Db`/`DbReader` instances, so the first `Db::close()` must not shut
/// it down for the others. The caller closes the inner cache themselves once every instance
/// using it is closed.
///
/// Note: every trait method must be forwarded explicitly, including the ones with default
/// implementations. Falling back to a default here would silently replace the inner cache's
/// behavior (e.g. foyer's dedup-aware `fetch_*`) for user-provided caches only.
pub(crate) struct UnownedDbCache {
    inner: Arc<dyn DbCache>,
}

impl UnownedDbCache {
    pub(crate) fn new(inner: Arc<dyn DbCache>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl DbCache for UnownedDbCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner.get_block(key).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner.get_index(key).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner.get_filter(key).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner.get_stats(key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value).await
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key).await
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    /// The point of this type: never propagate close to a cache we don't own.
    async fn close(&self) -> Result<(), crate::Error> {
        Ok(())
    }

    async fn spill_and_evict(&self, key: &CachedKey) -> Result<(), crate::Error> {
        self.inner.spill_and_evict(key).await
    }

    async fn wait_for_spills(&self) -> Result<(), crate::Error> {
        self.inner.wait_for_spills().await
    }

    fn touched_sst_ids(&self) -> Vec<SsTableId> {
        self.inner.touched_sst_ids()
    }

    fn touched_generation(&self, id: SsTableId) -> Option<u64> {
        self.inner.touched_generation(id)
    }

    fn clear_touched_if_unchanged(&self, id: SsTableId, observed_generation: Option<u64>) {
        self.inner
            .clear_touched_if_unchanged(id, observed_generation)
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.inner.fetch_block(key, loader).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.inner.fetch_index(key, loader).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.inner.fetch_filter(key, loader).await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.inner.fetch_stats(key, loader).await
    }
}

/// Tracks whether the loader closure was actually invoked. Used by `DbCacheWrapper`
/// to attribute fetches as hits (loader skipped, value served from cache or a
/// concurrent fetch) or misses (this caller's loader ran).
#[derive(Clone)]
struct LoaderRan(Arc<AtomicBool>);

impl LoaderRan {
    fn was_called(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

fn instrumented_loader(loader: CacheLoader) -> (CacheLoader, LoaderRan) {
    let flag = Arc::new(AtomicBool::new(false));
    let flag_for_closure = flag.clone();
    let wrapped: CacheLoader = Box::new(move || {
        flag_for_closure.store(true, Ordering::Relaxed);
        loader()
    });
    (wrapped, LoaderRan(flag))
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
        async fn spill_and_evict(&self, _: &CachedKey) -> Result<(), crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
        async fn wait_for_spills(&self) -> Result<(), crate::Error> {
            Err(
                crate::error::SlateDBError::from(Arc::new(std::io::Error::other("injected error")))
                    .into(),
            )
        }
    }

    pub(crate) struct TestCache {
        items: Mutex<HashMap<CachedKey, CachedEntry>>,
        /// Keys "spilled" via `spill_and_evict`, simulating a disk tier. Kept separate
        /// from `items` (the simulated memory tier) so tests can assert both that the
        /// key left memory and that it landed in the (simulated) durable tier.
        spilled: Mutex<HashMap<CachedKey, CachedEntry>>,
    }

    impl TestCache {
        pub(crate) fn new() -> Self {
            Self {
                items: Mutex::new(HashMap::new()),
                spilled: Mutex::new(HashMap::new()),
            }
        }

        pub(crate) fn keys(&self) -> Vec<CachedKey> {
            self.items.lock().unwrap().keys().cloned().collect()
        }

        pub(crate) fn spilled_keys(&self) -> Vec<CachedKey> {
            self.spilled.lock().unwrap().keys().cloned().collect()
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

        async fn spill_and_evict(&self, key: &CachedKey) -> Result<(), crate::Error> {
            let removed = self.items.lock().unwrap().remove(key);
            if let Some(value) = removed {
                self.spilled.lock().unwrap().insert(key.clone(), value);
            }
            Ok(())
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
    use std::collections::HashMap;
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
        let failing_cache: Arc<dyn DbCache> = Arc::new(super::test_utils::FailingCache);
        let cache = DbCacheWrapper::new(
            failing_cache,
            &helper,
            Arc::new(DefaultSystemClock::default()),
            1,
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
        let cache_a =
            DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone(), 1);
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock, 2);
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
        let cache_a =
            DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone(), 1);
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock, 2);

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
        let cache_a =
            DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone(), 1);
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock, 2);

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

    #[test]
    fn test_evict_oldest_if_over_cap_is_a_noop_within_cap() {
        let mut touched: HashMap<SsTableId, u64> = (0..15u64)
            .map(|i| (SsTableId::Compacted(Ulid::from_parts(i, 0)), i))
            .collect();

        super::evict_oldest_if_over_cap(&mut touched, 10, 5);

        assert_eq!(
            15,
            touched.len(),
            "at cap + slack, nothing should be evicted"
        );
    }

    #[test]
    fn test_evict_oldest_if_over_cap_drops_the_oldest_by_touch_seq() {
        // 16 entries with cap=10, slack=5: over the cap+slack threshold of 15, so
        // one eviction pass runs and removes exactly the 5 oldest (seq 0..5),
        // leaving 11 (not 10 — eviction removes `slack` entries, not down to `cap`).
        let mut touched: HashMap<SsTableId, u64> = (0..16u64)
            .map(|i| (SsTableId::Compacted(Ulid::from_parts(i, 0)), i))
            .collect();

        super::evict_oldest_if_over_cap(&mut touched, 10, 5);

        assert_eq!(11, touched.len());
        for i in 0..5u64 {
            assert!(
                !touched.contains_key(&SsTableId::Compacted(Ulid::from_parts(i, 0))),
                "seq {i} is among the oldest and should have been evicted"
            );
        }
        for i in 5..16u64 {
            assert!(
                touched.contains_key(&SsTableId::Compacted(Ulid::from_parts(i, 0))),
                "seq {i} is recent and should have survived eviction"
            );
        }
    }

    #[tokio::test]
    async fn test_cache_wrapper_scopes_spill_and_evict() {
        let recorder_a = MetricsRecorderHelper::noop();
        let recorder_b = MetricsRecorderHelper::noop();
        let system_clock = Arc::new(DefaultSystemClock::default());
        let shared_cache = Arc::new(TestCache::new());
        let cache_a =
            DbCacheWrapper::new(shared_cache.clone(), &recorder_a, system_clock.clone(), 1);
        let cache_b = DbCacheWrapper::new(shared_cache.clone(), &recorder_b, system_clock, 2);

        let mut builder = BlockBuilder::new_latest(4096);
        assert!(builder.add(RowEntry::new_value(b"k1", b"v1", 0)).unwrap());
        let block = Arc::new(builder.build().unwrap());
        let key = CachedKey::from((SST_ID, 4u64));

        // given: both wrappers insert an entry under what looks like the same
        // unscoped key.
        cache_a
            .insert(key.clone(), CachedEntry::with_block(block.clone()))
            .await;
        cache_b
            .insert(key.clone(), CachedEntry::with_block(block))
            .await;
        assert_eq!(2, shared_cache.entry_count());

        // when: only wrapper A's copy is spilled.
        cache_a.spill_and_evict(&key).await.unwrap();

        // then: A's entry left the (simulated) memory tier and landed in the
        // (simulated) disk tier; B's identically-shaped key was untouched.
        assert!(cache_a.get_block(&key).await.unwrap().is_none());
        assert!(cache_b.get_block(&key).await.unwrap().is_some());
        assert_eq!(1, shared_cache.entry_count());
        assert_eq!(1, shared_cache.spilled_keys().len());
    }

    #[tokio::test]
    async fn test_split_cache_spill_and_evict_attempts_meta_cache_despite_block_cache_error() {
        let meta = Arc::new(TestCache::new());
        let split = SplitCache::new()
            .with_block_cache(Some(Arc::new(super::test_utils::FailingCache)))
            .with_meta_cache(Some(meta.clone()))
            .build();
        let key = CachedKey::from((SST_ID, 6u64));
        meta.insert(
            key.clone(),
            CachedEntry::with_sst_stats(Arc::new(crate::sst_stats::SstStats::default())),
        )
        .await;

        let result = split.spill_and_evict(&key).await;

        assert!(result.is_err(), "block cache's error should surface");
        assert!(
            meta.spilled_keys().contains(&key),
            "meta cache's spill should still run despite the block cache's error"
        );
    }

    #[tokio::test]
    async fn test_split_cache_wait_for_spills_attempts_meta_cache_despite_block_cache_error() {
        use std::sync::atomic::{AtomicBool, Ordering};

        /// Records whether `wait_for_spills` was called; every other method is an
        /// unused-in-this-test stub.
        struct SpyCache {
            wait_called: AtomicBool,
        }

        #[async_trait::async_trait]
        impl DbCache for SpyCache {
            async fn get_block(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_index(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_filter(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_stats(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn insert(&self, _: CachedKey, _: CachedEntry) {}
            async fn remove(&self, _: &CachedKey) {}
            fn entry_count(&self) -> u64 {
                0
            }
            async fn wait_for_spills(&self) -> Result<(), crate::Error> {
                self.wait_called.store(true, Ordering::SeqCst);
                Ok(())
            }
        }

        let spy = Arc::new(SpyCache {
            wait_called: AtomicBool::new(false),
        });
        let split = SplitCache::new()
            .with_block_cache(Some(Arc::new(super::test_utils::FailingCache)))
            .with_meta_cache(Some(spy.clone()))
            .build();

        let result = split.wait_for_spills().await;

        assert!(result.is_err(), "block cache's error should surface");
        assert!(
            spy.wait_called.load(Ordering::SeqCst),
            "meta cache's wait_for_spills should still run despite the block cache's error"
        );
    }

    #[tokio::test]
    async fn test_cache_wrapper_recovers_same_scope_for_same_scope_id() {
        let recorder_1 = MetricsRecorderHelper::noop();
        let recorder_2 = MetricsRecorderHelper::noop();
        let system_clock = Arc::new(DefaultSystemClock::default());
        let shared_cache: Arc<dyn DbCache> = Arc::new(TestCache::new());

        // given: a wrapper (simulating a `Db` reopen passed the same caller-supplied
        // scope_id) populates an entry...
        let first_open =
            DbCacheWrapper::new(shared_cache.clone(), &recorder_1, system_clock.clone(), 1);
        let key = CachedKey::from((SST_ID, 5u64));
        let mut builder = BlockBuilder::new_latest(4096);
        assert!(builder.add(RowEntry::new_value(b"k1", b"v1", 0)).unwrap());
        let block = Arc::new(builder.build().unwrap());
        first_open
            .insert(key.clone(), CachedEntry::with_block(block))
            .await;
        drop(first_open); // simulates `Db::close()`

        // when: a brand-new wrapper is constructed with the *same* scope_id...
        let reopened = DbCacheWrapper::new(shared_cache, &recorder_2, system_clock, 1);

        // then: it recovers the same scope, so it sees the earlier instance's entry —
        // this is what lets a caller that reuses a scope_id across a `Db` reopen see
        // disk entries an earlier instance evacuated there before closing.
        assert!(
            reopened.get_block(&key).await.unwrap().is_some(),
            "reopening with the same scope_id should recover the previous instance's entries"
        );
    }

    fn build_test_block() -> CachedEntry {
        let mut builder = BlockBuilder::new_latest(4096);
        assert!(builder.add(RowEntry::new_value(b"k1", b"v1", 0)).unwrap());
        CachedEntry::with_block(Arc::new(builder.build().unwrap()))
    }

    #[tokio::test]
    async fn test_touched_ssts_survives_a_concurrent_touch_during_a_walk() {
        let recorder = MetricsRecorderHelper::noop();
        let cache = DbCacheWrapper::new(
            Arc::new(TestCache::new()),
            &recorder,
            Arc::new(DefaultSystemClock::default()),
            1,
        );
        let key = CachedKey::from((SST_ID, 1u64));
        cache.insert(key.clone(), build_test_block()).await;
        let observed_generation = cache.touched_generation(SST_ID);
        assert_eq!(
            observed_generation,
            Some(0),
            "a fresh insert should mark the SST touched exactly once"
        );

        // Simulates a concurrent touch landing mid-walk, after this snapshot.
        cache.insert(key, build_test_block()).await;

        cache.clear_touched_if_unchanged(SST_ID, observed_generation);
        assert!(
            cache.touched_sst_ids().contains(&SST_ID),
            "a concurrent touch during the walk must not be silently dropped"
        );

        // With nothing touching it since, the clear now takes effect.
        let observed_generation = cache.touched_generation(SST_ID);
        cache.clear_touched_if_unchanged(SST_ID, observed_generation);
        assert!(
            !cache.touched_sst_ids().contains(&SST_ID),
            "an unchanged entry should be cleared once the walk finishes"
        );
    }

    #[tokio::test]
    async fn test_touched_ssts_survives_two_independent_concurrent_clearers() {
        // Two independent callers (e.g. `evict_cached_sst` racing
        // `flush_cache_to_disk`) can each snapshot the same generation and
        // clear it once their own walk finishes — see `touch_seq`'s doc for
        // the ABA hazard this must not allow.
        let recorder = MetricsRecorderHelper::noop();
        let cache = DbCacheWrapper::new(
            Arc::new(TestCache::new()),
            &recorder,
            Arc::new(DefaultSystemClock::default()),
            1,
        );
        let key = CachedKey::from((SST_ID, 1u64));
        cache.insert(key.clone(), build_test_block()).await;

        // Both A and C snapshot the same generation before either clears.
        let observed_by_a = cache.touched_generation(SST_ID);
        let observed_by_c = cache.touched_generation(SST_ID);
        assert_eq!(observed_by_a, observed_by_c);

        // C clears first — its walk was fast (e.g. a plain in-memory evict).
        cache.clear_touched_if_unchanged(SST_ID, observed_by_c);
        assert!(!cache.touched_sst_ids().contains(&SST_ID));

        // A genuine concurrent reader re-touches the SST in the gap between
        // C's clear and A's clear.
        cache.insert(key, build_test_block()).await;

        // A clears using its now-stale snapshot — must not erase the fresh touch above.
        cache.clear_touched_if_unchanged(SST_ID, observed_by_a);
        assert!(
            cache.touched_sst_ids().contains(&SST_ID),
            "a fresh touch racing two independent clearers must not be silently dropped"
        );
    }

    #[tokio::test]
    async fn test_touched_ssts_excludes_wal() {
        let recorder = MetricsRecorderHelper::noop();
        let cache = DbCacheWrapper::new(
            Arc::new(TestCache::new()),
            &recorder,
            Arc::new(DefaultSystemClock::default()),
            1,
        );
        let wal_id = SsTableId::Wal(7);
        let key = CachedKey::from((wal_id, 1u64));
        cache.insert(key, build_test_block()).await;
        assert!(
            cache.touched_sst_ids().is_empty(),
            "WAL entries must never be tracked as touched"
        );
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
            1,
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

    /// Canary for `UnownedDbCache`'s delegation: every defaulted trait method must be
    /// forwarded to the inner cache — except `close()`, which must be suppressed. If a
    /// forwarding method is dropped (e.g. when a new defaulted method is added to
    /// `DbCache`), the trait default runs against the wrapper instead of the inner
    /// cache's override and this test fails.
    #[tokio::test]
    async fn test_unowned_cache_suppresses_close_and_forwards_overrides() {
        use crate::db_cache::{CacheLoader, UnownedDbCache};
        use crate::format::block::Block;
        use std::sync::atomic::{AtomicBool, Ordering};

        fn build_block(key: &[u8]) -> Arc<Block> {
            let mut builder = BlockBuilder::new_latest(4096);
            assert!(builder.add(RowEntry::new_value(key, b"v", 0)).unwrap());
            Arc::new(builder.build().unwrap())
        }

        /// A cache whose `fetch_*` overrides always return `marker` without running the
        /// loader (like foyer's dedup-aware fetches). If the trait's default fetch runs
        /// instead, the loader's entry comes back and the marker assertion fails.
        struct ProbeCache {
            close_called: AtomicBool,
            spill_and_evict_called: AtomicBool,
            wait_for_spills_called: AtomicBool,
            marker: CachedEntry,
        }

        #[async_trait::async_trait]
        impl DbCache for ProbeCache {
            async fn get_block(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_index(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_filter(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn get_stats(&self, _: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
                Ok(None)
            }
            async fn insert(&self, _: CachedKey, _: CachedEntry) {}
            async fn remove(&self, _: &CachedKey) {}
            fn entry_count(&self) -> u64 {
                0
            }
            async fn close(&self) -> Result<(), crate::Error> {
                self.close_called.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn spill_and_evict(&self, _: &CachedKey) -> Result<(), crate::Error> {
                self.spill_and_evict_called.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn wait_for_spills(&self) -> Result<(), crate::Error> {
                self.wait_for_spills_called.store(true, Ordering::SeqCst);
                Ok(())
            }
            async fn fetch_block(
                &self,
                _: CachedKey,
                _: CacheLoader,
            ) -> Result<CachedEntry, crate::Error> {
                Ok(self.marker.clone())
            }
            async fn fetch_index(
                &self,
                _: CachedKey,
                _: CacheLoader,
            ) -> Result<CachedEntry, crate::Error> {
                Ok(self.marker.clone())
            }
            async fn fetch_filter(
                &self,
                _: CachedKey,
                _: CacheLoader,
            ) -> Result<CachedEntry, crate::Error> {
                Ok(self.marker.clone())
            }
            async fn fetch_stats(
                &self,
                _: CachedKey,
                _: CacheLoader,
            ) -> Result<CachedEntry, crate::Error> {
                Ok(self.marker.clone())
            }
        }

        let marker_block = build_block(b"marker");
        let probe = Arc::new(ProbeCache {
            close_called: AtomicBool::new(false),
            spill_and_evict_called: AtomicBool::new(false),
            wait_for_spills_called: AtomicBool::new(false),
            marker: CachedEntry::with_block(marker_block.clone()),
        });
        let unowned = UnownedDbCache::new(probe.clone());

        let loader = || -> CacheLoader {
            Box::new(|| Box::pin(async { Ok(CachedEntry::with_block(build_block(b"loader"))) }))
        };
        let key = || CachedKey::from((SST_ID, 0u64));

        let fetched = [
            unowned.fetch_block(key(), loader()).await.unwrap(),
            unowned.fetch_index(key(), loader()).await.unwrap(),
            unowned.fetch_filter(key(), loader()).await.unwrap(),
            unowned.fetch_stats(key(), loader()).await.unwrap(),
        ];
        for entry in fetched {
            assert!(
                Arc::ptr_eq(&entry.block().unwrap(), &marker_block),
                "fetch was not forwarded to the inner cache's override"
            );
        }

        unowned.close().await.unwrap();
        assert!(
            !probe.close_called.load(Ordering::SeqCst),
            "close() must not propagate to a cache slatedb does not own"
        );

        unowned.spill_and_evict(&key()).await.unwrap();
        assert!(
            probe.spill_and_evict_called.load(Ordering::SeqCst),
            "spill_and_evict was not forwarded to the inner cache"
        );

        unowned.wait_for_spills().await.unwrap();
        assert!(
            probe.wait_for_spills_called.load(Ordering::SeqCst),
            "wait_for_spills was not forwarded to the inner cache"
        );
    }
}
