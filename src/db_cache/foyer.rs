//! # Foyer Cache
//!
//! This module provides an implementation of an in-memory cache using the Foyer library.
//! The cache is designed to store and retrieve cached blocks associated with SSTable IDs.
//! It supports configurable capacity and block size.
//!
//! ## Features
//!
//! - **Asynchronous Operations**: Utilizes Foyer's `Cache` to perform cache operations asynchronously.
//! - **Custom Weigher**: Implements a custom weigher to account for the size of cached blocks.
//! - **Flexible Configuration**: Allows customization of cache parameters such as maximum capacity and block size.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use slatedb::db::Db;
//! use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
//! use object_store::local::LocalFileSystem;
//! use std::path::Path;
//! use std::sync::Arc;
//!
//! let object_store = Arc::new(LocalFileSystem::new());
//! let cache = Arc::new(FoyerCache::new());
//! let db = Db::open_with_cache(Path::from("path/to/db"), object_store, cache).await;
//! ```
//!
use crate::db_cache::{
    CachedBlock, CachedEntry, DbCache, SsTableId, DEFAULT_CACHED_BLOCK_SIZE, DEFAULT_MAX_CAPACITY,
};
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
            max_capacity: DEFAULT_MAX_CAPACITY,
            cached_block_size: DEFAULT_CACHED_BLOCK_SIZE,
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
    pub fn new() -> Self {
        Self::new_with_opts(FoyerCacheOptions::default())
    }

    pub fn new_with_opts(options: FoyerCacheOptions) -> Self {
        let builder = foyer::CacheBuilder::new(options.max_capacity as _)
            .with_weighter(move |_, _| options.cached_block_size as _);

        let cache = builder.build();

        Self { inner: cache }
    }
}

impl Default for FoyerCache {
    fn default() -> Self {
        Self::new()
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
