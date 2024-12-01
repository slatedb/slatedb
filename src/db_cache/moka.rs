//! # Moka Cache
//!
//! This module provides an implementation of an in-memory cache using the Moka library.
//! The cache is designed to store and retrieve cached blocks, indexes, and bloom filters
//! associated with SSTable IDs.
//!
//! ## Features
//!
//! - **Asynchronous Operations**: Utilizes Moka's `future::Cache` to perform cache operations asynchronously.
//! - **Custom Weigher**: Implements a custom weigher to account for the size of cached blocks.
//! - **Flexible Configuration**: Allows customization of cache parameters such as maximum capacity, TTL, and TTI.
//!
//! ## Examples
//!
//! ```rust,no_run
//! use object_store::local::LocalFileSystem;
//! use slatedb::db::Db;
//! use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
//! use slatedb::config::DbOptions;
//! use std::sync::Arc;
//!
//! #[::tokio::main]
//! async fn main() {
//!     let object_store = Arc::new(LocalFileSystem::new());
//!     let options = DbOptions {
//!         block_cache: Some(Arc::new(MokaCache::new())),
//!         ..Default::default()
//!     };
//!     let db = Db::open_with_opts("path/to/db", options, object_store).await;
//! }
//! ```
//!
use crate::db_cache::{CachedEntry, CachedKey, DbCache, DEFAULT_MAX_CAPACITY};
use async_trait::async_trait;
use std::time::Duration;

/// The options for the Moka cache.
#[derive(Clone, Copy, Debug)]
pub struct MokaCacheOptions {
    pub max_capacity: u64,
    pub time_to_live: Option<Duration>,
    pub time_to_idle: Option<Duration>,
}

impl Default for MokaCacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: DEFAULT_MAX_CAPACITY,
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
/// * `inner` - The underlying Moka cache instance, which maps `CachedKey`
///   keys to `CachedEntry` values.
///
/// # Notes
///
/// The cache is configured based on the provided `MokaCacheOptions`,
/// including settings for capacity, time-to-live (TTL), and time-to-idle (TTI).
/// It uses a custom weigher to account for the size of cached blocks.
pub struct MokaCache {
    inner: moka::future::Cache<CachedKey, CachedEntry>,
}

impl MokaCache {
    pub fn new() -> Self {
        Self::new_with_opts(MokaCacheOptions::default())
    }

    pub fn new_with_opts(options: MokaCacheOptions) -> Self {
        let mut builder = moka::future::Cache::builder()
            .weigher(|_, v: &CachedEntry| v.size() as u32)
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

impl Default for MokaCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DbCache for MokaCache {
    async fn get(&self, key: CachedKey) -> Option<CachedEntry> {
        self.inner.get(&key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value).await;
    }

    async fn remove(&self, key: CachedKey) {
        self.inner.remove(&key).await;
    }

    fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }
}
