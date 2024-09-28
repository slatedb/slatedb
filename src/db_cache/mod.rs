//! # DB Cache
//!
//! This module provides an in-memory caching solution for storing and retrieving
//! cached blocks, index and bloom filters associated with SSTable IDs.
//!
//! There are currently two built-in cache implementations:
//! - [Foyer](crate::db_cache::foyer::FoyerCache): Requires the `foyer` feature flag.
//! - [Moka](crate::db_cache::moka::MokaCache): Requires the `moka` feature flag. (Enabled by default)
//!
//! ## Usage
//!
//! To use the cache, you need to configure the [DbOptions](crate::config::DbOptions) with the desired cache implementation.
//!
use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    block::Block, db_state::SsTableId, filter::BloomFilter, flatbuffer_types::SsTableIndexOwned,
};

/// The default max capacity for the cache. (64MB)
pub const DEFAULT_MAX_CAPACITY: u64 = 64 * 1024 * 1024;
/// The default cached block size for the cache. (32 bytes)
pub const DEFAULT_CACHED_BLOCK_SIZE: u32 = 32;

#[cfg(feature = "foyer")]
pub mod foyer;
#[cfg(feature = "moka")]
pub mod moka;

/// A trait for in-memory caches.
///
/// This trait defines the interface for an in-memory cache,
/// which is used to store and retrieve cached blocks associated with SSTable IDs.
///
/// Example:
///
/// ```rust,no_run,compile_fail
/// use async_trait::async_trait;
/// use object_store::local::LocalFileSystem;
/// use slatedb::db::Db;
/// use slatedb::config::DbOptions;
/// use slatedb::db_cache::{DbCache, CachedEntry};
/// use slatedb::db_state::SsTableId;
/// use ssc::HashMap;
/// use std::sync::Arc;
///
/// struct MyCache {
///     inner: HashMap<(SsTableId, u64), CachedEntry>,
/// }
///
/// impl MyCache {
///     pub fn new() -> Self {
///         Self {
///             inner: HashMap::new(),
///         }
///     }
/// }
///
/// #[async_trait]
/// impl DbCache for MyCache {
///     async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry> {
///         self.inner.get_async(&key).await.cloned()
///     }
///
///     async fn insert(&self, key: (SsTableId, u64), value: CachedEntry) {
///         self.inner.insert_async(key, value).await;
///     }
///
///     async fn remove(&self, key: (SsTableId, u64)) {
///         self.inner.remove_async(&key).await;
///     }
///
///     fn entry_count(&self) -> u64 {
///         self.inner.len() as u64
///     }
/// }
///
/// #[::tokio::main]
/// async fn main() {
///     let object_store = Arc::new(LocalFileSystem::new());
///     let options = DbOptions {
///         block_cache: Some(Arc::new(MyCache::new())),
///         ..Default::default()
///     };
///     let db = Db::open_with_opts("path/to/db".into(), options, object_store).await;
/// }
/// ```
#[async_trait]
pub trait DbCache: Send + Sync {
    async fn get(&self, key: (SsTableId, u64)) -> Option<CachedEntry>;
    async fn insert(&self, key: (SsTableId, u64), value: CachedEntry);
    #[allow(dead_code)]
    async fn remove(&self, key: (SsTableId, u64));
    #[allow(dead_code)]
    fn entry_count(&self) -> u64;
}

#[derive(Clone)]
enum CachedItem {
    Block(Arc<Block>),
    SsTableIndex(Arc<SsTableIndexOwned>),
    BloomFilter(Arc<BloomFilter>),
}

/// A cached entry stored in the cache.
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
}
