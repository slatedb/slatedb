//! # Foyer Cache
//!
//! This module provides an implementation of an in-memory cache using the Foyer library.
//! The cache is designed to store and retrieve cached blocks, indexes, and filters
//! associated with SSTable IDs.
//!
//! ## Features
//!
//! - **Asynchronous Operations**: Utilizes Foyer's `Cache` to perform cache operations asynchronously.
//! - **Custom Weigher**: Implements a custom weigher to account for the size of cached blocks.
//! - **Flexible Configuration**: Allows customization of cache parameters such as maximum capacity.
//!
//! ## Examples
//!
//!
//! ```
//! use slatedb::{Db, Error};
//! use slatedb::db_cache::foyer::FoyerCache;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::builder("test_db", object_store)
//!         .with_db_cache(Arc::new(FoyerCache::new()))
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!

use crate::db_cache::{
    instrumented_loader, CacheLoader, CachedEntry, CachedKey, DbCache, DEFAULT_MAX_CAPACITY,
};
use crate::error::SlateDBError;
use async_trait::async_trait;
use std::sync::Arc;
use sysinfo::{CpuRefreshKind, System};

/// The options for the Foyer cache.
#[derive(Clone, Copy, Debug)]
pub struct FoyerCacheOptions {
    pub max_capacity: u64,
    pub shards: usize,
}

impl Default for FoyerCacheOptions {
    fn default() -> Self {
        Self {
            max_capacity: DEFAULT_MAX_CAPACITY,
            shards: {
                let mut sys = System::new();
                sys.refresh_cpu_specifics(CpuRefreshKind::nothing());
                sys.cpus().len()
            },
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
/// * `inner` - The underlying Foyer cache instance, which maps `CachedKey`
///   keys to `CachedEntry` values.
///
/// # Notes
///
/// The cache is configured based on the provided `FoyerCacheOptions`,
/// including settings for the maximum capacity of the cache.
/// It uses a custom weigher to account for the size of cached blocks.
pub struct FoyerCache {
    inner: foyer::Cache<CachedKey, CachedEntry>,
}

impl FoyerCache {
    pub fn new() -> Self {
        Self::new_with_opts(FoyerCacheOptions::default())
    }

    pub fn new_with_opts(options: FoyerCacheOptions) -> Self {
        let cache = foyer::CacheBuilder::new(options.max_capacity as _)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .with_shards(options.shards)
            .build();
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
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        Ok(self.inner.get(key).map(|entry| entry.value().clone()))
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value);
    }

    async fn remove(&self, key: &CachedKey) {
        self.inner.remove(key);
    }

    fn entry_count(&self) -> u64 {
        // foyer cache doesn't support an entry count estimate
        0
    }

    async fn fetch_block(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<(CachedEntry, bool), crate::Error> {
        let (loader, loader_ran) = instrumented_loader(loader);
        self.dedup_fetch(key, loader)
            .await
            .map(|entry| (entry, !loader_ran.was_called()))
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        self.dedup_fetch(key, loader).await
    }
}

impl FoyerCache {
    /// Use foyer's `Cache::get_or_fetch`, which deduplicates concurrent loads for the same key.
    ///
    /// Loader errors round-trip via anyhow's source chain on the foyer error. Foyer wraps them
    /// as `ErrorKind::External` (see foyer-memory's raw.rs). We don't try to recover the original
    /// `crate::Error` value: foyer's broadcast path makes one-to-one recovery impossible for
    /// concurrent waiters, so all error returns are normalized to `SlateDBError::FoyerError`
    /// with the original chained as a source.
    async fn dedup_fetch(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let fetch = self
            .inner
            .get_or_fetch(&key, move || async move { loader().await });
        match fetch.await {
            Ok(entry) => Ok(entry.value().clone()),
            Err(err) => Err(SlateDBError::FoyerError(Arc::new(err)).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
    use crate::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache};
    use crate::db_state::SsTableId;
    use crate::filter_policy::{BloomFilterPolicy, FilterPolicy, NamedFilter};
    use crate::types::{RowEntry, ValueDeletable};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use ulid::Ulid;

    const SST_ID: SsTableId = SsTableId::new(Ulid::from_parts(123, 0));

    fn filter_entry() -> CachedEntry {
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
        CachedEntry::with_filters(Arc::from([named]))
    }

    fn counting_filter_loader(calls: Arc<AtomicUsize>) -> CacheLoader {
        Box::new(move || {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(filter_entry())
            })
        })
    }

    #[tokio::test]
    async fn test_fetch_filter_reports_miss_then_hit() {
        let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 1024 * 1024,
            shards: 1,
        });
        let key = CachedKey::from((SST_ID, 12345u64));
        let loader_calls = Arc::new(AtomicUsize::new(0));

        let (entry, cached) = cache
            .fetch_filter(key.clone(), counting_filter_loader(loader_calls.clone()))
            .await
            .unwrap();

        assert!(!cached);
        assert_eq!(1, entry.filters().unwrap().len());
        assert_eq!(1, loader_calls.load(Ordering::SeqCst));

        loader_calls.store(0, Ordering::SeqCst);
        let (entry, cached) = cache
            .fetch_filter(key, counting_filter_loader(loader_calls.clone()))
            .await
            .unwrap();

        assert!(cached);
        assert_eq!(1, entry.filters().unwrap().len());
        assert_eq!(0, loader_calls.load(Ordering::SeqCst));
    }
}
