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

use crate::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache, DEFAULT_MAX_CAPACITY};
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
    ) -> Result<CachedEntry, crate::Error> {
        self.dedup_fetch(key, loader).await
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache};
    use crate::db_state::SsTableId;
    use crate::format::sst::BlockBuilder;
    use tokio::sync::Notify;
    use ulid::Ulid;

    use super::FoyerCache;

    const SST_ID: SsTableId = SsTableId::Compacted(Ulid::from_parts(0u64, 0u128));

    fn build_block_entry() -> CachedEntry {
        let mut builder = BlockBuilder::new_latest(1024);
        assert!(builder
            .add(crate::types::RowEntry::new_value(b"k", b"v", 0))
            .unwrap());
        CachedEntry::with_block(Arc::new(builder.build().unwrap()))
    }

    /// Verify that foyer's fetch path deduplicates concurrent loads.
    ///
    /// Determinism is anchored by two synchronization points:
    ///
    /// 1. A `Notify` (`loader_started`) signaled from inside A's loader — after we observe
    ///    this we know A's loader is running, which in turn means A has already registered
    ///    itself in foyer's `waiters` map (the registration happens synchronously on
    ///    fetch_block's first poll, before the loader task is spawned).
    /// 2. `tokio::join!(task_b, release_task)` polls members in source order. The first
    ///    iteration polls B's `fetch_block` future, whose first poll synchronously enters
    ///    foyer's `fetch_queue` and finds A's existing entry — joining as a `Wait` rather
    ///    than spawning a second loader. Only afterwards does `release_task` run and
    ///    release A's loader.
    ///
    /// If dedup is broken, B's panicking loader runs and fails the test.
    #[tokio::test]
    async fn should_dedup_concurrent_fetches() {
        // given: a foyer cache with no entry for the key
        let cache = Arc::new(FoyerCache::new());
        let key = CachedKey::from((SST_ID, 1u64));
        let loader_calls = Arc::new(AtomicUsize::new(0));
        let loader_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        // caller A: triggers the (only) loader, which parks until release fires
        let handle_a = tokio::spawn({
            let cache = cache.clone();
            let key = key.clone();
            let counter = loader_calls.clone();
            let started = loader_started.clone();
            let release = release.clone();
            async move {
                let loader: CacheLoader = Box::new(move || {
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        started.notify_one();
                        release.notified().await;
                        Ok(build_block_entry())
                    })
                });
                cache.fetch_block(key, loader).await
            }
        });

        // wait until A's loader is in flight, proving A is registered in foyer's waiter map
        loader_started.notified().await;
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);

        // when: caller B races A for the same key, with a loader that panics if invoked
        let task_b = {
            let cache = cache.clone();
            let key = key.clone();
            let counter = loader_calls.clone();
            async move {
                let loader: CacheLoader = Box::new(move || {
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        panic!("B's loader must not run while A's load is in flight");
                    })
                });
                cache.fetch_block(key, loader).await
            }
        };
        let release_task = {
            let release = release.clone();
            async move {
                // join! polls task_b first, which synchronously registers B as a waiter
                // on A's load before this branch runs. yield_now ensures task_b has had a
                // chance to reach the dedup check even if it isn't reached on the first
                // poll path (current foyer implementation hits it synchronously).
                tokio::task::yield_now().await;
                release.notify_one();
            }
        };
        let (b_result, _) = tokio::join!(task_b, release_task);

        // then: B observed A's in-flight load, did not run its own loader, and got a value
        let a_result = handle_a.await.expect("task A panicked");
        assert!(a_result.is_ok(), "caller A failed: {:?}", a_result.err());
        assert!(b_result.is_ok(), "caller B failed: {:?}", b_result.err());
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "exactly one loader should run across both concurrent fetches"
        );
    }
}
