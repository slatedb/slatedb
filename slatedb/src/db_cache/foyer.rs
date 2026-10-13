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
//!         .with_db_cache(Arc::new(FoyerCache::new()), 0)
//!         .build()
//!         .await?;
//!     Ok(())
//! }
//! ```
//!

use crate::db_cache::{
    instrumented_loader, CacheFetch, CacheLoader, CacheLookup, CachedEntry, CachedKey, DbCache,
    DEFAULT_MAX_CAPACITY,
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

    /// Wraps a Foyer cache configured by the caller.
    ///
    /// To measure capacity in bytes, use
    /// `.with_weighter(|_, v: &CachedEntry| v.size())`. Without a custom
    /// weigher, capacity counts entries. Keep a clone of the cache to read
    /// `usage()` after passing it here.
    ///
    /// ```
    /// use slatedb::db_cache::foyer::FoyerCache;
    /// use slatedb::db_cache::{CachedEntry, CachedKey};
    ///
    /// let cache: foyer::Cache<CachedKey, CachedEntry> = foyer::CacheBuilder::new(512 << 20)
    ///     .with_weighter(|_, v: &CachedEntry| v.size().max(1))
    ///     .with_filter(|_, v: &CachedEntry| v.size() <= 4 << 20)
    ///     .with_eviction_config(foyer::FifoConfig::default())
    ///     .build();
    /// let indexed = cache.clone();
    /// let db_cache = FoyerCache::new_with_cache(cache);
    /// assert_eq!(indexed.usage(), 0);
    /// ```
    pub fn new_with_cache(cache: foyer::Cache<CachedKey, CachedEntry>) -> Self {
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
    ) -> Result<CacheFetch, crate::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_index(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, crate::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_filter(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, crate::Error> {
        self.dedup_fetch(key, loader).await
    }

    async fn fetch_stats(
        &self,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CacheFetch, crate::Error> {
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
    ) -> Result<CacheFetch, crate::Error> {
        let (loader, loader_ran) = instrumented_loader(loader);
        let fetch = self
            .inner
            .get_or_fetch(&key, move || async move { loader().await });
        match fetch.await {
            Ok(entry) => Ok(CacheFetch {
                entry: entry.value().clone(),
                lookup: if loader_ran.was_called() {
                    CacheLookup::Miss
                } else {
                    CacheLookup::Hit
                },
            }),
            Err(err) => Err(SlateDBError::FoyerError(Arc::new(err)).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_state::SsTableId;
    use crate::format::block::Block;
    use crate::format::sst::{BlockBuilder, SsTableFormat};
    use crate::sst_stats::SstStats;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::watch;
    use ulid::Ulid;

    fn key(id: u64) -> CachedKey {
        CachedKey::from((SsTableId::new(Ulid::from_parts(1, 1)), id))
    }

    fn block(weight: usize) -> CachedEntry {
        assert!(weight >= 2);
        CachedEntry::with_block(Arc::new(Block {
            data: Bytes::from(vec![42; weight - 2]),
            offsets: vec![],
        }))
    }

    /// A caller-built FIFO cache that refuses any entry heavier than a shard's share of
    /// `capacity`. Foyer splits capacity evenly across shards and indexes an entry heavier
    /// than its shard anyway, so a filter keyed to the whole capacity only holds the
    /// ceiling with one shard.
    fn bounded(capacity: usize, shards: usize) -> FoyerCache {
        let share = capacity / shards;
        FoyerCache::new_with_cache(
            foyer::CacheBuilder::new(capacity)
                .with_weighter(|_, v: &CachedEntry| v.size().max(1))
                .with_filter(move |_, v: &CachedEntry| v.size().max(1) <= share)
                .with_shards(shards)
                .with_eviction_config(foyer::FifoConfig::default())
                .build(),
        )
    }

    async fn entries() -> [CachedEntry; 4] {
        let format = SsTableFormat::default();
        let mut builder = format.table_builder();
        builder
            .add(RowEntry::new_value(b"key", b"value", 1))
            .await
            .unwrap();
        let sst = builder.build().await.unwrap();
        let bytes = sst.remaining_as_bytes();
        let index = format.read_index_raw(&sst.info, &bytes).await.unwrap();
        [
            block(32),
            CachedEntry::with_sst_index(Arc::new(index)),
            CachedEntry::with_filters(sst.filters),
            CachedEntry::with_sst_stats(Arc::new(SstStats::default())),
        ]
    }

    async fn fetch(
        cache: &FoyerCache,
        kind: usize,
        key: CachedKey,
        loader: CacheLoader,
    ) -> Result<CachedEntry, crate::Error> {
        let fetched = match kind {
            0 => cache.fetch_block(key, loader).await,
            1 => cache.fetch_index(key, loader).await,
            2 => cache.fetch_filter(key, loader).await,
            3 => cache.fetch_stats(key, loader).await,
            _ => unreachable!(),
        };
        fetched.map(|f| f.entry)
    }

    #[tokio::test]
    async fn new_with_cache_keeps_the_callers_admission_and_eviction() {
        let cache = bounded(10, 1);
        cache.insert(key(0), block(6)).await;
        assert_eq!(cache.inner.usage(), 6);
        assert!(cache.get_block(&key(0)).await.unwrap().is_some());

        // Heavier than the cache: the filter refuses it and nothing is indexed.
        cache.insert(key(1), block(12)).await;
        assert!(cache.get_block(&key(1)).await.unwrap().is_none());
        assert_eq!(cache.inner.usage(), 6);

        // Fits the cache but not beside the first entry: FIFO evicts the first.
        cache.insert(key(2), block(6)).await;
        assert!(cache.get_block(&key(0)).await.unwrap().is_none());
        assert!(cache.get_block(&key(2)).await.unwrap().is_some());
        assert_eq!(cache.inner.usage(), 6);
    }

    /// Keys hash to shards, so the ceiling must hold whichever shard an entry lands in.
    #[tokio::test]
    async fn new_with_cache_holds_the_ceiling_across_shards() {
        let cache = bounded(10, 2);

        // Lighter than the cache but heavier than a shard: refused rather than indexed
        // over the shard's share.
        cache.insert(key(0), block(6)).await;
        assert!(cache.get_block(&key(0)).await.unwrap().is_none());
        assert_eq!(cache.inner.usage(), 0);

        // Every shard ends up holding one entry of its share and no more.
        for id in 1..=64 {
            cache.insert(key(id), block(5)).await;
            assert!(cache.inner.usage() <= 10);
        }
        assert_eq!(cache.inner.usage(), 10);
    }

    /// A refused value must still reach every waiter, loaded once, for each entry kind.
    #[tokio::test]
    async fn fetches_deduplicate_and_return_values_the_filter_refuses() {
        for (kind, entry) in entries().await.into_iter().enumerate() {
            let weight = entry.size().max(1);
            assert!(weight > 1);
            for admit in [true, false] {
                let cache = Arc::new(bounded(if admit { weight } else { weight - 1 }, 1));
                let loads = Arc::new(AtomicUsize::new(0));
                let (release, ready) = watch::channel(false);
                let mut pending = Vec::new();
                for _ in 0..8 {
                    let cache = cache.clone();
                    let loads = loads.clone();
                    let entry = entry.clone();
                    let mut ready = ready.clone();
                    let loader: CacheLoader = Box::new(move || {
                        Box::pin(async move {
                            loads.fetch_add(1, Ordering::SeqCst);
                            ready.wait_for(|ready| *ready).await.unwrap();
                            Ok(entry)
                        })
                    });
                    let mut future =
                        Box::pin(async move { fetch(&cache, kind, key(0), loader).await.unwrap() });
                    assert!(futures::poll!(future.as_mut()).is_pending());
                    pending.push(future);
                }
                release.send_replace(true);
                for returned in futures::future::join_all(pending).await {
                    assert_eq!(returned.size(), entry.size());
                    assert!(match kind {
                        0 => Arc::ptr_eq(&returned.block().unwrap(), &entry.block().unwrap()),
                        1 =>
                            Arc::ptr_eq(&returned.sst_index().unwrap(), &entry.sst_index().unwrap()),
                        2 => Arc::ptr_eq(&returned.filters().unwrap(), &entry.filters().unwrap()),
                        3 =>
                            Arc::ptr_eq(&returned.sst_stats().unwrap(), &entry.sst_stats().unwrap()),
                        _ => unreachable!(),
                    });
                }
                assert_eq!(loads.load(Ordering::SeqCst), 1);
                assert_eq!(cache.inner.usage(), if admit { weight } else { 0 });
                assert_eq!(cache.inner.get(&key(0)).is_some(), admit);
            }
        }
    }

    #[tokio::test]
    async fn test_fetch_lookup_from_memory() {
        let cache = FoyerCache::new();
        let key = CachedKey::from((SsTableId::new(Ulid::new()), 0));
        let mut builder = BlockBuilder::new_latest(4096);
        assert!(builder
            .add(RowEntry::new_value(b"key", b"value", 0))
            .unwrap());
        let block = Arc::new(builder.build().unwrap());
        let entry = CachedEntry::with_block(block.clone());
        let loader: CacheLoader = Box::new(move || Box::pin(async move { Ok(entry) }));

        let first = cache.fetch_block(key.clone(), loader).await.unwrap();
        assert_eq!(first.lookup, CacheLookup::Miss);
        assert!(Arc::ptr_eq(&first.entry.block().unwrap(), &block));
        let second = cache
            .fetch_block(
                key,
                Box::new(|| panic!("A cache hit must not run the loader.")),
            )
            .await
            .unwrap();
        assert_eq!(second.lookup, CacheLookup::Hit);
        assert!(Arc::ptr_eq(&second.entry.block().unwrap(), &block));
        cache.close().await.unwrap();
    }
}
