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

    /// Creates a FIFO cache with a synchronous ceiling on indexed entry weight.
    ///
    /// Entries weigh at least one byte. An entry larger than the smallest shard's
    /// capacity is returned to its reader without admission. FIFO keeps indexed
    /// entries evictable even while a reader holds them. Allocations retained by
    /// readers or pending loads are outside this ceiling; it is not an RSS limit.
    pub fn new_bounded(options: FoyerCacheOptions) -> Result<Self, crate::Error> {
        let capacity = usize::try_from(options.max_capacity).map_err(|_| {
            crate::Error::invalid("bounded cache capacity exceeds usize".to_owned())
        })?;
        if capacity == 0 || options.shards == 0 || options.shards > capacity {
            return Err(crate::Error::invalid(
                "bounded cache requires positive capacity and 1 <= shards <= capacity".to_owned(),
            ));
        }
        let max_entry_weight = capacity / options.shards;
        let cache = foyer::CacheBuilder::new(capacity)
            .with_weighter(|_, v: &CachedEntry| v.size().max(1))
            .with_filter(move |_, v: &CachedEntry| v.size().max(1) <= max_entry_weight)
            .with_shards(options.shards)
            .with_eviction_config(foyer::FifoConfig::default())
            .build();
        Ok(Self { inner: cache })
    }

    /// Current indexed weight, excluding entries retained after eviction and
    /// allocations held by readers or pending loads. Shards are sampled separately.
    pub fn indexed_weight(&self) -> u64 {
        self.inner.usage() as u64
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
    use crate::format::sst::BlockBuilder;
    use crate::types::RowEntry;
    use ulid::Ulid;

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

#[cfg(test)]
mod bounded_tests {
    use super::*;
    use crate::db_state::SsTableId;
    use crate::error::ErrorKind;
    use crate::format::block::Block;
    use crate::format::sst::SsTableFormat;
    use crate::sst_stats::SstStats;
    use crate::types::RowEntry;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{watch, Barrier};
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

    fn bounded(capacity: u64, shards: usize) -> FoyerCache {
        FoyerCache::new_bounded(FoyerCacheOptions {
            max_capacity: capacity,
            shards,
        })
        .unwrap()
    }

    fn shard_keys(cache: &FoyerCache, shard: usize, count: usize) -> Vec<CachedKey> {
        let keys: Vec<_> = (0..100_000)
            .map(key)
            .filter(|key| cache.inner.hash(key) as usize % cache.inner.shards() == shard)
            .take(count)
            .collect();
        assert_eq!(keys.len(), count);
        keys
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

    #[test]
    fn bounded_rejects_invalid_options() {
        for (max_capacity, shards) in [(0, 1), (1, 0), (7, 8)] {
            let err = FoyerCache::new_bounded(FoyerCacheOptions {
                max_capacity,
                shards,
            })
            .err()
            .expect("invalid options were admitted");
            assert_eq!(err.kind(), ErrorKind::Invalid);
        }
    }

    #[test]
    #[cfg(target_pointer_width = "32")]
    fn bounded_rejects_capacity_conversion_overflow() {
        let err = FoyerCache::new_bounded(FoyerCacheOptions {
            max_capacity: u64::from(u32::MAX) + 1,
            shards: 1,
        })
        .err()
        .expect("unrepresentable capacity was admitted");
        assert_eq!(err.kind(), ErrorKind::Invalid);
    }

    #[tokio::test]
    async fn bounded_direct_admission_respects_smallest_shard() {
        let cache = bounded(11, 3);
        let largest_shard = shard_keys(&cache, 0, 2);
        cache.insert(largest_shard[0].clone(), block(3)).await;
        assert_eq!(cache.indexed_weight(), 3);
        assert!(cache.get_block(&largest_shard[0]).await.unwrap().is_some());

        // Shard zero has four bytes, but admission is the common three-byte floor.
        cache.insert(largest_shard[1].clone(), block(4)).await;
        assert!(cache.get_block(&largest_shard[1]).await.unwrap().is_none());
        assert_eq!(cache.indexed_weight(), 3);

        cache.insert(largest_shard[0].clone(), block(12)).await;
        assert!(cache.get_block(&largest_shard[0]).await.unwrap().is_none());
        assert_eq!(cache.indexed_weight(), 0);
    }

    #[tokio::test]
    async fn bounded_zero_size_entries_consume_weight() {
        let cache = bounded(7, 3);
        for shard in 0..3 {
            for key in shard_keys(&cache, shard, 20) {
                cache
                    .insert(key, CachedEntry::with_filters(Arc::from([])))
                    .await;
                assert!(cache.indexed_weight() <= 7);
            }
        }
        assert_eq!(cache.inner.entries(), 7);
        assert_eq!(cache.indexed_weight(), 7);
    }

    #[tokio::test]
    async fn bounded_fifo_evicts_entries_held_by_readers() {
        let cache = bounded(10, 1);
        for id in 0..2 {
            cache.insert(key(id), block(5)).await;
        }
        // Retain Foyer entries to hold the same acquisition that the adapter
        // briefly holds while cloning a value, even if that thread is preempted.
        let first = cache.inner.get(&key(0)).unwrap();
        let second = cache.inner.get(&key(1)).unwrap();
        let payload = cache.get_block(&key(0)).await.unwrap().unwrap();
        cache.insert(key(2), block(5)).await;
        assert!(cache.indexed_weight() <= 10);
        assert!(cache.get_block(&key(0)).await.unwrap().is_none());
        assert_eq!(
            first.value().block().unwrap().data,
            payload.block().unwrap().data
        );
        assert_eq!(second.value().size(), 5);
        assert_eq!(payload.size(), 5);
    }

    #[tokio::test]
    async fn bounded_fetches_deduplicate_and_return_unindexed_oversized_values() {
        for (kind, entry) in entries().await.into_iter().enumerate() {
            let weight = entry.size().max(1) as u64;
            assert!(weight > 1);
            for admit in [true, false] {
                let capacity = if admit { weight } else { weight - 1 };
                let cache = Arc::new(bounded(capacity, 1));
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
                assert_eq!(cache.indexed_weight(), if admit { weight } else { 0 });
                assert_eq!(cache.inner.get(&key(0)).is_some(), admit);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bounded_concurrent_admission_keeps_weight_within_capacity() {
        let cache = Arc::new(bounded(37, 3));
        let mut held = Vec::new();
        for shard in 0..3 {
            for key in shard_keys(&cache, shard, 2) {
                cache.insert(key.clone(), block(6)).await;
                held.push(cache.inner.get(&key).unwrap());
            }
        }
        assert_eq!(cache.indexed_weight(), 36);
        let start = Arc::new(Barrier::new(9));
        let mut tasks = Vec::new();
        for worker in 0..8 {
            let cache = cache.clone();
            let start = start.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                let mut payloads = Vec::new();
                for iteration in 0..32 {
                    let key = key(1_000_000 + worker * 32 + iteration);
                    let entry = block(if iteration % 3 == 0 { 13 } else { 6 });
                    if iteration % 2 == 0 {
                        cache.insert(key, entry).await;
                    } else {
                        let returned = cache
                            .fetch_block(key, Box::new(move || Box::pin(async move { Ok(entry) })))
                            .await
                            .unwrap();
                        payloads.push(returned);
                    }
                    assert!(cache.indexed_weight() <= 37);
                    tokio::task::yield_now().await;
                }
                payloads
            }));
        }
        start.wait().await;
        for task in tasks {
            assert!(!task.await.unwrap().is_empty());
        }
        assert!(cache.indexed_weight() <= 37);
        assert!(held.iter().all(|entry| entry.value().size() == 6));
    }

    #[tokio::test]
    async fn ordinary_constructor_keeps_existing_oversized_admission() {
        let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 4,
            shards: 1,
        });
        cache.insert(key(0), block(6)).await;
        assert_eq!(cache.indexed_weight(), 6);
        assert!(cache.get_block(&key(0)).await.unwrap().is_some());
    }
}
