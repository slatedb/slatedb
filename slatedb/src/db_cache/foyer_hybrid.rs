//! # Foyer Hybrid Cache
//!
//! This module provides an implementation of a hybrid (in-memory + on-disk) cache using the Foyer
//! library. The cache is designed to store and retrieve cached blocks, indexes, and filters
//! associated with SSTable IDs.
//!
//! ## Features
//!
//! - **Hybrid Cache**: Caches blocks using a tiered cache that stores data across memory and
//!   local disk.
//! - **Custom Weigher**: Implements a custom weigher to account for the size of cached blocks.
//! - **Flexible Configuration**: The HybridCache instance is passed to the cache, so you are free
//!   to configure it as needed for your use case.
//!
//! ## Notes
//! Foyer HybridCache manages its own memory and disk cache, so when it coexists with `CachedObjectStore`
//! there may be some duplication of cached data. In other words, disk cache may suffer from write
//! amplification. If you don't want to suffer from write amplification, then we recommend that you do not
//! coexist Foyer HybridCache with `CachedObjectStore`.
//!
//! Benefits of coexistence:
//! `CachedObjectStore` provides prefetching, which may speed up your read performance and
//! reduce unnecessary object store accesses.
//!
//! Drawbacks of coexistence:
//! Disk may suffer from write amplification. Data will first be written to `CachedObjectStore`, then
//! flow into the memory part of Foyer HybridCache, and finally written back to the disk area managed
//! by Foyer HybridCache itself. slateDB cannot control Foyer HybridCache to write data to its disk area.
//!
//! ## Examples
//!
//! ```
//! use foyer::{
//!     BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
//! };
//! use slatedb::Db;
//! use slatedb::db_cache::CachedEntry;
//! use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[::tokio::main]
//! async fn main() {
//!     let object_store = Arc::new(InMemory::new());
//!     let cache = HybridCacheBuilder::new()
//!         .with_name("hybrid_cache")
//!         .memory(1024 * 1024)
//!         .with_weighter(|_, v: &CachedEntry| v.size())
//!         .storage()
//!         .with_io_engine_config(PsyncIoEngineConfig::new())
//!         .with_engine_config(
//!             BlockEngineConfig::new(
//!                 FsDeviceBuilder::new("/tmp/slatedb-cache")
//!                     .with_capacity(1024 * 1024)
//!                     .build()
//!                     .unwrap(),
//!             )
//!             .with_block_size(64 * 1024),
//!         )
//!         .build()
//!         .await
//!         .unwrap();
//!     let cache = Arc::new(FoyerHybridCache::new_with_cache(cache));
//!     let db = Db::builder("path/to/db", object_store)
//!         .with_db_cache(cache, 0)
//!         .build()
//!         .await
//!         .unwrap();
//! }
//! ```
//!

use crate::{
    db_cache::{CacheLoader, CachedEntry, CachedKey, DbCache},
    error::SlateDBError,
    utils::format_bytes_si,
};
use async_trait::async_trait;
use log::info;
use std::sync::Arc;

pub struct FoyerHybridCache {
    inner: foyer::HybridCache<CachedKey, CachedEntry>,
}

impl FoyerHybridCache {
    pub fn new_with_cache(cache: foyer::HybridCache<CachedKey, CachedEntry>) -> Self {
        Self { inner: cache }
    }
}

impl FoyerHybridCache {
    async fn get(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner
            .get(key)
            .await
            .map_err(|e| SlateDBError::from(e).into())
            .map(|maybe_v| maybe_v.map(|v| v.value().clone()))
    }
}

#[async_trait]
impl DbCache for FoyerHybridCache {
    async fn get_block(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn get_index(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn get_filter(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn get_stats(&self, key: &CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
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

    async fn close(&self) -> Result<(), crate::Error> {
        let memory_bytes = self.inner.memory().usage();
        info!(
            "foyer hybrid cache: closing \
             (flushing {} from memory to disk)",
            format_bytes_si(memory_bytes as u64)
        );
        let result = self
            .inner
            .close()
            .await
            .map_err(|e| crate::Error::from(SlateDBError::from(e)));
        match &result {
            Ok(()) => info!("foyer hybrid cache: closed successfully"),
            Err(e) => info!("foyer hybrid cache: close failed [error={e:?}]"),
        }
        result
    }

    /// Best-effort: `Ok(())` means handed off to foyer's disk-write queue, not
    /// durably landed. `force = true` bypasses the admission filter but not
    /// foyer 0.22.3's closed-engine or submit-queue-overflow drops — neither
    /// surfaces as an error, and `enqueue` returns `()` so there's nothing
    /// more this method can do. Two real mitigations for a deployment sharing
    /// one cache across many `Db`s (this crate's own intended pattern):
    /// `BlockEngineConfig::with_submit_queue_size_threshold` to size the
    /// shared queue for that fan-in, and `HybridCacheBuilder::with_metrics_registry`
    /// so the `channel_overflow` counter foyer increments on every drop is
    /// observable instead of discarded (the default registry is a no-op).
    async fn spill_and_evict(&self, key: &CachedKey) -> Result<(), crate::Error> {
        // `memory().remove` returns the entry (holding the value) only if it was
        // actually resident; a non-resident key is a no-op, so we never fabricate
        // a disk entry for something we never had.
        if let Some(entry) = self.inner.memory().remove(key) {
            // `force = true` pushes the piece past the admission filter so a hot
            // block reaches disk even if the filter would otherwise sample it out.
            self.inner.storage().enqueue(entry.piece(), true);
        }
        Ok(())
    }

    async fn wait_for_spills(&self) -> Result<(), crate::Error> {
        self.inner.storage().wait().await;
        Ok(())
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

impl FoyerHybridCache {
    /// Use foyer's `HybridCache::get_or_fetch`, which deduplicates concurrent loads across
    /// memory + disk tiers. See [`crate::db_cache::foyer::FoyerCache::dedup_fetch`] for the
    /// error-handling rationale.
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
    use crate::db_cache::foyer_hybrid::FoyerHybridCache;
    use crate::db_cache::{CachedEntry, CachedKey, DbCache};
    use crate::db_state::SsTableId;
    use crate::format::sst::BlockBuilder;
    use foyer::{
        BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, PsyncIoEngineConfig,
    };
    use rand::RngCore;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    const SST_ID: SsTableId = SsTableId::Wal(123);

    #[tokio::test]
    async fn test_hybrid_cache() {
        let (cache, _dir) = setup().await;
        let mut items = HashMap::new();
        for b in 0u64..256 {
            let k = CachedKey::from((SST_ID, b));
            let v = build_block();
            cache.insert(k.clone(), v.clone()).await;
            items.insert(k, v);
        }
        let mut found = 0;
        let mut notfound = 0;
        for (k, v) in items {
            let cached_v = cache.get_block(&k).await.unwrap();
            if let Some(cached_v) = cached_v {
                assert!(v.block().unwrap().as_ref() == cached_v.block().unwrap().as_ref());
                found += 1;
            } else {
                notfound += 1;
            }
        }
        println!("found: {}, notfound: {}", found, notfound);
        assert!(found > 0);
    }

    fn build_block() -> CachedEntry {
        let mut rng = rand::rng();
        let mut builder = BlockBuilder::new_latest(1024);
        loop {
            let mut k = vec![0u8; 32];
            rng.fill_bytes(&mut k);
            let mut v = vec![0u8; 128];
            rng.fill_bytes(&mut v);
            if builder.add_value(&k, &v, None, None) {
                break;
            }
        }
        let block = Arc::new(builder.build().unwrap());
        CachedEntry::with_block(block)
    }

    async fn setup() -> (FoyerHybridCache, TempDir) {
        let tempdir = tempdir().unwrap();
        let cache = open_cache(tempdir.path()).await;
        (cache, tempdir)
    }

    async fn open_cache(path: &std::path::Path) -> FoyerHybridCache {
        let cache = HybridCacheBuilder::new()
            .with_name("hybrid_cache_test")
            .memory(1024 * 1024)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(path)
                        .with_capacity(4 * 1024 * 1024)
                        .build()
                        .unwrap(),
                )
                .with_block_size(64 * 1024),
            )
            .build()
            .await
            .unwrap();
        FoyerHybridCache::new_with_cache(cache)
    }

    /// Like `open_cache`, but with the submit-queue ceiling dropped to zero,
    /// so a second `spill_and_evict` before the first drains is guaranteed
    /// (not just probable) to hit foyer's queue-overflow gate.
    async fn open_cache_with_zero_submit_queue(path: &std::path::Path) -> FoyerHybridCache {
        let cache = HybridCacheBuilder::new()
            .with_name("hybrid_cache_test_zero_submit_queue")
            .memory(1024 * 1024)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(
                BlockEngineConfig::new(
                    FsDeviceBuilder::new(path)
                        .with_capacity(4 * 1024 * 1024)
                        .build()
                        .unwrap(),
                )
                .with_block_size(64 * 1024)
                .with_submit_queue_size_threshold(0),
            )
            .build()
            .await
            .unwrap();
        FoyerHybridCache::new_with_cache(cache)
    }

    #[tokio::test]
    async fn should_silently_drop_a_spill_when_the_submit_queue_is_full() {
        // given: a cache with no submit-queue headroom and two resident entries
        let dir = tempdir().unwrap();
        let cache = open_cache_with_zero_submit_queue(dir.path()).await;
        let key1 = CachedKey::from((SST_ID, 1u64));
        let key2 = CachedKey::from((SST_ID, 2u64));
        cache.insert(key1.clone(), build_block()).await;
        cache.insert(key2.clone(), build_block()).await;

        // when: spilling both without waiting between them. foyer accounts
        // for queue space synchronously on submit, so the second call is
        // guaranteed to see the queue as full.
        let first = cache.spill_and_evict(&key1).await;
        let second = cache.spill_and_evict(&key2).await;
        cache.wait_for_spills().await.unwrap();

        // then: both report success (`enqueue` returns `()`, nothing to
        // surface), despite key2 being silently dropped.
        assert!(first.is_ok());
        assert!(second.is_ok(), "Ok(()) despite the silent drop");
        assert!(
            cache.inner.memory().get(&key1).is_none() && cache.inner.memory().get(&key2).is_none(),
            "spill_and_evict always removes from memory regardless of enqueue outcome"
        );
        let loaded1 = cache.inner.storage().load(&key1).await.unwrap();
        assert!(
            loaded1.entry().is_some(),
            "the first spill had queue headroom and should have landed on disk"
        );
        let loaded2 = cache.inner.storage().load(&key2).await.unwrap();
        assert!(
            loaded2.entry().is_none(),
            "the second spill was silently dropped by foyer's queue-overflow gate: \
             gone from both tiers despite Ok(())"
        );
    }

    #[tokio::test]
    async fn should_persist_blocks_to_disk_on_close() {
        // given: a cache with entries
        let dir = tempdir().unwrap();
        let cache = open_cache(dir.path()).await;
        let mut keys = Vec::new();
        for b in 0u64..64 {
            let k = CachedKey::from((SST_ID, b));
            cache.insert(k.clone(), build_block()).await;
            keys.push(k);
        }

        // when: close (flush to disk) and reopen
        cache.close().await.unwrap();
        drop(cache);
        let cache = open_cache(dir.path()).await;

        // then: all entries should be recoverable from disk
        let mut found = 0;
        for k in &keys {
            if cache.get_block(k).await.unwrap().is_some() {
                found += 1;
            }
        }
        assert_eq!(found, keys.len(), "all entries should survive close+reopen");
    }

    #[tokio::test]
    async fn should_move_resident_key_from_memory_to_disk_on_spill() {
        // given: a cache with one resident entry
        let dir = tempdir().unwrap();
        let cache = open_cache(dir.path()).await;
        let key = CachedKey::from((SST_ID, 1u64));
        cache.insert(key.clone(), build_block()).await;
        assert!(
            cache.inner.memory().get(&key).is_some(),
            "entry should be resident in memory right after insert"
        );

        // when: spilling (without ever calling close())
        cache.spill_and_evict(&key).await.unwrap();
        cache.wait_for_spills().await.unwrap();

        // then: gone from memory, landed on disk
        assert!(
            cache.inner.memory().get(&key).is_none(),
            "entry should have been evicted from the memory tier"
        );
        let loaded = cache.inner.storage().load(&key).await.unwrap();
        assert!(
            loaded.entry().is_some(),
            "entry should have been spilled to the disk tier"
        );
    }

    #[tokio::test]
    async fn should_noop_spill_and_evict_for_non_resident_key() {
        // given: an empty cache
        let dir = tempdir().unwrap();
        let cache = open_cache(dir.path()).await;
        let key = CachedKey::from((SST_ID, 1u64));

        // when: spilling a key that was never inserted
        cache.spill_and_evict(&key).await.unwrap();
        cache.wait_for_spills().await.unwrap();

        // then: no entry is fabricated on disk
        assert!(cache.inner.memory().get(&key).is_none());
        let loaded = cache.inner.storage().load(&key).await.unwrap();
        assert!(
            loaded.is_miss(),
            "spilling a non-resident key must not create a disk entry"
        );
    }

    #[tokio::test]
    async fn should_leave_other_keys_untouched_when_spilling_one_key() {
        // given: two resident entries
        let dir = tempdir().unwrap();
        let cache = open_cache(dir.path()).await;
        let spilled_key = CachedKey::from((SST_ID, 1u64));
        let kept_key = CachedKey::from((SST_ID, 2u64));
        cache.insert(spilled_key.clone(), build_block()).await;
        cache.insert(kept_key.clone(), build_block()).await;

        // when: only one key is spilled
        cache.spill_and_evict(&spilled_key).await.unwrap();
        cache.wait_for_spills().await.unwrap();

        // then: the other key is still resident in memory and was never
        // written to disk
        assert!(
            cache.inner.memory().get(&kept_key).is_some(),
            "untouched key should remain resident in memory"
        );
        let loaded = cache.inner.storage().load(&kept_key).await.unwrap();
        assert!(
            loaded.is_miss(),
            "untouched key should not have been spilled to disk"
        );
    }
}
