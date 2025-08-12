//! # Foyer Hybrid Cache
//!
//! This module provides an implementation of a hybrid (in-memory + on-disk) cache using the Foyer
//! library. The cache is designed to store and retrieve cached blocks, indexes, and bloom filters
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
//! use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder};
//! use slatedb::Db;
//! use slatedb::db_cache::CachedEntry;
//! use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
//! use slatedb::object_store::local::LocalFileSystem;
//! use std::sync::Arc;
//!
//! #[::tokio::main]
//! async fn main() {
//!     let object_store = Arc::new(LocalFileSystem::new());
//!     let cache = HybridCacheBuilder::new()
//!             .with_name("hybrid_cache")
//!             .memory(1024)
//!             .with_weighter(|_, v: &CachedEntry| v.size())
//!             .storage(Engine::Large)
//!             .with_device_options(
//!                 DirectFsDeviceOptions::new("/tmp/slatedb-cache").with_capacity(1024 * 1024))
//!             .build()
//!             .await
//!             .unwrap();
//!     let cache = Arc::new(FoyerHybridCache::new_with_cache(cache));
//!     let db = Db::builder("path/to/db", object_store)
//!         .with_block_cache(cache)
//!         .build()
//!         .await;
//! }
//! ```
//!

use std::sync::Arc;

use crate::db_cache::{CachedEntry, CachedKey, DbCache};
use crate::error::SlateDBError::FoyerCacheReadingError;
use async_trait::async_trait;

pub struct FoyerHybridCache {
    inner: foyer::HybridCache<CachedKey, CachedEntry>,
}

impl FoyerHybridCache {
    pub fn new_with_cache(cache: foyer::HybridCache<CachedKey, CachedEntry>) -> Self {
        Self { inner: cache }
    }
}

impl FoyerHybridCache {
    async fn get(&self, key: CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.inner
            .get(&key)
            .await
            .map_err(|e| FoyerCacheReadingError(Arc::new(e.into())).into())
            .map(|maybe_v| maybe_v.map(|v| v.value().clone()))
    }
}

#[async_trait]
impl DbCache for FoyerHybridCache {
    async fn get_block(&self, key: CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn get_index(&self, key: CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn get_filter(&self, key: CachedKey) -> Result<Option<CachedEntry>, crate::Error> {
        self.get(key).await
    }

    async fn insert(&self, key: CachedKey, value: CachedEntry) {
        self.inner.insert(key, value);
    }

    async fn remove(&self, key: CachedKey) {
        self.inner.remove(&key);
    }

    fn entry_count(&self) -> u64 {
        // foyer cache doesn't support an entry count estimate
        0
    }
}

#[cfg(test)]
mod tests {
    use crate::block::BlockBuilder;
    use crate::db_cache::foyer_hybrid::FoyerHybridCache;
    use crate::db_cache::{CachedEntry, CachedKey, DbCache};
    use crate::db_state::SsTableId;
    use crate::types::RowAttributes;
    use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder};
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
            let k = CachedKey(SST_ID, b);
            let v = build_block();
            cache.insert(k.clone(), v.clone()).await;
            items.insert(k, v);
        }
        let mut found = 0;
        let mut notfound = 0;
        for (k, v) in items {
            let cached_v = cache.get_block(k).await.unwrap();
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
        let mut builder = BlockBuilder::new(1024);
        loop {
            let mut k = vec![0u8; 32];
            rng.fill_bytes(&mut k);
            let mut v = vec![0u8; 128];
            rng.fill_bytes(&mut v);
            if builder.add_value(
                &k,
                &v,
                RowAttributes {
                    ts: None,
                    expire_ts: None,
                },
            ) {
                break;
            }
        }
        let block = Arc::new(builder.build().unwrap());
        CachedEntry::with_block(block)
    }

    async fn setup() -> (FoyerHybridCache, TempDir) {
        let tempdir = tempdir().unwrap();
        let cache = HybridCacheBuilder::new()
            .with_name("hybrid_cache_test")
            .memory(1024)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage(Engine::large())
            .with_device_options(
                DirectFsDeviceOptions::new(tempdir.path()).with_capacity(1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        (FoyerHybridCache::new_with_cache(cache), tempdir)
    }
}
