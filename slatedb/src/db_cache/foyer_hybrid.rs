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

use crate::db_cache::{CachedEntry, CachedKey, DbCache};
use crate::SlateDBError;
use crate::SlateDBError::DbCacheError;
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
    async fn get(&self, key: CachedKey) -> Result<Option<CachedEntry>, SlateDBError> {
        self.inner
            .get(&key)
            .await
            .map_err(|e| DbCacheError { msg: e.to_string() })
            .map(|maybe_v| maybe_v.map(|v| v.value().clone()))
    }
}

#[async_trait]
impl DbCache for FoyerHybridCache {
    async fn get_block(&self, key: CachedKey) -> Result<Option<CachedEntry>, SlateDBError> {
        self.get(key).await
    }

    async fn get_index(&self, key: CachedKey) -> Result<Option<CachedEntry>, SlateDBError> {
        self.get(key).await
    }

    async fn get_filter(&self, key: CachedKey) -> Result<Option<CachedEntry>, SlateDBError> {
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
        let mut rng = crate::rand::thread_rng();
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
            .storage(Engine::Large)
            .with_device_options(
                DirectFsDeviceOptions::new(tempdir.path()).with_capacity(1024 * 1024),
            )
            .build()
            .await
            .unwrap();
        (FoyerHybridCache::new_with_cache(cache), tempdir)
    }
}
