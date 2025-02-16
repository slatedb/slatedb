use crate::stats::{Counter, Gauge, StatRegistry};
use std::sync::Arc;

macro_rules! oscache_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("gc", $suffix)
    };
}

pub const OBJECT_STORE_CACHE_PART_HITS: &str = oscache_stat_name!("part_hits");
pub const OBJECT_STORE_CACHE_PART_ACCESS: &str = oscache_stat_name!("part_access");
pub const OBJECT_STORE_CACHE_KEYS: &str = oscache_stat_name!("cache_keys");
pub const OBJECT_STORE_CACHE_BYTES: &str = oscache_stat_name!("cache_bytes");
pub const OBJECT_STORE_CACHE_EVICTED_KEYS: &str = oscache_stat_name!("evicted_keys");
pub const OBJECT_STORE_CACHE_EVICTED_BYTES: &str = oscache_stat_name!("evicted_bytes");

#[derive(Debug, Clone)]
pub(crate) struct CachedObjectStoreStats {
    pub(super) object_store_cache_part_hits: Arc<Counter>,
    pub(super) object_store_cache_part_access: Arc<Counter>,
    pub(super) object_store_cache_keys: Arc<Gauge<u64>>,
    pub(super) object_store_cache_bytes: Arc<Gauge<u64>>,
    pub(super) object_store_cache_evicted_keys: Arc<Counter>,
    pub(super) object_store_cache_evicted_bytes: Arc<Counter>,
}

impl CachedObjectStoreStats {
    pub(crate) fn new(registry: &StatRegistry) -> Self {
        let stats = Self {
            object_store_cache_part_hits: Arc::new(Counter::default()),
            object_store_cache_part_access: Arc::new(Counter::default()),
            object_store_cache_bytes: Arc::new(Gauge::default()),
            object_store_cache_keys: Arc::new(Gauge::default()),
            object_store_cache_evicted_bytes: Arc::new(Counter::default()),
            object_store_cache_evicted_keys: Arc::new(Counter::default()),
        };
        registry.register(
            OBJECT_STORE_CACHE_PART_HITS,
            stats.object_store_cache_part_hits.clone(),
        );
        registry.register(
            OBJECT_STORE_CACHE_PART_ACCESS,
            stats.object_store_cache_part_access.clone(),
        );
        registry.register(
            OBJECT_STORE_CACHE_KEYS,
            stats.object_store_cache_keys.clone(),
        );
        registry.register(
            OBJECT_STORE_CACHE_BYTES,
            stats.object_store_cache_bytes.clone(),
        );
        registry.register(
            OBJECT_STORE_CACHE_EVICTED_BYTES,
            stats.object_store_cache_evicted_bytes.clone(),
        );
        registry.register(
            OBJECT_STORE_CACHE_EVICTED_KEYS,
            stats.object_store_cache_evicted_keys.clone(),
        );
        stats
    }
}
