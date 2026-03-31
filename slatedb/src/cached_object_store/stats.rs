use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper};
use std::sync::Arc;

macro_rules! oscache_stat_name {
    ($suffix:expr) => {
        concat!("oscache", "/", $suffix)
    };
}

pub const OBJECT_STORE_CACHE_PART_HITS: &str = oscache_stat_name!("part_hits");
pub const OBJECT_STORE_CACHE_PART_ACCESS: &str = oscache_stat_name!("part_access");
pub const OBJECT_STORE_CACHE_KEYS: &str = oscache_stat_name!("cache_keys");
pub const OBJECT_STORE_CACHE_BYTES: &str = oscache_stat_name!("cache_bytes");
pub const OBJECT_STORE_CACHE_EVICTED_KEYS: &str = oscache_stat_name!("evicted_keys");
pub const OBJECT_STORE_CACHE_EVICTED_BYTES: &str = oscache_stat_name!("evicted_bytes");

#[derive(Debug, Clone)]
pub struct CachedObjectStoreStats {
    pub(super) object_store_cache_part_hits: Arc<dyn CounterFn>,
    pub(super) object_store_cache_part_access: Arc<dyn CounterFn>,
    pub(super) object_store_cache_keys: Arc<dyn GaugeFn>,
    pub(super) object_store_cache_bytes: Arc<dyn GaugeFn>,
    pub(super) object_store_cache_evicted_keys: Arc<dyn CounterFn>,
    pub(super) object_store_cache_evicted_bytes: Arc<dyn CounterFn>,
}

impl CachedObjectStoreStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> Self {
        Self {
            object_store_cache_part_hits: recorder.counter(OBJECT_STORE_CACHE_PART_HITS).register(),
            object_store_cache_part_access: recorder
                .counter(OBJECT_STORE_CACHE_PART_ACCESS)
                .register(),
            object_store_cache_keys: recorder.gauge(OBJECT_STORE_CACHE_KEYS).register(),
            object_store_cache_bytes: recorder.gauge(OBJECT_STORE_CACHE_BYTES).register(),
            object_store_cache_evicted_keys: recorder
                .counter(OBJECT_STORE_CACHE_EVICTED_KEYS)
                .register(),
            object_store_cache_evicted_bytes: recorder
                .counter(OBJECT_STORE_CACHE_EVICTED_BYTES)
                .register(),
        }
    }
}
