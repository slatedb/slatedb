use std::sync::Arc;

use atomic::{Atomic, Ordering};
use bytemuck::NoUninit;

#[derive(Clone, Debug)]
pub struct Counter {
    pub value: Arc<Atomic<u64>>,
}

impl Counter {
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }

    pub fn add(&self, value: u64) -> u64 {
        self.value.fetch_add(value, Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self {
            value: Arc::new(Atomic::<u64>::default()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Gauge<T: std::fmt::Debug + NoUninit> {
    value: Arc<Atomic<T>>,
}

impl<T: NoUninit + std::fmt::Debug> Gauge<T> {
    pub fn get(&self) -> T {
        self.value.load(Ordering::Relaxed)
    }

    pub fn set(&self, value: T) -> T {
        self.value.swap(value, Ordering::Relaxed)
    }
}

impl<T: Default + NoUninit + std::fmt::Debug> Default for Gauge<T> {
    fn default() -> Self {
        Self {
            value: Arc::new(Atomic::<T>::default()),
        }
    }
}

impl Gauge<i64> {
    pub fn add(&self, value: i64) -> i64 {
        self.value.fetch_add(value, Ordering::Relaxed)
    }

    pub fn inc(&self) -> i64 {
        self.add(1)
    }

    pub fn sub(&self, value: i64) -> i64 {
        self.value.fetch_add(-value, Ordering::Relaxed)
    }

    pub fn dec(&self) -> i64 {
        self.sub(1)
    }
}

#[derive(Clone, Debug)]
pub struct DbStats {
    pub immutable_memtable_flushes: Counter,
    pub last_compaction_ts: Gauge<u64>,
    pub gc_manifest_count: Counter,
    pub gc_wal_count: Counter,
    pub gc_compacted_count: Counter,
    pub gc_count: Counter,
    pub object_store_cache_part_hits: Counter,
    pub object_store_cache_write_num: Counter,
    pub object_store_cache_part_access: Counter,
    pub object_store_cache_keys: Gauge<u64>,
    pub object_store_cache_bytes: Gauge<u64>,
    pub object_store_cache_evicted_keys: Counter,
    pub object_store_cache_evicted_bytes: Counter,
    pub running_compactions: Gauge<i64>,
    pub bytes_compacted: Counter,
}

impl DbStats {
    pub(crate) fn new() -> Self {
        Self {
            immutable_memtable_flushes: Counter::default(),
            last_compaction_ts: Gauge::default(),
            gc_manifest_count: Counter::default(),
            gc_wal_count: Counter::default(),
            gc_compacted_count: Counter::default(),
            gc_count: Counter::default(),
            object_store_cache_part_hits: Counter::default(),
            object_store_cache_write_num: Counter::default(),
            object_store_cache_part_access: Counter::default(),
            object_store_cache_bytes: Gauge::default(),
            object_store_cache_keys: Gauge::default(),
            object_store_cache_evicted_bytes: Counter::default(),
            object_store_cache_evicted_keys: Counter::default(),
            running_compactions: Gauge::default(),
            bytes_compacted: Counter::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::default();
        counter.inc();
        assert_eq!(counter.get(), 1);
        counter.inc();
        assert_eq!(counter.get(), 2);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::default();
        gauge.set(42);
        assert_eq!(gauge.get(), 42);
        gauge.set(24);
        assert_eq!(gauge.get(), 24);
    }

    #[test]
    fn test_gauge_bool() {
        let gauge = Gauge::<bool>::default();
        assert_eq!(gauge.get(), bool::default());
        gauge.set(true);
        assert!(gauge.get());
        gauge.set(false);
        assert!(!gauge.get());
    }

    #[test]
    fn test_gauge_i64() {
        let gauge = Gauge::<i64>::default();
        assert_eq!(gauge.get(), 0);
        gauge.add(200);
        assert_eq!(gauge.get(), 200);
        gauge.sub(42);
        assert_eq!(gauge.get(), 158);
    }
}
