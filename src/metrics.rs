use atomic::{Atomic, Ordering};
use bytemuck::NoUninit;
use std::sync::Arc;

#[derive(Clone)]
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
}

impl Default for Counter {
    fn default() -> Self {
        Self {
            value: Arc::new(Atomic::<u64>::default()),
        }
    }
}

#[derive(Clone)]
pub struct Gauge<T> {
    value: Arc<Atomic<T>>,
}

impl<T: NoUninit> Gauge<T> {
    pub fn get(&self) -> T {
        self.value.load(Ordering::Relaxed)
    }

    pub fn set(&self, value: T) -> T {
        self.value.swap(value, Ordering::Relaxed)
    }
}

impl<T: Default> Default for Gauge<T> {
    fn default() -> Self {
        Self {
            value: Arc::new(Atomic::<T>::default()),
        }
    }
}

#[derive(Clone)]
pub struct DbStats {
    pub immutable_memtable_flushes: Counter,
    pub last_compaction_ts: Gauge<u64>,
}

impl DbStats {
    pub(crate) fn new() -> Self {
        Self {
            immutable_memtable_flushes: Counter::default(),
            last_compaction_ts: Gauge::default(),
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
}
