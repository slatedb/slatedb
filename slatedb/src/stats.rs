//! # Statistics Module
//!
//! Rather than integrate with observability platforms such as Prometheus or InfluxDB,
//! SlateDB exposes metrics through [`Db::metrics`]. Applications can get the registry
//! and poll it periodically to expose SlateDB metrics to their observability systems.
//!
//! This module provides a flexible and thread-safe metrics collection system for tracking
//! and monitoring various runtime statistics in SlateDB.
//!
//! ## Components
//!
//! * [`ReadableStat`]: Core trait implemented by all metric types, providing a way to read
//!   the current value as an `i64`.
//!
//! * [`StatRegistry`]: Central repository for registering and looking up metrics by name.
//!   Provides atomic, thread-safe access to all registered metrics.
//!
//! * [`Counter`]: Atomic counter for tracking incrementing values.
//!
//! * [`Gauge<T>`]: Generic value holder for any type that implements `NoUninit + Debug`.
//!   Special implementations exist for common types like `i64`, `u64`, `i32`, and `bool`.
//!   Gauges for numeric types provide additional operations like [`add()`], [`sub()`], etc.
//!
//! * [`stat_name!`]: Macro for standardizing metric name formats by combining a prefix
//!   and suffix with a separator.
//!
//! ## Thread Safety
//!
//! All metric types are designed to be thread-safe.
//!
//! ## Usage Examples
//!
//! See [`crate::compactor::stats`] for examples of how to use the metrics in a SlateDB
//! component.
//!
//! [`Db::metrics`]: crate::db::Db::metrics
//!
//! Example:
//!
//! Read compactor stats:
//!
//! ```
//! use slatedb::{Db, error::SlateDBError};
//! use slatedb::object_store::memory::InMemory;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let object_store = Arc::new(InMemory::new());
//!     let db = Db::open("test_db", object_store).await?;
//!     let metrics = db.metrics();
//!     if let Some(bytes_compacted) = metrics.lookup("bytes_compacted") {
//!         println!("bytes_compacted: {}", bytes_compacted.get());
//!     }
//!     Ok(())
//! }
//! ```
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use atomic::{Atomic, Ordering};
use bytemuck::NoUninit;
use tracing::warn;

pub trait ReadableStat: Send + Sync + std::fmt::Debug {
    fn get(&self) -> i64;
}

pub struct StatRegistry {
    stats: Mutex<BTreeMap<&'static str, Arc<dyn ReadableStat>>>,
}

impl StatRegistry {
    pub(crate) fn new() -> Self {
        Self {
            stats: Mutex::new(BTreeMap::new()),
        }
    }

    /// Get a metric with a specific name, or `None` if no metric was registered
    /// for the name.
    pub fn lookup(&self, name: &'static str) -> Option<Arc<dyn ReadableStat>> {
        let guard = self.stats.lock().expect("lock poisoned");
        guard.get(name).cloned()
    }

    pub fn names(&self) -> Vec<&'static str> {
        let guard = self.stats.lock().expect("lock poisoned");
        guard.keys().copied().collect()
    }

    /// Register a new metric with the registry.
    pub(crate) fn register(&self, name: &'static str, stat: Arc<dyn ReadableStat>) {
        let mut guard = self.stats.lock().expect("lock poisoned");
        debug_assert!(!guard.contains_key(name));
        if guard.contains_key(name) {
            warn!(
                "registry already contains metric with name: {}. will not register again",
                name
            );
            return;
        }
        guard.insert(name, stat);
    }
}

#[derive(Clone)]
pub struct Counter {
    pub(crate) value: Arc<Atomic<u64>>,
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.value.load(Ordering::Relaxed))
    }
}

impl ReadableStat for Counter {
    fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed) as i64
    }
}

impl Counter {
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

#[derive(Clone)]
pub struct Gauge<T: std::fmt::Debug + NoUninit> {
    value: Arc<Atomic<T>>,
}

impl<T: std::fmt::Debug + NoUninit> std::fmt::Debug for Gauge<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.value.load(Ordering::Relaxed))
    }
}

impl ReadableStat for Gauge<i32> {
    fn get(&self) -> i64 {
        self.value() as i64
    }
}

impl ReadableStat for Gauge<i64> {
    fn get(&self) -> i64 {
        self.value()
    }
}

impl ReadableStat for Gauge<u64> {
    fn get(&self) -> i64 {
        self.value() as i64
    }
}

impl ReadableStat for Gauge<bool> {
    fn get(&self) -> i64 {
        self.value() as i64
    }
}

impl<T: NoUninit + std::fmt::Debug> Gauge<T> {
    pub fn set(&self, value: T) -> T {
        self.value.swap(value, Ordering::Relaxed)
    }

    pub fn value(&self) -> T {
        self.value.load(Ordering::Relaxed)
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

#[macro_export]
macro_rules! stat_name {
    ($prefix:expr, $suffix:expr) => {
        concat!($prefix, "/", $suffix)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_stat_name {
        ($suffix:expr) => {
            crate::stat_name!("test", $suffix)
        };
    }

    #[test]
    fn test_should_generate_stat_name_with_prefix() {
        assert_eq!(test_stat_name!("foobar"), "test/foobar");
    }

    #[test]
    fn test_should_lookup_stat_by_name() {
        let registry = StatRegistry::new();
        let stat1 = Arc::new(Gauge::<i32>::default());
        stat1.set(1);
        registry.register("stat1", stat1);
        let stat2 = Arc::new(Gauge::<i32>::default());
        stat2.set(2);
        registry.register("stat2", stat2);

        assert_eq!(registry.lookup("stat1").unwrap().get(), 1);
        assert_eq!(registry.lookup("stat2").unwrap().get(), 2);
        assert!(registry.lookup("stat3").is_none());
    }

    #[test]
    fn test_should_list_registered_stats() {
        let registry = StatRegistry::new();
        let stat1 = Arc::new(Gauge::<i32>::default());
        registry.register("stat1", stat1);
        let stat2 = Arc::new(Gauge::<i32>::default());
        registry.register("stat2", stat2);
        let stat3 = Arc::new(Gauge::<i32>::default());
        registry.register("stat3", stat3);

        let names = registry.names();
        assert_eq!(names, vec!["stat1", "stat2", "stat3"]);
    }

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
        assert_eq!(gauge.get(), bool::default() as i64);
        gauge.set(true);
        assert_eq!(gauge.get(), true as i64);
        gauge.set(false);
        assert_eq!(gauge.get(), false as i64);
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
