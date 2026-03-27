//! # Statistics Module
//!
//! Provides backward-compatible access to SlateDB metrics through [`Db::metrics`].
//! Internally delegates to the [`MetricsRecorder`] framework in `slatedb-common`.
//!
//! [`StatRegistry`] wraps a [`DefaultMetricsRecorder`] and exposes snapshot-based
//! lookup so that `db.metrics().lookup(name).unwrap().get()` continues to work.
//!
//! [`Db::metrics`]: crate::db::Db::metrics

use std::sync::Arc;

use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

/// The type of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    UpDownCounter,
    Histogram,
}

/// A readable metric value. Implemented by [`SnapshotStat`] which captures
/// a point-in-time value from the underlying recorder.
pub trait ReadableStat: Send + Sync + std::fmt::Debug {
    fn get(&self) -> i64;
    fn metric_type(&self) -> MetricType;
}

/// A frozen metric value captured from a [`DefaultMetricsRecorder`] snapshot.
#[derive(Debug)]
struct SnapshotStat {
    value: i64,
    metric_type: MetricType,
}

impl ReadableStat for SnapshotStat {
    fn get(&self) -> i64 {
        self.value
    }

    fn metric_type(&self) -> MetricType {
        self.metric_type
    }
}

/// Backward-compatible wrapper around [`DefaultMetricsRecorder`].
///
/// Provides `lookup()` and `names()` by snapshotting the recorder internally.
/// Values returned from `lookup()` are frozen at the time of the call.
pub struct StatRegistry {
    default_recorder: Arc<DefaultMetricsRecorder>,
}

impl StatRegistry {
    pub(crate) fn new(default_recorder: Arc<DefaultMetricsRecorder>) -> Self {
        Self { default_recorder }
    }

    /// Get a metric with a specific name, or `None` if no metric was registered
    /// for the name.
    ///
    /// Returns a snapshot value — the returned [`ReadableStat`] captures the
    /// value at the time of this call.
    pub fn lookup(&self, name: &str) -> Option<Arc<dyn ReadableStat>> {
        let snapshot = self.default_recorder.snapshot();
        let metric = snapshot.by_name(name).into_iter().next()?;
        let (value, metric_type) = match &metric.value {
            MetricValue::Counter(v) => (*v as i64, MetricType::Counter),
            MetricValue::Gauge(v) => (*v, MetricType::Gauge),
            MetricValue::UpDownCounter(v) => (*v, MetricType::UpDownCounter),
            MetricValue::Histogram { count, .. } => (*count as i64, MetricType::Histogram),
        };
        Some(Arc::new(SnapshotStat { value, metric_type }))
    }

    /// List all registered metric names.
    pub fn names(&self) -> Vec<String> {
        let snapshot = self.default_recorder.snapshot();
        snapshot.all().iter().map(|m| m.name.clone()).collect()
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
    use slatedb_common::metrics::{DefaultMetricsRecorder, MetricsRecorder};

    #[test]
    fn test_should_lookup_stat_by_name() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let counter = recorder.register_counter("stat1", "", &[]);
        counter.increment(1);
        let gauge = recorder.register_gauge("stat2", "", &[]);
        gauge.set(2);

        let registry = StatRegistry::new(recorder);
        assert_eq!(registry.lookup("stat1").unwrap().get(), 1);
        assert_eq!(registry.lookup("stat2").unwrap().get(), 2);
        assert!(registry.lookup("stat3").is_none());
    }

    #[test]
    fn test_should_list_registered_stats() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        recorder.register_counter("stat1", "", &[]);
        recorder.register_counter("stat2", "", &[]);
        recorder.register_counter("stat3", "", &[]);

        let registry = StatRegistry::new(recorder);
        let mut names = registry.names();
        names.sort();
        assert_eq!(names, vec!["stat1", "stat2", "stat3"]);
    }

    #[test]
    fn test_counter_via_registry() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let counter = recorder.register_counter("counter", "", &[]);
        counter.increment(1);

        let registry = StatRegistry::new(recorder);
        assert_eq!(registry.lookup("counter").unwrap().get(), 1);
    }

    #[test]
    fn test_gauge_via_registry() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let gauge = recorder.register_gauge("gauge", "", &[]);
        gauge.set(42);

        let registry = StatRegistry::new(recorder);
        assert_eq!(registry.lookup("gauge").unwrap().get(), 42);
    }

    #[test]
    fn test_up_down_counter_via_registry() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let udc = recorder.register_up_down_counter("udc", "", &[]);
        udc.increment(10);
        udc.increment(-3);

        let registry = StatRegistry::new(recorder);
        assert_eq!(registry.lookup("udc").unwrap().get(), 7);
    }
}
