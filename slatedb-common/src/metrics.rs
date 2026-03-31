//! # Metrics Module
//!
//! A recorder-based metrics system that supports labels, histograms, and
//! user-pluggable backends (Prometheus, OTLP, etc.) while preserving a
//! zero-dependency, always-on default for debugging.
//!
//! ## Architecture
//!
//! - [`MetricsRecorder`] is the user-facing trait for bridging metrics to
//!   observability backends.
//! - [`DefaultMetricsRecorder`] is the built-in atomic-backed recorder that
//!   powers metric snapshots via [`DefaultMetricsRecorder::snapshot`].
//! - [`NoopMetricsRecorder`] discards all registrations and returns zero-cost
//!   no-op handles. This is the default when no user recorder is configured.
//! - [`MetricsRecorderHelper`] provides a builder API with level filtering
//!   for internal metric registration.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Boundary constants
// ---------------------------------------------------------------------------

/// Standard histogram boundaries for latency metrics (in seconds).
/// Based on Prometheus defaults (`0.005..=10.0`) with an additional 1ms
/// bucket prepended for sub-5ms resolution.
pub const LATENCY_BOUNDARIES: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Standard histogram boundaries for size metrics (in bytes).
/// Powers of two from 128B to 4MB, tailored for storage engine I/O sizes.
pub const SIZE_BOUNDARIES: &[f64] = &[
    128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
];

// ---------------------------------------------------------------------------
// Core traits
// ---------------------------------------------------------------------------

/// User-implemented trait to bridge metrics to an observability system
/// (Prometheus, OTLP, etc.). If not provided, SlateDB uses
/// [`NoopMetricsRecorder`] and does not publish metrics.
pub trait MetricsRecorder: Send + Sync {
    /// Register a monotonically increasing counter.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name (e.g. `"slatedb.compaction.bytes"`).
    /// - `description` – Human-readable description of the metric.
    /// - `labels` – Key-value label pairs attached to this metric instance.
    ///
    /// ## Returns
    /// A handle that can be used to increment the counter.
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn>;

    /// Register a gauge that can be set to arbitrary values.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    /// - `description` – Human-readable description.
    /// - `labels` – Key-value label pairs.
    ///
    /// ## Returns
    /// A handle that can be used to set the gauge value.
    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn>;

    /// Register a counter that can be incremented or decremented.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    /// - `description` – Human-readable description.
    /// - `labels` – Key-value label pairs.
    ///
    /// ## Returns
    /// A handle that can be used to increment or decrement the counter.
    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn>;

    /// Register a histogram that records value distributions.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    /// - `description` – Human-readable description.
    /// - `labels` – Key-value label pairs.
    /// - `boundaries` – Bucket boundaries for the histogram.
    ///
    /// ## Returns
    /// A handle that can be used to record values into the histogram.
    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn>;
}

/// Handle for incrementing a monotonic counter.
pub trait CounterFn: Send + Sync {
    /// Add `value` to the counter.
    fn increment(&self, value: u64);
}

/// Handle for setting a gauge to an arbitrary value.
pub trait GaugeFn: Send + Sync {
    /// Set the gauge to `value`.
    fn set(&self, value: i64);
}

/// Handle for incrementing or decrementing a bidirectional counter.
pub trait UpDownCounterFn: Send + Sync {
    /// Add `value` to the counter (may be negative).
    fn increment(&self, value: i64);
}

/// Handle for recording observations into a histogram.
pub trait HistogramFn: Send + Sync {
    /// Record a single observation.
    fn record(&self, value: f64);
}

// ---------------------------------------------------------------------------
// MetricLevel
// ---------------------------------------------------------------------------

/// Controls which metrics are active. Metrics with a level below the configured
/// threshold are replaced with zero-cost no-op handles at registration time.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MetricLevel {
    /// High-frequency or high-cardinality metrics on the hot path.
    /// Only active when the configured level is `Debug`.
    Debug,
    /// Standard operational metrics. Always active at the default level.
    #[default]
    Info,
}

// ---------------------------------------------------------------------------
// Snapshot types
// ---------------------------------------------------------------------------

/// A single metric with its name, labels, description, and current value.
#[derive(Debug, Clone)]
pub struct Metric {
    /// The dotted metric name (e.g. `"slatedb.compaction.bytes"`).
    pub name: String,
    /// Key-value label pairs attached to this metric instance.
    pub labels: Vec<(String, String)>,
    /// Human-readable description of the metric.
    pub description: String,
    /// The current value of the metric.
    pub value: MetricValue,
}

/// The value of a metric.
#[derive(Debug, Clone)]
pub enum MetricValue {
    /// A monotonically increasing counter value.
    Counter(u64),
    /// A gauge value that can be set to arbitrary values.
    Gauge(i64),
    /// A bidirectional counter value.
    UpDownCounter(i64),
    /// A histogram with bucket counts, sum, min, max, and boundaries.
    Histogram {
        /// Total number of observations.
        count: u64,
        /// Sum of all observed values.
        sum: f64,
        /// Minimum observed value.
        min: f64,
        /// Maximum observed value.
        max: f64,
        /// Bucket boundaries.
        boundaries: Vec<f64>,
        /// Number of observations in each bucket (len = boundaries.len() + 1).
        bucket_counts: Vec<u64>,
    },
}

impl std::fmt::Display for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricValue::Counter(v) => write!(f, "{v}"),
            MetricValue::Gauge(v) => write!(f, "{v}"),
            MetricValue::UpDownCounter(v) => write!(f, "{v}"),
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
                ..
            } => {
                write!(f, "count={count}, sum={sum}, min={min}, max={max}")
            }
        }
    }
}

/// Materialized snapshot of all registered metrics, with lookup methods.
#[derive(Debug, Clone)]
pub struct Metrics {
    // Lookup is a naive linear scan for now, this is probably fine since it
    // only happens on startup when we register metrics and if the default
    // metric snapshot is used (which is intended for debugging use cases,
    // prod use cases should use a dedicated metrics backend)
    metrics: Vec<Metric>,
}

impl Metrics {
    /// Return all metrics in this snapshot.
    pub fn all(&self) -> &[Metric] {
        &self.metrics
    }

    /// Look up all metrics matching a given name (any labels).
    pub fn by_name(&self, name: &str) -> Vec<&Metric> {
        self.metrics.iter().filter(|m| m.name == name).collect()
    }

    /// Look up the unique metric matching a name and exact canonical label set.
    /// Input label order does not matter.
    pub fn by_name_and_labels(&self, name: &str, labels: &[(&str, &str)]) -> Option<&Metric> {
        let mut canonical: Vec<(&str, &str)> = labels.to_vec();
        canonical.sort();
        self.metrics.iter().find(|m| {
            m.name == name && {
                let mut m_labels: Vec<(&str, &str)> = m
                    .labels
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                m_labels.sort();
                m_labels == canonical
            }
        })
    }
}

// ---------------------------------------------------------------------------
// No-op recorder and handles
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NoopCounter;
impl CounterFn for NoopCounter {
    fn increment(&self, _value: u64) {}
}

#[derive(Debug)]
struct NoopGauge;
impl GaugeFn for NoopGauge {
    fn set(&self, _value: i64) {}
}

#[derive(Debug)]
struct NoopUpDownCounter;
impl UpDownCounterFn for NoopUpDownCounter {
    fn increment(&self, _value: i64) {}
}

#[derive(Debug)]
struct NoopHistogram;
impl HistogramFn for NoopHistogram {
    fn record(&self, _value: f64) {}
}

fn noop_counter() -> Arc<dyn CounterFn> {
    Arc::new(NoopCounter)
}

fn noop_gauge() -> Arc<dyn GaugeFn> {
    Arc::new(NoopGauge)
}

fn noop_up_down_counter() -> Arc<dyn UpDownCounterFn> {
    Arc::new(NoopUpDownCounter)
}

fn noop_histogram() -> Arc<dyn HistogramFn> {
    Arc::new(NoopHistogram)
}

/// Recorder that discards all metric registrations and returns no-op handles.
#[derive(Debug, Default)]
pub struct NoopMetricsRecorder;

impl NoopMetricsRecorder {
    /// Create a new no-op recorder.
    pub fn new() -> Self {
        Self
    }
}

impl MetricsRecorder for NoopMetricsRecorder {
    fn register_counter(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        noop_counter()
    }

    fn register_gauge(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        noop_gauge()
    }

    fn register_up_down_counter(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        noop_up_down_counter()
    }

    fn register_histogram(
        &self,
        _name: &str,
        _description: &str,
        _labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        noop_histogram()
    }
}

// ---------------------------------------------------------------------------
// Default recorder (atomic-backed)
// ---------------------------------------------------------------------------

/// Atomic-backed counter handle for the default recorder.
#[derive(Debug)]
struct DefaultCounter {
    value: AtomicU64,
}

impl CounterFn for DefaultCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }
}

/// Atomic-backed gauge handle for the default recorder.
#[derive(Debug)]
struct DefaultGauge {
    value: AtomicI64,
}

impl GaugeFn for DefaultGauge {
    fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

/// Atomic-backed up/down counter handle for the default recorder.
#[derive(Debug)]
struct DefaultUpDownCounter {
    value: AtomicI64,
}

impl UpDownCounterFn for DefaultUpDownCounter {
    fn increment(&self, value: i64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }
}

/// Atomic-backed histogram handle for the default recorder.
#[derive(Debug)]
struct DefaultHistogram {
    count: AtomicU64,
    sum: AtomicU64, // f64 bit-cast, updated via CAS
    min: AtomicU64, // f64 bit-cast, updated via CAS
    max: AtomicU64, // f64 bit-cast, updated via CAS
    boundaries: Vec<f64>,
    bucket_counts: Vec<AtomicU64>,
}

impl DefaultHistogram {
    fn new(boundaries: &[f64]) -> Self {
        // bucket_counts has len = boundaries.len() + 1 (one overflow bucket)
        let bucket_counts: Vec<AtomicU64> = (0..boundaries.len() + 1)
            .map(|_| AtomicU64::new(0))
            .collect();
        Self {
            count: AtomicU64::new(0),
            sum: AtomicU64::new(f64::to_bits(0.0)),
            min: AtomicU64::new(f64::to_bits(f64::INFINITY)),
            max: AtomicU64::new(f64::to_bits(f64::NEG_INFINITY)),
            boundaries: boundaries.to_vec(),
            bucket_counts,
        }
    }
}

/// CAS-loop to atomically update an f64 stored as AtomicU64 bits.
fn atomic_f64_update(atomic: &AtomicU64, f: impl Fn(f64) -> f64) {
    let mut current = atomic.load(Ordering::Relaxed);
    loop {
        let new_bits = f64::to_bits(f(f64::from_bits(current)));
        match atomic.compare_exchange_weak(current, new_bits, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

impl HistogramFn for DefaultHistogram {
    fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        atomic_f64_update(&self.sum, |s| s + value);
        atomic_f64_update(&self.min, |m| m.min(value));
        atomic_f64_update(&self.max, |m| m.max(value));

        // Find the first bucket whose boundary exceeds the value.
        let idx = self
            .boundaries
            .iter()
            .position(|&b| value < b)
            .unwrap_or(self.boundaries.len());
        self.bucket_counts[idx].fetch_add(1, Ordering::Relaxed);
    }
}

// Metadata for a registered metric in the default recorder.
enum DefaultMetricHandle {
    Counter(Arc<DefaultCounter>),
    Gauge(Arc<DefaultGauge>),
    UpDownCounter(Arc<DefaultUpDownCounter>),
    Histogram(Arc<DefaultHistogram>),
}

struct DefaultMetricEntry {
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    handle: DefaultMetricHandle,
}

/// The built-in atomic-backed metrics recorder. Always active, powers
/// metric snapshots for debugging.
///
/// If the same `(name, canonical_labels)` pair is registered twice, the
/// duplicate is logged as a warning and the existing handle is returned.
pub struct DefaultMetricsRecorder {
    entries: Mutex<Vec<DefaultMetricEntry>>,
}

impl DefaultMetricsRecorder {
    /// Create a new, empty default metrics recorder.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Check whether a metric with the given name and labels already exists.
    /// Returns the existing entry if a duplicate was found (and logs a warning).
    fn find_duplicate<'a>(
        entries: &'a [DefaultMetricEntry],
        name: &str,
        labels: &[(String, String)],
    ) -> Option<&'a DefaultMetricEntry> {
        let entry = entries
            .iter()
            .find(|e| e.name == name && e.labels == labels);
        if entry.is_some() {
            log::warn!("duplicate metric registration: name={name}, labels={labels:?}");
        }
        entry
    }

    /// Read all registered metrics and produce a point-in-time snapshot.
    pub fn snapshot(&self) -> Metrics {
        let entries = self.entries.lock().expect("lock poisoned");
        let metrics = entries
            .iter()
            .map(|entry| {
                let value = match &entry.handle {
                    DefaultMetricHandle::Counter(c) => {
                        MetricValue::Counter(c.value.load(Ordering::Relaxed))
                    }
                    DefaultMetricHandle::Gauge(g) => {
                        MetricValue::Gauge(g.value.load(Ordering::Relaxed))
                    }
                    DefaultMetricHandle::UpDownCounter(u) => {
                        MetricValue::UpDownCounter(u.value.load(Ordering::Relaxed))
                    }
                    DefaultMetricHandle::Histogram(h) => MetricValue::Histogram {
                        count: h.count.load(Ordering::Relaxed),
                        sum: f64::from_bits(h.sum.load(Ordering::Relaxed)),
                        min: f64::from_bits(h.min.load(Ordering::Relaxed)),
                        max: f64::from_bits(h.max.load(Ordering::Relaxed)),
                        boundaries: h.boundaries.clone(),
                        bucket_counts: h
                            .bucket_counts
                            .iter()
                            .map(|b| b.load(Ordering::Relaxed))
                            .collect(),
                    },
                };
                Metric {
                    name: entry.name.clone(),
                    labels: entry.labels.clone(),
                    description: entry.description.clone(),
                    value,
                }
            })
            .collect();
        Metrics { metrics }
    }
}

impl Default for DefaultMetricsRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let canonical = canonicalize_labels(labels);
        let mut entries = self.entries.lock().expect("lock poisoned");
        if let Some(entry) = Self::find_duplicate(&entries, name, &canonical) {
            if let DefaultMetricHandle::Counter(ref h) = entry.handle {
                return h.clone();
            }
        }
        let handle = Arc::new(DefaultCounter {
            value: AtomicU64::new(0),
        });
        entries.push(DefaultMetricEntry {
            name: name.to_owned(),
            description: description.to_owned(),
            labels: canonical,
            handle: DefaultMetricHandle::Counter(handle.clone()),
        });
        handle
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let canonical = canonicalize_labels(labels);
        let mut entries = self.entries.lock().expect("lock poisoned");
        if let Some(entry) = Self::find_duplicate(&entries, name, &canonical) {
            if let DefaultMetricHandle::Gauge(ref h) = entry.handle {
                return h.clone();
            }
        }
        let handle = Arc::new(DefaultGauge {
            value: AtomicI64::new(0),
        });
        entries.push(DefaultMetricEntry {
            name: name.to_owned(),
            description: description.to_owned(),
            labels: canonical,
            handle: DefaultMetricHandle::Gauge(handle.clone()),
        });
        handle
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        let canonical = canonicalize_labels(labels);
        let mut entries = self.entries.lock().expect("lock poisoned");
        if let Some(entry) = Self::find_duplicate(&entries, name, &canonical) {
            if let DefaultMetricHandle::UpDownCounter(ref h) = entry.handle {
                return h.clone();
            }
        }
        let handle = Arc::new(DefaultUpDownCounter {
            value: AtomicI64::new(0),
        });
        entries.push(DefaultMetricEntry {
            name: name.to_owned(),
            description: description.to_owned(),
            labels: canonical,
            handle: DefaultMetricHandle::UpDownCounter(handle.clone()),
        });
        handle
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        let canonical = canonicalize_labels(labels);
        let mut entries = self.entries.lock().expect("lock poisoned");
        if let Some(entry) = Self::find_duplicate(&entries, name, &canonical) {
            if let DefaultMetricHandle::Histogram(ref h) = entry.handle {
                return h.clone();
            }
        }
        let handle = Arc::new(DefaultHistogram::new(boundaries));
        entries.push(DefaultMetricEntry {
            name: name.to_owned(),
            description: description.to_owned(),
            labels: canonical,
            handle: DefaultMetricHandle::Histogram(handle.clone()),
        });
        handle
    }
}

// ---------------------------------------------------------------------------
// MetricsRecorderHelper and builders
// ---------------------------------------------------------------------------

/// Internal helper that wraps a [`MetricsRecorder`] and the configured
/// [`MetricLevel`]. Provides a builder API that resolves defaults and handles
/// level filtering.
#[derive(Clone)]
pub struct MetricsRecorderHelper {
    recorder: Arc<dyn MetricsRecorder>,
    level: MetricLevel,
}

impl MetricsRecorderHelper {
    /// Create a new helper that delegates to `recorder` and filters at `level`.
    ///
    /// ## Arguments
    /// - `recorder` – The recorder to delegate metric registration to.
    /// - `level` – The minimum metric level required for a metric to be active.
    pub fn new(recorder: Arc<dyn MetricsRecorder>, level: MetricLevel) -> Self {
        Self { recorder, level }
    }

    /// Create a no-op helper whose metrics are silently discarded.
    /// Useful as the default when no user recorder is configured, and in tests
    /// that don't need to assert on metric values.
    pub fn noop() -> Self {
        Self {
            recorder: Arc::new(NoopMetricsRecorder::new()),
            level: MetricLevel::default(),
        }
    }

    /// Start building a counter metric with the given name.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name (e.g. `"slatedb.compaction.bytes"`).
    pub fn counter(&self, name: &str) -> CounterBuilder<'_> {
        CounterBuilder {
            recorder: &self.recorder,
            threshold: self.level,
            name: name.to_owned(),
            description: String::new(),
            labels: Vec::new(),
            level: MetricLevel::Info,
        }
    }

    /// Start building a gauge metric with the given name.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    pub fn gauge(&self, name: &str) -> GaugeBuilder<'_> {
        GaugeBuilder {
            recorder: &self.recorder,
            threshold: self.level,
            name: name.to_owned(),
            description: String::new(),
            labels: Vec::new(),
            level: MetricLevel::Info,
        }
    }

    /// Start building an up-down counter metric with the given name.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    pub fn up_down_counter(&self, name: &str) -> UpDownCounterBuilder<'_> {
        UpDownCounterBuilder {
            recorder: &self.recorder,
            threshold: self.level,
            name: name.to_owned(),
            description: String::new(),
            labels: Vec::new(),
            level: MetricLevel::Info,
        }
    }

    /// Start building a histogram metric with the given name and boundaries.
    ///
    /// ## Arguments
    /// - `name` – Dotted metric name.
    /// - `boundaries` – Bucket boundaries for the histogram.
    pub fn histogram<'a>(&'a self, name: &str, boundaries: &[f64]) -> HistogramBuilder<'a> {
        HistogramBuilder {
            recorder: &self.recorder,
            threshold: self.level,
            name: name.to_owned(),
            description: String::new(),
            labels: Vec::new(),
            level: MetricLevel::Info,
            boundaries: boundaries.to_vec(),
        }
    }
}

/// Builder for registering a counter metric.
pub struct CounterBuilder<'a> {
    recorder: &'a Arc<dyn MetricsRecorder>,
    threshold: MetricLevel,
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    level: MetricLevel,
}

impl CounterBuilder<'_> {
    /// Set the human-readable description for the counter.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_owned();
        self
    }

    /// Set the labels for the counter.
    pub fn labels(mut self, labels: &[(&str, &str)]) -> Self {
        self.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self
    }

    /// Set the metric level for level-based filtering.
    pub fn level(mut self, level: MetricLevel) -> Self {
        self.level = level;
        self
    }

    /// Finalize and register the counter, returning a handle.
    ///
    /// ## Returns
    /// A [`CounterFn`] handle, or a no-op if the metric level is below the threshold.
    pub fn register(self) -> Arc<dyn CounterFn> {
        if self.level < self.threshold {
            return noop_counter();
        }
        let canonical = canonicalize_owned_labels(&self.labels);
        let label_refs: Vec<(&str, &str)> = canonical
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.recorder
            .register_counter(&self.name, &self.description, &label_refs)
    }
}

/// Builder for registering a gauge metric.
pub struct GaugeBuilder<'a> {
    recorder: &'a Arc<dyn MetricsRecorder>,
    threshold: MetricLevel,
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    level: MetricLevel,
}

impl GaugeBuilder<'_> {
    /// Set the human-readable description for the gauge.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_owned();
        self
    }

    /// Set the labels for the gauge.
    pub fn labels(mut self, labels: &[(&str, &str)]) -> Self {
        self.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self
    }

    /// Set the metric level for level-based filtering.
    pub fn level(mut self, level: MetricLevel) -> Self {
        self.level = level;
        self
    }

    /// Finalize and register the gauge, returning a handle.
    ///
    /// ## Returns
    /// A [`GaugeFn`] handle, or a no-op if the metric level is below the threshold.
    pub fn register(self) -> Arc<dyn GaugeFn> {
        if self.level < self.threshold {
            return noop_gauge();
        }
        let canonical = canonicalize_owned_labels(&self.labels);
        let label_refs: Vec<(&str, &str)> = canonical
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.recorder
            .register_gauge(&self.name, &self.description, &label_refs)
    }
}

/// Builder for registering an up-down counter metric.
pub struct UpDownCounterBuilder<'a> {
    recorder: &'a Arc<dyn MetricsRecorder>,
    threshold: MetricLevel,
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    level: MetricLevel,
}

impl UpDownCounterBuilder<'_> {
    /// Set the human-readable description for the up-down counter.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_owned();
        self
    }

    /// Set the labels for the up-down counter.
    pub fn labels(mut self, labels: &[(&str, &str)]) -> Self {
        self.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self
    }

    /// Set the metric level for level-based filtering.
    pub fn level(mut self, level: MetricLevel) -> Self {
        self.level = level;
        self
    }

    /// Finalize and register the up-down counter, returning a handle.
    ///
    /// ## Returns
    /// An [`UpDownCounterFn`] handle, or a no-op if the metric level is below the threshold.
    pub fn register(self) -> Arc<dyn UpDownCounterFn> {
        if self.level < self.threshold {
            return noop_up_down_counter();
        }
        let canonical = canonicalize_owned_labels(&self.labels);
        let label_refs: Vec<(&str, &str)> = canonical
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.recorder
            .register_up_down_counter(&self.name, &self.description, &label_refs)
    }
}

/// Builder for registering a histogram metric.
pub struct HistogramBuilder<'a> {
    recorder: &'a Arc<dyn MetricsRecorder>,
    threshold: MetricLevel,
    name: String,
    description: String,
    labels: Vec<(String, String)>,
    level: MetricLevel,
    boundaries: Vec<f64>,
}

impl HistogramBuilder<'_> {
    /// Set the human-readable description for the histogram.
    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.to_owned();
        self
    }

    /// Set the labels for the histogram.
    pub fn labels(mut self, labels: &[(&str, &str)]) -> Self {
        self.labels = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self
    }

    /// Set the metric level for level-based filtering.
    pub fn level(mut self, level: MetricLevel) -> Self {
        self.level = level;
        self
    }

    /// Finalize and register the histogram, returning a handle.
    ///
    /// ## Returns
    /// A [`HistogramFn`] handle, or a no-op if the metric level is below the threshold.
    pub fn register(self) -> Arc<dyn HistogramFn> {
        if self.level < self.threshold {
            return noop_histogram();
        }
        let canonical = canonicalize_owned_labels(&self.labels);
        let label_refs: Vec<(&str, &str)> = canonical
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.recorder.register_histogram(
            &self.name,
            &self.description,
            &label_refs,
            &self.boundaries,
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Canonicalize labels by sorting by (key, value).
fn canonicalize_labels(labels: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut canonical: Vec<(String, String)> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    canonical.sort();
    canonical
}

fn canonicalize_owned_labels(labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut canonical = labels.to_vec();
    canonical.sort();
    canonical
}

// ---------------------------------------------------------------------------
// Test utilities
// ---------------------------------------------------------------------------

/// Create a [`DefaultMetricsRecorder`] and a [`MetricsRecorderHelper`] wired
/// together with the default metric level. Useful in tests that need both
/// handles without repeating the boilerplate.
#[cfg(any(test, feature = "test-util"))]
pub fn test_recorder_helper() -> (Arc<DefaultMetricsRecorder>, MetricsRecorderHelper) {
    let default_recorder = Arc::new(DefaultMetricsRecorder::new());
    let helper = MetricsRecorderHelper::new(
        default_recorder.clone() as Arc<dyn MetricsRecorder>,
        MetricLevel::default(),
    );
    (default_recorder, helper)
}

/// Test-only helper to look up a scalar metric by name from a
/// [`DefaultMetricsRecorder`] snapshot. Returns `None` if the metric does not
/// exist. Panics on histograms since they cannot be represented as a single
/// `i64` — use [`DefaultMetricsRecorder::snapshot`] directly when you need
/// histogram data.
#[cfg(any(test, feature = "test-util"))]
#[allow(clippy::panic)]
pub fn lookup_metric(recorder: &DefaultMetricsRecorder, name: &str) -> Option<i64> {
    let snap = recorder.snapshot();
    snap.by_name(name).first().map(|m| match &m.value {
        MetricValue::Counter(v) => *v as i64,
        MetricValue::Gauge(v) => *v,
        MetricValue::UpDownCounter(v) => *v,
        MetricValue::Histogram { .. } => panic!("unexpected histogram metric"),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Default recorder tests --

    #[test]
    fn should_track_counter() {
        let recorder = DefaultMetricsRecorder::new();
        let counter = recorder.register_counter("test.counter", "A test counter", &[]);
        counter.increment(1);
        counter.increment(5);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.counter");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 6),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_track_gauge() {
        let recorder = DefaultMetricsRecorder::new();
        let gauge = recorder.register_gauge("test.gauge", "A test gauge", &[]);
        gauge.set(42);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.gauge");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Gauge(v) => assert_eq!(*v, 42),
            other => panic!("expected Gauge, got {:?}", other),
        }

        gauge.set(0);
        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.gauge");
        match &metric[0].value {
            MetricValue::Gauge(v) => assert_eq!(*v, 0),
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn should_track_up_down_counter() {
        let recorder = DefaultMetricsRecorder::new();
        let counter =
            recorder.register_up_down_counter("test.up_down", "A test up/down counter", &[]);
        counter.increment(3);
        counter.increment(-1);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.up_down");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::UpDownCounter(v) => assert_eq!(*v, 2),
            other => panic!("expected UpDownCounter, got {:?}", other),
        }
    }

    #[test]
    fn should_track_histogram() {
        let recorder = DefaultMetricsRecorder::new();
        let boundaries = &[1.0, 5.0, 10.0];
        let histogram =
            recorder.register_histogram("test.hist", "A test histogram", &[], boundaries);

        histogram.record(0.5); // bucket 0 (< 1.0)
        histogram.record(3.0); // bucket 1 (< 5.0)
        histogram.record(7.0); // bucket 2 (< 10.0)
        histogram.record(15.0); // bucket 3 (overflow)
        histogram.record(2.0); // bucket 1 (< 5.0)

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.hist");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
                boundaries: b,
                bucket_counts,
            } => {
                assert_eq!(*count, 5);
                assert!((sum - 27.5).abs() < f64::EPSILON);
                assert!((min - 0.5).abs() < f64::EPSILON);
                assert!((max - 15.0).abs() < f64::EPSILON);
                assert_eq!(b, &vec![1.0, 5.0, 10.0]);
                assert_eq!(bucket_counts, &vec![1, 2, 1, 1]);
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn should_track_empty_histogram() {
        let recorder = DefaultMetricsRecorder::new();
        let _histogram =
            recorder.register_histogram("test.empty_hist", "Empty histogram", &[], &[1.0, 5.0]);

        let snapshot = recorder.snapshot();
        let metric = snapshot.by_name("test.empty_hist");
        assert_eq!(metric.len(), 1);
        match &metric[0].value {
            MetricValue::Histogram {
                count,
                sum,
                min,
                max,
                bucket_counts,
                ..
            } => {
                assert_eq!(*count, 0);
                assert!((sum - 0.0).abs() < f64::EPSILON);
                assert!(min.is_infinite() && min.is_sign_positive());
                assert!(max.is_infinite() && max.is_sign_negative());
                assert_eq!(bucket_counts, &vec![0, 0, 0]);
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn should_track_metrics_with_labels() {
        let recorder = DefaultMetricsRecorder::new();
        let hit = recorder.register_counter(
            "cache.access",
            "Cache accesses",
            &[("kind", "filter"), ("result", "hit")],
        );
        let miss = recorder.register_counter(
            "cache.access",
            "Cache accesses",
            &[("kind", "filter"), ("result", "miss")],
        );
        hit.increment(10);
        miss.increment(3);

        let snapshot = recorder.snapshot();

        // by_name returns both
        let all = snapshot.by_name("cache.access");
        assert_eq!(all.len(), 2);

        // by_name_and_labels finds exact match
        let hit_metric = snapshot
            .by_name_and_labels("cache.access", &[("kind", "filter"), ("result", "hit")])
            .unwrap();
        match &hit_metric.value {
            MetricValue::Counter(v) => assert_eq!(*v, 10),
            other => panic!("expected Counter, got {:?}", other),
        }

        // Label order doesn't matter
        let miss_metric = snapshot
            .by_name_and_labels("cache.access", &[("result", "miss"), ("kind", "filter")])
            .unwrap();
        match &miss_metric.value {
            MetricValue::Counter(v) => assert_eq!(*v, 3),
            other => panic!("expected Counter, got {:?}", other),
        }

        // No match for wrong labels
        assert!(snapshot
            .by_name_and_labels("cache.access", &[("kind", "index"), ("result", "hit")])
            .is_none());
    }

    // -- Level filtering tests --

    #[test]
    fn should_filter_debug_metrics_at_info_level() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(
            default.clone() as Arc<dyn MetricsRecorder>,
            MetricLevel::Info,
        );

        // Debug-level metric should be a no-op
        let counter = helper
            .counter("test.debug_counter")
            .description("debug counter")
            .level(MetricLevel::Debug)
            .register();
        counter.increment(100);

        // Info-level metric should be active
        let info_counter = helper
            .counter("test.info_counter")
            .description("info counter")
            .register();
        info_counter.increment(42);

        let snap = default.snapshot();
        // Debug metric should not appear in snapshot
        assert!(snap.by_name("test.debug_counter").is_empty());
        // Info metric should appear
        match &snap.by_name("test.info_counter")[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 42),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_activate_debug_metrics_at_debug_level() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(
            default.clone() as Arc<dyn MetricsRecorder>,
            MetricLevel::Debug,
        );

        let counter = helper
            .counter("test.debug_counter")
            .description("debug counter")
            .level(MetricLevel::Debug)
            .register();
        counter.increment(100);

        let snap = default.snapshot();
        match &snap.by_name("test.debug_counter")[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 100),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_filter_debug_histogram_at_info_level() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(
            default.clone() as Arc<dyn MetricsRecorder>,
            MetricLevel::Info,
        );

        let hist = helper
            .histogram("test.debug_hist", LATENCY_BOUNDARIES)
            .level(MetricLevel::Debug)
            .register();
        hist.record(1.0);

        let snap = default.snapshot();
        assert!(snap.by_name("test.debug_hist").is_empty());
    }

    // -- Builder tests --

    #[test]
    fn should_register_with_builder_labels() {
        let default = Arc::new(DefaultMetricsRecorder::new());
        let helper = MetricsRecorderHelper::new(
            default.clone() as Arc<dyn MetricsRecorder>,
            MetricLevel::Info,
        );

        let counter = helper
            .counter("test.requests")
            .description("Request count")
            .labels(&[("op", "get")])
            .register();
        counter.increment(5);

        let snap = default.snapshot();
        let metric = snap
            .by_name_and_labels("test.requests", &[("op", "get")])
            .unwrap();
        match &metric.value {
            MetricValue::Counter(v) => assert_eq!(*v, 5),
            other => panic!("expected Counter, got {:?}", other),
        }
        assert_eq!(metric.description, "Request count");
    }

    #[test]
    fn should_store_histogram_bucket_boundaries() {
        let recorder = DefaultMetricsRecorder::new();
        let boundaries = &[0.1, 0.5, 1.0];
        let hist = recorder.register_histogram("test.latency", "Latency", &[], boundaries);

        // Record a value exactly at a boundary -- should go in the next bucket
        hist.record(0.5); // 0.5 < 1.0, so bucket index 1 (the [0.1, 0.5) boundary)
                          // Actually: 0.5 is NOT < 0.5, so it goes to bucket 2 (< 1.0)

        let snap = recorder.snapshot();
        match &snap.by_name("test.latency")[0].value {
            MetricValue::Histogram {
                bucket_counts,
                boundaries: b,
                ..
            } => {
                assert_eq!(b, &vec![0.1, 0.5, 1.0]);
                // 0.5 is not < 0.5, so it falls into bucket[2] (< 1.0)
                assert_eq!(bucket_counts, &vec![0, 0, 1, 0]);
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    // -- Duplicate detection tests --

    #[test]
    fn should_warn_and_return_existing_on_duplicate_counter() {
        let recorder = DefaultMetricsRecorder::new();
        let first = recorder.register_counter("dup.counter", "first", &[("k", "v")]);
        first.increment(10);

        // Second registration returns the same handle
        let second = recorder.register_counter("dup.counter", "second", &[("k", "v")]);
        second.increment(5);

        let snap = recorder.snapshot();
        let metrics = snap.by_name("dup.counter");
        // Only one entry should exist
        assert_eq!(metrics.len(), 1);
        // Both increments should be on the same handle
        match &metrics[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 15),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_warn_and_return_existing_on_duplicate_gauge() {
        let recorder = DefaultMetricsRecorder::new();
        let first = recorder.register_gauge("dup.gauge", "first", &[]);
        first.set(1);

        let second = recorder.register_gauge("dup.gauge", "second", &[]);
        second.set(2);

        let snap = recorder.snapshot();
        let metrics = snap.by_name("dup.gauge");
        assert_eq!(metrics.len(), 1);
        match &metrics[0].value {
            MetricValue::Gauge(v) => assert_eq!(*v, 2),
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn should_warn_and_return_existing_on_duplicate_counter_with_different_label_order() {
        let recorder = DefaultMetricsRecorder::new();
        let first = recorder.register_counter("dup.counter", "first", &[("a", "1"), ("b", "2")]);
        first.increment(10);

        let second = recorder.register_counter("dup.counter", "second", &[("b", "2"), ("a", "1")]);
        second.increment(5);

        let snap = recorder.snapshot();
        let metrics = snap.by_name("dup.counter");
        assert_eq!(metrics.len(), 1);
        match &metrics[0].value {
            MetricValue::Counter(v) => assert_eq!(*v, 15),
            other => panic!("expected Counter, got {:?}", other),
        }
    }

    #[test]
    fn should_allow_same_name_different_labels() {
        let recorder = DefaultMetricsRecorder::new();
        recorder.register_counter("shared.name", "first", &[("op", "get")]);
        recorder.register_counter("shared.name", "second", &[("op", "put")]);

        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.by_name("shared.name").len(), 2);
    }
}
