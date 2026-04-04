use std::sync::Arc;

use slatedb_common::metrics as core_metrics;
use slatedb_common::metrics::MetricsRecorder as _;

/// Key-value label attached to a metric.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MetricLabel {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

/// Histogram payload captured in a metric snapshot.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct HistogramMetricValue {
    /// Total number of recorded observations.
    pub count: u64,
    /// Sum of all observed values.
    pub sum: f64,
    /// Minimum observed value.
    pub min: f64,
    /// Maximum observed value.
    pub max: f64,
    /// Histogram bucket boundaries.
    pub boundaries: Vec<f64>,
    /// Number of observations in each bucket.
    pub bucket_counts: Vec<u64>,
}

/// Value stored in a metric snapshot.
#[derive(Clone, Debug, PartialEq, uniffi::Enum)]
pub enum MetricValue {
    /// Monotonic counter value.
    Counter(u64),
    /// Gauge value.
    Gauge(i64),
    /// Up/down counter value.
    UpDownCounter(i64),
    /// Histogram summary and buckets.
    Histogram(HistogramMetricValue),
}

/// One metric from a [`DefaultMetricsRecorder`] snapshot.
#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct Metric {
    /// Dotted metric name.
    pub name: String,
    /// Canonical label set for the metric instance.
    pub labels: Vec<MetricLabel>,
    /// Human-readable description.
    pub description: String,
    /// Current metric value.
    pub value: MetricValue,
}

/// Handle for a monotonic counter metric.
#[uniffi::export(with_foreign)]
pub trait Counter: Send + Sync {
    /// Adds `value` to the counter.
    fn increment(&self, value: u64);
}

/// Handle for a gauge metric.
#[uniffi::export(with_foreign)]
pub trait Gauge: Send + Sync {
    /// Sets the gauge to `value`.
    fn set(&self, value: i64);
}

/// Handle for an up/down counter metric.
#[uniffi::export(with_foreign)]
pub trait UpDownCounter: Send + Sync {
    /// Adds `value` to the counter.
    fn increment(&self, value: i64);
}

/// Handle for a histogram metric.
#[uniffi::export(with_foreign)]
pub trait Histogram: Send + Sync {
    /// Records `value` in the histogram.
    fn record(&self, value: f64);
}

/// Application-defined metrics recorder used to publish SlateDB metrics.
#[uniffi::export(with_foreign)]
pub trait MetricsRecorder: Send + Sync {
    /// Registers a monotonically increasing counter.
    fn register_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Counter>;

    /// Registers a gauge.
    fn register_gauge(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Gauge>;

    /// Registers an up/down counter.
    fn register_up_down_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn UpDownCounter>;

    /// Registers a histogram with explicit bucket boundaries.
    fn register_histogram(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
        boundaries: Vec<f64>,
    ) -> Arc<dyn Histogram>;
}

/// Built-in atomic-backed metrics recorder with snapshot access.
#[derive(uniffi::Object)]
pub struct DefaultMetricsRecorder {
    inner: Arc<core_metrics::DefaultMetricsRecorder>,
}

#[uniffi::export]
impl DefaultMetricsRecorder {
    /// Creates an empty default metrics recorder.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(core_metrics::DefaultMetricsRecorder::new()),
        })
    }

    /// Returns a point-in-time snapshot of every registered metric.
    pub fn snapshot(&self) -> Vec<Metric> {
        self.inner
            .snapshot()
            .all()
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    /// Returns every metric with the requested name.
    pub fn metrics_by_name(&self, name: String) -> Vec<Metric> {
        let snapshot = self.inner.snapshot();
        snapshot
            .by_name(&name)
            .into_iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    /// Returns the metric matching `name` and the exact label set, if present.
    pub fn metric_by_name_and_labels(
        &self,
        name: String,
        labels: Vec<MetricLabel>,
    ) -> Option<Metric> {
        let snapshot = self.inner.snapshot();
        let label_refs: Vec<(&str, &str)> = labels
            .iter()
            .map(|label| (label.key.as_str(), label.value.as_str()))
            .collect();
        snapshot
            .by_name_and_labels(&name, &label_refs)
            .cloned()
            .map(Into::into)
    }
}

#[uniffi::export]
impl MetricsRecorder for DefaultMetricsRecorder {
    fn register_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Counter> {
        let label_refs = to_label_refs(&labels);
        Arc::new(CoreCounterHandle {
            inner: self
                .inner
                .register_counter(&name, &description, &label_refs),
        })
    }

    fn register_gauge(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Gauge> {
        let label_refs = to_label_refs(&labels);
        Arc::new(CoreGaugeHandle {
            inner: self.inner.register_gauge(&name, &description, &label_refs),
        })
    }

    fn register_up_down_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn UpDownCounter> {
        let label_refs = to_label_refs(&labels);
        Arc::new(CoreUpDownCounterHandle {
            inner: self
                .inner
                .register_up_down_counter(&name, &description, &label_refs),
        })
    }

    fn register_histogram(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
        boundaries: Vec<f64>,
    ) -> Arc<dyn Histogram> {
        let label_refs = to_label_refs(&labels);
        Arc::new(CoreHistogramHandle {
            inner: self
                .inner
                .register_histogram(&name, &description, &label_refs, &boundaries),
        })
    }
}

pub(crate) fn adapt_metrics_recorder(
    metrics_recorder: Arc<dyn MetricsRecorder>,
) -> Arc<dyn core_metrics::MetricsRecorder> {
    Arc::new(MetricsRecorderAdapter {
        inner: metrics_recorder,
    })
}

struct MetricsRecorderAdapter {
    inner: Arc<dyn MetricsRecorder>,
}

impl core_metrics::MetricsRecorder for MetricsRecorderAdapter {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn core_metrics::CounterFn> {
        Arc::new(CounterAdapter {
            inner: self.inner.register_counter(
                name.to_owned(),
                description.to_owned(),
                to_metric_labels(labels),
            ),
        })
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn core_metrics::GaugeFn> {
        Arc::new(GaugeAdapter {
            inner: self.inner.register_gauge(
                name.to_owned(),
                description.to_owned(),
                to_metric_labels(labels),
            ),
        })
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn core_metrics::UpDownCounterFn> {
        Arc::new(UpDownCounterAdapter {
            inner: self.inner.register_up_down_counter(
                name.to_owned(),
                description.to_owned(),
                to_metric_labels(labels),
            ),
        })
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        boundaries: &[f64],
    ) -> Arc<dyn core_metrics::HistogramFn> {
        Arc::new(HistogramAdapter {
            inner: self.inner.register_histogram(
                name.to_owned(),
                description.to_owned(),
                to_metric_labels(labels),
                boundaries.to_vec(),
            ),
        })
    }
}

struct CounterAdapter {
    inner: Arc<dyn Counter>,
}

impl core_metrics::CounterFn for CounterAdapter {
    fn increment(&self, value: u64) {
        self.inner.increment(value);
    }
}

struct GaugeAdapter {
    inner: Arc<dyn Gauge>,
}

impl core_metrics::GaugeFn for GaugeAdapter {
    fn set(&self, value: i64) {
        self.inner.set(value);
    }
}

struct UpDownCounterAdapter {
    inner: Arc<dyn UpDownCounter>,
}

impl core_metrics::UpDownCounterFn for UpDownCounterAdapter {
    fn increment(&self, value: i64) {
        self.inner.increment(value);
    }
}

struct HistogramAdapter {
    inner: Arc<dyn Histogram>,
}

impl core_metrics::HistogramFn for HistogramAdapter {
    fn record(&self, value: f64) {
        self.inner.record(value);
    }
}

struct CoreCounterHandle {
    inner: Arc<dyn core_metrics::CounterFn>,
}

impl Counter for CoreCounterHandle {
    fn increment(&self, value: u64) {
        self.inner.increment(value);
    }
}

struct CoreGaugeHandle {
    inner: Arc<dyn core_metrics::GaugeFn>,
}

impl Gauge for CoreGaugeHandle {
    fn set(&self, value: i64) {
        self.inner.set(value);
    }
}

struct CoreUpDownCounterHandle {
    inner: Arc<dyn core_metrics::UpDownCounterFn>,
}

impl UpDownCounter for CoreUpDownCounterHandle {
    fn increment(&self, value: i64) {
        self.inner.increment(value);
    }
}

struct CoreHistogramHandle {
    inner: Arc<dyn core_metrics::HistogramFn>,
}

impl Histogram for CoreHistogramHandle {
    fn record(&self, value: f64) {
        self.inner.record(value);
    }
}

fn to_metric_labels(labels: &[(&str, &str)]) -> Vec<MetricLabel> {
    labels
        .iter()
        .map(|(key, value)| MetricLabel {
            key: (*key).to_owned(),
            value: (*value).to_owned(),
        })
        .collect()
}

fn to_label_refs(labels: &[MetricLabel]) -> Vec<(&str, &str)> {
    labels
        .iter()
        .map(|label| (label.key.as_str(), label.value.as_str()))
        .collect()
}

impl From<core_metrics::Metric> for Metric {
    fn from(metric: core_metrics::Metric) -> Self {
        Self {
            name: metric.name,
            labels: metric
                .labels
                .into_iter()
                .map(|(key, value)| MetricLabel { key, value })
                .collect(),
            description: metric.description,
            value: metric.value.into(),
        }
    }
}

impl From<core_metrics::MetricValue> for MetricValue {
    fn from(value: core_metrics::MetricValue) -> Self {
        match value {
            core_metrics::MetricValue::Counter(value) => Self::Counter(value),
            core_metrics::MetricValue::Gauge(value) => Self::Gauge(value),
            core_metrics::MetricValue::UpDownCounter(value) => Self::UpDownCounter(value),
            core_metrics::MetricValue::Histogram {
                count,
                sum,
                min,
                max,
                boundaries,
                bucket_counts,
            } => Self::Histogram(HistogramMetricValue {
                count,
                sum,
                min,
                max,
                boundaries,
                bucket_counts,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use slatedb::db_stats::{REQUEST_COUNT, WRITE_OPS};
    use slatedb::object_store::memory::InMemory;

    use super::{
        Counter, DefaultMetricsRecorder, Gauge, Histogram, MetricLabel, MetricValue, UpDownCounter,
    };
    use crate::{DbBuilder, DbReaderBuilder, MetricsRecorder, ObjectStore};

    #[derive(Default)]
    struct RecorderState {
        counters: BTreeMap<String, u64>,
        gauges: BTreeMap<String, i64>,
        up_down_counters: BTreeMap<String, i64>,
        histograms: BTreeMap<String, Vec<f64>>,
    }

    #[derive(Default)]
    struct TestMetricsRecorder {
        state: Arc<Mutex<RecorderState>>,
    }

    impl TestMetricsRecorder {
        fn counter_value(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
            let key = metric_key(name, &to_test_labels(labels));
            self.state
                .lock()
                .expect("lock poisoned")
                .counters
                .get(&key)
                .copied()
        }
    }

    impl MetricsRecorder for TestMetricsRecorder {
        fn register_counter(
            &self,
            name: String,
            _description: String,
            labels: Vec<MetricLabel>,
        ) -> Arc<dyn Counter> {
            let key = metric_key(&name, &labels);
            self.state
                .lock()
                .expect("lock poisoned")
                .counters
                .entry(key.clone())
                .or_insert(0);
            Arc::new(TestCounter {
                state: self.state.clone(),
                key,
            })
        }

        fn register_gauge(
            &self,
            name: String,
            _description: String,
            labels: Vec<MetricLabel>,
        ) -> Arc<dyn Gauge> {
            let key = metric_key(&name, &labels);
            self.state
                .lock()
                .expect("lock poisoned")
                .gauges
                .entry(key.clone())
                .or_insert(0);
            Arc::new(TestGauge {
                state: self.state.clone(),
                key,
            })
        }

        fn register_up_down_counter(
            &self,
            name: String,
            _description: String,
            labels: Vec<MetricLabel>,
        ) -> Arc<dyn UpDownCounter> {
            let key = metric_key(&name, &labels);
            self.state
                .lock()
                .expect("lock poisoned")
                .up_down_counters
                .entry(key.clone())
                .or_insert(0);
            Arc::new(TestUpDownCounter {
                state: self.state.clone(),
                key,
            })
        }

        fn register_histogram(
            &self,
            name: String,
            _description: String,
            labels: Vec<MetricLabel>,
            _boundaries: Vec<f64>,
        ) -> Arc<dyn Histogram> {
            let key = metric_key(&name, &labels);
            self.state
                .lock()
                .expect("lock poisoned")
                .histograms
                .entry(key.clone())
                .or_default();
            Arc::new(TestHistogram {
                state: self.state.clone(),
                key,
            })
        }
    }

    struct TestCounter {
        state: Arc<Mutex<RecorderState>>,
        key: String,
    }

    impl Counter for TestCounter {
        fn increment(&self, value: u64) {
            *self
                .state
                .lock()
                .expect("lock poisoned")
                .counters
                .entry(self.key.clone())
                .or_insert(0) += value;
        }
    }

    struct TestGauge {
        state: Arc<Mutex<RecorderState>>,
        key: String,
    }

    impl Gauge for TestGauge {
        fn set(&self, value: i64) {
            self.state
                .lock()
                .expect("lock poisoned")
                .gauges
                .insert(self.key.clone(), value);
        }
    }

    struct TestUpDownCounter {
        state: Arc<Mutex<RecorderState>>,
        key: String,
    }

    impl UpDownCounter for TestUpDownCounter {
        fn increment(&self, value: i64) {
            *self
                .state
                .lock()
                .expect("lock poisoned")
                .up_down_counters
                .entry(self.key.clone())
                .or_insert(0) += value;
        }
    }

    struct TestHistogram {
        state: Arc<Mutex<RecorderState>>,
        key: String,
    }

    impl Histogram for TestHistogram {
        fn record(&self, value: f64) {
            self.state
                .lock()
                .expect("lock poisoned")
                .histograms
                .entry(self.key.clone())
                .or_default()
                .push(value);
        }
    }

    #[tokio::test]
    async fn db_builder_accepts_custom_metrics_recorder() {
        let (binding_store, _) = new_test_store();
        let recorder = Arc::new(TestMetricsRecorder::default());
        let builder = DbBuilder::new(
            "bindings-uniffi-metrics-custom-db".to_owned(),
            binding_store,
        );

        builder
            .with_metrics_recorder(recorder.clone())
            .expect("failed to attach recorder");

        let db = builder.build().await.expect("failed to build db");
        db.put(b"k1".to_vec(), b"v1".to_vec())
            .await
            .expect("failed to put first value");
        db.put(b"k2".to_vec(), b"v2".to_vec())
            .await
            .expect("failed to put second value");

        assert_eq!(recorder.counter_value(WRITE_OPS, &[]), Some(2));

        db.close().await.expect("failed to close db");
    }

    #[tokio::test]
    async fn db_builder_accepts_default_metrics_recorder() {
        let (binding_store, _) = new_test_store();
        let recorder = DefaultMetricsRecorder::new();
        let builder = DbBuilder::new(
            "bindings-uniffi-metrics-default-db".to_owned(),
            binding_store,
        );

        builder
            .with_metrics_recorder(recorder.clone())
            .expect("failed to attach recorder");

        let db = builder.build().await.expect("failed to build db");
        db.put(b"k1".to_vec(), b"v1".to_vec())
            .await
            .expect("failed to put first value");
        db.put(b"k2".to_vec(), b"v2".to_vec())
            .await
            .expect("failed to put second value");

        let metric = recorder
            .metric_by_name_and_labels(WRITE_OPS.to_owned(), Vec::new())
            .expect("write_ops metric should exist");
        assert_eq!(metric.value, MetricValue::Counter(2));

        db.close().await.expect("failed to close db");
    }

    #[tokio::test]
    async fn db_reader_builder_accepts_custom_metrics_recorder() {
        let (binding_store, inner_store) = new_test_store();
        let path = "bindings-uniffi-metrics-custom-reader";
        seed_reader_data(path, inner_store).await;
        let recorder = Arc::new(TestMetricsRecorder::default());
        let builder = DbReaderBuilder::new(path.to_owned(), binding_store);

        builder
            .with_metrics_recorder(recorder.clone())
            .expect("failed to attach recorder");

        let reader = builder.build().await.expect("failed to build reader");
        assert_eq!(
            reader
                .get(b"key1".to_vec())
                .await
                .expect("failed to read value"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            recorder.counter_value(REQUEST_COUNT, &[("op", "get")]),
            Some(1)
        );

        reader.close().await.expect("failed to close reader");
    }

    #[tokio::test]
    async fn db_reader_builder_accepts_default_metrics_recorder() {
        let (binding_store, inner_store) = new_test_store();
        let path = "bindings-uniffi-metrics-default-reader";
        seed_reader_data(path, inner_store).await;
        let recorder = DefaultMetricsRecorder::new();
        let builder = DbReaderBuilder::new(path.to_owned(), binding_store);

        builder
            .with_metrics_recorder(recorder.clone())
            .expect("failed to attach recorder");

        let reader = builder.build().await.expect("failed to build reader");
        assert_eq!(
            reader
                .get(b"key1".to_vec())
                .await
                .expect("failed to read value"),
            Some(b"value1".to_vec())
        );

        let metric = recorder
            .metric_by_name_and_labels(
                REQUEST_COUNT.to_owned(),
                vec![MetricLabel {
                    key: "op".to_owned(),
                    value: "get".to_owned(),
                }],
            )
            .expect("get request metric should exist");
        assert_eq!(metric.value, MetricValue::Counter(1));

        reader.close().await.expect("failed to close reader");
    }

    fn new_test_store() -> (
        Arc<ObjectStore>,
        Arc<dyn slatedb::object_store::ObjectStore>,
    ) {
        let inner: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let binding = Arc::new(ObjectStore {
            inner: inner.clone(),
        });
        (binding, inner)
    }

    async fn seed_reader_data(
        path: &str,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) {
        let db = slatedb::Db::builder(path.to_owned(), object_store)
            .build()
            .await
            .expect("failed to build seed db");
        db.put(b"key1", b"value1")
            .await
            .expect("failed to write seed value");
        db.flush().await.expect("failed to flush seed db");
        db.close().await.expect("failed to close seed db");
    }

    fn to_test_labels(labels: &[(&str, &str)]) -> Vec<MetricLabel> {
        labels
            .iter()
            .map(|(key, value)| MetricLabel {
                key: (*key).to_owned(),
                value: (*value).to_owned(),
            })
            .collect()
    }

    fn metric_key(name: &str, labels: &[MetricLabel]) -> String {
        let mut labels = labels.to_vec();
        labels.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.value.cmp(&right.value))
        });
        if labels.is_empty() {
            return name.to_owned();
        }
        let labels = labels
            .iter()
            .map(|label| format!("{}={}", label.key, label.value))
            .collect::<Vec<_>>()
            .join(",");
        format!("{name}|{labels}")
    }
}
