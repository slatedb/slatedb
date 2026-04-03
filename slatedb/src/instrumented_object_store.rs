//! Instruments object-store operations with recorder-backed metrics.
//!
//! The wrapper sits beneath `RetryingObjectStore` so each retry attempt is
//! counted separately. When used beneath `CachedObjectStore`, cache hits that
//! never reach the remote store are not counted.
//!
//! Every metric is tagged with two independent label dimensions:
//!
//! - [`ObjectStoreComponent`] — *who* is making the request (db, reader,
//!   gc, compactor).
//! - [`ObjectStoreType`] — *which* store the request targets (main vs wal).
//!
//! A single component may interact with both stores (e.g. the `Db`
//! component writes to both the main store and a separate WAL store),
//! so each `InstrumentedObjectStore` instance is constructed with one
//! specific (component, type) pair. The cross-product of these two
//! dimensions lets operators slice metrics by either axis.
// `Instant` is intentionally used here for monotonic elapsed-time measurement.
// SlateDB's clock abstraction is for wall-clock timestamps, not request timing.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::FutureExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use slatedb_common::metrics::MetricsRecorderHelper;

use crate::object_stores::ObjectStoreType;

/// Which SlateDB component is issuing object store requests.
///
/// Used as a metric label to attribute storage I/O to the responsible
/// subsystem.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ObjectStoreComponent {
    /// The core database (writes WAL/manifest, reads during open/recovery).
    Db,
    /// A read-only database reader.
    Reader,
    /// The garbage collector.
    Gc,
    /// The compactor.
    Compactor,
}

impl ObjectStoreComponent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Db => "db",
            Self::Reader => "reader",
            Self::Gc => "gc",
            Self::Compactor => "compactor",
        }
    }
}

#[derive(Clone)]
pub(crate) struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    stats: Arc<stats::ObjectStoreStats>,
}

impl InstrumentedObjectStore {
    /// Wraps an existing [`ObjectStore`] with per-request metrics recording.
    ///
    /// Every request through the returned store increments counters and
    /// records latency histograms, tagged with the given `component` and
    /// `store_type` labels.
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        recorder: &MetricsRecorderHelper,
        component: ObjectStoreComponent,
        store_type: ObjectStoreType,
    ) -> Self {
        Self {
            inner,
            stats: Arc::new(stats::ObjectStoreStats::new(
                recorder, component, store_type,
            )),
        }
    }
}

impl std::fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedObjectStore({})", self.inner)
    }
}

impl std::fmt::Debug for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedObjectStore").finish()
    }
}

struct InstrumentedMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    stats: Arc<stats::ObjectStoreStats>,
}

impl std::fmt::Debug for InstrumentedMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedMultipartUpload").finish()
    }
}

#[async_trait]
impl MultipartUpload for InstrumentedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let start = Instant::now();
        let stats = Arc::clone(&self.stats);
        self.inner
            .put_part(data)
            .map(move |result| {
                stats.multipart_part.record(start.elapsed(), result.is_ok());
                result
            })
            .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let start = Instant::now();
        let result = self.inner.complete().await;
        self.stats
            .multipart_complete
            .record(start.elapsed(), result.is_ok());
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let start = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        self.stats.get.record(start.elapsed(), result.is_ok());
        result
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> object_store::Result<Bytes> {
        let start = Instant::now();
        let result = self.inner.get_range(location, range).await;
        self.stats.get_range.record(start.elapsed(), result.is_ok());
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let start = Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        self.stats
            .get_ranges
            .record(start.elapsed(), result.is_ok());
        result
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let start = Instant::now();
        let result = self.inner.head(location).await;
        self.stats.head.record(start.elapsed(), result.is_ok());
        result
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let start = Instant::now();
        let result = self.inner.put_opts(location, payload, opts).await;
        self.stats.put.record(start.elapsed(), result.is_ok());
        result
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let start = Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        self.stats
            .multipart_init
            .record(start.elapsed(), result.is_ok());
        result.map(|inner| {
            Box::new(InstrumentedMultipartUpload {
                inner,
                stats: Arc::clone(&self.stats),
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let start = Instant::now();
        let result = self.inner.delete(location).await;
        self.stats.delete.record(start.elapsed(), result.is_ok());
        result
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}

pub(crate) mod stats {
    use std::sync::Arc;
    use std::time::Duration;

    use slatedb_common::metrics::{
        CounterFn, HistogramFn, MetricsRecorderHelper, LATENCY_BOUNDARIES,
    };

    use crate::instrumented_object_store::ObjectStoreComponent;
    use crate::object_stores::ObjectStoreType;

    macro_rules! object_store_stat_name {
        ($suffix:expr) => {
            concat!("slatedb.object_store.", $suffix)
        };
    }

    pub(crate) const REQUEST_COUNT: &str = object_store_stat_name!("request_count");
    pub(crate) const ERROR_COUNT: &str = object_store_stat_name!("error_count");
    pub(crate) const REQUEST_DURATION_SECONDS: &str =
        object_store_stat_name!("request_duration_seconds");

    /// Pre-registered [`RequestMetrics`] for every object store API that
    /// SlateDB calls.
    ///
    /// One instance is created per `InstrumentedObjectStore`, so all
    /// metrics within a single `ObjectStoreStats` share the same
    /// (component, target) label pair. Each field corresponds to one
    /// object store API (e.g. `get`, `put`, `delete`).
    pub(crate) struct ObjectStoreStats {
        pub(crate) get: RequestMetrics,
        pub(crate) get_range: RequestMetrics,
        pub(crate) get_ranges: RequestMetrics,
        pub(crate) head: RequestMetrics,
        pub(crate) put: RequestMetrics,
        pub(crate) multipart_init: RequestMetrics,
        pub(crate) multipart_part: RequestMetrics,
        pub(crate) multipart_complete: RequestMetrics,
        pub(crate) delete: RequestMetrics,
    }

    impl ObjectStoreStats {
        pub(crate) fn new(
            recorder: &MetricsRecorderHelper,
            component: ObjectStoreComponent,
            store_type: ObjectStoreType,
        ) -> Self {
            Self {
                get: RequestMetrics::new(recorder, component, store_type, "get", "get"),
                get_range: RequestMetrics::new(recorder, component, store_type, "get", "get_range"),
                get_ranges: RequestMetrics::new(
                    recorder,
                    component,
                    store_type,
                    "get",
                    "get_ranges",
                ),
                head: RequestMetrics::new(recorder, component, store_type, "get", "head"),
                put: RequestMetrics::new(recorder, component, store_type, "put", "put"),
                multipart_init: RequestMetrics::new(
                    recorder,
                    component,
                    store_type,
                    "put",
                    "multipart_init",
                ),
                multipart_part: RequestMetrics::new(
                    recorder,
                    component,
                    store_type,
                    "put",
                    "multipart_part",
                ),
                multipart_complete: RequestMetrics::new(
                    recorder,
                    component,
                    store_type,
                    "put",
                    "multipart_complete",
                ),
                delete: RequestMetrics::new(recorder, component, store_type, "delete", "delete"),
            }
        }
    }

    /// Metrics for a single object store API (e.g. `get` or `put`).
    ///
    /// Each instance holds three pre-registered metric handles that
    /// share the same label set:
    /// - `request_count` — total calls (success + error).
    /// - `error_count` — failed calls only.
    /// - `duration_seconds` — latency histogram of every call.
    ///
    /// Call [`record`](Self::record) after each request to update all
    /// three atomically.
    pub(crate) struct RequestMetrics {
        request_count: Arc<dyn CounterFn>,
        error_count: Arc<dyn CounterFn>,
        duration_seconds: Arc<dyn HistogramFn>,
    }

    impl RequestMetrics {
        fn new(
            recorder: &MetricsRecorderHelper,
            component: ObjectStoreComponent,
            store_type: ObjectStoreType,
            op: &'static str,
            api: &'static str,
        ) -> Self {
            let labels = [
                ("component", component.as_str()),
                ("store_type", store_type.as_str()),
                ("op", op),
                ("api", api),
            ];

            Self {
                request_count: recorder
                    .counter(REQUEST_COUNT)
                    .description("Object store API requests")
                    .labels(&labels)
                    .register(),
                error_count: recorder
                    .counter(ERROR_COUNT)
                    .description("Object store API request errors")
                    .labels(&labels)
                    .register(),
                duration_seconds: recorder
                    .histogram(REQUEST_DURATION_SECONDS, LATENCY_BOUNDARIES)
                    .description("Object store API request latency in seconds")
                    .labels(&labels)
                    .register(),
            }
        }

        pub(crate) fn record(&self, duration: Duration, success: bool) {
            self.request_count.increment(1);
            if !success {
                self.error_count.increment(1);
            }
            self.duration_seconds.record(duration.as_secs_f64());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use std::sync::Arc;

    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::metrics::{lookup_metric_with_labels, test_recorder_helper, MetricValue};

    use crate::instrumented_object_store::stats::{
        ERROR_COUNT, REQUEST_COUNT, REQUEST_DURATION_SECONDS,
    };
    use crate::instrumented_object_store::{InstrumentedObjectStore, ObjectStoreComponent};
    use crate::object_stores::ObjectStoreType;
    use crate::rand::DbRand;
    use crate::retrying_object_store::RetryingObjectStore;
    use crate::test_utils::FlakyObjectStore;

    fn labels(
        component: &'static str,
        store_type: &'static str,
        op: &'static str,
        api: &'static str,
    ) -> [(&'static str, &'static str); 4] {
        [
            ("component", component),
            ("store_type", store_type),
            ("op", op),
            ("api", api),
        ]
    }

    fn histogram_count(
        recorder: &slatedb_common::metrics::DefaultMetricsRecorder,
        labels: &[(&str, &str)],
    ) -> Option<u64> {
        let snapshot = recorder.snapshot();
        snapshot
            .by_name_and_labels(REQUEST_DURATION_SECONDS, labels)
            .map(|metric| match &metric.value {
                MetricValue::Histogram { count, .. } => *count,
                other => panic!("expected histogram metric, got {other:?}"),
            })
    }

    #[tokio::test]
    async fn test_instrumented_object_store_records_get_put_delete_and_histograms() {
        // given:
        let (recorder, helper) = test_recorder_helper();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = InstrumentedObjectStore::new(
            inner,
            &helper,
            ObjectStoreComponent::Db,
            ObjectStoreType::Main,
        );
        let path = Path::from("a");

        // when:
        store.put(&path, "hello".into()).await.unwrap();
        let _ = store.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();

        // then:
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "put")
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "get", "get")
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "delete", "delete"),
            ),
            Some(1)
        );
        assert_eq!(
            histogram_count(&recorder, &labels("db", "main", "put", "put")),
            Some(1)
        );
        assert_eq!(
            histogram_count(&recorder, &labels("db", "main", "get", "get")),
            Some(1)
        );
        assert_eq!(
            histogram_count(&recorder, &labels("db", "main", "delete", "delete")),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_instrumented_object_store_records_errors() {
        // given:
        let (recorder, helper) = test_recorder_helper();
        let flaky =
            Arc::new(FlakyObjectStore::new(Arc::new(InMemory::new()), 0).with_head_failures(1));
        let store = InstrumentedObjectStore::new(
            flaky,
            &helper,
            ObjectStoreComponent::Db,
            ObjectStoreType::Main,
        );

        // when:
        let err = store.head(&Path::from("missing")).await;

        // then:
        assert!(err.is_err());
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "get", "head")
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(&recorder, ERROR_COUNT, &labels("db", "main", "get", "head")),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_retrying_store_counts_each_put_attempt() {
        // given:
        let (recorder, helper) = test_recorder_helper();
        let flaky = Arc::new(FlakyObjectStore::new(Arc::new(InMemory::new()), 2));
        let instrumented = Arc::new(InstrumentedObjectStore::new(
            flaky.clone(),
            &helper,
            ObjectStoreComponent::Db,
            ObjectStoreType::Main,
        ));
        let retrying = RetryingObjectStore::new(
            instrumented,
            Arc::new(DbRand::default()),
            Arc::new(DefaultSystemClock::default()),
        );

        // when:
        retrying
            .put(&Path::from("a"), "hello".into())
            .await
            .unwrap();

        // then:
        assert_eq!(flaky.put_attempts(), 3);
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "put")
            ),
            Some(3)
        );
    }

    #[tokio::test]
    async fn test_put_multipart_records_each_api_call() {
        // given:
        let (recorder, helper) = test_recorder_helper();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = InstrumentedObjectStore::new(
            inner,
            &helper,
            ObjectStoreComponent::Db,
            ObjectStoreType::Main,
        );
        let path = Path::from("multipart");

        // when:
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload.put_part("hello".into()).await.unwrap();
        upload.complete().await.unwrap();

        // then:
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "multipart_init"),
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "multipart_part"),
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "multipart_complete"),
            ),
            Some(1)
        );
    }

    #[test]
    fn test_distinguishes_series_by_component_and_store_type() {
        // given:
        let (recorder, helper) = test_recorder_helper();
        let db_main = crate::instrumented_object_store::stats::ObjectStoreStats::new(
            &helper,
            ObjectStoreComponent::Db,
            ObjectStoreType::Main,
        );
        let gc_wal = crate::instrumented_object_store::stats::ObjectStoreStats::new(
            &helper,
            ObjectStoreComponent::Gc,
            ObjectStoreType::Wal,
        );

        // when:
        db_main.put.record(Duration::from_millis(1), true);
        gc_wal.put.record(Duration::from_millis(1), false);

        // then:
        assert_eq!(
            lookup_metric_with_labels(
                &recorder,
                REQUEST_COUNT,
                &labels("db", "main", "put", "put")
            ),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(&recorder, ERROR_COUNT, &labels("db", "main", "put", "put")),
            Some(0)
        );
        assert_eq!(
            lookup_metric_with_labels(&recorder, REQUEST_COUNT, &labels("gc", "wal", "put", "put")),
            Some(1)
        );
        assert_eq!(
            lookup_metric_with_labels(&recorder, ERROR_COUNT, &labels("gc", "wal", "put", "put")),
            Some(1)
        );
    }
}
