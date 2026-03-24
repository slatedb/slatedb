# SlateDB Metrics 

Status: Draft

Authors:

* [Almog Gavra](https://github.com/agavra)

## Summary

Replace SlateDB's `StatRegistry` with a recorder-based metrics system that supports labels,
histograms, and user-pluggable backends (Prometheus, OTLP, etc.) while preserving a
zero-dependency, always-on default for `db.metrics()` debugging. The new system uses a
`MetricsRecorder` trait that users can implement to bridge metrics to their observability
stack, and an internal composite recorder that forwards events to both the built-in default
(atomic-backed) and the user's recorder.

## Motivation

The current system (`StatRegistry` with `Counter`/`Gauge<T>`, flat `"prefix/suffix"` names)
has several limitations:

1. **No labels/dimensions.** Cache hits are tracked as 8 separate counters (`dbcache/filter_hit`,
   `dbcache/filter_miss`, `dbcache/index_hit`, ...) instead of one metric with `{entry_kind, result}`
   labels. This makes it hard to aggregate in Prometheus/Grafana.

2. **No histograms.** Latency-sensitive operations (object store requests, DB gets) have no
   distribution tracking.

3. **Hard to integrate.** Users must poll `StatRegistry`, match on string names, and manually
   bridge to their observability stack. There's no hook to receive events as they happen.

## Goals

- Support multi-dimensional metrics (labels) to reduce metric proliferation
- Add histogram support for latency/size distributions
- Provide a pluggable `MetricsRecorder` trait for Prometheus/OTLP/custom backends
- Keep an always-on default recorder so `db.metrics()` works without configuration
- Return FFI-friendly types from `db.metrics()`
- No new external dependencies

## Non-Goals

- Providing built-in Prometheus/OTLP recorder implementations (users bring their own)

## Design

### Core traits

```rust
// slatedb/src/metrics.rs

/// User-implemented trait to bridge SlateDB metrics to their observability system.
/// Passed via DbBuilder. If not provided, only the built-in default recorder is used.
pub trait MetricsRecorder: Send + Sync {
    fn register_counter(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn CounterFn>;
    fn register_gauge(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn GaugeFn>;
    fn register_histogram(&self, name: &str, description: &str, labels: &[(&str, &str)]) -> Arc<dyn HistogramFn>;
}

pub trait CounterFn: Send + Sync {
    fn increment(&self, value: u64);
}

pub trait GaugeFn: Send + Sync {
    fn set(&self, value: f64);
    fn increment(&self, value: f64);  // negative to decrement
}

pub trait HistogramFn: Send + Sync {
    fn record(&self, value: f64);
}
```

**Key decisions:**
- **Labels are `&[(&str, &str)]`** fixed at registration time. Label identity is an unordered set of unique key/value pairs, not the input slice order. Duplicate keys are invalid. SlateDB canonicalizes labels during registration (for example by sorting by key then value) so each unique `(name, labels)` combo maps to exactly one handle.
- **Single gauge abstraction.** SlateDB exposes one `GaugeFn` for both absolute values (`set`) and additive changes (`increment` with negative values allowed). This keeps the API simple even though some backends, notably OpenTelemetry, distinguish between gauges and up/down counters more strictly.
- **Counters stay integer; gauges and histograms use floating point.** `CounterFn` uses `u64`, while `GaugeFn` and `HistogramFn` use `f64`. This preserves natural counter semantics while simplifying the trait surface vs. the current generic `Gauge<T>` (which has separate impls for `i64`, `u64`, `i32`, `bool`). Precision loss for large integer-valued gauges is acceptable for metrics.
- **No external dependency.** SlateDB owns all trait definitions.

### Internal architecture

```
DbBuilder {
    metrics_recorder: Option<Arc<dyn MetricsRecorder>>,
}
         |
         v
  +------------------------+
  | CompositeMetricsRecorder|  (wraps default + user's recorder)
  +------------------------+
       /              \
      v                v
  DefaultMetrics-    User's
  Recorder           (Prometheus, OTLP, etc.)
  (atomics)
```

**CompositeMetricsRecorder** implements `MetricsRecorder` and is what all internal components
receive (as `Arc<dyn MetricsRecorder>`). When a metric is registered:

1. Always registers with `DefaultMetricsRecorder` (atomic-backed, for `db.metrics()`)
2. If user provided a recorder, also registers with it
3. Returns a composite handle that forwards `increment`/`set`/`record` to both

This means:
- `db.metrics()` always works (reads from default recorder's atomics)
- User's recorder gets all events too
- When no user recorder is provided, the composite just delegates to the default (single virtual dispatch, no branch)

### Default recorder internals

| Metric type | Backing | Read |
|------------|---------|-------------------|
| Counter | `AtomicU64` | `.load(Relaxed)` |
| Gauge | `AtomicU64` (f64 bit-cast) | `.load(Relaxed)` -> `f64::from_bits` |
| Histogram | `AtomicU64` x4 (count, sum, min, max) | CAS loops on write; `.load(Relaxed)` on read |

**Histogram writes:** As of this RFC, we do **not** maintain bucket
distributions, so users who need percentiles, heat maps, or
Prometheus/OpenTelemetry-native histogram aggregation should provide their own
recorder with proper bucket boundaries.

The default recorder stores registered metrics in a `Mutex<Vec<MetricEntry>>` (write-once at
startup, never contended on the hot path since handles are cached).

### Metrics API

`db.metrics()` returns a materialized snapshot of the current metric values. This API is intended
for "lightweight" observability and debugging usecases. If performance is desired, it's better to
configure a proper recorder.

In this implementation, each metric is read individually via `Relaxed` atomic
loads and copied into the returned `Metrics` value, so there is no guarantee
that all values in a single result are from the same instant. 

```rust
pub struct Metric {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub description: String,
    pub value: MetricValue,
}

pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram { count: u64, sum: f64, min: f64, max: f64 },
}

/// Materialized snapshot of all registered metrics, with lookup methods.
pub struct Metrics {
    // private so we can change the impl in the future
    metrics: Vec<Metric>,
}

impl Metrics {
    /// Return all metrics.
    pub fn all(&self) -> &[Metric] { ... }

    /// Look up all metrics matching a given name (any labels).
    pub fn by_name(&self, name: &str) -> Vec<&Metric> { ... }

    /// Look up the unique metric matching a name and exact canonical label set.
    /// Input label order does not matter.
    pub fn by_name_and_labels(&self, name: &str, labels: &[(&str, &str)]) -> Option<&Metric> { ... }
}

impl Db {
    /// Returns a snapshot of the current values of all registered metrics.
    pub fn metrics(&self) -> Metrics;
}
```

This replaces `pub fn metrics(&self) -> Arc<StatRegistry>`. The new return type preserves the
ability to look up specific metrics by name (like `StatRegistry::lookup` today) while also
supporting label-based filtering. It's FFI-friendly (the inner `Vec<Metric>` maps
directly to a UniFFI sequence) and doesn't expose internal state. Additional query methods
(e.g. `by_label`, `names()`) can be added later without breaking changes.

The intended split is:
- `db.metrics()`: snapshot API for debugging, tests, and language bindings
- `MetricsRecorder`: event/export path for observability backends

### Migration

Naming convention is dot-separated: `slatedb.<subsystem>.<metric_name>` to conform with
typical OpenTelemetry stndards (Prometheus users can easily convert when they register).

Here is an AI-assisted listing of the current metrics and proposed mapped metrics:

| Current name | New name | Labels |
|-------------|----------|--------|
| `db/get_requests` | `slatedb.db.request_count` | `{op=get}` |
| `db/scan_requests` | `slatedb.db.request_count` | `{op=scan}` |
| `db/flush_requests` | `slatedb.db.request_count` | `{op=flush}` |
| `db/write_ops` | `slatedb.db.write_ops` | |
| `db/write_batch_count` | `slatedb.db.write_batch_count` | |
| `db/backpressure_count` | `slatedb.db.backpressure_count` | |
| `db/immutable_memtable_flushes` | `slatedb.db.immutable_memtable_flushes` | |
| `db/wal_buffer_flushes` | `slatedb.db.wal_buffer_flushes` | |
| `db/wal_buffer_estimated_bytes` | `slatedb.db.wal_buffer_estimated_bytes` | |
| `db/total_mem_size_bytes` | `slatedb.db.total_mem_size_bytes` | |
| `db/l0_sst_count` | `slatedb.db.l0_sst_count` | |
| `db/sst_filter_false_positives` | `slatedb.db.sst_filter_false_positive_count` | |
| `db/sst_filter_positives` | `slatedb.db.sst_filter_positive_count` | |
| `db/sst_filter_negatives` | `slatedb.db.sst_filter_negative_count` | |
| `dbcache/filter_hit` | `slatedb.db_cache.access_count` | `{entry_kind=filter, result=hit}` |
| `dbcache/filter_miss` | `slatedb.db_cache.access_count` | `{entry_kind=filter, result=miss}` |
| `dbcache/index_hit` | `slatedb.db_cache.access_count` | `{entry_kind=index, result=hit}` |
| `dbcache/index_miss` | `slatedb.db_cache.access_count` | `{entry_kind=index, result=miss}` |
| `dbcache/data_block_hit` | `slatedb.db_cache.access_count` | `{entry_kind=data_block, result=hit}` |
| `dbcache/data_block_miss` | `slatedb.db_cache.access_count` | `{entry_kind=data_block, result=miss}` |
| `dbcache/stats_hit` | `slatedb.db_cache.access_count` | `{entry_kind=stats, result=hit}` |
| `dbcache/stats_miss` | `slatedb.db_cache.access_count` | `{entry_kind=stats, result=miss}` |
| `dbcache/get_error` | `slatedb.db_cache.error_count` | |
| `gc/manifest_count` | `slatedb.gc.deleted_count` | `{resource=manifest}` |
| `gc/wal_count` | `slatedb.gc.deleted_count` | `{resource=wal}` |
| `gc/compacted_count` | `slatedb.gc.deleted_count` | `{resource=compacted}` |
| `gc/compactions_count` | `slatedb.gc.deleted_count` | `{resource=compactions}` |
| `compactor/bytes_compacted` | `slatedb.compactor.bytes_compacted` | |
| `compactor/last_compaction_timestamp_sec` | `slatedb.compactor.last_compaction_timestamp_sec` | |
| `compactor/running_compactions` | `slatedb.compactor.running_compactions` | |
| `compactor/total_bytes_being_compacted` | `slatedb.compactor.total_bytes_being_compacted` | |
| `compactor/total_throughput_bytes_per_sec` | `slatedb.compactor.total_throughput_bytes_per_sec` | |
| `oscache/part_hits` | `slatedb.object_store_cache.part_hit_count` | |
| `oscache/part_access` | `slatedb.object_store_cache.part_access_count` | |
| `oscache/cache_keys` | `slatedb.object_store_cache.cache_keys` | |
| `oscache/cache_bytes` | `slatedb.object_store_cache.cache_bytes` | |
| `oscache/evicted_keys` | `slatedb.object_store_cache.evicted_keys` | |
| `oscache/evicted_bytes` | `slatedb.object_store_cache.evicted_bytes` | |

**Standard label vocabulary:**
- `op`: `get`, `put`, `delete`, `scan`, `flush`
- `entry_kind`: `filter`, `index`, `data_block`, `stats`
- `result`: `hit`, `miss`
- `resource`: `manifest`, `wal`, `compacted`, `compactions`

### Registration ergonomics

Internal code registers metrics during component initialization:

```rust
// In DbStats::new(recorder: &dyn MetricsRecorder)
let get_requests = recorder.register_counter(
    "slatedb.db.request_count",
    "Number of DB requests",
    &[("op", "get")],
);

// Hot path (unchanged pattern from today)
self.db_stats.get_requests.increment(1);
```

Each stats struct changes its field types:

```rust
// Before:
pub(crate) struct DbStats {
    pub(crate) get_requests: Arc<Counter>,
    pub(crate) wal_buffer_estimated_bytes: Arc<Gauge<i64>>,
}

// After:
pub(crate) struct DbStats {
    pub(crate) get_requests: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_estimated_bytes: Arc<dyn GaugeFn>,
}
```

### User integration examples

#### Prometheus

```rust
use prometheus_client::metrics::counter::Counter as PromCounter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge as PromGauge;
use prometheus_client::metrics::histogram::Histogram as PromHistogram;
use prometheus_client::registry::Registry;

// Sketch only: the recorder should keep one Family per metric definition and
// materialize the labeled child metric for each (name, labels) registration.
struct PrometheusRecorder {
    registry: Mutex<Registry>,
    counters: Mutex<HashMap<String, Family<PromLabels, PromCounter>>>,
    gauges: Mutex<HashMap<String, Family<PromLabels, PromGauge>>>,
    histograms: Mutex<HashMap<String, Family<PromLabels, PromHistogram>>>,
}

impl MetricsRecorder for PrometheusRecorder {
    fn register_counter(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn CounterFn> {
        let labels = PromLabels::from_slice(labels);
        let family = self.lookup_or_register_counter_family(name, desc);
        let counter = family.get_or_create(&labels).clone();
        Arc::new(PrometheusCounter(counter))
    }

    fn register_gauge(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn GaugeFn> {
        let labels = PromLabels::from_slice(labels);
        let family = self.lookup_or_register_gauge_family(name, desc);
        let gauge = family.get_or_create(&labels).clone();
        Arc::new(PrometheusGauge(gauge))
    }

    fn register_histogram(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn HistogramFn> {
        let labels = PromLabels::from_slice(labels);
        let family = self.lookup_or_register_histogram_family(
            name,
            desc,
            || PromHistogram::new(exponential_buckets(0.001, 2.0, 16)),
        );
        let histogram = family.get_or_create(&labels).clone();
        Arc::new(PrometheusHistogram(histogram))
    }
}

let db = Db::builder("my_db", object_store)
    .with_metrics_recorder(Arc::new(PrometheusRecorder::new()))
    .open()
    .await?;
```

Because SlateDB passes labels at registration time, a Prometheus recorder should treat each
registration as one labeled time series within a metric family, not as a completely separate
top-level metric. `prometheus-client`'s `Family<Labels, Metric>` is the natural fit here.

#### OpenTelemetry

```rust
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;

struct OtelRecorder { meter: opentelemetry::metrics::Meter }

impl OtelRecorder {
    fn new(provider: &SdkMeterProvider) -> Self {
        Self { meter: provider.meter("slatedb") }
    }
}

impl MetricsRecorder for OtelRecorder {
    fn register_counter(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn CounterFn> {
        let attrs: Vec<opentelemetry::KeyValue> = labels.iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        let counter = self.meter.u64_counter(name).with_description(desc).build();
        Arc::new(OtelCounter { counter, attrs })
    }

    fn register_gauge(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn GaugeFn> {
        let attrs: Vec<opentelemetry::KeyValue> = labels.iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        let gauge = self.meter.f64_gauge(name).with_description(desc).build();
        Arc::new(OtelGauge::new(gauge, attrs))
    }

    fn register_histogram(&self, name: &str, desc: &str, labels: &[(&str, &str)]) -> Arc<dyn HistogramFn> {
        let attrs: Vec<opentelemetry::KeyValue> = labels.iter()
            .map(|(k, v)| opentelemetry::KeyValue::new(k.to_string(), v.to_string()))
            .collect();
        let histogram = self.meter.f64_histogram(name).with_description(desc).build();
        Arc::new(OtelHistogram { histogram, attrs })
    }
}

// OTel exporters (OTLP, stdout, Prometheus bridge) are configured via SdkMeterProvider.
// See: https://docs.rs/opentelemetry/latest/opentelemetry/metrics/index.html
let provider = SdkMeterProvider::builder()
    .with_periodic_exporter(opentelemetry_otlp::MetricExporter::builder().build()?)
    .build();

let db = Db::builder("my_db", object_store)
    .with_metrics_recorder(Arc::new(OtelRecorder::new(&provider)))
    .open()
    .await?;
```

OpenTelemetry is stricter about instrument semantics than Prometheus, but this RFC chooses a
simpler SlateDB-facing API over exact backend parity. A practical OTel recorder can implement
`GaugeFn` in either of these ways:

- map both `set` and `increment` onto a gauge by keeping local state and recording the current value
- use a gauge for `set`-heavy metrics and an up/down counter internally for additive metrics

The important point is that SlateDB exposes a simple recorder contract; exact instrument choice
inside a backend adapter is an implementation detail.

### UniFFI integration

The existing bindings expose `db.metrics()` as `HashMap<String, i64>`. The new design
improves the read path and keeps the bindings aligned across the existing UniFFI surfaces:

**Read path:** `Metric` maps to a UniFFI Record, `MetricValue` to a UniFFI Enum, and
`db.metrics()` returns a sequence of `Metric` values. Labels use a `MetricLabel { key, value }`
Record since UniFFI doesn't support tuple fields like `(String, String)`. This keeps the
foreign-language surface simple: Go, Java, and other bindings receive a fully materialized
snapshot and can do name/label filtering locally.

**Write path:** Out of scope for this RFC. Although the repository already uses UniFFI callback
interfaces for some features, exposing `MetricsRecorder` itself to foreign languages would require
additional design work around callback handle lifetimes, callback error handling, and the hot-path
cost of forwarding every metric operation across FFI. This RFC only standardizes the core Rust
trait and the structured read-side binding surface.

## Impact Analysis

### Core API & Query Semantics

- [ ] Basic KV API (`get`/`put`/`delete`)
- [ ] Range queries, iterators, seek semantics
- [ ] Range deletions
- [ ] Error model, API errors

### Consistency, Isolation, and Multi-Versioning

- [ ] Transactions
- [ ] Snapshots
- [ ] Sequence numbers

### Time, Retention, and Derived State

- [ ] Time to live (TTL)
- [ ] Compaction filters
- [ ] Merge operator
- [ ] Change Data Capture (CDC)

### Metadata, Coordination, and Lifecycles

- [ ] Manifest format
- [ ] Checkpoints
- [ ] Clones
- [x] Garbage collection
- [ ] Database splitting and merging
- [ ] Multi-writer

### Compaction

- [x] Compaction state persistence
- [ ] Compaction filters
- [ ] Compaction strategies
- [ ] Distributed compaction
- [ ] Compactions format

### Storage Engine Internals

- [ ] Write-ahead log (WAL)
- [x] Block cache
- [x] Object store cache
- [x] Indexing (bloom filters, metadata)
- [ ] SST format or block format

### Ecosystem & Operations

- [ ] CLI tools
- [x] Language bindings (Go/Python/etc)
- [x] Observability (metrics/logging/tracing)

## Operations

### Performance & Cost

- **Hot path:** One additional virtual dispatch per metric operation (through composite handle). When no user recorder is configured, the composite delegates directly to the default atomic handle. This is negligible compared to I/O costs.
- **Registration:** Takes a mutex lock in `DefaultMetricsRecorder`, but only during startup. No contention on the hot path since handles are cached.
- **Memory:** Slightly more per metric (two `Arc<dyn Fn>` handles instead of one `Arc<Counter>`) when a user recorder is present. Negligible.
- No impact on object-store request patterns, space/read/write amplification.

### Observability

- **Configuration changes:** New `DbBuilder::with_metrics_recorder(Arc<dyn MetricsRecorder>)` method. No changes to `Settings` (the recorder is a runtime component, not serializable config).
- **New components:** `metrics.rs` module (~300 lines). `stats.rs` is removed.
- **Metrics:** All 39 existing metrics preserved with new names/labels (see naming table above). Histogram support added as a new metric type.
- **Logging:** No changes.

### Compatibility

- **On-disk/object storage formats:** No impact. Metrics are purely in-memory.
- **Public API:** Breaking change:
  - `db.metrics()` return type: `Arc<StatRegistry>` -> `Metrics`
  - Removed: `StatRegistry`, `ReadableStat`, `Counter`, `Gauge<T>`, `stat_name!` macro
  - Removed: Public stat name constants (`db_stats::GET_REQUESTS`, etc.)
  - New: `MetricsRecorder`, `CounterFn`, `GaugeFn`, `HistogramFn`, `Metrics`, `Metric`, `MetricValue`
- **Bindings:** The `db.metrics()` return type is simpler to expose via UniFFI than the current trait-object-based `StatRegistry`. The bindings can return a sequence of `Metric` Records directly.

## Testing

- **Unit tests:** `metrics.rs` tests for default recorder (counter/gauge/histogram tracking), composite recorder (forwards to both), edge cases (empty histogram).
- **Integration tests:** Register a mock `MetricsRecorder`, perform DB operations, verify handles receive correct calls with expected names and labels. Verify `db.metrics()` returns metrics with correct labels and non-zero values.
- **Port existing tests:** All existing tests that read metrics (`compactor::test_compactor_compressed_block_size`, db cache hit/miss tests, `sst_iter` bloom filter tests) updated to new API.
- Fault-injection/chaos tests: N/A
- Deterministic simulation tests: N/A
- Formal methods verification: N/A
- Performance tests: N/A

## Rollout

Direct, one-time breaking change will be included with an 0.X.X release.

## Alternatives

### 1. Adopt the `metrics` crate facade

The Rust [`metrics`](https://crates.io/crates/metrics) crate provides a global recorder pattern
similar to what we're proposing. We rejected this because:
- It adds an external dependency that may conflict with users' own `metrics` usage.
- Global state doesn't work well with multiple `Db` instances in the same process.
- UniFFI bindings need to own the trait definitions.

### 2. Keep `StatRegistry` and add labels as a map field

We could extend `StatRegistry` to store `HashMap<String, String>` labels per metric. This was
rejected because:
- It doesn't solve the pluggable-backend problem (users still poll).
- The `ReadableStat` trait returning `i64` can't represent histograms or `f64` gauges.
- The `Gauge<T>` generic makes FFI difficult.

### 3. Status quo

Do nothing. Users continue to poll `StatRegistry` and manually bridge metrics. Rejected because
this doesn't meet the goal of first-class Prometheus/OTLP support, and the label/histogram gaps
make operational use painful.

### 4. Avoid dynamic dispatch on metric handles

The proposed `Arc<dyn CounterFn>` fields mean every hot-path `increment` call goes through a
vtable. Two alternatives were considered to avoid this:

- **Enum dispatch.** Replace `Arc<dyn CounterFn>` with a concrete enum:
  ```rust
  enum CounterHandle {
      Default(Arc<AtomicCounter>),
      Composite { default: Arc<AtomicCounter>, user: Arc<dyn CounterFn> },
  }
  ```
  This avoids one vtable indirection for the no-user-recorder case (the common path), but
  still needs `dyn CounterFn` for the user handle since its type is unknown. It also leaks the
  composite abstraction into every stats struct.

- **Generic stats structs.** Make `DbStats<R: MetricsRecorder>` generic over the recorder.
  This eliminates all dynamic dispatch but requires monomorphization through 20+ files and
  makes the `Db` type itself generic, which is a non-starter for the public API and FFI.

Rejected because a vtable call costs ~2-5ns on modern CPUs, which is negligible on paths that
do I/O. If profiling shows it matters, we can switch to enum dispatch for the composite handles
without changing any external API.

## Open Questions

1. **Histogram default bucket boundaries:** For the default recorder, we track only count/sum/min/max. Should we also store a lightweight fixed-bucket histogram for richer `db.metrics()` output? Recommendation: start minimal, add later if needed.

## References

- Current stats module: `slatedb/src/stats.rs`
- Rust `metrics` crate (prior art): https://crates.io/crates/metrics
- Prometheus client_rust: https://crates.io/crates/prometheus-client
- OpenTelemetry Rust SDK: https://crates.io/crates/opentelemetry

## Updates

(none yet)
