from __future__ import annotations

import pytest

from conftest import new_memory_store, open_db, open_reader
from slatedb.uniffi import (
    Counter,
    DefaultMetricsRecorder,
    Gauge,
    Histogram,
    MetricLabel,
    MetricsRecorder,
    MetricValue,
    UpDownCounter,
)

DB_REQUEST_COUNT = "slatedb.db.request_count"
DB_WRITE_OPS = "slatedb.db.write_ops"


def metric_label_key(labels: list[MetricLabel]) -> str:
    return ",".join(sorted(f"{label.key}={label.value}" for label in labels))


class _CounterHandle(Counter):
    def __init__(self, callback):
        self._callback = callback

    def increment(self, value: int) -> None:
        self._callback(value)


class _GaugeHandle(Gauge):
    def __init__(self, callback):
        self._callback = callback

    def set(self, value: int) -> None:
        self._callback(value)


class _UpDownCounterHandle(UpDownCounter):
    def __init__(self, callback):
        self._callback = callback

    def increment(self, value: int) -> None:
        self._callback(value)


class _HistogramHandle(Histogram):
    def __init__(self, callback):
        self._callback = callback

    def record(self, value: float) -> None:
        self._callback(value)


class RecordingMetricsRecorder(MetricsRecorder):
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.gauges: dict[str, int] = {}
        self.up_down_counters: dict[str, int] = {}
        self.histograms: dict[str, list[float]] = {}

    def register_counter(self, name: str, description: str, labels: list[MetricLabel]) -> Counter:
        del description
        key = f"{name}|{metric_label_key(labels)}"
        self.counters[key] = 0

        return _CounterHandle(lambda value: self.counters.__setitem__(key, self.counters[key] + value))

    def register_gauge(self, name: str, description: str, labels: list[MetricLabel]) -> Gauge:
        del description
        key = f"{name}|{metric_label_key(labels)}"
        self.gauges[key] = 0

        return _GaugeHandle(lambda value: self.gauges.__setitem__(key, value))

    def register_up_down_counter(
        self,
        name: str,
        description: str,
        labels: list[MetricLabel],
    ) -> UpDownCounter:
        del description
        key = f"{name}|{metric_label_key(labels)}"
        self.up_down_counters[key] = 0

        return _UpDownCounterHandle(
            lambda value: self.up_down_counters.__setitem__(key, self.up_down_counters[key] + value)
        )

    def register_histogram(
        self,
        name: str,
        description: str,
        labels: list[MetricLabel],
        boundaries: list[float],
    ) -> Histogram:
        del description, boundaries
        key = f"{name}|{metric_label_key(labels)}"
        self.histograms[key] = []

        return _HistogramHandle(lambda value: self.histograms[key].append(value))

    def counter_value(self, name: str, labels: list[MetricLabel] | None = None) -> int | None:
        return self.counters.get(f"{name}|{metric_label_key(labels or [])}")


def _metric_counter_value(value: MetricValue) -> int:
    assert isinstance(value, MetricValue.COUNTER)
    return value[0]


@pytest.mark.asyncio
async def test_default_metrics_recorder_snapshot_and_lookups() -> None:
    recorder = DefaultMetricsRecorder()

    recorder.register_counter("test.counter", "counter", []).increment(3)
    recorder.register_gauge("test.gauge", "gauge", []).set(-7)
    up_down_counter = recorder.register_up_down_counter("test.up_down", "up/down", [])
    up_down_counter.increment(5)
    up_down_counter.increment(-2)
    histogram = recorder.register_histogram("test.histogram", "histogram", [], [1.0, 2.0])
    histogram.record(1.5)
    histogram.record(3.0)

    metrics = recorder.metrics_by_name("test.counter")
    assert len(metrics) == 1
    assert _metric_counter_value(metrics[0].value) == 3

    histogram_metric = recorder.metric_by_name_and_labels("test.histogram", [])
    assert histogram_metric is not None
    assert isinstance(histogram_metric.value, MetricValue.HISTOGRAM)
    assert histogram_metric.value[0].count == 2
    assert histogram_metric.value[0].sum == 4.5
    assert histogram_metric.value[0].boundaries == [1.0, 2.0]
    assert histogram_metric.value[0].bucket_counts == [0, 1, 1]

    snapshot = recorder.snapshot()
    assert len(snapshot) >= 4


@pytest.mark.asyncio
async def test_db_builder_accepts_custom_metrics_recorder() -> None:
    store = new_memory_store()
    recorder = RecordingMetricsRecorder()

    async with open_db(
        store,
        configure=lambda builder: builder.with_metrics_recorder(recorder),
    ) as db:
        await db.put(b"k1", b"v1")
        await db.put(b"k2", b"v2")

    assert recorder.counter_value(DB_WRITE_OPS) == 2


@pytest.mark.asyncio
async def test_reader_builder_accepts_default_metrics_recorder() -> None:
    store = new_memory_store()

    async with open_db(store) as db:
        await db.put(b"key1", b"value1")
        await db.flush()

    recorder = DefaultMetricsRecorder()
    async with open_reader(
        store,
        configure=lambda builder: builder.with_metrics_recorder(recorder),
    ) as reader:
        assert await reader.get(b"key1") == b"value1"

    metric = recorder.metric_by_name_and_labels(
        DB_REQUEST_COUNT,
        [MetricLabel(key="op", value="get")],
    )
    assert metric is not None
    assert _metric_counter_value(metric.value) == 1
