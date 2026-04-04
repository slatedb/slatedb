package io.slatedb.uniffi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

class SlateDbMetricsTest {
    private static final String DB_REQUEST_COUNT = "slatedb.db.request_count";
    private static final String DB_WRITE_OPS = "slatedb.db.write_ops";

    @Test
    void defaultMetricsRecorderSnapshotAndLookups() throws Exception {
        try (DefaultMetricsRecorder recorder = new DefaultMetricsRecorder()) {
            Counter counter = recorder.registerCounter("test.counter", "counter", List.of());
            Gauge gauge = recorder.registerGauge("test.gauge", "gauge", List.of());
            UpDownCounter upDownCounter =
                    recorder.registerUpDownCounter("test.up_down", "up/down", List.of());
            Histogram histogram =
                    recorder.registerHistogram("test.histogram", "histogram", List.of(), new double[] {1.0, 2.0});

            counter.increment(3);
            gauge.set(-7);
            upDownCounter.increment(5);
            upDownCounter.increment(-2);
            histogram.record(1.5);
            histogram.record(3.0);

            List<Metric> counterMetrics = recorder.metricsByName("test.counter");
            assertEquals(1, counterMetrics.size());

            Metric counterMetric = recorder.metricByNameAndLabels("test.counter", List.of());
            assertNotNull(counterMetric);
            MetricValue.Counter counterValue =
                    assertInstanceOf(MetricValue.Counter.class, counterMetric.value());
            assertEquals(3L, counterValue.v1().longValue());

            Metric histogramMetric = recorder.metricByNameAndLabels("test.histogram", List.of());
            assertNotNull(histogramMetric);
            MetricValue.Histogram histogramValue =
                    assertInstanceOf(MetricValue.Histogram.class, histogramMetric.value());
            assertEquals(2L, histogramValue.v1().count());
            assertEquals(4.5, histogramValue.v1().sum());
            assertArrayEquals(new double[] {1.0, 2.0}, histogramValue.v1().boundaries(), 0.0);
            assertArrayEquals(new long[] {0L, 1L, 1L}, histogramValue.v1().bucketCounts());

            assertTrue(recorder.snapshot().size() >= 4);
        }
    }

    @Test
    void dbBuilderAcceptsCustomMetricsRecorder() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore()) {
            RecordingMetricsRecorder recorder = new RecordingMetricsRecorder();

            try (TestSupport.ManagedDb handle =
                    TestSupport.openDb(store, builder -> builder.withMetricsRecorder(recorder))) {
                Db db = handle.db();
                TestSupport.await(db.put(TestSupport.bytes("k1"), TestSupport.bytes("v1")));
                TestSupport.await(db.put(TestSupport.bytes("k2"), TestSupport.bytes("v2")));
            }

            assertEquals(2L, recorder.counterValue(DB_WRITE_OPS, List.of()));
        }
    }

    @Test
    void readerBuilderAcceptsDefaultMetricsRecorder() throws Exception {
        try (ObjectStore store = TestSupport.newMemoryStore();
                TestSupport.ManagedDb dbHandle = TestSupport.openDb(store);
                DefaultMetricsRecorder recorder = new DefaultMetricsRecorder()) {
            Db db = dbHandle.db();
            TestSupport.await(db.put(TestSupport.bytes("key1"), TestSupport.bytes("value1")));
            TestSupport.await(db.flush());

            try (TestSupport.ManagedReader readerHandle =
                    TestSupport.openReader(store, builder -> builder.withMetricsRecorder(recorder))) {
                byte[] value = TestSupport.await(readerHandle.reader().get(TestSupport.bytes("key1")));
                assertArrayEquals(TestSupport.bytes("value1"), value);
            }

            Metric metric = recorder.metricByNameAndLabels(
                    DB_REQUEST_COUNT,
                    List.of(new MetricLabel("op", "get")));
            assertNotNull(metric);
            MetricValue.Counter counterValue =
                    assertInstanceOf(MetricValue.Counter.class, metric.value());
            assertEquals(1L, counterValue.v1().longValue());
        }
    }

    private static final class RecordingMetricsRecorder implements MetricsRecorder {
        private final Map<String, Long> counters = new ConcurrentHashMap<>();
        private final Map<String, Long> gauges = new ConcurrentHashMap<>();
        private final Map<String, Long> upDownCounters = new ConcurrentHashMap<>();
        private final Map<String, java.util.List<Double>> histograms = new ConcurrentHashMap<>();

        @Override
        public Counter registerCounter(String name, String description, List<MetricLabel> labels) {
            String key = metricKey(name, labels);
            counters.put(key, 0L);
            return value -> counters.compute(key, (_ignored, current) -> current + value);
        }

        @Override
        public Gauge registerGauge(String name, String description, List<MetricLabel> labels) {
            String key = metricKey(name, labels);
            gauges.put(key, 0L);
            return value -> gauges.put(key, value);
        }

        @Override
        public UpDownCounter registerUpDownCounter(String name, String description, List<MetricLabel> labels) {
            String key = metricKey(name, labels);
            upDownCounters.put(key, 0L);
            return value -> upDownCounters.compute(key, (_ignored, current) -> current + value);
        }

        @Override
        public Histogram registerHistogram(
                String name,
                String description,
                List<MetricLabel> labels,
                double[] boundaries) {
            String key = metricKey(name, labels);
            histograms.put(key, java.util.Collections.synchronizedList(new java.util.ArrayList<>()));
            return value -> histograms.get(key).add(value);
        }

        Long counterValue(String name, List<MetricLabel> labels) {
            return counters.get(metricKey(name, labels));
        }

        private static String metricKey(String name, List<MetricLabel> labels) {
            String labelKey = labels.stream()
                    .sorted(Comparator.comparing(MetricLabel::key).thenComparing(MetricLabel::value))
                    .map(label -> label.key() + "=" + label.value())
                    .reduce((left, right) -> left + "," + right)
                    .orElse("");
            return name + "|" + labelKey;
        }
    }
}
