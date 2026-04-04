import assert from "node:assert/strict";
import test from "node:test";

import { DefaultMetricsRecorder } from "../index.js";
import {
  bytes,
  createCleanup,
  newMemoryStore,
  openDb,
  openReader,
  putOptions,
  writeOptions,
} from "./support.mjs";

const DB_REQUEST_COUNT = "slatedb.db.request_count";
const DB_WRITE_OPS = "slatedb.db.write_ops";

function toBigInt(value) {
  return typeof value === "bigint" ? value : BigInt(value);
}

function metricLabelKey(labels) {
  if (labels.length === 0) {
    return "";
  }

  return labels
    .map((label) => `${label.key}=${label.value}`)
    .sort()
    .join(",");
}

class TestMetricsRecorder {
  counters = new Map();

  gauges = new Map();

  upDownCounters = new Map();

  histograms = new Map();

  register_counter(name, _description, labels) {
    const key = `${name}|${metricLabelKey(labels)}`;
    this.counters.set(key, 0n);

    return {
      increment: (value) => {
        this.counters.set(key, this.counters.get(key) + toBigInt(value));
      },
    };
  }

  register_gauge(name, _description, labels) {
    const key = `${name}|${metricLabelKey(labels)}`;
    this.gauges.set(key, 0n);

    return {
      set: (value) => {
        this.gauges.set(key, toBigInt(value));
      },
    };
  }

  register_up_down_counter(name, _description, labels) {
    const key = `${name}|${metricLabelKey(labels)}`;
    this.upDownCounters.set(key, 0n);

    return {
      increment: (value) => {
        this.upDownCounters.set(key, this.upDownCounters.get(key) + toBigInt(value));
      },
    };
  }

  register_histogram(name, _description, labels, _boundaries) {
    const key = `${name}|${metricLabelKey(labels)}`;
    this.histograms.set(key, []);

    return {
      record: (value) => {
        this.histograms.get(key).push(value);
      },
    };
  }

  counterValue(name, labels = []) {
    return this.counters.get(`${name}|${metricLabelKey(labels)}`);
  }
}

test("default metrics recorder snapshots and lookups", async (t) => {
  const cleanup = createCleanup(t);
  const recorder = cleanup.track(new DefaultMetricsRecorder(), { shutdown: false });

  recorder.register_counter("test.counter", "counter", []).increment(3);
  recorder.register_gauge("test.gauge", "gauge", []).set(-7);
  const upDownCounter = recorder.register_up_down_counter("test.up-down", "up-down", []);
  upDownCounter.increment(5);
  upDownCounter.increment(-2);
  const histogram = recorder.register_histogram("test.histogram", "histogram", [], [1, 2]);
  histogram.record(1.5);
  histogram.record(3);

  const metricsByName = recorder.metrics_by_name("test.counter");
  assert.equal(metricsByName.length, 1);
  assert.equal(metricsByName[0].value.tag, "Counter");
  assert.equal(toBigInt(metricsByName[0].value[""]), 3n);

  const histogramMetric = recorder.metric_by_name_and_labels("test.histogram", []);
  assert.notEqual(histogramMetric, undefined);
  assert.equal(histogramMetric.value.tag, "Histogram");
  assert.equal(toBigInt(histogramMetric.value[""].count), 2n);
  assert.equal(histogramMetric.value[""].sum, 4.5);
  assert.deepEqual(histogramMetric.value[""].boundaries, [1, 2]);
  assert.deepEqual(
    histogramMetric.value[""].bucket_counts.map(toBigInt),
    [0n, 1n, 1n],
  );

  const snapshot = recorder.snapshot();
  assert.ok(snapshot.length >= 4);
});

test("db builder accepts custom metrics recorder", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const recorder = new TestMetricsRecorder();
  const db = await openDb(store, {
    cleanup,
    configure(builder) {
      builder.with_metrics_recorder(recorder);
    },
  });

  await db.put(bytes("k1"), bytes("v1"));
  await db.put(bytes("k2"), bytes("v2"));

  assert.equal(recorder.counterValue(DB_WRITE_OPS), 2n);
});

test("reader builder accepts default metrics recorder", async (t) => {
  const cleanup = createCleanup(t);
  const store = cleanup.track(newMemoryStore());
  const db = await openDb(store, { cleanup });

  await db.put_with_options(
    bytes("key1"),
    bytes("value1"),
    putOptions(),
    writeOptions(false),
  );
  await db.flush();

  const recorder = cleanup.track(new DefaultMetricsRecorder(), { shutdown: false });
  const reader = await openReader(store, {
    cleanup,
    configure(builder) {
      builder.with_metrics_recorder(recorder);
    },
  });

  assert.deepEqual(await reader.get(bytes("key1")), bytes("value1"));

  const metric = recorder.metric_by_name_and_labels(DB_REQUEST_COUNT, [
    { key: "op", value: "get" },
  ]);
  assert.notEqual(metric, undefined);
  assert.equal(metric.value.tag, "Counter");
  assert.equal(toBigInt(metric.value[""]), 1n);
});
