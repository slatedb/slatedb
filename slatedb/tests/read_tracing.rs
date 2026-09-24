#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

//! Integration test for read tracing span instrumentation
//!
//! This test verifies that the correct tracing spans are produced when read operations
//! are executed with a trace ID in their options.

use bytes::{Bytes, BytesMut};
use slatedb::config::{
    CompactionWorkerOptions, CompactorOptions, FlushOptions, FlushType, ReadOptions, ScanOptions,
    Settings, SizeTieredCompactionSchedulerOptions, TracingOptions,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::size_tiered_compaction::SizeTieredCompactionSchedulerSupplier;
use slatedb::{CompactorBuilder, Db, MergeOperator, MergeOperatorError};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tracing_subscriber::fmt::{format::FmtSpan, Subscriber as FmtSubscriber};

const SST_SPAN_NAMES: [&str; 4] = [
    "slatedb.read.read_filters",
    "slatedb.read.evaluate_filter",
    "slatedb.read.read_index",
    "slatedb.read.read_blocks",
];

#[derive(Clone, Default)]
struct CapturedOutput(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter<'a>(MutexGuard<'a, Vec<u8>>);

impl Write for CapturedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedOutput {
    type Writer = CapturedWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.lock().expect("trace output lock failed"))
    }
}

impl CapturedOutput {
    fn mark(&self) -> usize {
        self.0.lock().expect("trace output lock failed").len()
    }

    fn since(&self, mark: usize) -> String {
        let output = self.0.lock().expect("trace output lock failed");
        String::from_utf8(output[mark..].to_vec()).expect("trace output is not UTF-8")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn read_spans_work_with_fmt_subscriber_on_one_thread() {
    run_read_tracing().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_spans_work_with_fmt_subscriber_on_multiple_threads() {
    run_read_tracing().await;
}

async fn run_read_tracing() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db_dir = tempfile::tempdir().expect("failed to create database directory");
    let path = db_dir.path().to_string_lossy().into_owned();
    let db = Db::builder(path.clone(), store.clone())
        .with_settings(test_settings(true))
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .with_compactor_builder(
            CompactorBuilder::new(path.clone(), store.clone())
                .with_options(compactor_options())
                .with_scheduler_supplier(Arc::new(SizeTieredCompactionSchedulerSupplier::new())),
        )
        .build()
        .await
        .expect("failed to open database");

    db.put(b"trace:sorted-a", b"sorted-a")
        .await
        .expect("failed to write sorted-run key");
    db.merge(b"trace:merged", b"sorted")
        .await
        .expect("failed to write sorted-run merge operand");
    flush_memtable(&db).await;
    db.put(b"trace:sorted-b", b"sorted-b")
        .await
        .expect("failed to write second sorted-run key");
    flush_memtable(&db).await;
    wait_for_manifest(&db, |manifest| {
        !manifest.compacted().is_empty() && manifest.l0().is_empty()
    })
    .await;
    db.close()
        .await
        .expect("failed to close compactor database");

    let db = Db::builder(path, store)
        .with_settings(test_settings(false))
        .with_merge_operator(Arc::new(ConcatMergeOperator))
        .build()
        .await
        .expect("failed to reopen database");
    db.put(b"trace:l0", b"l0")
        .await
        .expect("failed to write L0 key");
    db.merge(b"trace:merged", b"+l0")
        .await
        .expect("failed to write L0 merge operand");
    flush_memtable(&db).await;
    wait_for_manifest(&db, |manifest| {
        !manifest.compacted().is_empty() && manifest.l0().len() == 1
    })
    .await;
    db.put(b"trace:memtable", b"memtable")
        .await
        .expect("failed to write memtable key");
    db.merge(b"trace:merged", b"+memtable")
        .await
        .expect("failed to write memtable merge operand");

    let output = CapturedOutput::default();
    let subscriber = FmtSubscriber::builder()
        .with_writer(output.clone())
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish();
    tracing::instrument::WithSubscriber::with_subscriber(
        run_read_operations(&db, &output),
        subscriber,
    )
    .await;
    db.close().await.expect("failed to close database");
}

async fn run_read_operations(db: &Db, output: &CapturedOutput) {
    test_merged_get(db, output).await;
    test_source_gets(db, output).await;
    test_range_scan(db, output).await;
    test_prefix_scan(db, output).await;
    test_recency_scan(db, output).await;
    test_info_level(db).await;
    test_untraced_get(db, output).await;
}

async fn test_merged_get(db: &Db, output: &CapturedOutput) {
    let mark = output.mark();
    let merged = db
        .get_key_value_with_options(b"trace:merged", &read_options("merged-get"))
        .await
        .expect("merged get failed")
        .expect("merged key is missing");
    assert_eq!(merged.value, Bytes::from_static(b"sorted+l0+memtable"));
    let get_log = output.since(mark);
    assert_root(&get_log, "merged-get");
    assert_span(&get_log, "slatedb.read.memtable", "merged-get", &[]);
    assert_merge_spans(&get_log, "merged-get");
    for level in ["l0", "sorted_run:"] {
        assert_sst_spans(&get_log, "merged-get", level);
    }
}

async fn test_source_gets(db: &Db, output: &CapturedOutput) {
    for (key, value, level, trace_id) in [
        (
            b"trace:memtable".as_slice(),
            b"memtable".as_slice(),
            None,
            "memtable-get",
        ),
        (
            b"trace:l0".as_slice(),
            b"l0".as_slice(),
            Some("l0"),
            "l0-get",
        ),
        (
            b"trace:sorted-a".as_slice(),
            b"sorted-a".as_slice(),
            Some("sorted_run:"),
            "sorted-get",
        ),
    ] {
        let mark = output.mark();
        let actual = db
            .get_with_options(key, &read_options(trace_id))
            .await
            .expect("source get failed");
        assert_eq!(actual.as_deref(), Some(value));
        let log = output.since(mark);
        assert_root(&log, trace_id);
        assert_span(&log, "slatedb.read.memtable", trace_id, &[]);
        assert_no_span(&log, "slatedb.read.merge", trace_id);
        if let Some(level) = level {
            assert_sst_spans(&log, trace_id, level);
            if level == "l0" {
                for name in SST_SPAN_NAMES {
                    assert_no_sst_span_at_level(&log, name, trace_id, "sorted_run:");
                }
            }
        } else {
            for name in SST_SPAN_NAMES {
                assert_no_span(&log, name, trace_id);
            }
        }
    }
}

async fn test_range_scan(db: &Db, output: &CapturedOutput) {
    let mark = output.mark();
    let mut scan = db
        .scan_with_options(
            b"trace:".as_slice()..b"trace;".as_slice(),
            &scan_options("range-scan"),
        )
        .await
        .expect("range scan failed");
    let mut row_count = 0;
    while scan.next().await.expect("range scan next failed").is_some() {
        row_count += 1;
    }
    drop(scan);
    assert!(row_count > 0);
    let scan_log = output.since(mark);
    assert_root(&scan_log, "range-scan");
    assert_span(&scan_log, "slatedb.read.memtable", "range-scan", &[]);
    assert_merge_spans(&scan_log, "range-scan");
    // The default bloom filter cannot answer range queries.
    assert_no_span(&scan_log, "slatedb.read.read_filters", "range-scan");
    assert_no_span(&scan_log, "slatedb.read.evaluate_filter", "range-scan");
    for level in ["l0", "sorted_run:"] {
        assert_sst_span(
            &scan_log,
            "slatedb.read.read_index",
            "range-scan",
            level,
            has_cache_flag,
        );
        assert_sst_span(
            &scan_log,
            "slatedb.read.read_blocks",
            "range-scan",
            level,
            has_block_activity,
        );
    }
}

async fn test_prefix_scan(db: &Db, output: &CapturedOutput) {
    let mark = output.mark();
    let mut prefix_scan = db
        .scan_prefix_with_options(b"trace:", .., &scan_options("prefix-scan"))
        .await
        .expect("prefix scan failed");
    let mut row_count = 0;
    while prefix_scan
        .next()
        .await
        .expect("prefix scan next failed")
        .is_some()
    {
        row_count += 1;
    }
    drop(prefix_scan);
    assert!(row_count > 0);
    let prefix_log = output.since(mark);
    assert_root(&prefix_log, "prefix-scan");
    assert_span(&prefix_log, "slatedb.read.memtable", "prefix-scan", &[]);
    assert_merge_spans(&prefix_log, "prefix-scan");
    for level in ["l0", "sorted_run:"] {
        assert_sst_spans(&prefix_log, "prefix-scan", level);
    }
}

async fn test_recency_scan(db: &Db, output: &CapturedOutput) {
    let mark = output.mark();
    let mut recency_scan = db
        .scan_prefix_by_recency_with_options(b"trace:", &scan_options("recency-scan"))
        .await
        .expect("recency scan failed");
    let mut recency_keys = Vec::new();
    while let Some(row) = recency_scan
        .next_entry()
        .await
        .expect("recency scan next failed")
    {
        recency_keys.push(row.key);
    }
    drop(recency_scan);
    for key in [b"trace:memtable".as_slice(), b"trace:l0", b"trace:sorted-a"] {
        assert!(recency_keys.iter().any(|actual| actual.as_ref() == key));
    }
    let recency_log = output.since(mark);
    assert_root(&recency_log, "recency-scan");
    assert_span(&recency_log, "slatedb.read.memtable", "recency-scan", &[]);
    assert_no_span(&recency_log, "slatedb.read.merge", "recency-scan");
    for level in ["l0", "sorted_run:"] {
        assert_sst_spans(&recency_log, "recency-scan", level);
    }
}

async fn test_info_level(db: &Db) {
    let info_output = CapturedOutput::default();
    let info_subscriber = FmtSubscriber::builder()
        .with_writer(info_output.clone())
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .finish();
    let info_result = tracing::instrument::WithSubscriber::with_subscriber(
        db.get_with_options(b"trace:merged", &read_options("info-get")),
        info_subscriber,
    )
    .await
    .expect("info-level get failed");
    assert_eq!(info_result, Some(Bytes::from_static(b"sorted+l0+memtable")));
    let info_log = info_output.since(0);
    assert_root(&info_log, "info-get");
    assert_merge_spans(&info_log, "info-get");
    for level in ["l0", "sorted_run:"] {
        assert_sst_spans(&info_log, "info-get", level);
    }
    assert!(
        span_lines(&info_log, "slatedb.read.memtable", "info-get")
            .next()
            .is_none(),
        "memtable span appeared at info level\n{info_log}"
    );
}

async fn test_untraced_get(db: &Db, output: &CapturedOutput) {
    let mark = output.mark();
    assert_eq!(
        db.get_with_options(b"trace:memtable", &ReadOptions::default())
            .await
            .expect("untraced get failed"),
        Some(Bytes::from_static(b"memtable"))
    );
    let untraced_log = output.since(mark);
    assert!(
        !untraced_log.contains("slatedb.read{"),
        "untraced get produced a read span\n{untraced_log}"
    );
}

fn read_options(trace_id: &str) -> ReadOptions {
    ReadOptions::default().with_tracing_options(Some(TracingOptions::new(trace_id)))
}

fn scan_options(trace_id: &str) -> ScanOptions {
    ScanOptions::default().with_tracing_options(Some(TracingOptions::new(trace_id)))
}

fn assert_root(log: &str, trace_id: &str) {
    assert_span(log, "slatedb.read", trace_id, &[]);
}

fn assert_no_span(log: &str, name: &str, trace_id: &str) {
    assert!(
        span_event_lines(log, name, trace_id).next().is_none(),
        "unexpected {name} with trace_id={trace_id}\n{log}"
    );
}

fn assert_merge_spans(log: &str, trace_id: &str) {
    assert_span(
        log,
        "slatedb.read.merge",
        trace_id,
        &[("num_operands", "3")],
    );
    assert_span(
        log,
        "slatedb.read.merge",
        trace_id,
        &[("num_operands", "1")],
    );
}

fn assert_span(log: &str, name: &str, trace_id: &str, fields: &[(&str, &str)]) {
    assert!(
        span_lines(log, name, trace_id).any(|line| {
            let values = span_fields(line, name);
            fields
                .iter()
                .all(|(key, value)| values.get(key) == Some(value))
        }),
        "missing {name} with trace_id={trace_id} and fields {fields:?}\n{log}"
    );
}

fn assert_no_sst_span_at_level(log: &str, name: &str, trace_id: &str, level: &str) {
    assert!(
        !span_event_lines(log, name, trace_id).any(|line| {
            span_fields(line, name)
                .get("sst_level")
                .is_some_and(|actual| actual.starts_with(level))
        }),
        "unexpected {name} at {level} with trace_id={trace_id}\n{log}"
    );
}

fn assert_sst_span(
    log: &str,
    name: &str,
    trace_id: &str,
    level: &str,
    predicate: impl Fn(&HashMap<&str, &str>) -> bool,
) {
    assert!(
        span_lines(log, name, trace_id).any(|line| {
            let values = span_fields(line, name);
            values
                .get("sst_level")
                .is_some_and(|actual| actual.starts_with(level))
                && values.get("sst_id").is_some_and(|id| !id.is_empty())
                && predicate(&values)
        }),
        "missing {name} at {level} with trace_id={trace_id}\n{log}"
    );
}

fn assert_sst_spans(log: &str, trace_id: &str, level: &str) {
    assert_sst_span(
        log,
        "slatedb.read.read_filters",
        trace_id,
        level,
        has_cache_flag,
    );
    assert_sst_span(
        log,
        "slatedb.read.evaluate_filter",
        trace_id,
        level,
        |fields| fields.get("filter_name") == Some(&"_bf") && fields.get("result") == Some(&"true"),
    );
    assert_sst_span(
        log,
        "slatedb.read.read_index",
        trace_id,
        level,
        has_cache_flag,
    );
    assert_sst_span(
        log,
        "slatedb.read.read_blocks",
        trace_id,
        level,
        has_block_activity,
    );
}

fn has_block_activity(fields: &HashMap<&str, &str>) -> bool {
    fields
        .get("cache_hits")
        .and_then(|hits| hits.parse::<u64>().ok())
        .zip(
            fields
                .get("cache_misses")
                .and_then(|misses| misses.parse::<u64>().ok()),
        )
        .is_some_and(|(hits, misses)| hits + misses > 0)
}

fn has_cache_flag(fields: &HashMap<&str, &str>) -> bool {
    matches!(fields.get("cached"), Some(&"true" | &"false"))
}

fn span_lines<'a>(log: &'a str, name: &str, trace_id: &str) -> impl Iterator<Item = &'a str> {
    span_event_lines(log, name, trace_id).filter(|line| line.contains(": close"))
}

fn span_event_lines<'a>(log: &'a str, name: &str, trace_id: &str) -> impl Iterator<Item = &'a str> {
    let name = format!("{name}{{");
    let trace_id = trace_id.to_string();
    log.lines().filter(move |line| {
        let Some(start) = line.rfind(&name) else {
            return false;
        };
        let Some(event) = line.rfind(": new").or_else(|| line.rfind(": close")) else {
            return false;
        };
        start < event
            && !line[start + name.len()..event].contains("}:slatedb.read.")
            && (name == "slatedb.read{" || line[..start].contains("slatedb.read{"))
            && span_fields(line, name.trim_end_matches('{')).get("trace_id")
                == Some(&trace_id.as_str())
    })
}

fn span_fields<'a>(line: &'a str, name: &str) -> HashMap<&'a str, &'a str> {
    let name = format!("{name}{{");
    let start = line.rfind(&name).expect("span name is missing") + name.len();
    let end = line[start..].find('}').expect("span fields are missing") + start;
    line[start..end]
        .split_whitespace()
        .filter_map(|field| field.split_once('='))
        .map(|(key, value)| (key, value.trim_matches('"')))
        .collect()
}

struct ConcatMergeOperator;

impl MergeOperator for ConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut result = BytesMut::new();
        if let Some(existing_value) = existing_value {
            result.extend_from_slice(&existing_value);
        }
        result.extend_from_slice(&value);
        Ok(result.freeze())
    }
}

fn test_settings(with_compactor: bool) -> Settings {
    Settings {
        flush_interval: None,
        manifest_poll_interval: Duration::from_millis(50),
        min_filter_keys: 0,
        compactor_options: with_compactor.then(compactor_options),
        ..Settings::default()
    }
}

fn compactor_options() -> CompactorOptions {
    CompactorOptions {
        poll_interval: Duration::from_millis(50),
        scheduler_options: SizeTieredCompactionSchedulerOptions {
            min_compaction_sources: 2,
            ..Default::default()
        }
        .into(),
        commit_compacted_interval: Duration::from_millis(50),
        worker: Some(CompactionWorkerOptions {
            compactions_poll_interval: Duration::from_millis(50),
            max_subcompactions: 1,
            min_filter_keys: 0,
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn flush_memtable(db: &Db) {
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("failed to flush memtable");
}

async fn wait_for_manifest(db: &Db, condition: impl Fn(&slatedb::VersionedManifest) -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if condition(&db.manifest()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("manifest condition timed out");
}
