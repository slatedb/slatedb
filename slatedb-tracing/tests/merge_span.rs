//! Verifies that `slatedb.query.merge` spans fire on the read path when a
//! merge operator is configured and the value being resolved is built from
//! one or more merge operands. The `QueryTracingLayer` should accumulate a
//! non-zero `merge_duration_micros` for the query id, and each span should
//! carry `key` and `num_operands` fields as described in the RFC.

use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use slatedb::bytes::Bytes;
use slatedb::config::{ReadOptions, Settings};
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, MergeOperator, MergeOperatorError};
use slatedb_tracing::QueryTracingLayer;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const MERGE_SPAN: &str = "slatedb.query.merge";

/// Concatenates merge operands with a `+` separator, slow enough to ensure
/// the merge span captures a measurable duration.
struct SlowConcatMergeOperator;

impl MergeOperator for SlowConcatMergeOperator {
    fn merge(
        &self,
        _key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        sleep(Duration::from_millis(1));
        let mut out = existing_value.map(|v| v.to_vec()).unwrap_or_default();
        if !out.is_empty() {
            out.push(b'+');
        }
        out.extend_from_slice(&operand);
        Ok(Bytes::from(out))
    }
}

#[derive(Debug, Default, Clone)]
struct MergeSpanFields {
    key: Option<String>,
    num_operands: Option<u64>,
}

#[derive(Clone, Default)]
struct MergeSpanCapture {
    spans: Arc<Mutex<Vec<MergeSpanFields>>>,
}

impl<S> Layer<S> for MergeSpanCapture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if attrs.metadata().name() != MERGE_SPAN {
            return;
        }
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        self.spans.lock().unwrap().push(MergeSpanFields {
            key: visitor.key,
            num_operands: visitor.num_operands,
        });
    }
}

#[derive(Default)]
struct FieldVisitor {
    key: Option<String>,
    num_operands: Option<u64>,
}

impl Visit for FieldVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "num_operands" {
            self.num_operands = Some(value);
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "num_operands" && value >= 0 {
            self.num_operands = Some(value as u64);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "key" {
            self.key = Some(format!("{value:?}"));
        }
    }
}

#[tokio::test]
async fn records_merge_duration_and_fields_for_traced_get() {
    let layer = QueryTracingLayer::new();
    let capture = MergeSpanCapture::default();
    tracing_subscriber::registry()
        .with(layer.clone())
        .with(capture.clone())
        .init();

    let object_store = Arc::new(InMemory::new());
    let db = Db::builder("/tmp/slatedb_merge_span_test", object_store)
        .with_settings(Settings::default())
        .with_merge_operator(Arc::new(SlowConcatMergeOperator))
        .build()
        .await
        .expect("open db");

    // Three merge operands and no base value → read path must call the
    // merge operator to fold them.
    db.merge(b"k", b"a").await.expect("merge a");
    db.merge(b"k", b"b").await.expect("merge b");
    db.merge(b"k", b"c").await.expect("merge c");

    let opts = ReadOptions::new().with_query_id("qmerge".to_string());
    let value = db
        .get_with_options(b"k", &opts)
        .await
        .expect("get")
        .expect("value");
    assert_eq!(value.as_ref(), b"a+b+c");

    db.close().await.expect("close db");

    let micros = layer.merge_duration_micros("qmerge");
    assert!(
        micros > 0,
        "expected non-zero merge_duration_micros for 'qmerge', got {micros}"
    );
    assert!(layer.memtable_consulted("qmerge") > 0);

    let spans = capture.spans.lock().unwrap().clone();
    assert!(!spans.is_empty(), "expected at least one merge span");
    for s in &spans {
        assert!(s.key.is_some(), "every merge span must record `key`: {s:?}");
        assert!(
            s.num_operands.is_some(),
            "every merge span must record `num_operands`: {s:?}"
        );
    }
    // At least one span should have observed all three operands folded together.
    assert!(
        spans.iter().any(|s| s.num_operands == Some(3)),
        "expected one merge span with num_operands=3, got {spans:?}"
    );
}
