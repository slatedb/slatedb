//! Verifies that `slatedb.query.bloom_filter` spans fire on the read
//! path when a query reaches an SST that has a filter, and that each
//! span records `sst_id`, `key`, and `result`. Drives both a hit
//! (`result=true`) and a miss (`result=false`) scenario.

use std::sync::{Arc, Mutex};

use slatedb::config::{FlushOptions, FlushType, ReadOptions, Settings};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;
use slatedb_tracing::QueryTracingLayer;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

const BLOOM_FILTER_SPAN: &str = "slatedb.query.bloom_filter";

#[derive(Debug, Default, Clone)]
struct BloomSpanFields {
    sst_id: Option<String>,
    key: Option<String>,
    result: Option<bool>,
}

#[derive(Clone, Default)]
struct BloomSpanCapture {
    spans: Arc<Mutex<Vec<BloomSpanFields>>>,
}

impl<S> Layer<S> for BloomSpanCapture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != BLOOM_FILTER_SPAN {
            return;
        }
        let Some(span) = ctx.span(id) else { return };
        let mut fields = BloomSpanFields::default();
        let mut v = FieldVisitor { fields: &mut fields };
        attrs.record(&mut v);
        span.extensions_mut().insert(Slot {
            fields: Arc::new(Mutex::new(fields)),
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.metadata().name() != BLOOM_FILTER_SPAN {
            return;
        }
        let exts = span.extensions();
        let Some(slot) = exts.get::<Slot>() else { return };
        let mut fields = slot.fields.lock().unwrap();
        let mut v = FieldVisitor { fields: &mut fields };
        values.record(&mut v);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        if span.metadata().name() != BLOOM_FILTER_SPAN {
            return;
        }
        let snapshot = span
            .extensions()
            .get::<Slot>()
            .map(|s| s.fields.lock().unwrap().clone());
        if let Some(fields) = snapshot {
            self.spans.lock().unwrap().push(fields);
        }
    }
}

struct Slot {
    fields: Arc<Mutex<BloomSpanFields>>,
}

struct FieldVisitor<'a> {
    fields: &'a mut BloomSpanFields,
}

impl<'a> Visit for FieldVisitor<'a> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "result" {
            self.fields.result = Some(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "sst_id" => self.fields.sst_id = Some(format!("{value:?}")),
            "key" => self.fields.key = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

#[tokio::test]
async fn records_bloom_filter_span_fields_for_hit_and_miss() {
    let layer = QueryTracingLayer::new();
    let capture = BloomSpanCapture::default();
    tracing_subscriber::registry()
        .with(layer.clone())
        .with(capture.clone())
        .init();

    let object_store = Arc::new(InMemory::new());
    // Force filter creation on a small SST.
    let settings = Settings {
        min_filter_keys: 1,
        ..Settings::default()
    };
    let db = Db::builder("/tmp/slatedb_bloom_span_test", object_store)
        .with_settings(settings)
        .build()
        .await
        .expect("open db");

    // Seed the SST with widely-spaced keys so its byte range covers many
    // potential lookup keys. The bloom filter is consulted only when the
    // lookup key falls inside the SST's `[first_key, last_key]` range; a
    // key outside that range is skipped by a cheaper pre-check.
    db.put(b"aa", b"v").await.expect("put aa");
    db.put(b"az", b"v").await.expect("put az");
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("flush");

    // hit: key actually stored — filter says "might match".
    let opts_hit = ReadOptions::new().with_query_id("hit".to_string());
    let v = db.get_with_options(b"aa", &opts_hit).await.expect("get hit");
    assert!(v.is_some());

    // miss: key inside the SST's range but not stored — filter says
    // "definitely absent".
    let opts_miss = ReadOptions::new().with_query_id("miss".to_string());
    let v = db
        .get_with_options(b"am", &opts_miss)
        .await
        .expect("get miss");
    assert!(v.is_none());

    db.close().await.expect("close");

    // Span fields: every span must have sst_id, key, and a recorded result.
    // Bloom-filter probes are sub-microsecond, so the duration counter can
    // round to 0 — the capture layer is the reliable proof that a span
    // fired.
    let spans = capture.spans.lock().unwrap().clone();
    assert!(!spans.is_empty(), "expected at least one bloom_filter span");
    for s in &spans {
        assert!(s.sst_id.is_some(), "missing sst_id: {s:?}");
        assert!(s.key.is_some(), "missing key: {s:?}");
        assert!(s.result.is_some(), "missing result: {s:?}");
    }
    assert!(
        spans.iter().any(|s| s.result == Some(true)),
        "expected at least one span with result=true, got {spans:?}"
    );
    assert!(
        spans.iter().any(|s| s.result == Some(false)),
        "expected at least one span with result=false, got {spans:?}"
    );

    // Sanity: the duration counter accumulates across both queries. Even if
    // any individual check rounds to 0us, the sum across all spans for at
    // least one query should be non-zero on a real machine.
    let total_us = layer.bloom_filter_check_duration_micros("hit")
        + layer.bloom_filter_check_duration_micros("miss");
    assert!(
        total_us > 0,
        "expected non-zero combined bloom_filter_check_duration across hit+miss"
    );
}
