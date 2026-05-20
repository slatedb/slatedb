//! Verifies that `slatedb::tablestore` records the `cached` field on the
//! surrounding `slatedb.query.read_index` and `slatedb.query.read_filter`
//! spans — `false` on the first read (cache miss, fetched from object
//! store) and `true` on the second read of the same SST (cache hit).

use std::collections::HashMap;
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

const READ_INDEX_SPAN: &str = "slatedb.query.read_index";
const READ_FILTER_SPAN: &str = "slatedb.query.read_filter";

#[derive(Clone, Default)]
struct CachedFieldCapture {
    /// Map of span name → list of `cached` values observed across all spans
    /// of that name, in close order. `None` means the field was never
    /// recorded for that span.
    by_span: Arc<Mutex<HashMap<&'static str, Vec<Option<bool>>>>>,
}

impl CachedFieldCapture {
    fn snapshot(&self, span: &'static str) -> Vec<Option<bool>> {
        self.by_span
            .lock()
            .unwrap()
            .get(span)
            .cloned()
            .unwrap_or_default()
    }
}

fn tracked_span_name(name: &str) -> Option<&'static str> {
    match name {
        READ_INDEX_SPAN => Some(READ_INDEX_SPAN),
        READ_FILTER_SPAN => Some(READ_FILTER_SPAN),
        _ => None,
    }
}

impl<S> Layer<S> for CachedFieldCapture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(name) = tracked_span_name(attrs.metadata().name()) else { return };
        let Some(span) = ctx.span(id) else { return };
        let slot = Arc::new(Mutex::new(None::<bool>));
        span.extensions_mut().insert(CachedSlot {
            value: slot.clone(),
        });
        // Reserve a slot for this span's eventual `cached` value; filled
        // in on_close in the same order spans were created.
        self.by_span
            .lock()
            .unwrap()
            .entry(name)
            .or_default()
            .push(None);
        let mut v = SlotVisitor { slot: &slot };
        attrs.record(&mut v);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if tracked_span_name(span.metadata().name()).is_none() {
            return;
        }
        let exts = span.extensions();
        let Some(slot) = exts.get::<CachedSlot>() else { return };
        let mut v = SlotVisitor { slot: &slot.value };
        values.record(&mut v);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(name) = tracked_span_name(span.metadata().name()) else { return };
        let final_value = span
            .extensions()
            .get::<CachedSlot>()
            .and_then(|s| *s.value.lock().unwrap());
        let mut by_span = self.by_span.lock().unwrap();
        let entries = by_span.entry(name).or_default();
        // Fill the most-recent placeholder reserved in on_new_span. Spans
        // may close out of creation order, but for this single-threaded
        // test that's not a concern.
        if let Some(empty) = entries.iter_mut().rev().find(|v| v.is_none()) {
            *empty = final_value;
        }
    }
}

struct CachedSlot {
    value: Arc<Mutex<Option<bool>>>,
}

struct SlotVisitor<'a> {
    slot: &'a Arc<Mutex<Option<bool>>>,
}

impl<'a> Visit for SlotVisitor<'a> {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "cached" {
            *self.slot.lock().unwrap() = Some(value);
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

#[tokio::test]
async fn records_cached_field_for_read_index_and_read_filter_spans() {
    let capture = CachedFieldCapture::default();
    let layer = QueryTracingLayer::new();
    tracing_subscriber::registry()
        .with(layer)
        .with(capture.clone())
        .init();

    let object_store = Arc::new(InMemory::new());
    let path = "/tmp/slatedb_cached_field_test";
    // Default `min_filter_keys` is 1000 — too high to materialize a filter
    // for a one-row SST. Lower it so `read_filters` returns a real filter
    // (and therefore actually caches it).
    let settings = Settings {
        min_filter_keys: 1,
        ..Settings::default()
    };

    // Phase 1 — populate an SST then close. Writing the SST seeds the block
    // cache with its index/filter, so we can't observe a miss against this
    // instance.
    {
        let db = Db::builder(path, object_store.clone())
            .with_settings(settings.clone())
            .build()
            .await
            .expect("open db");
        db.put(b"k", b"v").await.expect("put");
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("flush");
        db.close().await.expect("close db");
    }

    // Phase 2 — reopen with a fresh in-memory cache. The first traced get
    // must load index + filter from object storage (cache miss → cached=false).
    // The second get hits the now-warm cache (cached=true).
    let db = Db::builder(path, object_store)
        .with_settings(settings)
        .build()
        .await
        .expect("reopen db");

    let opts1 = ReadOptions::new().with_query_id("cold".to_string());
    db.get_with_options(b"k", &opts1)
        .await
        .expect("get cold")
        .expect("value");

    let opts2 = ReadOptions::new().with_query_id("warm".to_string());
    db.get_with_options(b"k", &opts2)
        .await
        .expect("get warm")
        .expect("value");

    db.close().await.expect("close db");

    for span in [READ_INDEX_SPAN, READ_FILTER_SPAN] {
        let observed = capture.snapshot(span);
        assert!(
            !observed.is_empty(),
            "expected at least one {span} span to fire"
        );
        assert!(
            observed.contains(&Some(false)),
            "expected at least one {span} span with cached=false, got {observed:?}"
        );
        assert!(
            observed.contains(&Some(true)),
            "expected at least one {span} span with cached=true, got {observed:?}"
        );
        assert!(
            !observed.iter().any(|v| v.is_none()),
            "every {span} span should have its `cached` field recorded, got {observed:?}"
        );
    }
}
