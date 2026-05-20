//! Tracing layer that aggregates SlateDB query spans into per-query
//! atomic counters.
//!
//! Install [`QueryTracingLayer`] in any `tracing-subscriber` `Registry`. The
//! layer accumulates counts and elapsed time across every `slatedb.query.*`
//! span it observes, bucketing them under the span's `query_id` field. The
//! application reads the totals for a specific query via the accessors.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

const MEMTABLE_SPAN: &str = "slatedb.query.memtable";
const READ_BLOCKS_SPAN: &str = "slatedb.query.read_blocks";
const READ_INDEX_SPAN: &str = "slatedb.query.read_index";
const READ_FILTER_SPAN: &str = "slatedb.query.read_filter";
const BLOOM_FILTER_SPAN: &str = "slatedb.query.bloom_filter";
const MERGE_SPAN: &str = "slatedb.query.merge";
const QUERY_ID_FIELD: &str = "query_id";
const CACHE_HITS_FIELD: &str = "cache_hits";
const CACHE_MISSES_FIELD: &str = "cache_misses";

/// `tracing-subscriber` layer that accumulates SlateDB query aggregates
/// into per-query atomic counters. Cheap to clone — the state lives in an
/// `Arc` so every clone observes the same totals. Hand one clone to the
/// registry and keep another to read the aggregates back.
#[derive(Clone, Default)]
pub struct QueryTracingLayer {
    inner: Arc<QueryTracingInner>,
}

#[derive(Default)]
struct QueryTracingInner {
    counters: Mutex<HashMap<String, Arc<QueryCounters>>>,
}

#[derive(Default)]
struct QueryCounters {
    memtable_consulted: AtomicU64,
    memtable_read_duration_micros: AtomicU64,
    block_read_duration_micros: AtomicU64,
    index_read_duration_micros: AtomicU64,
    filter_read_duration_micros: AtomicU64,
    bloom_filter_check_duration_micros: AtomicU64,
    merge_duration_micros: AtomicU64,
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
}

impl QueryTracingLayer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of memtables consulted for `query_id`. Returns 0 if no spans
    /// have been observed for that id.
    pub fn memtable_consulted(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.memtable_consulted)
    }

    /// Cumulative memtable read time for `query_id`, in microseconds.
    pub fn memtable_read_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.memtable_read_duration_micros)
    }

    /// Cumulative SST block read time for `query_id`, in microseconds.
    pub fn block_read_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.block_read_duration_micros)
    }

    /// Cumulative SST index read time for `query_id`, in microseconds.
    pub fn index_read_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.index_read_duration_micros)
    }

    /// Cumulative SST filter read time for `query_id`, in microseconds.
    pub fn filter_read_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.filter_read_duration_micros)
    }

    /// Cumulative bloom-filter `might_contain` check time for `query_id`,
    /// in microseconds.
    pub fn bloom_filter_check_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.bloom_filter_check_duration_micros)
    }

    /// Cumulative merge-operator time for `query_id`, in microseconds.
    pub fn merge_duration_micros(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.merge_duration_micros)
    }

    /// Total number of SST data blocks served from the block cache during
    /// `query_id`. Summed across every `slatedb.query.read_blocks` span
    /// that fires for the query.
    pub fn block_cache_hits(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.block_cache_hits)
    }

    /// Total number of SST data blocks that missed the block cache and had
    /// to be fetched from object storage during `query_id`. Summed across
    /// every `slatedb.query.read_blocks` span that fires for the query.
    pub fn block_cache_misses(&self, query_id: &str) -> u64 {
        self.read_counter(query_id, |c| &c.block_cache_misses)
    }

    /// Snapshot of all query ids the layer has observed so far.
    pub fn query_ids(&self) -> Vec<String> {
        self.lock_counters().keys().cloned().collect()
    }

    fn read_counter(
        &self,
        query_id: &str,
        select: impl FnOnce(&QueryCounters) -> &AtomicU64,
    ) -> u64 {
        self.lock_counters()
            .get(query_id)
            .map(|c| select(c).load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    fn counters_for(&self, query_id: &str) -> Arc<QueryCounters> {
        self.lock_counters()
            .entry(query_id.to_string())
            .or_insert_with(|| Arc::new(QueryCounters::default()))
            .clone()
    }

    fn lock_counters(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<QueryCounters>>> {
        self.inner
            .counters
            .lock()
            .expect("QueryTracingLayer query map mutex poisoned")
    }
}

/// Per-query duration snapshot taken under the queries-map lock so the
/// `Display` impl can format without holding it.
struct QueryDurations {
    memtable: Duration,
    block: Duration,
    index: Duration,
    filter: Duration,
    bloom_filter: Duration,
    merge: Duration,
    block_cache_hits: u64,
    block_cache_misses: u64,
}

impl QueryDurations {
    fn snapshot(c: &QueryCounters) -> Self {
        let load = |a: &AtomicU64| Duration::from_micros(a.load(Ordering::Relaxed));
        Self {
            memtable: load(&c.memtable_read_duration_micros),
            block: load(&c.block_read_duration_micros),
            index: load(&c.index_read_duration_micros),
            filter: load(&c.filter_read_duration_micros),
            bloom_filter: load(&c.bloom_filter_check_duration_micros),
            merge: load(&c.merge_duration_micros),
            block_cache_hits: c.block_cache_hits.load(Ordering::Relaxed),
            block_cache_misses: c.block_cache_misses.load(Ordering::Relaxed),
        }
    }
}

/// Pretty-prints aggregated per-query durations. Layout is intended for
/// human debugging (`println!("{layer}")`) and is not part of the API
/// contract — callers that need stable values should use the typed
/// accessors.
impl fmt::Display for QueryTracingLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut snapshots: Vec<(String, QueryDurations)> = {
            let map = self.lock_counters();
            map.iter()
                .map(|(id, c)| (id.clone(), QueryDurations::snapshot(c)))
                .collect()
        };
        snapshots.sort_by(|a, b| a.0.cmp(&b.0));

        writeln!(f, "QueryTracingLayer {{")?;
        for (id, d) in &snapshots {
            writeln!(
                f,
                "  {id}: memtable={:?} block={:?} index={:?} filter={:?} bloom_filter={:?} merge={:?} block_cache_hits={} block_cache_misses={}",
                d.memtable,
                d.block,
                d.index,
                d.filter,
                d.bloom_filter,
                d.merge,
                d.block_cache_hits,
                d.block_cache_misses,
            )?;
        }
        write!(f, "}}")
    }
}

impl<S> Layer<S> for QueryTracingLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let name = attrs.metadata().name();
        if name != MEMTABLE_SPAN
            && name != READ_BLOCKS_SPAN
            && name != READ_INDEX_SPAN
            && name != READ_FILTER_SPAN
            && name != BLOOM_FILTER_SPAN
            && name != MERGE_SPAN
        {
            return;
        }
        let mut visitor = QueryIdVisitor::default();
        attrs.record(&mut visitor);
        let Some(query_id) = visitor.query_id else { return };
        let counters = self.counters_for(&query_id);
        if name == MEMTABLE_SPAN {
            counters.memtable_consulted.fetch_add(1, Ordering::Relaxed);
        }
        let Some(span) = ctx.span(id) else { return };
        span.extensions_mut().insert(SpanCounters(counters));
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.metadata().name() != READ_BLOCKS_SPAN {
            return;
        }
        let exts = span.extensions();
        let Some(SpanCounters(counters)) = exts.get::<SpanCounters>() else {
            return;
        };
        let mut visitor = BlockCacheFieldsVisitor::default();
        values.record(&mut visitor);
        if let Some(hits) = visitor.cache_hits {
            counters.block_cache_hits.fetch_add(hits, Ordering::Relaxed);
        }
        if let Some(misses) = visitor.cache_misses {
            counters
                .block_cache_misses
                .fetch_add(misses, Ordering::Relaxed);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.extensions().get::<SpanCounters>().is_none() {
            return;
        }
        span.extensions_mut().insert(EnterTime(Instant::now()));
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let entered = span.extensions_mut().remove::<EnterTime>();
        let Some(EnterTime(start)) = entered else { return };
        let elapsed = start.elapsed().as_micros() as u64;
        let exts = span.extensions();
        let Some(SpanCounters(counters)) = exts.get::<SpanCounters>() else {
            return;
        };
        let counter = match span.metadata().name() {
            MEMTABLE_SPAN => &counters.memtable_read_duration_micros,
            READ_BLOCKS_SPAN => &counters.block_read_duration_micros,
            READ_INDEX_SPAN => &counters.index_read_duration_micros,
            READ_FILTER_SPAN => &counters.filter_read_duration_micros,
            BLOOM_FILTER_SPAN => &counters.bloom_filter_check_duration_micros,
            MERGE_SPAN => &counters.merge_duration_micros,
            _ => return,
        };
        counter.fetch_add(elapsed, Ordering::Relaxed);
    }
}

/// Per-span handle to the [`QueryCounters`] for the span's `query_id`,
/// resolved once at span creation and stashed in the span's extensions so
/// later `on_enter` / `on_exit` calls don't have to take the query-map lock.
struct SpanCounters(Arc<QueryCounters>);

/// Span extension recording the most recent `on_enter` timestamp.
struct EnterTime(Instant);

/// Extracts the `query_id` field from a span's [`Attributes`].
#[derive(Default)]
struct QueryIdVisitor {
    query_id: Option<String>,
}

/// Captures the `cache_hits` / `cache_misses` u64 fields recorded on a
/// `slatedb.query.read_blocks` span by `tablestore::read_blocks_using_index`.
/// Both are set once per span via `tracing::Span::current().record(...)`
/// after the cache lookups complete, so they surface through `on_record`.
#[derive(Default)]
struct BlockCacheFieldsVisitor {
    cache_hits: Option<u64>,
    cache_misses: Option<u64>,
}

impl Visit for BlockCacheFieldsVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            CACHE_HITS_FIELD => self.cache_hits = Some(value),
            CACHE_MISSES_FIELD => self.cache_misses = Some(value),
            _ => {}
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn fmt::Debug) {}
}

impl Visit for QueryIdVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == QUERY_ID_FIELD {
            self.query_id = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        // SlateDB instruments spans with `query_id = %id`, which routes
        // through `record_debug` (the `Debug` impl of `fmt::Arguments`
        // delegates to `Display`, so this yields the same string as the
        // caller's `Display` formatting).
        if field.name() == QUERY_ID_FIELD && self.query_id.is_none() {
            self.query_id = Some(format!("{:?}", value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    use tracing::dispatcher;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    fn with_layer<F: FnOnce(&QueryTracingLayer)>(f: F) {
        let layer = QueryTracingLayer::new();
        let subscriber = Registry::default().with(layer.clone());
        dispatcher::with_default(&subscriber.into(), || f(&layer));
    }

    #[test]
    fn should_count_each_memtable_span_creation() {
        with_layer(|layer| {
            let s1 = tracing::debug_span!("slatedb.query.memtable", query_id = "q1");
            let s2 = tracing::debug_span!("slatedb.query.memtable", query_id = "q1");
            let _ = (s1, s2);
            assert_eq!(layer.memtable_consulted("q1"), 2);
            assert_eq!(layer.memtable_read_duration_micros("q1"), 0);
        });
    }

    #[test]
    fn should_sum_memtable_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.memtable", query_id = "q2");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            assert_eq!(layer.memtable_consulted("q2"), 1);
            let micros = layer.memtable_read_duration_micros("q2");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_ignore_unrelated_spans() {
        with_layer(|layer| {
            let _ = tracing::debug_span!("unrelated", query_id = "q3");
            assert_eq!(layer.memtable_consulted("q3"), 0);
            assert_eq!(layer.memtable_read_duration_micros("q3"), 0);
            assert_eq!(layer.block_read_duration_micros("q3"), 0);
            assert!(layer.query_ids().is_empty());
        });
    }

    #[test]
    fn should_sum_block_read_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.read_blocks", query_id = "q4");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            // read_blocks does not contribute to memtable_consulted.
            assert_eq!(layer.memtable_consulted("q4"), 0);
            assert_eq!(layer.memtable_read_duration_micros("q4"), 0);
            let micros = layer.block_read_duration_micros("q4");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_sum_index_read_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.read_index", query_id = "qi");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            // read_index does not contribute to memtable_consulted nor to
            // block/memtable durations.
            assert_eq!(layer.memtable_consulted("qi"), 0);
            assert_eq!(layer.memtable_read_duration_micros("qi"), 0);
            assert_eq!(layer.block_read_duration_micros("qi"), 0);
            let micros = layer.index_read_duration_micros("qi");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_sum_filter_read_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.read_filter", query_id = "qf");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            assert_eq!(layer.memtable_consulted("qf"), 0);
            assert_eq!(layer.memtable_read_duration_micros("qf"), 0);
            assert_eq!(layer.block_read_duration_micros("qf"), 0);
            assert_eq!(layer.index_read_duration_micros("qf"), 0);
            let micros = layer.filter_read_duration_micros("qf");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_sum_bloom_filter_check_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.bloom_filter", query_id = "qb");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            assert_eq!(layer.memtable_consulted("qb"), 0);
            assert_eq!(layer.filter_read_duration_micros("qb"), 0);
            let micros = layer.bloom_filter_check_duration_micros("qb");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_sum_merge_duration_across_polls() {
        with_layer(|layer| {
            let span = tracing::debug_span!("slatedb.query.merge", query_id = "qm");
            for _ in 0..3 {
                let _enter = span.enter();
                sleep(Duration::from_millis(2));
            }
            drop(span);
            assert_eq!(layer.memtable_consulted("qm"), 0);
            assert_eq!(layer.memtable_read_duration_micros("qm"), 0);
            assert_eq!(layer.block_read_duration_micros("qm"), 0);
            assert_eq!(layer.index_read_duration_micros("qm"), 0);
            assert_eq!(layer.filter_read_duration_micros("qm"), 0);
            let micros = layer.merge_duration_micros("qm");
            assert!(micros >= 6_000, "micros = {micros}");
        });
    }

    #[test]
    fn should_accumulate_block_cache_hits_and_misses_across_read_blocks_spans() {
        with_layer(|layer| {
            // First read_blocks span: 2 hits, 1 miss.
            let s1 = tracing::debug_span!(
                "slatedb.query.read_blocks",
                query_id = "qbc",
                cache_hits = tracing::field::Empty,
                cache_misses = tracing::field::Empty,
            );
            s1.record("cache_hits", 2u64);
            s1.record("cache_misses", 1u64);
            drop(s1);

            // Second read_blocks span on the same query: 3 hits, 4 misses.
            let s2 = tracing::debug_span!(
                "slatedb.query.read_blocks",
                query_id = "qbc",
                cache_hits = tracing::field::Empty,
                cache_misses = tracing::field::Empty,
            );
            s2.record("cache_hits", 3u64);
            s2.record("cache_misses", 4u64);
            drop(s2);

            assert_eq!(layer.block_cache_hits("qbc"), 5);
            assert_eq!(layer.block_cache_misses("qbc"), 5);
        });
    }

    #[test]
    fn should_not_count_block_cache_for_non_read_blocks_spans() {
        with_layer(|layer| {
            // `cache_hits` / `cache_misses` on a different span name must
            // not bump the block_cache counters.
            let span = tracing::debug_span!(
                "slatedb.query.read_index",
                query_id = "qbcn",
                cache_hits = tracing::field::Empty,
                cache_misses = tracing::field::Empty,
            );
            span.record("cache_hits", 7u64);
            span.record("cache_misses", 9u64);
            drop(span);
            assert_eq!(layer.block_cache_hits("qbcn"), 0);
            assert_eq!(layer.block_cache_misses("qbcn"), 0);
        });
    }

    #[test]
    fn should_isolate_counters_by_query_id() {
        with_layer(|layer| {
            let a1 = tracing::debug_span!("slatedb.query.memtable", query_id = "qa");
            let a2 = tracing::debug_span!("slatedb.query.memtable", query_id = "qa");
            let b1 = tracing::debug_span!("slatedb.query.memtable", query_id = "qb");
            let _ = (a1, a2, b1);

            assert_eq!(layer.memtable_consulted("qa"), 2);
            assert_eq!(layer.memtable_consulted("qb"), 1);
            assert_eq!(layer.memtable_consulted("unknown"), 0);

            let mut ids = layer.query_ids();
            ids.sort();
            assert_eq!(ids, vec!["qa".to_string(), "qb".to_string()]);
        });
    }

    #[test]
    fn should_render_durations_per_query_via_display() {
        with_layer(|layer| {
            let m = tracing::debug_span!("slatedb.query.memtable", query_id = "qa");
            {
                let _e = m.enter();
                sleep(Duration::from_millis(2));
            }
            drop(m);

            let b = tracing::debug_span!("slatedb.query.read_blocks", query_id = "qb");
            {
                let _e = b.enter();
                sleep(Duration::from_millis(3));
            }
            drop(b);

            let rendered = format!("{layer}");
            // Header + one line per query.
            assert!(rendered.starts_with("QueryTracingLayer {\n"));
            assert!(rendered.ends_with("}"));
            // Queries are listed in sorted order.
            let qa_pos = rendered.find("qa:").expect("qa line");
            let qb_pos = rendered.find("qb:").expect("qb line");
            assert!(qa_pos < qb_pos, "expected qa before qb in:\n{rendered}");
            // Each duration label appears on every line. Use leading-space
            // matching so `filter=` doesn't also count `bloom_filter=`.
            for label in [
                " memtable=",
                " block=",
                " index=",
                " filter=",
                " bloom_filter=",
                " merge=",
            ] {
                let count = rendered.matches(label).count();
                assert_eq!(count, 2, "label {label} should appear twice in:\n{rendered}");
            }
            // qa accumulated memtable time; qb did not.
            let qa_line = rendered
                .lines()
                .find(|l| l.contains("qa:"))
                .expect("qa line");
            assert!(
                !qa_line.contains("memtable=0ns"),
                "qa should have non-zero memtable duration: {qa_line}"
            );
            let qb_line = rendered
                .lines()
                .find(|l| l.contains("qb:"))
                .expect("qb line");
            assert!(
                qb_line.contains("memtable=0ns"),
                "qb should have zero memtable duration: {qb_line}"
            );
        });
    }

    #[test]
    fn should_render_empty_display_when_no_queries_observed() {
        let layer = QueryTracingLayer::new();
        let rendered = format!("{layer}");
        assert_eq!(rendered, "QueryTracingLayer {\n}");
    }

    #[test]
    fn should_capture_query_id_from_display_value() {
        with_layer(|layer| {
            // SlateDB's instrumentation uses `query_id = %id`, which routes
            // through `record_debug`. Reproduce that here.
            let id: &str = "qd";
            let _ = tracing::debug_span!("slatedb.query.memtable", query_id = %id);
            assert_eq!(layer.memtable_consulted("qd"), 1);
        });
    }
}
