use std::sync::{Arc, Weak};

use tracing::span::{Attributes, Id};
use tracing::{Span, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, SpanRef};
use tracing_subscriber::{Layer, Registry};

use crate::http_trace::HttpTraceCapture;

const MAX_TRACE_ID_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ReadSpanKind {
    Read,
    Memtable,
    ReadFilters,
    EvaluateFilter,
    ReadIndex,
    ReadBlocks,
    Merge,
}

impl ReadSpanKind {
    pub(crate) fn from_name(name: &str) -> Option<Self> {
        match name {
            "slatedb.read" => Some(Self::Read),
            "slatedb.read.memtable" => Some(Self::Memtable),
            "slatedb.read.read_filters" => Some(Self::ReadFilters),
            "slatedb.read.evaluate_filter" => Some(Self::EvaluateFilter),
            "slatedb.read.read_index" => Some(Self::ReadIndex),
            "slatedb.read.read_blocks" => Some(Self::ReadBlocks),
            "slatedb.read.merge" => Some(Self::Merge),
            _ => None,
        }
    }
}

/// Identities and times belong to the HTTP capture. Only read roots carry caller trace IDs.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ReadSpanRecord {
    pub span: u64,
    pub parent_span: Option<u64>,
    pub kind: ReadSpanKind,
    pub trace_id: Option<String>,
    pub start_us: u64,
    pub finish_us: u64,
}

/// Links contain capture-local identities for selected read spans only.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ReadDependencyRecord {
    pub predecessor: u64,
    pub dependent: u64,
}

struct CapturedSpan {
    owner: Weak<HttpTraceCapture>,
    record: ReadSpanRecord,
}

fn parent<S: Subscriber + for<'lookup> LookupSpan<'lookup>>(
    capture: &Arc<HttpTraceCapture>,
    mut current: Option<SpanRef<'_, S>>,
) -> Option<u64> {
    let owner = Arc::downgrade(capture);
    while let Some(span) = current {
        if let Some(record) = span.extensions().get::<CapturedSpan>() {
            if Weak::ptr_eq(&record.owner, &owner) {
                return Some(record.record.span);
            }
        }
        if span.metadata().name() == "slatedb.read" {
            return None;
        }
        current = span.parent();
    }
    None
}

pub(crate) fn read_parent(capture: &Arc<HttpTraceCapture>) -> Option<u64> {
    Span::current()
        .with_subscriber(|(id, subscriber)| {
            subscriber
                .downcast_ref::<Registry>()
                .and_then(|registry| parent(capture, registry.span(id)))
        })
        .flatten()
}

pub(crate) struct ReadTraceLayer {
    pub(crate) capture: Arc<HttpTraceCapture>,
}

impl<S> Layer<S> for ReadTraceLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let start_us = self.capture.now_us();
        let Some(kind) = ReadSpanKind::from_name(attrs.metadata().name()) else {
            return;
        };
        let span = ctx.span(id).expect("registered span");
        let mut trace_id = TraceId::default();
        if kind == ReadSpanKind::Read {
            attrs.record(&mut trace_id);
        }
        let record = ReadSpanRecord {
            span: 0,
            parent_span: parent(&self.capture, span.parent()),
            kind,
            trace_id: trace_id.value,
            start_us,
            finish_us: 0,
        };
        if let Some(record) = self.capture.start_read_span(record, trace_id.rejected) {
            span.extensions_mut().insert(CapturedSpan {
                owner: Arc::downgrade(&self.capture),
                record,
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let finish_us = self.capture.now_us();
        let span = ctx.span(&id).expect("registered span");
        if ReadSpanKind::from_name(span.metadata().name()).is_none() {
            return;
        }
        let record = span
            .extensions_mut()
            .remove::<CapturedSpan>()
            .map(|mut captured| {
                captured.record.finish_us = finish_us;
                captured.record
            });
        self.capture.finish_read_span(record);
    }

    fn on_follows_from(&self, dependent: &Id, predecessor: &Id, ctx: Context<'_, S>) {
        let owner = Arc::downgrade(&self.capture);
        let retained = |id| {
            let span = ctx.span(id)?;
            let extensions = span.extensions();
            let captured = extensions.get::<CapturedSpan>()?;
            Weak::ptr_eq(&captured.owner, &owner).then_some(captured.record.span)
        };
        self.capture
            .record_read_dependency(retained(predecessor), retained(dependent));
    }
}

#[derive(Default)]
struct TraceId {
    value: Option<String>,
    rejected: bool,
}

impl tracing::field::Visit for TraceId {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "trace_id" {
            if value.len() > MAX_TRACE_ID_BYTES {
                self.rejected = true;
            } else {
                self.value = Some(value.to_owned());
            }
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Instrument;
    use tracing_subscriber::layer::SubscriberExt;

    fn subscriber(capture: &Arc<HttpTraceCapture>) -> tracing::Dispatch {
        tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                ReadTraceLayer {
                    capture: capture.clone(),
                }
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    metadata.is_span() && ReadSpanKind::from_name(metadata.name()).is_some()
                })),
            ),
        )
    }

    #[test]
    fn dependencies_keep_selected_capture_local_endpoints() {
        let capture = HttpTraceCapture::new(8);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let root = tracing::info_span!("slatedb.read", trace_id = "query");
            root.in_scope(|| {
                let index = tracing::info_span!("slatedb.read.read_index");
                let first = tracing::info_span!("slatedb.read.read_blocks");
                let second = tracing::info_span!("slatedb.read.read_blocks");
                first.follows_from(&index);
                second.follows_from(&index);
                let snapshot = capture.snapshot();
                assert!(snapshot.read_spans.is_empty());
                assert_eq!(snapshot.open_read_spans, 4);
                assert_eq!(
                    snapshot.read_dependencies,
                    vec![
                        ReadDependencyRecord {
                            predecessor: 2,
                            dependent: 3
                        },
                        ReadDependencyRecord {
                            predecessor: 2,
                            dependent: 4
                        },
                    ]
                );
            });
        });
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.read_spans.len(), 4);
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.dropped_read_dependencies, 0);
        assert_eq!(snapshot.unresolved_read_dependencies, 0);
        for dependency in &snapshot.read_dependencies {
            let span = snapshot
                .read_spans
                .iter()
                .find(|span| span.span == dependency.dependent)
                .unwrap();
            assert_eq!(span.parent_span, Some(1));
        }
    }

    #[test]
    fn dependency_capacity_is_separate_from_spans_and_calls() {
        let capture = HttpTraceCapture::new(3);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let index = tracing::info_span!("slatedb.read.read_index");
            let blocks = tracing::info_span!("slatedb.read.read_blocks");
            for _ in 0..4 {
                blocks.follows_from(&index);
            }
        });
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.read_dependencies.len(), 3);
        assert_eq!(snapshot.dropped_read_dependencies, 1);
        assert_eq!(snapshot.unresolved_read_dependencies, 0);
        assert_eq!(snapshot.read_spans.len(), 2);
        assert_eq!(snapshot.dropped_read_spans, 0);
        assert_eq!(snapshot.completed_call_count, Some(0));
    }

    #[test]
    fn dependencies_report_unretained_endpoints() {
        let capture = HttpTraceCapture::new(2);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let long_id = "x".repeat(MAX_TRACE_ID_BYTES + 1);
            let rejected = tracing::info_span!("slatedb.read", trace_id = long_id.as_str());
            let blocks = tracing::info_span!("slatedb.read.read_blocks");
            let overflow = tracing::info_span!("slatedb.read.read_index");
            blocks.follows_from(&rejected);
            blocks.follows_from(&overflow);
        });
        let snapshot = capture.snapshot();
        assert!(snapshot.read_dependencies.is_empty());
        assert_eq!(snapshot.unresolved_read_dependencies, 2);
        assert_eq!(snapshot.dropped_read_dependencies, 0);
        assert_eq!(snapshot.dropped_read_spans, 2);
        assert_eq!(snapshot.open_read_spans, 0);
    }

    #[test]
    fn filtered_dependency_endpoints_do_not_reach_capture() {
        let capture = HttpTraceCapture::new(4);
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(
                    ReadTraceLayer {
                        capture: capture.clone(),
                    }
                    .with_filter(tracing_subscriber::filter::filter_fn(
                        |metadata| {
                            metadata.is_span() && ReadSpanKind::from_name(metadata.name()).is_some()
                        },
                    )),
                )
                .with(tracing_subscriber::fmt::layer().with_writer(std::io::sink)),
        );
        tracing::dispatcher::with_default(&dispatch, || {
            let index = tracing::info_span!("slatedb.read.read_index");
            let outside = tracing::info_span!("outside");
            assert!(outside.id().is_some());
            let blocks = tracing::info_span!("slatedb.read.read_blocks");
            blocks.follows_from(&outside);
            outside.follows_from(&index);
        });
        let snapshot = capture.snapshot();
        assert!(snapshot.read_dependencies.is_empty());
        assert_eq!(snapshot.unresolved_read_dependencies, 0);
        assert_eq!(snapshot.dropped_read_dependencies, 0);
        assert_eq!(snapshot.read_spans.len(), 2);
    }

    #[test]
    fn dependencies_refuse_extensions_owned_by_another_capture() {
        let capture = HttpTraceCapture::new(4);
        let foreign = HttpTraceCapture::new(4);
        let dispatch = subscriber(&capture);
        tracing::dispatcher::with_default(&dispatch, || {
            let index = tracing::info_span!("slatedb.read.read_index");
            let blocks = tracing::info_span!("slatedb.read.read_blocks");
            index
                .with_subscriber(|(id, subscriber)| {
                    let registry = subscriber.downcast_ref::<Registry>().unwrap();
                    let span = registry.span(id).unwrap();
                    span.extensions_mut()
                        .get_mut::<CapturedSpan>()
                        .unwrap()
                        .owner = Arc::downgrade(&foreign);
                })
                .unwrap();
            blocks.follows_from(&index);
        });
        let snapshot = capture.snapshot();
        assert!(snapshot.read_dependencies.is_empty());
        assert_eq!(snapshot.unresolved_read_dependencies, 1);
        assert_eq!(snapshot.dropped_read_dependencies, 0);
        assert!(foreign.snapshot().read_dependencies.is_empty());
    }

    #[test]
    fn read_spans_keep_local_parents_and_close_times() {
        let capture = HttpTraceCapture::new(8);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let root =
                tracing::info_span!("slatedb.read", trace_id = "query-a", private = "secret");
            {
                let _root = root.enter();
                let child = tracing::info_span!("slatedb.read.read_blocks");
                let _child = child.enter();
                assert_eq!(read_parent(&capture), Some(2));
                assert_eq!(capture.snapshot().open_read_spans, 2);
                assert_eq!(read_parent(&HttpTraceCapture::new(8)), None);
            }
            drop(root);
            let second = tracing::info_span!("slatedb.read", trace_id = "query-b");
            assert_eq!(second.in_scope(|| read_parent(&capture)), Some(3));
        });
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.dropped_read_spans, 0);
        assert_eq!(snapshot.read_spans.len(), 3);
        let root = snapshot
            .read_spans
            .iter()
            .find(|span| span.span == 1)
            .unwrap();
        let child = snapshot
            .read_spans
            .iter()
            .find(|span| span.span == 2)
            .unwrap();
        assert_eq!(root.trace_id.as_deref(), Some("query-a"));
        assert_eq!(child.parent_span, Some(root.span));
        assert_eq!(child.trace_id, None);
        assert!(root.start_us <= child.start_us);
        assert!(child.finish_us <= root.finish_us);
        assert!(snapshot
            .read_spans
            .iter()
            .all(|span| span.start_us <= span.finish_us));
        assert!(!format!("{snapshot:?}").contains("secret"));
    }

    #[test]
    fn read_span_capacity_reserves_open_spans_and_reports_loss() {
        let capture = HttpTraceCapture::new(1);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let root = tracing::info_span!("slatedb.read", trace_id = "first");
            let second = tracing::info_span!("slatedb.read", trace_id = "second");
            let snapshot = capture.snapshot();
            assert_eq!(snapshot.open_read_spans, 2);
            assert_eq!(snapshot.dropped_read_spans, 1);
            assert!(snapshot.read_spans.is_empty());
            assert_eq!(second.in_scope(|| read_parent(&capture)), None);
            drop(second);
            drop(root);
        });
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.read_spans.len(), 1);
        assert_eq!(snapshot.dropped_read_spans, 1);
        assert_eq!(snapshot.completed_call_count, Some(0));
    }

    #[test]
    fn a_rejected_query_root_never_inherits_an_outer_query() {
        let capture = HttpTraceCapture::new(8);
        tracing::dispatcher::with_default(&subscriber(&capture), || {
            let outer = tracing::info_span!("slatedb.read", trace_id = "outer");
            outer.in_scope(|| {
                let long_id = "x".repeat(MAX_TRACE_ID_BYTES + 1);
                let inner = tracing::info_span!("slatedb.read", trace_id = long_id.as_str());
                assert_eq!(inner.in_scope(|| read_parent(&capture)), None);
            });
        });
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.dropped_read_spans, 1);
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.read_spans.len(), 1);
        assert_eq!(snapshot.read_spans[0].trace_id.as_deref(), Some("outer"));
    }

    #[tokio::test]
    async fn cold_reads_and_later_scan_batches_keep_read_roots() {
        use slatedb::config::{FlushOptions, FlushType, ReadOptions, ScanOptions, TracingOptions};
        use tracing::instrument::WithSubscriber;

        let store = Arc::new(object_store::memory::InMemory::new());
        let db = slatedb::Db::builder("span-test", store.clone())
            .with_db_cache_disabled()
            .build()
            .await
            .unwrap();
        for i in 0..64 {
            db.put(format!("key-{i:03}"), vec![42; 8192]).await.unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
        let db = slatedb::Db::builder("span-test", store)
            .with_db_cache_disabled()
            .build()
            .await
            .unwrap();
        let capture = HttpTraceCapture::new(256);
        let dispatch = subscriber(&capture);
        let options = ReadOptions {
            tracing_options: Some(TracingOptions::new("cold-get")),
            ..Default::default()
        };
        assert_eq!(
            db.get_with_options(b"key-000", &options)
                .with_subscriber(dispatch.clone())
                .await
                .unwrap()
                .unwrap()
                .len(),
            8192
        );
        let options = ScanOptions {
            tracing_options: Some(TracingOptions::new("paged-scan")),
            read_ahead_bytes: 1,
            max_fetch_tasks: 1,
            cache_blocks: false,
            ..Default::default()
        };
        let mut iter = db
            .scan_with_options(.., &options)
            .with_subscriber(dispatch.clone())
            .await
            .unwrap();
        let at_open = capture.now_us();
        let mut rows = 0;
        while iter
            .next()
            .with_subscriber(dispatch.clone())
            .await
            .unwrap()
            .is_some()
        {
            rows += 1
        }
        assert_eq!(rows, 64);
        let at_eof = capture.snapshot();
        assert!(at_eof.open_read_spans > 0);
        drop(iter);
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.dropped_read_spans, 0);
        let root = snapshot
            .read_spans
            .iter()
            .find(|span| span.trace_id.as_deref() == Some("paged-scan"))
            .unwrap();
        assert!(snapshot
            .read_spans
            .iter()
            .any(|span| span.kind == ReadSpanKind::ReadBlocks
                && span.parent_span == Some(root.span)
                && span.start_us >= at_open));
        assert!(snapshot
            .read_spans
            .iter()
            .any(|span| span.trace_id.as_deref() == Some("cold-get")));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_cancelled_read_future_closes_its_retained_root() {
        use tracing::instrument::WithSubscriber;

        let capture = HttpTraceCapture::new(8);
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(
            async move {
                let root = tracing::info_span!("slatedb.read", trace_id = "cancelled");
                async move {
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                }
                .instrument(root)
                .await;
            }
            .with_subscriber(subscriber(&capture)),
        );
        ready.await.unwrap();
        assert_eq!(capture.snapshot().open_read_spans, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.open_read_spans, 0);
        assert_eq!(snapshot.dropped_read_spans, 0);
        assert_eq!(snapshot.read_spans.len(), 1);
        assert_eq!(
            snapshot.read_spans[0].trace_id.as_deref(),
            Some("cancelled")
        );
    }
}
