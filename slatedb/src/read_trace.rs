use crate::config::TracingOptions;
use crate::db_state::SsTableId;

#[derive(Clone, Debug)]
pub(crate) struct ReadTrace {
    tracing_options: Option<TracingOptions>,
    subscriber: Option<tracing::Dispatch>,
    read_span: ReadSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadSpan(tracing::Span);

impl ReadSpan {
    pub(crate) fn new(span: tracing::Span) -> Self {
        Self(span)
    }

    pub(crate) fn span(&self) -> &tracing::Span {
        &self.0
    }
}

impl Drop for ReadSpan {
    fn drop(&mut self) {
        let span = std::mem::replace(&mut self.0, tracing::Span::none());
        if let Some(subscriber) =
            tracing::Span::with_subscriber(&span, |(_, subscriber)| subscriber.clone())
        {
            tracing::dispatcher::with_default(&subscriber, || drop(span));
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SstTraceLevel {
    L0,
    SortedRun(u32),
}

impl SstTraceLevel {
    fn span_value(&self) -> String {
        match self {
            Self::L0 => "l0".to_string(),
            Self::SortedRun(id) => format!("sorted_run:{id}"),
        }
    }
}

impl ReadTrace {
    pub(crate) fn new(tracing_options: Option<TracingOptions>) -> Self {
        let subscriber = tracing_options
            .as_ref()
            .map(|_| tracing::dispatcher::get_default(Clone::clone));
        let read_span = tracing_options
            .as_ref()
            .map(|tracing_options| {
                tracing::info_span!("slatedb.read", trace_id = tracing_options.trace_id.as_str(),)
            })
            .unwrap_or_else(tracing::Span::none);
        Self {
            tracing_options,
            subscriber,
            read_span: ReadSpan::new(read_span),
        }
    }

    pub(crate) fn none() -> Self {
        Self::new(None)
    }

    pub(crate) fn read_span(&self) -> ReadSpan {
        self.read_span.clone()
    }

    pub(crate) fn subscriber(&self) -> Option<tracing::Dispatch> {
        self.subscriber.clone()
    }

    fn enter(&self) -> Option<tracing::dispatcher::DefaultGuard> {
        self.subscriber
            .as_ref()
            .map(tracing::dispatcher::set_default)
    }

    pub(crate) fn new_memtable_span(&self) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            tracing::debug_span!(
                parent: self.read_span.span(),
                "slatedb.read.memtable",
                trace_id = tracing_options.trace_id.as_str(),
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }

    fn format_sst_level(sst_level: Option<&SstTraceLevel>) -> String {
        sst_level
            .map(SstTraceLevel::span_value)
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub(crate) fn new_read_filter_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
    ) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: self.read_span.span(),
                "slatedb.read.read_filters",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cached = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }

    pub(crate) fn new_evaluate_filter_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
        filter_name: impl AsRef<str>,
    ) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: self.read_span.span(),
                "slatedb.read.evaluate_filter",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                filter_name = filter_name.as_ref(),
                result = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }

    pub(crate) fn new_read_index_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
    ) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: self.read_span.span(),
                "slatedb.read.read_index",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cached = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }

    pub(crate) fn new_read_block_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
    ) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: self.read_span.span(),
                "slatedb.read.read_blocks",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cache_hits = tracing::field::Empty,
                cache_misses = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }

    pub(crate) fn new_read_merge_span(&self, num_operands: usize) -> ReadSpan {
        let _entered = self.enter();
        let span = if let Some(tracing_options) = self.tracing_options.as_ref() {
            tracing::info_span!(
                parent: self.read_span.span(),
                "slatedb.read.merge",
                trace_id = tracing_options.trace_id.as_str(),
                num_operands,
            )
        } else {
            tracing::Span::none()
        };
        ReadSpan::new(span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{RecordedSpan, SpanRecorder};
    use rstest::rstest;
    use std::collections::HashMap;
    use tracing_subscriber::layer::SubscriberExt;
    use ulid::Ulid;

    const TRACE_ID: &str = "read-trace";
    const READ_SPAN_NAME: &str = "slatedb.read";

    fn test_sst_id() -> SsTableId {
        SsTableId::from(Ulid::from_parts(1, 0))
    }

    fn record_spans(f: impl FnOnce()) -> Vec<RecordedSpan> {
        let recorder = SpanRecorder::default();
        let subscriber = tracing_subscriber::registry().with(recorder.clone());
        tracing::subscriber::with_default(subscriber, f);
        recorder.spans()
    }

    fn find_span(spans: &[RecordedSpan], name: &str) -> RecordedSpan {
        let matching = spans
            .iter()
            .filter(|span| span.name == name)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "expected one span named {name}");
        matching[0].clone()
    }

    fn fields(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn new_all_child_spans(trace: &ReadTrace) -> Vec<ReadSpan> {
        let sst_id = test_sst_id();
        let sst_level = Some(&SstTraceLevel::L0);
        vec![
            trace.new_memtable_span(),
            trace.new_read_filter_span(sst_id, sst_level),
            trace.new_evaluate_filter_span(sst_id, sst_level, "filter"),
            trace.new_read_index_span(sst_id, sst_level),
            trace.new_read_block_span(sst_id, sst_level),
            trace.new_read_merge_span(2),
        ]
    }

    #[rstest]
    #[case::new_without_tracing_options(ReadTrace::new as fn(Option<TracingOptions>) -> ReadTrace)]
    #[case::none(|_| ReadTrace::none())]
    fn test_read_trace_without_tracing_options_creates_no_spans(
        #[case] create: fn(Option<TracingOptions>) -> ReadTrace,
    ) {
        let spans = record_spans(|| {
            let trace = create(None);
            assert!(trace.read_span().span().is_none());
            assert!(trace.subscriber().is_none());
            for span in new_all_child_spans(&trace) {
                assert!(span.span().is_none());
            }
        });

        assert!(spans.is_empty());
    }

    #[rstest]
    #[case::disabled_root("slatedb.read")]
    #[case::disabled_index("slatedb.read.read_index")]
    fn span_factories_keep_the_query_subscriber(#[case] disabled: &'static str) {
        let recorder_a = SpanRecorder::default();
        let dispatch_a = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(recorder_a.clone())
                .with(tracing_subscriber::filter::filter_fn(move |metadata| {
                    metadata.name() != disabled
                })),
        );
        let trace = tracing::dispatcher::with_default(&dispatch_a, || {
            ReadTrace::new(Some(TracingOptions::new(TRACE_ID)))
        });
        let recorder_b = SpanRecorder::default();
        let dispatch_b =
            tracing::Dispatch::new(tracing_subscriber::registry().with(recorder_b.clone()));
        let foreign = tracing::dispatcher::with_default(&dispatch_b, || {
            let root = tracing::info_span!("foreign-root");
            let index = tracing::info_span!(parent: &root, "foreign-index");
            (root, index)
        });
        let children: Vec<_> =
            tracing::dispatcher::with_default(&dispatch_b, || new_all_child_spans(&trace));
        assert_eq!(
            recorder_b.spans().len(),
            2,
            "Span factories must keep the query subscriber."
        );
        let spans = recorder_a.spans();
        assert_eq!(spans.len(), 6);
        assert!(spans.iter().all(|span| span.name != disabled));
        tracing::dispatcher::with_default(&dispatch_b, || drop((children, trace)));
        assert_eq!(recorder_a.closed().len(), spans.len());
        assert!(recorder_b.closed().is_empty());
        tracing::dispatcher::with_default(&dispatch_b, || drop(foreign));
        assert_eq!(recorder_b.closed().len(), 2);
    }

    #[test]
    fn query_root_closes_its_parent_with_the_query_subscriber() {
        let recorder_a = SpanRecorder::default();
        let dispatch_a =
            tracing::Dispatch::new(tracing_subscriber::registry().with(recorder_a.clone()));
        let (trace, parent_id) = tracing::dispatcher::with_default(&dispatch_a, || {
            let parent = tracing::info_span!("query-parent");
            let parent_id = parent.id();
            let trace = parent.in_scope(|| ReadTrace::new(Some(TracingOptions::new(TRACE_ID))));
            drop(parent);
            (trace, parent_id)
        });
        let recorder_b = SpanRecorder::default();
        let dispatch_b =
            tracing::Dispatch::new(tracing_subscriber::registry().with(recorder_b.clone()));
        let foreign = tracing::dispatcher::with_default(&dispatch_b, || {
            let root = tracing::info_span!("foreign-root");
            let child = tracing::info_span!(parent: &root, "foreign-child");
            (root, child)
        });
        assert_eq!(parent_id, foreign.0.id());
        assert_eq!(trace.read_span().span().id(), foreign.1.id());
        assert!(recorder_a.closed().is_empty());
        tracing::dispatcher::with_default(&dispatch_b, || drop(trace));
        assert_eq!(
            recorder_a.closed().len(),
            2,
            "The read root must close its parent with the query subscriber."
        );
        assert!(recorder_b.closed().is_empty());
        tracing::dispatcher::with_default(&dispatch_b, || drop(foreign));
        assert_eq!(recorder_b.closed().len(), 2);
    }

    #[test]
    fn test_new_with_tracing_options_creates_read_span() {
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let read_span = trace.read_span();
            assert!(!read_span.span().is_none());
            assert_eq!(
                read_span.span().metadata().map(|metadata| metadata.name()),
                Some(READ_SPAN_NAME)
            );
        });

        let span = find_span(&spans, READ_SPAN_NAME);
        assert_eq!(spans.len(), 1);
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name, None);
        assert_eq!(span.fields, fields(&[("trace_id", TRACE_ID)]));
    }

    #[test]
    fn test_new_memtable_span() {
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            trace.new_memtable_span();
        });

        let span = find_span(&spans, "slatedb.read.memtable");
        assert_eq!(span.level, "DEBUG");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(span.fields, fields(&[("trace_id", TRACE_ID)]));
    }

    #[rstest]
    fn test_new_read_filter_span(
        #[values(
            (Some(SstTraceLevel::L0), "l0"),
            (Some(SstTraceLevel::SortedRun(7)), "sorted_run:7"),
            (None, "unknown")
        )]
        sst_level: (Option<SstTraceLevel>, &str),
    ) {
        let (sst_level, expected_sst_level) = sst_level;
        let sst_id = test_sst_id();
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let span = trace.new_read_filter_span(sst_id, sst_level.as_ref());
            span.span().record("cached", true);
        });

        let span = find_span(&spans, "slatedb.read.read_filters");
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(
            span.fields,
            fields(&[
                ("trace_id", TRACE_ID),
                ("sst_id", &sst_id.value().to_string()),
                ("sst_level", expected_sst_level),
                ("cached", "true"),
            ])
        );
    }

    #[rstest]
    fn test_new_evaluate_filter_span(
        #[values(
            (Some(SstTraceLevel::L0), "l0"),
            (Some(SstTraceLevel::SortedRun(7)), "sorted_run:7"),
            (None, "unknown")
        )]
        sst_level: (Option<SstTraceLevel>, &str),
    ) {
        const FILTER_NAME: &str = "test.filter";

        let (sst_level, expected_sst_level) = sst_level;
        let sst_id = test_sst_id();
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let span = trace.new_evaluate_filter_span(sst_id, sst_level.as_ref(), FILTER_NAME);
            span.span().record("result", false);
        });

        let span = find_span(&spans, "slatedb.read.evaluate_filter");
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(
            span.fields,
            fields(&[
                ("trace_id", TRACE_ID),
                ("sst_id", &sst_id.value().to_string()),
                ("sst_level", expected_sst_level),
                ("filter_name", FILTER_NAME),
                ("result", "false"),
            ])
        );
    }

    #[rstest]
    fn test_new_read_index_span(
        #[values(
            (Some(SstTraceLevel::L0), "l0"),
            (Some(SstTraceLevel::SortedRun(7)), "sorted_run:7"),
            (None, "unknown")
        )]
        sst_level: (Option<SstTraceLevel>, &str),
    ) {
        let (sst_level, expected_sst_level) = sst_level;
        let sst_id = test_sst_id();
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let span = trace.new_read_index_span(sst_id, sst_level.as_ref());
            span.span().record("cached", false);
        });

        let span = find_span(&spans, "slatedb.read.read_index");
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(
            span.fields,
            fields(&[
                ("trace_id", TRACE_ID),
                ("sst_id", &sst_id.value().to_string()),
                ("sst_level", expected_sst_level),
                ("cached", "false"),
            ])
        );
    }

    #[rstest]
    fn test_new_read_block_span(
        #[values(
            (Some(SstTraceLevel::L0), "l0"),
            (Some(SstTraceLevel::SortedRun(7)), "sorted_run:7"),
            (None, "unknown")
        )]
        sst_level: (Option<SstTraceLevel>, &str),
    ) {
        let (sst_level, expected_sst_level) = sst_level;
        let sst_id = test_sst_id();
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let span = trace.new_read_block_span(sst_id, sst_level.as_ref());
            span.span().record("cache_hits", 2_u64);
            span.span().record("cache_misses", 3_u64);
        });

        let span = find_span(&spans, "slatedb.read.read_blocks");
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(
            span.fields,
            fields(&[
                ("trace_id", TRACE_ID),
                ("sst_id", &sst_id.value().to_string()),
                ("sst_level", expected_sst_level),
                ("cache_hits", "2"),
                ("cache_misses", "3"),
            ])
        );
    }

    #[test]
    fn test_new_read_merge_span() {
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            trace.new_read_merge_span(3);
        });

        let span = find_span(&spans, "slatedb.read.merge");
        assert_eq!(span.level, "INFO");
        assert_eq!(span.parent_name.as_deref(), Some(READ_SPAN_NAME));
        assert_eq!(
            span.fields,
            fields(&[("trace_id", TRACE_ID), ("num_operands", "3")])
        );
    }
}
