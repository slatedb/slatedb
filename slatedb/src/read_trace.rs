use crate::config::TracingOptions;
use crate::db_state::SsTableId;

#[derive(Clone, Debug)]
pub(crate) struct ReadTrace {
    tracing_options: Option<TracingOptions>,
    read_span: tracing::Span,
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
        let read_span = tracing_options
            .as_ref()
            .map(|tracing_options| {
                tracing::info_span!("slatedb.read", trace_id = tracing_options.trace_id.as_str(),)
            })
            .unwrap_or_else(tracing::Span::none);
        Self {
            tracing_options,
            read_span,
        }
    }

    pub(crate) fn none() -> Self {
        Self::new(None)
    }

    pub(crate) fn read_span(&self) -> tracing::Span {
        self.read_span.clone()
    }

    pub(crate) fn new_memtable_span(&self) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            tracing::debug_span!(
                parent: &self.read_span,
                "slatedb.read.memtable",
                trace_id = tracing_options.trace_id.as_str(),
            )
        } else {
            tracing::Span::none()
        }
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
    ) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: &self.read_span,
                "slatedb.read.read_filters",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cached = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        }
    }

    pub(crate) fn new_evaluate_filter_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
        filter_name: impl AsRef<str>,
    ) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: &self.read_span,
                "slatedb.read.evaluate_filter",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                filter_name = filter_name.as_ref(),
                result = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        }
    }

    pub(crate) fn new_read_index_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
    ) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: &self.read_span,
                "slatedb.read.read_index",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cached = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        }
    }

    pub(crate) fn new_read_block_span(
        &self,
        sst_id: SsTableId,
        sst_level: Option<&SstTraceLevel>,
    ) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            let sst_id = sst_id.value().to_string();
            let sst_level = Self::format_sst_level(sst_level);
            tracing::info_span!(
                parent: &self.read_span,
                "slatedb.read.read_blocks",
                trace_id = tracing_options.trace_id.as_str(),
                sst_id = sst_id.as_str(),
                sst_level = sst_level.as_str(),
                cache_hits = tracing::field::Empty,
                cache_misses = tracing::field::Empty,
            )
        } else {
            tracing::Span::none()
        }
    }

    pub(crate) fn new_read_merge_span(&self, num_operands: usize) -> tracing::Span {
        if let Some(tracing_options) = self.tracing_options.as_ref() {
            tracing::info_span!(
                parent: &self.read_span,
                "slatedb.read.merge",
                trace_id = tracing_options.trace_id.as_str(),
                num_operands,
            )
        } else {
            tracing::Span::none()
        }
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

    fn new_all_child_spans(trace: &ReadTrace) -> Vec<tracing::Span> {
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
            assert!(trace.read_span().is_none());
            for span in new_all_child_spans(&trace) {
                assert!(span.is_none());
            }
        });

        assert!(spans.is_empty());
    }

    #[test]
    fn test_new_with_tracing_options_creates_read_span() {
        let spans = record_spans(|| {
            let trace = ReadTrace::new(Some(TracingOptions::new(TRACE_ID)));
            let read_span = trace.read_span();
            assert!(!read_span.is_none());
            assert_eq!(
                read_span.metadata().map(|metadata| metadata.name()),
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
            span.record("cached", true);
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
            span.record("result", false);
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
            span.record("cached", false);
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
            span.record("cache_hits", 2_u64);
            span.record("cache_misses", 3_u64);
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
