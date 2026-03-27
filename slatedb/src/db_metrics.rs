use std::sync::Arc;

use slatedb_common::metrics::{
    CompositeMetricsRecorder, CounterBuilder, DefaultMetricsRecorder, GaugeBuilder,
    HistogramBuilder, MetricLevel, Metrics, MetricsRecorder, MetricsRecorderHelper,
    UpDownCounterBuilder,
};

use crate::stats::StatRegistry;

/// Internal metrics infrastructure for a SlateDB instance.
///
/// Bundles the default recorder (for snapshots/reads) with the recorder helper
/// (for registering metrics). Components receive this single struct instead of
/// separate recorder pieces.
#[derive(Clone)]
pub(crate) struct DbMetrics {
    default_recorder: Arc<DefaultMetricsRecorder>,
    helper: MetricsRecorderHelper,
}

impl DbMetrics {
    /// Create metrics infrastructure with an optional user-provided recorder.
    pub(crate) fn new(user_recorder: Option<Arc<dyn MetricsRecorder>>) -> Self {
        let default_recorder = Arc::new(DefaultMetricsRecorder::new());
        let composite: Arc<dyn MetricsRecorder> = if let Some(user) = user_recorder {
            Arc::new(CompositeMetricsRecorder::new(vec![
                default_recorder.clone() as Arc<dyn MetricsRecorder>,
                user,
            ]))
        } else {
            default_recorder.clone()
        };
        let helper = MetricsRecorderHelper::new(composite, MetricLevel::default());
        Self {
            default_recorder,
            helper,
        }
    }

    pub(crate) fn counter(&self, name: &str) -> CounterBuilder<'_> {
        self.helper.counter(name)
    }

    pub(crate) fn gauge(&self, name: &str) -> GaugeBuilder<'_> {
        self.helper.gauge(name)
    }

    pub(crate) fn up_down_counter(&self, name: &str) -> UpDownCounterBuilder<'_> {
        self.helper.up_down_counter(name)
    }

    #[allow(dead_code)]
    pub(crate) fn histogram<'a>(&'a self, name: &str, boundaries: &[f64]) -> HistogramBuilder<'a> {
        self.helper.histogram(name, boundaries)
    }

    /// Take a point-in-time snapshot of all registered metrics.
    pub(crate) fn snapshot(&self) -> Metrics {
        self.default_recorder.snapshot()
    }

    /// Create a backward-compatible StatRegistry for `db.metrics()`.
    pub(crate) fn stat_registry(&self) -> Arc<StatRegistry> {
        Arc::new(StatRegistry::new(self.default_recorder.clone()))
    }
}
