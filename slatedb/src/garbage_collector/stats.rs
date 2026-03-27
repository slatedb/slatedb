use crate::db_metrics::DbMetrics;
use slatedb_common::metrics::CounterFn;
use std::sync::Arc;

macro_rules! gc_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("gc", $suffix)
    };
}

pub const GC_MANIFEST_COUNT: &str = gc_stat_name!("manifest_count");
pub const GC_WAL_COUNT: &str = gc_stat_name!("wal_count");
pub const GC_COMPACTED_COUNT: &str = gc_stat_name!("compacted_count");
pub const GC_COMPACTIONS_COUNT: &str = gc_stat_name!("compactions_count");
pub const GC_COUNT: &str = gc_stat_name!("count");

/// Stats for the garbage collector.
pub struct GcStats {
    pub gc_manifest_count: Arc<dyn CounterFn>,
    pub gc_wal_count: Arc<dyn CounterFn>,
    pub gc_compacted_count: Arc<dyn CounterFn>,
    pub gc_compactions_count: Arc<dyn CounterFn>,
    pub gc_count: Arc<dyn CounterFn>,
}

impl GcStats {
    pub(crate) fn new(recorder: &DbMetrics) -> Self {
        Self {
            gc_manifest_count: recorder.counter(GC_MANIFEST_COUNT).register(),
            gc_wal_count: recorder.counter(GC_WAL_COUNT).register(),
            gc_compacted_count: recorder.counter(GC_COMPACTED_COUNT).register(),
            gc_compactions_count: recorder.counter(GC_COMPACTIONS_COUNT).register(),
            gc_count: recorder.counter(GC_COUNT).register(),
        }
    }
}
