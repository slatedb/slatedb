use crate::stats::{Counter, Gauge, StatRegistry};
use std::sync::Arc;

macro_rules! db_stat_name {
    ($suffix:expr) => {
        crate::stat_name!("db", $suffix)
    };
}

pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const SST_FILTER_FALSE_POSITIVES: &str = db_stat_name!("sst_filter_false_positives");
pub const SST_FILTER_POSITIVES: &str = db_stat_name!("sst_filter_positives");
pub const SST_FILTER_NEGATIVES: &str = db_stat_name!("sst_filter_negatives");
pub const BACKPRESSURE_COUNT: &str = db_stat_name!("backpressure_count");
pub const WAL_BUFFER_ESTIMATED_BYTES: &str = db_stat_name!("wal_buffer_estimated_bytes");
pub const WAL_BUFFER_FLUSHES: &str = db_stat_name!("wal_buffer_flushes");

#[non_exhaustive]
#[derive(Clone, Debug)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<Counter>,
    pub(crate) wal_buffer_estimated_bytes: Arc<Gauge<i64>>,
    pub(crate) wal_buffer_flushes: Arc<Counter>,
    pub(crate) sst_filter_false_positives: Arc<Counter>,
    pub(crate) sst_filter_positives: Arc<Counter>,
    pub(crate) sst_filter_negatives: Arc<Counter>,
    pub(crate) backpressure_count: Arc<Counter>,
}

impl DbStats {
    pub(crate) fn new(registry: &StatRegistry) -> DbStats {
        let stats = Self {
            immutable_memtable_flushes: Arc::new(Counter::default()),
            wal_buffer_estimated_bytes: Arc::new(Gauge::default()),
            wal_buffer_flushes: Arc::new(Counter::default()),
            sst_filter_false_positives: Arc::new(Counter::default()),
            sst_filter_positives: Arc::new(Counter::default()),
            sst_filter_negatives: Arc::new(Counter::default()),
            backpressure_count: Arc::new(Counter::default()),
        };
        registry.register(
            IMMUTABLE_MEMTABLE_FLUSHES,
            stats.immutable_memtable_flushes.clone(),
        );
        registry.register(
            WAL_BUFFER_ESTIMATED_BYTES,
            stats.wal_buffer_estimated_bytes.clone(),
        );
        registry.register(WAL_BUFFER_FLUSHES, stats.wal_buffer_flushes.clone());
        registry.register(
            SST_FILTER_FALSE_POSITIVES,
            stats.sst_filter_false_positives.clone(),
        );
        registry.register(SST_FILTER_POSITIVES, stats.sst_filter_positives.clone());
        registry.register(SST_FILTER_NEGATIVES, stats.sst_filter_negatives.clone());
        registry.register(BACKPRESSURE_COUNT, stats.backpressure_count.clone());
        stats
    }
}
