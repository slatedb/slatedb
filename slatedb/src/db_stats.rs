use slatedb_common::metrics::{CounterFn, GaugeFn, MetricsRecorderHelper};
use std::sync::Arc;

macro_rules! db_stat_name {
    ($suffix:expr) => {
        concat!("db", "/", $suffix)
    };
}

pub const IMMUTABLE_MEMTABLE_FLUSHES: &str = db_stat_name!("immutable_memtable_flushes");
pub const SST_FILTER_FALSE_POSITIVES: &str = db_stat_name!("sst_filter_false_positives");
pub const SST_FILTER_POSITIVES: &str = db_stat_name!("sst_filter_positives");
pub const SST_FILTER_NEGATIVES: &str = db_stat_name!("sst_filter_negatives");
pub const BACKPRESSURE_COUNT: &str = db_stat_name!("backpressure_count");
pub const WAL_BUFFER_ESTIMATED_BYTES: &str = db_stat_name!("wal_buffer_estimated_bytes");
pub const WAL_BUFFER_FLUSHES: &str = db_stat_name!("wal_buffer_flushes");
pub const WAL_BUFFER_FLUSH_REQUESTS: &str = db_stat_name!("wal_buffer_flush_requests");
pub const GET_REQUESTS: &str = db_stat_name!("get_requests");
pub const SCAN_REQUESTS: &str = db_stat_name!("scan_requests");
pub const FLUSH_REQUESTS: &str = db_stat_name!("flush_requests");
pub const WRITE_BATCH_COUNT: &str = db_stat_name!("write_batch_count");
pub const WRITE_OPS: &str = db_stat_name!("write_ops");
pub const TOTAL_MEM_SIZE_BYTES: &str = db_stat_name!("total_mem_size_bytes");
pub const L0_SST_COUNT: &str = db_stat_name!("l0_sst_count");

#[non_exhaustive]
#[derive(Clone)]
pub(crate) struct DbStats {
    pub(crate) immutable_memtable_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_estimated_bytes: Arc<dyn GaugeFn>,
    pub(crate) wal_buffer_flushes: Arc<dyn CounterFn>,
    pub(crate) wal_buffer_flush_requests: Arc<dyn CounterFn>,
    pub(crate) sst_filter_false_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_positives: Arc<dyn CounterFn>,
    pub(crate) sst_filter_negatives: Arc<dyn CounterFn>,
    pub(crate) backpressure_count: Arc<dyn CounterFn>,
    pub(crate) get_requests: Arc<dyn CounterFn>,
    pub(crate) scan_requests: Arc<dyn CounterFn>,
    pub(crate) flush_requests: Arc<dyn CounterFn>,
    pub(crate) write_batch_count: Arc<dyn CounterFn>,
    pub(crate) write_ops: Arc<dyn CounterFn>,
    pub(crate) total_mem_size_bytes: Arc<dyn GaugeFn>,
    pub(crate) l0_sst_count: Arc<dyn GaugeFn>,
}

impl DbStats {
    pub(crate) fn new(recorder: &MetricsRecorderHelper) -> DbStats {
        DbStats {
            immutable_memtable_flushes: recorder.counter(IMMUTABLE_MEMTABLE_FLUSHES).register(),
            wal_buffer_estimated_bytes: recorder.gauge(WAL_BUFFER_ESTIMATED_BYTES).register(),
            wal_buffer_flushes: recorder.counter(WAL_BUFFER_FLUSHES).register(),
            wal_buffer_flush_requests: recorder.counter(WAL_BUFFER_FLUSH_REQUESTS).register(),
            sst_filter_false_positives: recorder.counter(SST_FILTER_FALSE_POSITIVES).register(),
            sst_filter_positives: recorder.counter(SST_FILTER_POSITIVES).register(),
            sst_filter_negatives: recorder.counter(SST_FILTER_NEGATIVES).register(),
            backpressure_count: recorder.counter(BACKPRESSURE_COUNT).register(),
            get_requests: recorder.counter(GET_REQUESTS).register(),
            scan_requests: recorder.counter(SCAN_REQUESTS).register(),
            flush_requests: recorder.counter(FLUSH_REQUESTS).register(),
            write_batch_count: recorder.counter(WRITE_BATCH_COUNT).register(),
            write_ops: recorder.counter(WRITE_OPS).register(),
            total_mem_size_bytes: recorder.gauge(TOTAL_MEM_SIZE_BYTES).register(),
            l0_sst_count: recorder.gauge(L0_SST_COUNT).register(),
        }
    }
}
