use std::sync::Arc;

mod builder;
mod config;
mod db;
mod db_reader;
mod db_snapshot;
mod db_transaction;
mod error;
mod iterator;
mod logging;
mod merge_operator;
mod metrics;
mod object_store;
mod settings;
mod types;
mod validation;
mod wal_reader;
mod write_batch;

pub use builder::{DbBuilder, DbReaderBuilder};
pub use config::{
    DurabilityLevel, FlushOptions, FlushType, IsolationLevel, MergeOptions, PutOptions,
    ReadOptions, ReaderOptions, ScanOptions, SstBlockSize, Ttl, WriteOptions,
};
pub use db::Db;
pub use db_reader::DbReader;
pub use db_snapshot::DbSnapshot;
pub use db_transaction::DbTransaction;
pub use error::{CloseReason, Error, MergeOperatorCallbackError};
pub use iterator::DbIterator;
pub use logging::{init_logging, LogCallback, LogLevel, LogRecord};
pub use merge_operator::MergeOperator;
pub use metrics::{
    Counter, DefaultMetricsRecorder, Gauge, Histogram, HistogramMetricValue, Metric, MetricLabel,
    MetricValue, UpDownCounter,
};
pub use object_store::ObjectStore;
pub use settings::Settings;
pub use types::{KeyRange, KeyValue, RowEntry, RowEntryKind, WriteHandle};
pub use wal_reader::{WalFile, WalFileIterator, WalFileMetadata, WalReader};
pub use write_batch::WriteBatch;

/// Application-defined metrics recorder used to publish SlateDB metrics.
#[uniffi::export(with_foreign)]
pub trait MetricsRecorder: Send + Sync {
    /// Registers a monotonically increasing counter.
    fn register_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Counter>;

    /// Registers a gauge.
    fn register_gauge(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn Gauge>;

    /// Registers an up/down counter.
    fn register_up_down_counter(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
    ) -> Arc<dyn UpDownCounter>;

    /// Registers a histogram with explicit bucket boundaries.
    fn register_histogram(
        &self,
        name: String,
        description: String,
        labels: Vec<MetricLabel>,
        boundaries: Vec<f64>,
    ) -> Arc<dyn Histogram>;
}

uniffi::setup_scaffolding!("slatedb");
