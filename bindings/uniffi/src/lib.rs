mod admin;
mod builder;
mod config;
mod db;
mod db_cache;
mod db_reader;
mod db_snapshot;
mod db_transaction;
mod error;
mod filter_policy;
mod iterator;
mod logging;
mod merge_operator;
mod metrics;
mod object_store;
mod runtime;
mod settings;
mod types;
mod validation;
mod wal_reader;
mod write_batch;

pub use admin::Admin;
pub use builder::{AdminBuilder, CloneBuilder, DbBuilder, DbReaderBuilder};
pub use config::{
    DurabilityLevel, FlushOptions, FlushType, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, GarbageCollectorScheduleOptions, IsolationLevel, IterationOrder,
    MergeOptions, PutOptions, ReadOptions, ReaderOptions, ScanOptions, SstBlockSize, Ttl,
    WriteOptions,
};
pub use db::Db;
pub use db_reader::DbReader;
pub use db_snapshot::DbSnapshot;
pub use db_transaction::DbTransaction;
pub use error::{CloseReason, Error, MergeOperatorCallbackError};
pub use filter_policy::{
    BloomFilterOptions, FilterContext, FilterPolicy, PrefixExtractor, PrefixTarget,
};
pub use iterator::DbIterator;
pub use logging::{init_logging, LogCallback, LogLevel, LogRecord};
pub use merge_operator::MergeOperator;
pub use metrics::{
    Counter, DefaultMetricsRecorder, Gauge, Histogram, HistogramMetricValue, Metric, MetricLabel,
    MetricValue, MetricsRecorder, UpDownCounter,
};
pub use object_store::ObjectStore;
pub use settings::Settings;
pub use types::{
    CacheTarget, Checkpoint, CloneSourceSpec, Compaction, CompactionSpec, CompactionStatus,
    CompactorStateView, CompressionCodec, DbStatus, ExternalDb, FilterFormat,
    IdentifiedObjectMetadata, KeyRange, KeyValue, ObjectMetadata, RowEntry, RowEntryKind, Segment,
    SegmentPrefix, SortedRun, SourceId, SsTableHandle, SsTableId, SsTableInfo, SsTableView,
    SstType, VersionedCompactions, VersionedManifest, WriteHandle,
};
pub use wal_reader::{WalFile, WalFileIterator, WalReader};
pub use write_batch::WriteBatch;

uniffi::setup_scaffolding!("slatedb");
