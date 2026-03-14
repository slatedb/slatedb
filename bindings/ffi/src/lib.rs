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
mod object_store;
mod settings;
mod types;
mod validation;
mod wal;
mod write_batch;

pub use builder::{FfiDbBuilder, FfiDbReaderBuilder};
pub use config::{
    FfiDurabilityLevel, FfiFlushOptions, FfiFlushType, FfiIsolationLevel, FfiMergeOptions,
    FfiPutOptions, FfiReadOptions, FfiReaderOptions, FfiScanOptions, FfiSstBlockSize, FfiTtl,
    FfiWriteOptions,
};
pub use db::FfiDb;
pub use db_reader::FfiDbReader;
pub use db_snapshot::FfiDbSnapshot;
pub use db_transaction::FfiDbTransaction;
pub use error::{FfiCloseReason, FfiError, FfiMergeOperatorCallbackError};
pub use iterator::FfiDbIterator;
pub use logging::{ffi_init_logging, FfiLogCallback, FfiLogLevel, FfiLogRecord};
pub use merge_operator::FfiMergeOperator;
pub use object_store::FfiObjectStore;
pub use settings::FfiSettings;
pub use types::{FfiKeyRange, FfiKeyValue, FfiRowEntry, FfiRowEntryKind, FfiWriteHandle};
pub use wal::{FfiWalFile, FfiWalFileIterator, FfiWalFileMetadata, FfiWalReader};
pub use write_batch::FfiWriteBatch;

uniffi::setup_scaffolding!("slatedb");
