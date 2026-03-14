mod builder;
mod config;
mod db;
mod db_reader;
mod error;
mod iterator;
mod merge_operator;
mod object_store;
mod settings;
mod transaction;
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
pub use db::{FfiDb, FfiDbSnapshot};
pub use db_reader::FfiDbReader;
pub use error::{FfiCloseReason, FfiMergeOperatorCallbackError, FfiSlatedbError};
pub use iterator::FfiDbIterator;
pub use merge_operator::FfiMergeOperator;
pub use object_store::FfiObjectStore;
pub use settings::FfiSettings;
pub use transaction::FfiDbTransaction;
pub use types::{FfiKeyRange, FfiKeyValue, FfiRowEntry, FfiRowEntryKind, FfiWriteHandle};
pub use wal::{FfiWalFile, FfiWalFileIterator, FfiWalFileMetadata, FfiWalReader};
pub use write_batch::FfiWriteBatch;

uniffi::setup_scaffolding!("slatedb");
