//! UniFFI wrappers for SlateDB.
//!
//! This crate exposes a builder-oriented FFI surface for opening a database,
//! performing reads and writes, scanning keys, and working with transactions.
//! Object stores are resolved explicitly and then passed into [`FfiDbBuilder`].

mod builder;
mod config;
mod db;
mod db_reader;
mod error;
mod iterator;
mod logging;
mod merge_operator;
mod object_store;
mod settings;
mod transaction;
mod validation;
mod wal;
mod write_batch;

pub use builder::{FfiDbBuilder, FfiDbReaderBuilder};
pub use config::{
    FfiDurabilityLevel, FfiFlushOptions, FfiFlushType, FfiIsolationLevel, FfiKeyRange, FfiKeyValue,
    FfiMergeOptions, FfiPutOptions, FfiReadOptions, FfiReaderOptions, FfiScanOptions,
    FfiSstBlockSize, FfiTtl, FfiWriteHandle, FfiWriteOperation, FfiWriteOptions,
};
pub use db::{FfiDb, FfiDbSnapshot};
pub use db_reader::FfiDbReader;
pub use error::{FfiCloseReason, FfiMergeOperatorCallbackError, FfiSlatedbError};
pub use iterator::FfiDbIterator;
pub use logging::{ffi_init_default_logging, ffi_init_logging, ffi_set_logging_level, FfiLogLevel};
pub use merge_operator::FfiMergeOperator;
pub use object_store::{ffi_resolve_object_store, FfiObjectStore};
pub use settings::ffi_default_settings_json;
pub use transaction::FfiDbTransaction;
pub use wal::{
    FfiRowEntry, FfiRowEntryKind, FfiWalFile, FfiWalFileIterator, FfiWalFileMetadata, FfiWalReader,
};
pub use write_batch::FfiWriteBatch;

uniffi::setup_scaffolding!("slatedb");
