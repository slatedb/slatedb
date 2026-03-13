//! UniFFI wrappers for SlateDB.
//!
//! This crate exposes a builder-oriented FFI surface for opening a database,
//! performing reads and writes, scanning keys, and working with transactions.
//! Object stores are resolved explicitly and then passed into [`DbBuilder`].

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

pub use builder::{DbBuilder, DbReaderBuilder};
pub use config::{
    FfiDurabilityLevel, FfiFlushOptions, FfiFlushType, FfiIsolationLevel, FfiKeyRange, FfiKeyValue,
    FfiMergeOptions, FfiPutOptions, FfiReadOptions, FfiReaderOptions, FfiScanOptions,
    FfiSstBlockSize, FfiTtl, FfiWriteHandle, FfiWriteOperation, FfiWriteOptions,
};
pub use db::{Db, DbSnapshot};
pub use db_reader::DbReader;
pub use error::{FfiCloseReason, FfiMergeOperatorCallbackError, FfiSlatedbError};
pub use iterator::DbIterator;
pub use logging::{init_default_logging, init_logging, set_logging_level, FfiLogLevel};
pub use merge_operator::MergeOperator;
pub use object_store::{resolve_object_store, ObjectStore};
pub use settings::default_settings_json;
pub use transaction::DbTransaction;
pub use wal::{
    FfiRowEntry, FfiRowEntryKind, FfiWalFileMetadata, WalFile, WalFileIterator, WalReader,
};
pub use write_batch::WriteBatch;

uniffi::setup_scaffolding!("slatedb");
