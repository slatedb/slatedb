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
mod object_store;
mod settings;
mod transaction;
mod validation;
mod wal;
mod write_batch;

pub use builder::{DbBuilder, MergeOperator};
pub use config::{
    DbFlushOptions, DbKeyRange, DbMergeOptions, DbPutOptions, DbReadOptions, DbReaderOptions,
    DbScanOptions, DbWriteOperation, DbWriteOptions, DurabilityLevel, FlushType, IsolationLevel,
    KeyValue, SstBlockSize, Ttl, WriteHandle,
};
pub use db::{Db, DbSnapshot};
pub use db_reader::{DbReader, DbReaderBuilder};
pub use error::{CloseReason, MergeOperatorCallbackError, SlatedbError};
pub use iterator::DbIterator;
pub use logging::{init_default_logging, init_logging, set_logging_level, LogLevel};
pub use object_store::{resolve_object_store, ObjectStore};
pub use settings::default_settings_json;
pub use transaction::DbTransaction;
pub use wal::{RowEntry, RowEntryKind, WalFile, WalFileIterator, WalFileMetadata, WalReader};
pub use write_batch::WriteBatch;

uniffi::setup_scaffolding!("slatedb");
