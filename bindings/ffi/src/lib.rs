mod builder;
mod config;
mod db;
mod error;
mod iterator;
mod object_store;
mod settings;
mod transaction;
mod validation;

pub use builder::{DbBuilder, MergeOperator};
pub use config::{
    DbFlushOptions, DbKeyRange, DbMergeOptions, DbPutOptions, DbReadOptions, DbScanOptions,
    DbWriteOperation, DbWriteOptions, DurabilityLevel, FlushType, IsolationLevel, KeyValue,
    SstBlockSize, Ttl, WriteHandle,
};
pub use db::{Db, DbSnapshot};
pub use error::SlatedbError;
pub use iterator::DbIterator;
pub use object_store::{resolve_object_store, ObjectStore};
pub use settings::default_settings_json;
pub use transaction::DbTransaction;

uniffi::setup_scaffolding!("slatedb");

#[cfg(test)]
mod tests;
