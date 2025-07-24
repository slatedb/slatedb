#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]
#![allow(clippy::result_large_err)]
// Disallow non-approved non-deterministic types and functions in production code
#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
#![cfg_attr(test, allow(clippy::disallowed_types, clippy::disallowed_methods))]

/// Re-export the bytes crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// without having to depend on the bytes crate directly.
pub use bytes;

/// Re-export the fail-parallel crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// with failpoints in their tests without having to depend on the
/// fail-parallel crate directly.
pub use fail_parallel;

/// Re-export the object store crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// without having to depend on the object store crate directly.
pub use object_store;

pub use batch::WriteBatch;
pub use cached_object_store::stats as cached_object_store_stats;
pub use checkpoint::{Checkpoint, CheckpointCreateResult};
pub use compactor::stats as compactor_stats;
pub use config::{Settings, SstBlockSize};
pub use db::{Db, DbBuilder};
pub use db_cache::stats as db_cache_stats;
pub use db_iter::DbIterator;
pub use db_read::DbRead;
pub use db_reader::DbReader;
pub use error::{SettingsError, SlateDBError};
pub use garbage_collector::stats as garbage_collector_stats;
pub use merge_operator::{MergeOperator, MergeOperatorError};
pub use types::KeyValue;

pub mod admin;
pub mod clock;
#[cfg(feature = "bencher")]
pub mod compaction_execute_bench;
pub mod config;
pub mod db_cache;
pub mod db_stats;
pub mod size_tiered_compaction;
pub mod stats;

mod batch;
mod batch_write;
mod blob;
mod block;
mod block_iterator;
#[cfg(any(test, feature = "bencher"))]
mod bytes_generator;
mod bytes_range;
mod cached_object_store;
mod checkpoint;
mod clone;
mod compactor;
mod compactor_executor;
mod compactor_state;
#[allow(dead_code)]
mod comparable_range;
mod db;
mod db_common;
mod db_iter;
mod db_read;
mod db_reader;
mod db_state;
mod error;
mod filter;
mod filter_iterator;
mod flatbuffer_types;
mod flush;
mod garbage_collector;
mod iter;
mod manifest;
mod mem_table;
mod mem_table_flush;
mod merge_iterator;
mod merge_operator;
mod object_stores;
mod oracle;
mod partitioned_keyspace;
mod paths;
#[cfg(test)]
mod proptest_util;
mod rand;
mod reader;
mod retention_iterator;
mod row_codec;
mod sorted_run_iterator;
mod sst;
mod sst_iter;
mod store_provider;
mod tablestore;
#[cfg(test)]
mod test_utils;
mod transactional_object_store;
mod types;
mod utils;
mod wal_buffer;
mod wal_id;
mod wal_replay;
