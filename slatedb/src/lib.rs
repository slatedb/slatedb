#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]
#![allow(clippy::result_large_err, clippy::too_many_arguments)]
// Disallow non-approved non-deterministic types and functions in production code
#![deny(clippy::disallowed_types, clippy::disallowed_methods)]
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods
    )
)]

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
#[cfg(feature = "compaction_filters")]
pub use compaction_filter::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext,
};
pub use compactor::CompactorBuilder;
pub use config::{Settings, SstBlockSize};
pub use db::{Db, DbBuilder, DbReaderBuilder, DbStatus, WriteHandle};
pub use db_cache::stats as db_cache_stats;
pub use db_iter::DbIterator;
pub use db_read::DbRead;
pub use db_reader::DbReader;
pub use db_snapshot::DbSnapshot;
pub use db_transaction::DbTransaction;
pub use error::{CloseReason, Error, ErrorKind};
pub use format::sst::BlockTransformer;
pub use garbage_collector::stats as garbage_collector_stats;
pub use garbage_collector::GarbageCollectorBuilder;
pub use merge_operator::{MergeOperator, MergeOperatorError};
pub use rand::DbRand;
#[cfg(test)]
pub use sst_builder::BlockFormat;
pub use transaction_manager::IsolationLevel;
pub use types::KeyValue;
pub use types::{RowEntry, ValueDeletable};
pub use wal_reader::{WalFile, WalFileIterator, WalFileMetadata, WalReader};

pub mod admin;
pub mod cached_object_store;
pub mod clock;
#[cfg(feature = "bencher")]
pub mod compaction_execute_bench;
pub mod compactor;
pub mod config;
pub mod db_cache;
pub mod db_stats;
pub mod manifest;
pub mod seq_tracker;
pub mod size_tiered_compaction;
pub mod stats;

mod batch;
mod batch_write;
mod blob;
mod block_iterator;
mod block_iterator_v2;
#[cfg(any(test, feature = "bencher"))]
mod bytes_generator;
mod bytes_range;
mod checkpoint;
mod clone;
#[cfg(feature = "compaction_filters")]
mod compaction_filter;
#[cfg(feature = "compaction_filters")]
mod compaction_filter_iterator;
mod compactions_store;
mod compactor_executor;
mod compactor_state;
mod compactor_state_protocols;
#[allow(dead_code)]
mod comparable_range;
mod db;
mod db_common;
mod db_iter;
mod db_read;
mod db_reader;
mod db_snapshot;
mod db_state;
mod db_status;
mod db_transaction;
mod dispatcher;
mod error;
mod filter;
mod filter_iterator;
mod flatbuffer_types;
mod flush;
mod format;
mod garbage_collector;
mod iter;
mod map_iter;
mod mem_table;
mod mem_table_flush;
mod merge_iterator;
mod merge_operator;
mod object_stores;
mod oracle;
mod partitioned_keyspace;
mod paths;
mod peeking_iterator;
#[cfg(test)]
mod proptest_util;
mod rand;
mod reader;
mod retention_iterator;
mod retrying_object_store;
mod sorted_run_iterator;
mod sst_builder;
mod sst_iter;
mod store_provider;
mod tablestore;
#[cfg(test)]
mod test_utils;
mod transaction_manager;
mod types;
mod utils;

mod wal;
mod wal_buffer;
mod wal_id;
mod wal_reader;
mod wal_replay;

// Initialize test infrastructure (deadlock detector, tracing) for all tests.
// This ctor runs at crate load time, ensuring these are set up even for tests
// that don't explicitly use test_utils.
#[cfg(test)]
#[ctor::ctor]
fn init_test_infrastructure() {
    crate::test_utils::init_test_infrastructure();
}
