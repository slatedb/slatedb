#![doc = include_str!("../README.md")]
#![warn(clippy::unwrap_used)]
#![cfg_attr(test, allow(clippy::unwrap_used))]
#![warn(clippy::panic)]
#![cfg_attr(test, allow(clippy::panic))]

pub mod admin;
pub mod batch;
mod batch_write;
mod blob;
mod block;
mod block_iterator;
#[cfg(any(test, feature = "bencher"))]
mod bytes;
mod bytes_range;
mod cached_object_store;
pub mod checkpoint;
#[cfg(feature = "bencher")]
pub mod compaction_execute_bench;
mod compactor;
mod compactor_executor;
mod compactor_state;
pub mod config;
pub mod db;
pub mod db_cache;
mod db_common;
pub mod db_iter;
mod db_state;
pub mod error;
mod filter;
mod flatbuffer_types;
mod flush;
mod garbage_collector;
mod iter;
mod manifest;
mod manifest_store;
mod mem_table;
mod mem_table_flush;
mod merge_iterator;
mod metrics;
#[cfg(test)]
mod proptest_util;
mod row_codec;
pub mod size_tiered_compaction;
mod sorted_run_iterator;
mod sst;
mod sst_iter;
mod tablestore;
#[cfg(test)]
mod test_utils;
mod transactional_object_store;
mod types;
mod utils;

/// Re-export the object store crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// without having to depend on the object store crate directly.
pub use object_store;

/// Re-export the fail-parallel crate.
///
/// This is useful for users of the crate who want to use SlateDB
/// with failpoints in their tests without having to depend on the fail-parallel crate directly.
pub use fail_parallel;
