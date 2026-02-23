//! SlateDB C FFI crate root.
//!
//! This module re-exports all C ABI types and functions implemented across
//! the `slatedb-c` package so they can be discovered and exported by cbindgen.

mod builder;
mod db;
mod db_reader;
mod ffi;
mod iterator;
mod logging;
mod memory;
mod merge_operator;
mod object_store;
mod settings;
mod wal_reader;
mod write_batch;

pub use ffi::{
    slatedb_bound_t, slatedb_close_reason_t, slatedb_db_builder_t, slatedb_db_reader_options_t,
    slatedb_db_reader_t, slatedb_db_t, slatedb_error_kind_t, slatedb_flush_options_t,
    slatedb_iterator_t, slatedb_log_callback_fn, slatedb_log_context_free_fn, slatedb_log_level_t,
    slatedb_merge_operator_context_free_fn, slatedb_merge_operator_fn,
    slatedb_merge_operator_result_free_fn, slatedb_merge_options_t, slatedb_object_store_t,
    slatedb_put_options_t, slatedb_range_t, slatedb_read_options_t, slatedb_result_t,
    slatedb_row_entry_t, slatedb_scan_options_t, slatedb_settings_t, slatedb_sst_block_size_t,
    slatedb_wal_file_iterator_t, slatedb_wal_file_metadata_t, slatedb_wal_file_t,
    slatedb_wal_reader_t, slatedb_write_batch_t, slatedb_write_options_t,
    SLATEDB_BOUND_KIND_EXCLUDED, SLATEDB_BOUND_KIND_INCLUDED, SLATEDB_BOUND_KIND_UNBOUNDED,
    SLATEDB_DURABILITY_FILTER_MEMORY, SLATEDB_DURABILITY_FILTER_REMOTE,
    SLATEDB_FLUSH_TYPE_MEMTABLE, SLATEDB_FLUSH_TYPE_WAL, SLATEDB_LOG_LEVEL_DEBUG,
    SLATEDB_LOG_LEVEL_ERROR, SLATEDB_LOG_LEVEL_INFO, SLATEDB_LOG_LEVEL_OFF,
    SLATEDB_LOG_LEVEL_TRACE, SLATEDB_LOG_LEVEL_WARN, SLATEDB_ROW_ENTRY_KIND_MERGE,
    SLATEDB_ROW_ENTRY_KIND_TOMBSTONE, SLATEDB_ROW_ENTRY_KIND_VALUE, SLATEDB_SST_BLOCK_SIZE_16KIB,
    SLATEDB_SST_BLOCK_SIZE_1KIB, SLATEDB_SST_BLOCK_SIZE_2KIB, SLATEDB_SST_BLOCK_SIZE_32KIB,
    SLATEDB_SST_BLOCK_SIZE_4KIB, SLATEDB_SST_BLOCK_SIZE_64KIB, SLATEDB_SST_BLOCK_SIZE_8KIB,
    SLATEDB_TTL_TYPE_DEFAULT, SLATEDB_TTL_TYPE_EXPIRE_AFTER, SLATEDB_TTL_TYPE_NO_EXPIRY,
};

pub use builder::{
    slatedb_db_builder_build, slatedb_db_builder_close, slatedb_db_builder_new,
    slatedb_db_builder_with_merge_operator, slatedb_db_builder_with_seed,
    slatedb_db_builder_with_settings, slatedb_db_builder_with_sst_block_size,
    slatedb_db_builder_with_wal_object_store,
};

pub use db::{
    slatedb_db_close, slatedb_db_delete, slatedb_db_delete_with_options, slatedb_db_flush,
    slatedb_db_flush_with_options, slatedb_db_get, slatedb_db_get_with_options, slatedb_db_merge,
    slatedb_db_merge_with_options, slatedb_db_metric_get, slatedb_db_metrics, slatedb_db_open,
    slatedb_db_put, slatedb_db_put_with_options, slatedb_db_scan, slatedb_db_scan_prefix,
    slatedb_db_scan_prefix_with_options, slatedb_db_scan_with_options, slatedb_db_status,
    slatedb_db_write, slatedb_db_write_with_options,
};
pub use db_reader::{
    slatedb_db_reader_close, slatedb_db_reader_get, slatedb_db_reader_get_with_options,
    slatedb_db_reader_open, slatedb_db_reader_scan, slatedb_db_reader_scan_prefix,
    slatedb_db_reader_scan_prefix_with_options, slatedb_db_reader_scan_with_options,
};

pub use iterator::{slatedb_iterator_close, slatedb_iterator_next, slatedb_iterator_seek};
pub use logging::{
    slatedb_logging_clear_callback, slatedb_logging_init, slatedb_logging_set_callback,
    slatedb_logging_set_level,
};
pub use memory::{slatedb_bytes_free, slatedb_result_free};
pub use object_store::{
    slatedb_object_store_close, slatedb_object_store_from_env, slatedb_object_store_from_url,
};
pub use settings::{
    slatedb_settings_apply_kv, slatedb_settings_close, slatedb_settings_default,
    slatedb_settings_from_env, slatedb_settings_from_env_with_default, slatedb_settings_from_file,
    slatedb_settings_from_json, slatedb_settings_load, slatedb_settings_to_json,
};
pub use wal_reader::{
    slatedb_row_entry_free, slatedb_wal_file_close, slatedb_wal_file_id, slatedb_wal_file_iterator,
    slatedb_wal_file_iterator_close, slatedb_wal_file_iterator_next, slatedb_wal_file_metadata,
    slatedb_wal_file_metadata_free, slatedb_wal_file_next_file, slatedb_wal_file_next_id,
    slatedb_wal_files_free, slatedb_wal_reader_close, slatedb_wal_reader_get,
    slatedb_wal_reader_list, slatedb_wal_reader_new,
};
pub use write_batch::{
    slatedb_write_batch_close, slatedb_write_batch_delete, slatedb_write_batch_merge,
    slatedb_write_batch_merge_with_options, slatedb_write_batch_new, slatedb_write_batch_put,
    slatedb_write_batch_put_with_options,
};
