mod db;
mod ffi;
mod iterator;
mod memory;
mod object_store;
mod write_batch;

pub use ffi::{
    slatedb_bound_t, slatedb_db_builder_t, slatedb_db_t, slatedb_error_t, slatedb_flush_options_t,
    slatedb_iterator_t, slatedb_key_value_t, slatedb_merge_options_t, slatedb_object_store_t,
    slatedb_put_options_t, slatedb_range_t, slatedb_read_options_t, slatedb_result_t,
    slatedb_scan_options_t, slatedb_sst_block_size_t, slatedb_value_t, slatedb_write_batch_t,
    slatedb_write_options_t,
};

pub use db::{
    slatedb_db_builder_build, slatedb_db_builder_close, slatedb_db_builder_new,
    slatedb_db_builder_with_seed, slatedb_db_builder_with_sst_block_size,
    slatedb_db_builder_with_wal_object_store, slatedb_db_close, slatedb_db_delete,
    slatedb_db_delete_with_options, slatedb_db_flush, slatedb_db_flush_with_options,
    slatedb_db_get, slatedb_db_get_with_options, slatedb_db_merge, slatedb_db_merge_with_options,
    slatedb_db_open, slatedb_db_put, slatedb_db_put_with_options, slatedb_db_scan,
    slatedb_db_scan_prefix, slatedb_db_scan_prefix_with_options, slatedb_db_scan_with_options,
    slatedb_db_status, slatedb_db_write, slatedb_db_write_with_options,
};

pub use iterator::{slatedb_iterator_close, slatedb_iterator_next, slatedb_iterator_seek};
pub use memory::{slatedb_key_value_free, slatedb_result_free, slatedb_value_free};
pub use object_store::{slatedb_db_resolve_object_store, slatedb_object_store_close};
pub use write_batch::{
    slatedb_write_batch_close, slatedb_write_batch_delete, slatedb_write_batch_merge,
    slatedb_write_batch_merge_with_options, slatedb_write_batch_new, slatedb_write_batch_put,
    slatedb_write_batch_put_with_options,
};
