// Import our modules
mod config;
mod error;
mod types;
mod object_store;

// FFI function modules
pub mod memory; // Make this public so extern "C" functions are exported
pub mod db_reader; // Make this public so extern "C" functions are exported
pub mod db; // Make this public so extern "C" functions are exported
pub mod iterator; // Make this public so extern "C" functions are exported
pub mod batch; // Make this public so extern "C" functions are exported

// Re-export types that need to be visible to C
pub use error::{CSdbError, CSdbResult};
pub use types::{
    CSdbHandle, CSdbValue, CSdbKeyValue, CSdbScanResult, 
    CSdbWriteOptions, CSdbPutOptions, CSdbReadOptions, CSdbScanOptions
};
pub use db_reader::{CSdbReaderHandle, CSdbReaderOptions};

// Re-export all FFI functions so they're available at the crate level
// This ensures the C header generation still works correctly

// Database functions
pub use db::{
    slatedb_open, slatedb_put_with_options, slatedb_delete_with_options,
    slatedb_get_with_options, slatedb_flush, slatedb_close, slatedb_scan_with_options
};

// Iterator functions
pub use iterator::{
    slatedb_iterator_next, slatedb_iterator_seek, slatedb_iterator_close
};

// WriteBatch functions
pub use batch::{
    slatedb_write_batch_new, slatedb_write_batch_put, slatedb_write_batch_put_with_options,
    slatedb_write_batch_delete, slatedb_write_batch_write, slatedb_write_batch_close
};

// Memory management functions (already in memory module)
pub use memory::{slatedb_free_result, slatedb_free_value, slatedb_free_scan_result};

// DbReader functions (already in db_reader module)
pub use db_reader::{
    slatedb_reader_open, slatedb_reader_get_with_options, 
    slatedb_reader_scan_with_options, slatedb_reader_close
};