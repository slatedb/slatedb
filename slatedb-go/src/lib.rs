// Import our modules
mod config;
mod error;
mod object_store;
mod types;

// FFI function modules
pub mod batch;
pub mod db; // Make this public so extern "C" functions are exported
pub mod db_reader; // Make this public so extern "C" functions are exported
pub mod iterator; // Make this public so extern "C" functions are exported
pub mod memory; // Make this public so extern "C" functions are exported // Make this public so extern "C" functions are exported

// Re-export types that need to be visible to C
pub use db_reader::{CSdbReaderHandle, CSdbReaderOptions};
pub use error::{CSdbError, CSdbResult};
pub use types::{
    CSdbHandle, CSdbKeyValue, CSdbPutOptions, CSdbReadOptions, CSdbScanOptions, CSdbScanResult,
    CSdbValue, CSdbWriteOptions,
};

use std::os::raw::c_char;
use std::sync::Once;

static INIT_LOGGER: Once = Once::new();

/// Initialize logging for SlateDB Go bindings
/// This should be called once before using any other SlateDB functions
///
/// # Safety
///
/// - `level` must be a valid C string pointer or null for default level
#[no_mangle]
pub extern "C" fn slatedb_init_logging(level: *const c_char) -> error::CSdbResult {
    let log_level = if level.is_null() {
        log::LevelFilter::Info
    } else {
        match error::safe_str_from_ptr(level) {
            Ok(level_str) => match level_str.to_lowercase().as_str() {
                "trace" => log::LevelFilter::Trace,
                "debug" => log::LevelFilter::Debug,
                "info" => log::LevelFilter::Info,
                "warn" => log::LevelFilter::Warn,
                "error" => log::LevelFilter::Error,
                _ => {
                    eprintln!("Invalid log level '{}', using Info", level_str);
                    log::LevelFilter::Info
                }
            },
            Err(_) => {
                eprintln!("Invalid log level parameter, using Info");
                log::LevelFilter::Info
            }
        }
    };

    INIT_LOGGER.call_once(|| {
        env_logger::Builder::from_default_env()
            .filter_level(log_level)
            .format_timestamp_secs()
            .init();

        log::info!("SlateDB logging initialized with level: {}", log_level);
    });

    error::create_success_result()
}

// Re-export all FFI functions so they're available at the crate level
// This ensures the C header generation still works correctly

// Database functions
pub use db::{
    slatedb_close, slatedb_delete_with_options, slatedb_flush, slatedb_get_with_options,
    slatedb_open, slatedb_put_with_options, slatedb_scan_with_options,
};

// Iterator functions
pub use iterator::{slatedb_iterator_close, slatedb_iterator_next, slatedb_iterator_seek};

// WriteBatch functions
pub use batch::{
    slatedb_write_batch_close, slatedb_write_batch_delete, slatedb_write_batch_new,
    slatedb_write_batch_put, slatedb_write_batch_put_with_options, slatedb_write_batch_write,
};

// Memory management functions (already in memory module)
pub use memory::{slatedb_free_result, slatedb_free_scan_result, slatedb_free_value};

// DbReader functions (already in db_reader module)
pub use db_reader::{
    slatedb_reader_close, slatedb_reader_get_with_options, slatedb_reader_open,
    slatedb_reader_scan_with_options,
};
