use std::ffi::{c_char, CString};
use std::time::Duration;

use crate::db_reader::CSdbReaderOptions;
use crate::error::safe_str_from_ptr;
use crate::types::{CSdbPutOptions, CSdbReadOptions, CSdbScanOptions, CSdbWriteOptions};
use slatedb::bytes::Bytes;
use slatedb::config::{
    DbReaderOptions, DurabilityLevel, PutOptions, ReadOptions, ScanOptions, Ttl, WriteOptions,
};
use std::ops::Bound;

// Convert C options to Rust options

// Convert C scan options to Rust ScanOptions
pub(crate) fn convert_scan_options(c_opts: *const CSdbScanOptions) -> ScanOptions {
    if c_opts.is_null() {
        return ScanOptions::default();
    }

    let opts = unsafe { &*c_opts };
    let durability = match opts.durability_filter {
        1 => DurabilityLevel::Remote,
        _ => DurabilityLevel::Memory,
    };

    ScanOptions::new()
        .with_durability_filter(durability)
        .with_dirty(opts.dirty)
        .with_read_ahead_bytes(opts.read_ahead_bytes as usize)
        .with_cache_blocks(opts.cache_blocks)
        .with_max_fetch_tasks(opts.max_fetch_tasks as usize)
}

// Convert C write options to Rust WriteOptions
pub(crate) fn convert_write_options(c_opts: *const CSdbWriteOptions) -> WriteOptions {
    if c_opts.is_null() {
        return WriteOptions {
            await_durable: true,
        };
    }

    let opts = unsafe { &*c_opts };
    WriteOptions {
        await_durable: opts.await_durable,
    }
}

// Convert C put options to Rust PutOptions
pub(crate) fn convert_put_options(c_opts: *const CSdbPutOptions) -> PutOptions {
    if c_opts.is_null() {
        return PutOptions::default();
    }

    let opts = unsafe { &*c_opts };
    let ttl = match opts.ttl_type {
        0 => Ttl::Default,
        1 => Ttl::NoExpiry,
        2 => Ttl::ExpireAfter(opts.ttl_value),
        _ => Ttl::Default, // fallback
    };

    PutOptions { ttl }
}

// Convert C read options to Rust ReadOptions
pub(crate) fn convert_read_options(c_opts: *const CSdbReadOptions) -> ReadOptions {
    if c_opts.is_null() {
        return ReadOptions::default();
    }

    let opts = unsafe { &*c_opts };
    let durability_filter = match opts.durability_filter {
        0 => DurabilityLevel::Memory,
        1 => DurabilityLevel::Remote,
        _ => DurabilityLevel::Memory, // fallback
    };

    ReadOptions {
        durability_filter,
        dirty: opts.dirty,
        cache_blocks: opts.cache_blocks,
    }
}

// Convert C range bounds to Rust range bounds
// Returns (start_bound, end_bound) tuple that can be used to create ranges
pub(crate) fn convert_range_bounds(
    start: *const u8,
    start_len: usize,
    end: *const u8,
    end_len: usize,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let start_bound = if start.is_null() {
        Bound::Unbounded
    } else {
        let start_slice = unsafe { std::slice::from_raw_parts(start, start_len) };
        Bound::Included(Bytes::copy_from_slice(start_slice))
    };

    let end_bound = if end.is_null() {
        Bound::Unbounded
    } else {
        let end_slice = unsafe { std::slice::from_raw_parts(end, end_len) };
        Bound::Excluded(Bytes::copy_from_slice(end_slice)) // Exclusive end bound
    };

    (start_bound, end_bound)
}

// Convert C reader options to Rust DbReaderOptions
pub(crate) fn convert_reader_options(c_opts: *const CSdbReaderOptions) -> DbReaderOptions {
    if c_opts.is_null() {
        return DbReaderOptions::default();
    }

    let opts = unsafe { &*c_opts };
    let defaults = DbReaderOptions::default();

    let manifest_poll_interval = if opts.manifest_poll_interval_ms == 0 {
        defaults.manifest_poll_interval
    } else {
        Duration::from_millis(opts.manifest_poll_interval_ms)
    };

    let checkpoint_lifetime = if opts.checkpoint_lifetime_ms == 0 {
        defaults.checkpoint_lifetime
    } else {
        Duration::from_millis(opts.checkpoint_lifetime_ms)
    };

    let max_memtable_bytes = if opts.max_memtable_bytes == 0 {
        defaults.max_memtable_bytes
    } else {
        opts.max_memtable_bytes
    };

    DbReaderOptions {
        manifest_poll_interval,
        checkpoint_lifetime,
        max_memtable_bytes,
        block_cache: defaults.block_cache,
        merge_operator: defaults.merge_operator,
    }
}

/// Create default Settings and return as JSON string
#[no_mangle]
#[allow(unreachable_pub)]
pub extern "C" fn slatedb_settings_default() -> *mut c_char {
    let settings = slatedb::config::Settings::default();
    match serde_json::to_string(&settings) {
        Ok(json) => match CString::new(json) {
            Ok(cstr) => cstr.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Load Settings from file and return as JSON string
#[no_mangle]
#[allow(unreachable_pub)]
pub extern "C" fn slatedb_settings_from_file(path: *const c_char) -> *mut c_char {
    let path_str = match safe_str_from_ptr(path) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    match slatedb::config::Settings::from_file(path_str) {
        Ok(settings) => match serde_json::to_string(&settings) {
            Ok(json) => match CString::new(json) {
                Ok(cstr) => cstr.into_raw(),
                Err(_) => std::ptr::null_mut(),
            },
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Load Settings from environment variables and return as JSON string
#[no_mangle]
#[allow(unreachable_pub)]
pub extern "C" fn slatedb_settings_from_env(prefix: *const c_char) -> *mut c_char {
    let prefix_str = match safe_str_from_ptr(prefix) {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };

    match slatedb::config::Settings::from_env(prefix_str) {
        Ok(settings) => match serde_json::to_string(&settings) {
            Ok(json) => match CString::new(json) {
                Ok(cstr) => cstr.into_raw(),
                Err(_) => std::ptr::null_mut(),
            },
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Load Settings using auto-detection and return as JSON string
#[no_mangle]
#[allow(unreachable_pub)]
pub extern "C" fn slatedb_settings_load() -> *mut c_char {
    match slatedb::config::Settings::load() {
        Ok(settings) => match serde_json::to_string(&settings) {
            Ok(json) => match CString::new(json) {
                Ok(cstr) => cstr.into_raw(),
                Err(_) => std::ptr::null_mut(),
            },
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}
