//! Core FFI types and conversion helpers for `slatedb-c`.
//!
//! This module defines all public C ABI data structures and opaque handle
//! wrappers used by the higher-level function modules (`db`, `write_batch`,
//! `iterator`, etc.). It also contains internal conversion and validation
//! helpers shared across those modules.

use slatedb::bytes::Bytes;
use slatedb::config::{
    DurabilityLevel, FlushOptions, FlushType, MergeOptions, PutOptions, ReadOptions, ScanOptions,
    SstBlockSize, Ttl, WriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::{CloseReason, Db, DbBuilder, DbIterator, ErrorKind, WriteBatch};
use std::ffi::{CStr, CString};
use std::ops::Bound;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

/// Opaque handle backing a resolved object store.
#[allow(non_camel_case_types)]
pub struct slatedb_object_store_t {
    /// Rust-side object store instance.
    pub object_store: Arc<dyn ObjectStore>,
}

/// Opaque handle backing a database builder.
#[allow(non_camel_case_types)]
pub struct slatedb_db_builder_t {
    /// Builder state. Wrapped in `Option` so it can be consumed by `build`.
    pub builder: Option<DbBuilder<String>>,
}

/// Opaque handle backing an open `Db` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_db_t {
    /// Runtime used to execute async SlateDB operations.
    pub runtime: Arc<Runtime>,
    /// Open database instance.
    pub db: Db,
}

/// Opaque handle backing a scan iterator plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_iterator_t {
    /// Runtime used to execute async iterator operations.
    pub runtime: Arc<Runtime>,
    /// Database iterator state.
    pub iter: DbIterator,
}

/// Opaque handle backing a mutable write batch.
#[allow(non_camel_case_types)]
pub struct slatedb_write_batch_t {
    /// Batch state. Wrapped in `Option` so writes can consume it exactly once.
    pub batch: Option<WriteBatch>,
}

/// Public error kind mirroring `slatedb::ErrorKind`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum slatedb_error_kind_t {
    /// No error.
    SLATEDB_ERROR_KIND_NONE = 0,
    /// Transaction conflict / transactional error.
    SLATEDB_ERROR_KIND_TRANSACTION = 1,
    /// Database closed.
    SLATEDB_ERROR_KIND_CLOSED = 2,
    /// Backend/storage unavailable.
    SLATEDB_ERROR_KIND_UNAVAILABLE = 3,
    /// Invalid request/argument/state for caller.
    SLATEDB_ERROR_KIND_INVALID = 4,
    /// Persisted data-level error.
    SLATEDB_ERROR_KIND_DATA = 5,
    /// Internal SlateDB error.
    SLATEDB_ERROR_KIND_INTERNAL = 6,
    /// Unknown error kind (forward compatibility).
    SLATEDB_ERROR_KIND_UNKNOWN = 255,
}

/// Closed reason mirroring `slatedb::CloseReason`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum slatedb_close_reason_t {
    /// Not a closed error.
    SLATEDB_CLOSE_REASON_NONE = 0,
    /// Closed cleanly.
    SLATEDB_CLOSE_REASON_CLEAN = 1,
    /// Closed due to fencing.
    SLATEDB_CLOSE_REASON_FENCED = 2,
    /// Closed due to background panic.
    SLATEDB_CLOSE_REASON_PANIC = 3,
    /// Unknown close reason (forward compatibility).
    SLATEDB_CLOSE_REASON_UNKNOWN = 255,
}

/// Standard result structure returned by all C ABI functions.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct slatedb_result_t {
    /// Top-level SlateDB error kind.
    pub kind: slatedb_error_kind_t,
    /// Additional closed reason when `kind == SLATEDB_ERROR_KIND_CLOSED`.
    pub close_reason: slatedb_close_reason_t,
    /// Optional error message allocated by Rust (free with `slatedb_result_free`).
    pub message: *mut c_char,
}

/// Read options passed to `slatedb_db_get_with_options`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_read_options_t {
    /// Durability filter: `0=Memory`, `1=Remote`.
    pub durability_filter: u8,
    /// Include dirty (uncommitted) data.
    pub dirty: bool,
    /// Cache fetched blocks.
    pub cache_blocks: bool,
}

/// Scan options passed to `slatedb_db_scan_with_options`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_scan_options_t {
    /// Durability filter: `0=Memory`, `1=Remote`.
    pub durability_filter: u8,
    /// Include dirty (uncommitted) data.
    pub dirty: bool,
    /// Read-ahead bytes.
    pub read_ahead_bytes: u64,
    /// Cache fetched blocks.
    pub cache_blocks: bool,
    /// Max concurrent fetch tasks.
    pub max_fetch_tasks: u64,
}

/// Write options passed to write operations.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_write_options_t {
    /// Wait for durable commit before returning.
    pub await_durable: bool,
}

/// Put options passed to put operations.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_put_options_t {
    /// TTL type: `0=Default`, `1=NoExpiry`, `2=ExpireAfter`.
    pub ttl_type: u8,
    /// TTL value in milliseconds when `ttl_type=2`.
    pub ttl_value: u64,
}

/// Merge options passed to merge operations.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_merge_options_t {
    /// TTL type: `0=Default`, `1=NoExpiry`, `2=ExpireAfter`.
    pub ttl_type: u8,
    /// TTL value in milliseconds when `ttl_type=2`.
    pub ttl_value: u64,
}

/// Flush options passed to `slatedb_db_flush_with_options`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_flush_options_t {
    /// Flush type: `0=MemTable`, `1=Wal`.
    pub flush_type: u8,
}

/// SST block size selector for builder config.
#[allow(non_camel_case_types)]
pub type slatedb_sst_block_size_t = u8;

/// C representation of a single range bound.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_bound_t {
    /// Bound kind: `0=Unbounded`, `1=Included`, `2=Excluded`.
    pub kind: u8,
    /// Bound bytes for included/excluded bounds.
    pub data: *const u8,
    /// Length of `data`.
    pub len: usize,
}

/// C representation of a byte-key range.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_range_t {
    /// Start bound.
    pub start: slatedb_bound_t,
    /// End bound.
    pub end: slatedb_bound_t,
}

pub(crate) fn success_result() -> slatedb_result_t {
    slatedb_result_t {
        kind: slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE,
        close_reason: slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        message: ptr::null_mut(),
    }
}

pub(crate) fn error_result_with_close_reason(
    kind: slatedb_error_kind_t,
    close_reason: slatedb_close_reason_t,
    message: &str,
) -> slatedb_result_t {
    slatedb_result_t {
        kind,
        close_reason,
        message: message_to_cstring(message).into_raw(),
    }
}

pub(crate) fn error_result(kind: slatedb_error_kind_t, message: &str) -> slatedb_result_t {
    error_result_with_close_reason(
        kind,
        slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        message,
    )
}

fn message_to_cstring(message: &str) -> CString {
    CString::new(message).unwrap_or_else(|_| CString::new("invalid error message").unwrap())
}

fn map_close_reason(reason: CloseReason) -> slatedb_close_reason_t {
    match reason {
        CloseReason::Clean => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_CLEAN,
        CloseReason::Fenced => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_FENCED,
        CloseReason::Panic => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_PANIC,
        _ => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_UNKNOWN,
    }
}

pub(crate) fn map_error(err: &slatedb::Error) -> (slatedb_error_kind_t, slatedb_close_reason_t) {
    match err.kind() {
        ErrorKind::Transaction => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_TRANSACTION,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
        ErrorKind::Closed(reason) => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_CLOSED,
            map_close_reason(reason),
        ),
        ErrorKind::Unavailable => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNAVAILABLE,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
        ErrorKind::Invalid => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
        ErrorKind::Data => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_DATA,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
        ErrorKind::Internal => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
        _ => (
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_UNKNOWN,
            slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        ),
    }
}

pub(crate) fn error_from_slate_error(err: &slatedb::Error, message: &str) -> slatedb_result_t {
    let (kind, close_reason) = map_error(err);
    error_result_with_close_reason(kind, close_reason, message)
}

pub(crate) fn create_runtime() -> Result<Arc<Runtime>, slatedb_result_t> {
    RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|e| {
            error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
                &format!("failed to create runtime: {e}"),
            )
        })
}

pub(crate) unsafe fn cstr_to_string(
    ptr: *const c_char,
    field_name: &str,
) -> Result<String, slatedb_result_t> {
    if ptr.is_null() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("{field_name} pointer is null"),
        ));
    }

    let cstr = CStr::from_ptr(ptr);
    cstr.to_str().map(|s| s.to_owned()).map_err(|_| {
        error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("{field_name} is not valid UTF-8"),
        )
    })
}

pub(crate) unsafe fn bytes_from_ptr<'a>(
    ptr: *const u8,
    len: usize,
    field_name: &str,
) -> Result<&'a [u8], slatedb_result_t> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("{field_name} pointer is null"),
        ));
    }

    Ok(std::slice::from_raw_parts(ptr, len))
}

pub(crate) fn alloc_bytes(bytes: &[u8]) -> (*mut u8, usize) {
    let boxed = bytes.to_vec().into_boxed_slice();
    let len = boxed.len();
    let data = Box::into_raw(boxed) as *mut u8;
    (data, len)
}

pub(crate) fn durability_level_from_u8(
    durability_filter: u8,
) -> Result<DurabilityLevel, slatedb_result_t> {
    match durability_filter {
        0 => Ok(DurabilityLevel::Memory),
        1 => Ok(DurabilityLevel::Remote),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid durability_filter (expected 0=Memory or 1=Remote)",
        )),
    }
}

pub(crate) unsafe fn read_options_from_ptr(
    ptr: *const slatedb_read_options_t,
) -> Result<ReadOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(ReadOptions::default());
    }

    let options = &*ptr;
    Ok(ReadOptions {
        durability_filter: durability_level_from_u8(options.durability_filter)?,
        dirty: options.dirty,
        cache_blocks: options.cache_blocks,
    })
}

pub(crate) unsafe fn scan_options_from_ptr(
    ptr: *const slatedb_scan_options_t,
) -> Result<ScanOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(ScanOptions::default());
    }

    let options = &*ptr;
    let read_ahead_bytes = usize::try_from(options.read_ahead_bytes).map_err(|_| {
        error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "read_ahead_bytes does not fit in usize",
        )
    })?;
    let max_fetch_tasks = usize::try_from(options.max_fetch_tasks).map_err(|_| {
        error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "max_fetch_tasks does not fit in usize",
        )
    })?;

    Ok(ScanOptions {
        durability_filter: durability_level_from_u8(options.durability_filter)?,
        dirty: options.dirty,
        read_ahead_bytes,
        cache_blocks: options.cache_blocks,
        max_fetch_tasks,
    })
}

pub(crate) unsafe fn write_options_from_ptr(ptr: *const slatedb_write_options_t) -> WriteOptions {
    if ptr.is_null() {
        return WriteOptions::default();
    }

    let options = &*ptr;
    WriteOptions {
        await_durable: options.await_durable,
    }
}

fn ttl_from_parts(ttl_type: u8, ttl_value: u64) -> Result<Ttl, slatedb_result_t> {
    match ttl_type {
        0 => Ok(Ttl::Default),
        1 => Ok(Ttl::NoExpiry),
        2 => Ok(Ttl::ExpireAfter(ttl_value)),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid ttl_type (expected 0=Default, 1=NoExpiry, 2=ExpireAfter)",
        )),
    }
}

pub(crate) unsafe fn put_options_from_ptr(
    ptr: *const slatedb_put_options_t,
) -> Result<PutOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(PutOptions::default());
    }

    let options = &*ptr;
    Ok(PutOptions {
        ttl: ttl_from_parts(options.ttl_type, options.ttl_value)?,
    })
}

pub(crate) unsafe fn merge_options_from_ptr(
    ptr: *const slatedb_merge_options_t,
) -> Result<MergeOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(MergeOptions::default());
    }

    let options = &*ptr;
    Ok(MergeOptions {
        ttl: ttl_from_parts(options.ttl_type, options.ttl_value)?,
    })
}

pub(crate) unsafe fn flush_options_from_ptr(
    ptr: *const slatedb_flush_options_t,
) -> Result<FlushOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(FlushOptions::default());
    }

    let options = &*ptr;
    let flush_type = match options.flush_type {
        0 => FlushType::MemTable,
        1 => FlushType::Wal,
        _ => {
            return Err(error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "invalid flush_type (expected 0=MemTable or 1=Wal)",
            ));
        }
    };

    Ok(FlushOptions { flush_type })
}

pub(crate) fn sst_block_size_from_u8(
    value: slatedb_sst_block_size_t,
) -> Result<SstBlockSize, slatedb_result_t> {
    match value {
        1 => Ok(SstBlockSize::Block1Kib),
        2 => Ok(SstBlockSize::Block2Kib),
        3 => Ok(SstBlockSize::Block4Kib),
        4 => Ok(SstBlockSize::Block8Kib),
        5 => Ok(SstBlockSize::Block16Kib),
        6 => Ok(SstBlockSize::Block32Kib),
        7 => Ok(SstBlockSize::Block64Kib),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid sst_block_size (expected 1..7)",
        )),
    }
}

pub(crate) unsafe fn range_from_c(
    range: slatedb_range_t,
) -> Result<(Bound<Bytes>, Bound<Bytes>), slatedb_result_t> {
    Ok((
        bound_from_c(range.start, "start")?,
        bound_from_c(range.end, "end")?,
    ))
}

unsafe fn bound_from_c(
    bound: slatedb_bound_t,
    name: &str,
) -> Result<Bound<Bytes>, slatedb_result_t> {
    match bound.kind {
        0 => Ok(Bound::Unbounded),
        1 => {
            let bytes = bytes_from_ptr(bound.data, bound.len, name)?;
            Ok(Bound::Included(Bytes::copy_from_slice(bytes)))
        }
        2 => {
            let bytes = bytes_from_ptr(bound.data, bound.len, name)?;
            Ok(Bound::Excluded(Bytes::copy_from_slice(bytes)))
        }
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid bound kind (expected 0=Unbounded, 1=Included, 2=Excluded)",
        )),
    }
}

pub(crate) fn validate_write_key(key: &[u8]) -> Result<(), slatedb_result_t> {
    if key.is_empty() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "key cannot be empty",
        ));
    }
    if key.len() > u16::MAX as usize {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "key size must be <= u16::MAX",
        ));
    }
    Ok(())
}

pub(crate) fn validate_write_key_value(key: &[u8], value: &[u8]) -> Result<(), slatedb_result_t> {
    validate_write_key(key)?;
    if value.len() > u32::MAX as usize {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "value size must be <= u32::MAX",
        ));
    }
    Ok(())
}
