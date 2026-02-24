//! Core FFI types and conversion helpers for `slatedb-c`.
//!
//! This module defines all public C ABI data structures and opaque handle
//! wrappers used by the higher-level function modules (`db`, `write_batch`,
//! `iterator`, etc.). It also contains internal conversion and validation
//! helpers shared across those modules.

use slatedb::bytes::Bytes;
use slatedb::config::{
    DbReaderOptions, DurabilityLevel, FlushOptions, FlushType, MergeOptions, PutOptions,
    ReadOptions, ScanOptions, SstBlockSize, Ttl, WriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::{
    CloseReason, Db, DbBuilder, DbIterator, DbReader, ErrorKind, Settings, WalFile,
    WalFileIterator, WalReader, WriteBatch,
};
use std::ffi::{c_void, CStr, CString};
use std::ops::Bound;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

/// Durability selector for in-memory visibility.
pub const SLATEDB_DURABILITY_FILTER_MEMORY: u8 = 0;
/// Durability selector for remote/durable visibility.
pub const SLATEDB_DURABILITY_FILTER_REMOTE: u8 = 1;
/// TTL selector for default behavior.
pub const SLATEDB_TTL_TYPE_DEFAULT: u8 = 0;
/// TTL selector for values that never expire.
pub const SLATEDB_TTL_TYPE_NO_EXPIRY: u8 = 1;
/// TTL selector for values that expire after `ttl_value` milliseconds.
pub const SLATEDB_TTL_TYPE_EXPIRE_AFTER: u8 = 2;
/// Flush selector for memtable flushes.
pub const SLATEDB_FLUSH_TYPE_MEMTABLE: u8 = 0;
/// Flush selector for WAL flushes.
pub const SLATEDB_FLUSH_TYPE_WAL: u8 = 1;
/// Logging level selector for disabling logs.
pub const SLATEDB_LOG_LEVEL_OFF: u8 = 0;
/// Logging level selector for error logs.
pub const SLATEDB_LOG_LEVEL_ERROR: u8 = 1;
/// Logging level selector for warning logs.
pub const SLATEDB_LOG_LEVEL_WARN: u8 = 2;
/// Logging level selector for informational logs.
pub const SLATEDB_LOG_LEVEL_INFO: u8 = 3;
/// Logging level selector for debug logs.
pub const SLATEDB_LOG_LEVEL_DEBUG: u8 = 4;
/// Logging level selector for trace logs.
pub const SLATEDB_LOG_LEVEL_TRACE: u8 = 5;
/// Range bound selector for unbounded edges.
pub const SLATEDB_BOUND_KIND_UNBOUNDED: u8 = 0;
/// Range bound selector for inclusive edges.
pub const SLATEDB_BOUND_KIND_INCLUDED: u8 = 1;
/// Range bound selector for exclusive edges.
pub const SLATEDB_BOUND_KIND_EXCLUDED: u8 = 2;
/// SST block size selector for 1 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_1KIB: u8 = 1;
/// SST block size selector for 2 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_2KIB: u8 = 2;
/// SST block size selector for 4 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_4KIB: u8 = 3;
/// SST block size selector for 8 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_8KIB: u8 = 4;
/// SST block size selector for 16 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_16KIB: u8 = 5;
/// SST block size selector for 32 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_32KIB: u8 = 6;
/// SST block size selector for 64 KiB blocks.
pub const SLATEDB_SST_BLOCK_SIZE_64KIB: u8 = 7;
/// Row entry kind: regular value.
pub const SLATEDB_ROW_ENTRY_KIND_VALUE: u8 = 0;
/// Row entry kind: tombstone (deletion marker).
pub const SLATEDB_ROW_ENTRY_KIND_TOMBSTONE: u8 = 1;
/// Row entry kind: merge operand.
pub const SLATEDB_ROW_ENTRY_KIND_MERGE: u8 = 2;

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

/// Opaque handle backing database settings.
#[allow(non_camel_case_types)]
pub struct slatedb_settings_t {
    /// Settings state owned by Rust.
    pub settings: Settings,
}

/// Opaque handle backing an open `Db` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_db_t {
    /// Runtime used to execute async SlateDB operations.
    pub runtime: Arc<Runtime>,
    /// Open database instance.
    pub db: Db,
}

/// Opaque handle backing an open `DbReader` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_db_reader_t {
    /// Runtime used to execute async SlateDB operations.
    pub runtime: Arc<Runtime>,
    /// Open database reader instance.
    pub reader: DbReader,
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

/// Opaque handle backing a `WalReader` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_wal_reader_t {
    /// Runtime used to execute async SlateDB operations.
    pub runtime: Arc<Runtime>,
    /// WAL reader instance.
    pub reader: WalReader,
}

/// Opaque handle backing a `WalFile` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_wal_file_t {
    /// Runtime used to execute async SlateDB operations.
    pub runtime: Arc<Runtime>,
    /// WAL file handle.
    pub file: WalFile,
}

/// Opaque handle backing a `WalFileIterator` plus runtime owner.
#[allow(non_camel_case_types)]
pub struct slatedb_wal_file_iterator_t {
    /// Runtime used to execute async iterator operations.
    pub runtime: Arc<Runtime>,
    /// WAL file iterator state.
    pub iter: WalFileIterator,
}

/// C representation of WAL file metadata.
///
/// `location` and `location_len` reference a Rust-allocated buffer that must
/// be freed by calling `slatedb_wal_file_metadata_free`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct slatedb_wal_file_metadata_t {
    /// Seconds since the Unix epoch for the last-modified time.
    pub last_modified_secs: i64,
    /// Sub-second nanoseconds component for the last-modified time.
    pub last_modified_nanos: u32,
    /// File size in bytes.
    pub size_bytes: u64,
    /// Object storage path as UTF-8 bytes (not null-terminated).
    pub location: *mut u8,
    /// Length of `location` in bytes.
    pub location_len: usize,
}

/// C representation of a single row entry returned by the iterator.
///
/// `key` and `value` reference Rust-allocated buffers that must be freed by
/// calling `slatedb_row_entry_free`. `value` is null when `kind` is
/// `SLATEDB_ROW_ENTRY_KIND_TOMBSTONE`.
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct slatedb_row_entry_t {
    /// Entry kind. Use `SLATEDB_ROW_ENTRY_KIND_*` constants.
    pub kind: u8,
    /// Key bytes.
    pub key: *mut u8,
    /// Length of `key` in bytes.
    pub key_len: usize,
    /// Value bytes. Null for tombstones.
    pub value: *mut u8,
    /// Length of `value` in bytes. Zero for tombstones.
    pub value_len: usize,
    /// Sequence number assigned to this entry.
    pub seq: u64,
    /// Whether `create_ts` is populated.
    pub create_ts_present: bool,
    /// Creation timestamp (valid when `create_ts_present` is true).
    pub create_ts: i64,
    /// Whether `expire_ts` is populated.
    pub expire_ts_present: bool,
    /// Expiration timestamp (valid when `expire_ts_present` is true).
    pub expire_ts: i64,
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

/// Handle returned from write operations, containing metadata.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug)]
pub struct slatedb_write_handle_t {
    /// Sequence number assigned to this write.
    pub seq: u64,
    /// Creation timestamp assigned to this write.
    pub create_ts: i64,
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
    /// Durability filter. Use `SLATEDB_DURABILITY_FILTER_*` constants.
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
    /// Durability filter. Use `SLATEDB_DURABILITY_FILTER_*` constants.
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

/// Reader options passed to `slatedb_db_reader_open`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_db_reader_options_t {
    /// How often to poll manifests/WAL for reader refreshes (milliseconds).
    pub manifest_poll_interval_ms: u64,
    /// Reader-owned checkpoint lifetime (milliseconds).
    pub checkpoint_lifetime_ms: u64,
    /// Maximum replay memtable size in bytes.
    pub max_memtable_bytes: u64,
    /// Whether to skip WAL replay entirely.
    pub skip_wal_replay: bool,
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
    /// TTL type. Use `SLATEDB_TTL_TYPE_*` constants.
    pub ttl_type: u8,
    /// TTL value in milliseconds when `ttl_type=SLATEDB_TTL_TYPE_EXPIRE_AFTER`.
    pub ttl_value: u64,
}

/// Merge options passed to merge operations.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_merge_options_t {
    /// TTL type. Use `SLATEDB_TTL_TYPE_*` constants.
    pub ttl_type: u8,
    /// TTL value in milliseconds when `ttl_type=SLATEDB_TTL_TYPE_EXPIRE_AFTER`.
    pub ttl_value: u64,
}

/// Flush options passed to `slatedb_db_flush_with_options`.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_flush_options_t {
    /// Flush type. Use `SLATEDB_FLUSH_TYPE_*` constants.
    pub flush_type: u8,
}

/// SST block size selector for builder config.
///
/// Use `SLATEDB_SST_BLOCK_SIZE_*` constants.
#[allow(non_camel_case_types)]
pub type slatedb_sst_block_size_t = u8;

/// Log level selector type for logging APIs.
///
/// Use `SLATEDB_LOG_LEVEL_*` constants.
#[allow(non_camel_case_types)]
pub type slatedb_log_level_t = u8;

/// Logging callback used by `slatedb_logging_set_callback`.
///
/// String arguments are UTF-8 byte slices represented as pointer + length.
/// Pointers are valid only for the duration of the callback.
#[allow(non_camel_case_types)]
pub type slatedb_log_callback_fn = Option<
    unsafe extern "C" fn(
        level: slatedb_log_level_t,
        target: *const c_char,
        target_len: usize,
        message: *const c_char,
        message_len: usize,
        module_path: *const c_char,
        module_path_len: usize,
        file: *const c_char,
        file_len: usize,
        line: u32,
        context: *mut c_void,
    ),
>;

/// Optional callback used to free logging context when replaced or cleared.
#[allow(non_camel_case_types)]
pub type slatedb_log_context_free_fn = Option<unsafe extern "C" fn(context: *mut c_void)>;

/// Merge operator callback used by `slatedb_db_builder_with_merge_operator`.
///
/// The callback receives key bytes, optional existing value bytes, and the new
/// merge operand bytes. It must set `*out_value`/`*out_value_len` to the merged
/// value bytes and return `true` on success.
///
/// `existing_value` is null and `existing_value_len` is 0 when
/// `has_existing_value` is false.
///
/// If this callback allocates `out_value`, provide a corresponding
/// `slatedb_merge_operator_result_free_fn` so Rust can release it after copying.
/// Do not require Rust to call `slatedb_bytes_free` for `out_value`.
#[allow(non_camel_case_types)]
pub type slatedb_merge_operator_fn = Option<
    unsafe extern "C" fn(
        key: *const u8,
        key_len: usize,
        existing_value: *const u8,
        existing_value_len: usize,
        has_existing_value: bool,
        operand: *const u8,
        operand_len: usize,
        out_value: *mut *mut u8,
        out_value_len: *mut usize,
        context: *mut c_void,
    ) -> bool,
>;

/// Optional callback used to free merge output returned by
/// `slatedb_merge_operator_fn`.
#[allow(non_camel_case_types)]
pub type slatedb_merge_operator_result_free_fn =
    Option<unsafe extern "C" fn(value: *mut u8, value_len: usize, context: *mut c_void)>;

/// Optional callback used to free merge operator context when the configured
/// merge operator is dropped.
#[allow(non_camel_case_types)]
pub type slatedb_merge_operator_context_free_fn =
    Option<unsafe extern "C" fn(context: *mut c_void)>;

/// C representation of a single range bound.
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_bound_t {
    /// Bound kind. Use `SLATEDB_BOUND_KIND_*` constants.
    pub kind: u8,
    /// Bound value for included/excluded bounds.
    pub data: *const c_void,
    /// Length of `data` if data is an array.
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

/// Builds a successful `slatedb_result_t` with no message payload.
pub(crate) fn success_result() -> slatedb_result_t {
    slatedb_result_t {
        kind: slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE,
        close_reason: slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        message: ptr::null_mut(),
    }
}

/// Builds an error result with an explicit close reason and message string.
///
/// The message is heap-allocated and must be released by calling
/// `slatedb_result_free` on the returned result.
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

/// Builds an error result with no close reason.
pub(crate) fn error_result(kind: slatedb_error_kind_t, message: &str) -> slatedb_result_t {
    error_result_with_close_reason(
        kind,
        slatedb_close_reason_t::SLATEDB_CLOSE_REASON_NONE,
        message,
    )
}

/// Converts an error message to a C string.
///
/// If the message contains an interior NUL byte, returns a generic fallback.
fn message_to_cstring(message: &str) -> CString {
    CString::new(message).unwrap_or_else(|_| CString::new("invalid error message").unwrap())
}

/// Maps a SlateDB close reason into the C ABI close-reason enum.
fn map_close_reason(reason: CloseReason) -> slatedb_close_reason_t {
    match reason {
        CloseReason::Clean => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_CLEAN,
        CloseReason::Fenced => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_FENCED,
        CloseReason::Panic => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_PANIC,
        _ => slatedb_close_reason_t::SLATEDB_CLOSE_REASON_UNKNOWN,
    }
}

/// Maps a SlateDB error into C ABI error kind and close reason fields.
///
/// Unknown future variants are mapped to `SLATEDB_ERROR_KIND_UNKNOWN`.
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

/// Converts a SlateDB error into a fully-populated C result object.
pub(crate) fn error_from_slate_error(err: &slatedb::Error) -> slatedb_result_t {
    let (kind, close_reason) = map_error(err);
    error_result_with_close_reason(kind, close_reason, &err.to_string())
}

/// Creates the Tokio runtime used by C ABI operations.
///
/// Returns a `slatedb_result_t` on failure so callers can propagate FFI errors
/// without panicking.
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

/// Converts a C string pointer to a UTF-8 Rust `String`.
///
/// ## Safety
/// - `ptr` must be non-null and point to a valid NUL-terminated C string.
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

/// Converts a raw pointer + length into a byte slice.
///
/// A zero length always returns an empty slice and allows a null pointer.
///
/// ## Safety
/// - When `len > 0`, `ptr` must be non-null and valid for reads of `len` bytes.
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

unsafe fn bytes_from_c_void<'a>(
    ptr: *const c_void,
    len: usize,
    field_name: &str,
) -> Result<&'a [u8], slatedb_result_t> {
    let raw_ptr = ptr as *const u8;
    bytes_from_ptr(raw_ptr, len, field_name)
}

unsafe fn u64_from_c_void(ptr: *const c_void, field_name: &str) -> Result<u64, slatedb_result_t> {
    let raw_ptr = ptr as *const u64;
    if raw_ptr.is_null() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("{field_name} pointer is null"),
        ));
    }
    Ok(*raw_ptr)
}

/// Validates an output pointer is non-null.
///
/// This helper standardizes error messages for required output pointers.
pub(crate) fn require_out_ptr<T>(ptr: *mut T, field_name: &str) -> Result<(), slatedb_result_t> {
    if ptr.is_null() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("{field_name} pointer is null"),
        ));
    }
    Ok(())
}

/// Validates a handle pointer is non-null.
///
/// This helper standardizes error messages for required opaque handles.
pub(crate) fn require_handle<T>(ptr: *const T, handle_name: &str) -> Result<(), slatedb_result_t> {
    if ptr.is_null() {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("invalid {handle_name} handle"),
        ));
    }
    Ok(())
}

/// Allocates a Rust-owned byte buffer and returns raw pointer + length for FFI.
///
/// The caller must free the returned pointer with `slatedb_bytes_free`.
pub(crate) fn alloc_bytes(bytes: &[u8]) -> (*mut u8, usize) {
    let boxed = bytes.to_vec().into_boxed_slice();
    let len = boxed.len();
    let data = Box::into_raw(boxed) as *mut u8;
    (data, len)
}

/// Converts a C durability selector to `DurabilityLevel`.
pub(crate) fn durability_level_from_u8(
    durability_filter: u8,
) -> Result<DurabilityLevel, slatedb_result_t> {
    match durability_filter {
        SLATEDB_DURABILITY_FILTER_MEMORY => Ok(DurabilityLevel::Memory),
        SLATEDB_DURABILITY_FILTER_REMOTE => Ok(DurabilityLevel::Remote),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid durability_filter (use SLATEDB_DURABILITY_FILTER_* constants)",
        )),
    }
}

/// Converts optional C read options into Rust `ReadOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_read_options_t`.
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

/// Converts optional C scan options into Rust `ScanOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_scan_options_t`.
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

/// Converts optional C reader options into Rust `DbReaderOptions`.
///
/// A null pointer uses defaults. Zero-valued numeric fields also use defaults
/// so callers can override selected fields without fully populating the struct.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_db_reader_options_t`.
pub(crate) unsafe fn db_reader_options_from_ptr(
    ptr: *const slatedb_db_reader_options_t,
) -> DbReaderOptions {
    let default_options = DbReaderOptions::default();
    if ptr.is_null() {
        return default_options;
    }

    let options = &*ptr;
    let manifest_poll_interval_ms = if options.manifest_poll_interval_ms == 0 {
        default_options.manifest_poll_interval.as_millis() as u64
    } else {
        options.manifest_poll_interval_ms
    };
    let checkpoint_lifetime_ms = if options.checkpoint_lifetime_ms == 0 {
        default_options.checkpoint_lifetime.as_millis() as u64
    } else {
        options.checkpoint_lifetime_ms
    };

    DbReaderOptions {
        manifest_poll_interval: Duration::from_millis(manifest_poll_interval_ms),
        checkpoint_lifetime: Duration::from_millis(checkpoint_lifetime_ms),
        max_memtable_bytes: if options.max_memtable_bytes == 0 {
            default_options.max_memtable_bytes
        } else {
            options.max_memtable_bytes
        },
        skip_wal_replay: options.skip_wal_replay,
        ..default_options
    }
}

/// Converts optional C write options into Rust `WriteOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_write_options_t`.
pub(crate) unsafe fn write_options_from_ptr(ptr: *const slatedb_write_options_t) -> WriteOptions {
    if ptr.is_null() {
        return WriteOptions::default();
    }

    let options = &*ptr;
    WriteOptions {
        await_durable: options.await_durable,
    }
}

/// Executes a closure with a mutable `WriteBatch` from an opaque handle.
///
/// Returns an invalid-handle error when `write_batch` is null and a consumed
/// error when the handle no longer owns a batch.
///
/// ## Safety
/// - `write_batch` must be either null or a valid pointer to a
///   `slatedb_write_batch_t` created by this crate.
pub(crate) unsafe fn with_write_batch_mut<R>(
    write_batch: *mut slatedb_write_batch_t,
    f: impl FnOnce(&mut WriteBatch) -> R,
) -> Result<R, slatedb_result_t> {
    require_handle(write_batch, "write batch")?;
    let handle = &mut *write_batch;
    let Some(batch) = handle.batch.as_mut() else {
        return Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        ));
    };
    Ok(f(batch))
}

/// Takes ownership of a `WriteBatch` from an opaque handle.
///
/// Returns an invalid-handle error when `write_batch` is null and a consumed
/// error when the handle no longer owns a batch.
///
/// ## Safety
/// - `write_batch` must be either null or a valid pointer to a
///   `slatedb_write_batch_t` created by this crate.
pub(crate) unsafe fn take_write_batch(
    write_batch: *mut slatedb_write_batch_t,
) -> Result<WriteBatch, slatedb_result_t> {
    require_handle(write_batch, "write batch")?;
    let handle = &mut *write_batch;
    handle.batch.take().ok_or_else(|| {
        error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "write batch has been consumed",
        )
    })
}

/// Converts a TTL selector + value into a Rust `Ttl` variant.
fn ttl_from_parts(ttl_type: u8, ttl_value: u64) -> Result<Ttl, slatedb_result_t> {
    match ttl_type {
        SLATEDB_TTL_TYPE_DEFAULT => Ok(Ttl::Default),
        SLATEDB_TTL_TYPE_NO_EXPIRY => Ok(Ttl::NoExpiry),
        SLATEDB_TTL_TYPE_EXPIRE_AFTER => Ok(Ttl::ExpireAfter(ttl_value)),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid ttl_type (use SLATEDB_TTL_TYPE_* constants)",
        )),
    }
}

/// Converts optional C put options into Rust `PutOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_put_options_t`.
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

/// Converts optional C merge options into Rust `MergeOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_merge_options_t`.
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

/// Converts optional C flush options into Rust `FlushOptions`.
///
/// A null pointer uses defaults.
///
/// ## Safety
/// - If `ptr` is non-null, it must point to a valid `slatedb_flush_options_t`.
pub(crate) unsafe fn flush_options_from_ptr(
    ptr: *const slatedb_flush_options_t,
) -> Result<FlushOptions, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(FlushOptions::default());
    }

    let options = &*ptr;
    let flush_type = match options.flush_type {
        SLATEDB_FLUSH_TYPE_MEMTABLE => FlushType::MemTable,
        SLATEDB_FLUSH_TYPE_WAL => FlushType::Wal,
        _ => {
            return Err(error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "invalid flush_type (use SLATEDB_FLUSH_TYPE_* constants)",
            ));
        }
    };

    Ok(FlushOptions { flush_type })
}

/// Converts a C block-size selector into `SstBlockSize`.
pub(crate) fn sst_block_size_from_u8(
    value: slatedb_sst_block_size_t,
) -> Result<SstBlockSize, slatedb_result_t> {
    match value {
        SLATEDB_SST_BLOCK_SIZE_1KIB => Ok(SstBlockSize::Block1Kib),
        SLATEDB_SST_BLOCK_SIZE_2KIB => Ok(SstBlockSize::Block2Kib),
        SLATEDB_SST_BLOCK_SIZE_4KIB => Ok(SstBlockSize::Block4Kib),
        SLATEDB_SST_BLOCK_SIZE_8KIB => Ok(SstBlockSize::Block8Kib),
        SLATEDB_SST_BLOCK_SIZE_16KIB => Ok(SstBlockSize::Block16Kib),
        SLATEDB_SST_BLOCK_SIZE_32KIB => Ok(SstBlockSize::Block32Kib),
        SLATEDB_SST_BLOCK_SIZE_64KIB => Ok(SstBlockSize::Block64Kib),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid sst_block_size (use SLATEDB_SST_BLOCK_SIZE_* constants)",
        )),
    }
}

/// Converts a C range struct into Rust `Bound<Bytes>` tuple.
///
/// ## Safety
/// - `range.start`/`range.end` pointers must be valid when their bound kinds
///   require payload bytes.
pub(crate) unsafe fn bytes_range_from_c(
    range: slatedb_range_t,
) -> Result<(Bound<Bytes>, Bound<Bytes>), slatedb_result_t> {
    Ok((
        bytes_bound_from_c(range.start, "start")?,
        bytes_bound_from_c(range.end, "end")?,
    ))
}

/// Converts one C range bound into a Rust `Bound<Bytes>`.
///
/// ## Safety
/// - If `bound.kind` is included/excluded, `bound.data` must be readable for
///   `bound.len` bytes.
unsafe fn bytes_bound_from_c(
    bound: slatedb_bound_t,
    name: &str,
) -> Result<Bound<Bytes>, slatedb_result_t> {
    match bound.kind {
        SLATEDB_BOUND_KIND_UNBOUNDED => Ok(Bound::Unbounded),
        SLATEDB_BOUND_KIND_INCLUDED => {
            let bytes = bytes_from_c_void(bound.data, bound.len, name)?;
            Ok(Bound::Included(Bytes::copy_from_slice(bytes)))
        }
        SLATEDB_BOUND_KIND_EXCLUDED => {
            let bytes = bytes_from_c_void(bound.data, bound.len, name)?;
            Ok(Bound::Excluded(Bytes::copy_from_slice(bytes)))
        }
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid bound kind (use SLATEDB_BOUND_KIND_* constants)",
        )),
    }
}

/// Converts one C range bound into a Rust `Bound<u64>`.
///
/// ## Safety
/// - If `bound.kind` is included/excluded, `bound.data` must be a pointer to u64 value.
unsafe fn u64_bound_from_c(
    bound: slatedb_bound_t,
    name: &str,
) -> Result<Bound<u64>, slatedb_result_t> {
    match bound.kind {
        SLATEDB_BOUND_KIND_UNBOUNDED => Ok(Bound::Unbounded),
        SLATEDB_BOUND_KIND_INCLUDED => Ok(Bound::Included(u64_from_c_void(bound.data, name)?)),
        SLATEDB_BOUND_KIND_EXCLUDED => Ok(Bound::Excluded(u64_from_c_void(bound.data, name)?)),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid bound kind (use SLATEDB_BOUND_KIND_* constants)",
        )),
    }
}

/// Converts a C range struct into a Rust `Bound<u64>` tuple.
///
/// ## Safety
/// - `range.start`/`range.end` pointers must be valid when their bound kinds require data.
pub(crate) unsafe fn u64_range_from_c(
    range: slatedb_range_t,
) -> Result<(Bound<u64>, Bound<u64>), slatedb_result_t> {
    Ok((
        u64_bound_from_c(range.start, "start")?,
        u64_bound_from_c(range.end, "end")?,
    ))
}

/// Validates a write key against SlateDB key constraints.
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

/// Validates write key and value sizes against SlateDB constraints.
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
