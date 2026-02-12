use slatedb::bytes::Bytes;
use slatedb::config::{
    DurabilityLevel, FlushOptions, FlushType, MergeOptions, PutOptions, ReadOptions, ScanOptions,
    SstBlockSize, Ttl, WriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::{Db, DbBuilder, DbIterator, ErrorKind, WriteBatch};
use std::ffi::{CStr, CString};
use std::ops::Bound;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

#[allow(non_camel_case_types)]
pub struct slatedb_object_store_t {
    pub object_store: Arc<dyn ObjectStore>,
}

#[allow(non_camel_case_types)]
pub struct slatedb_db_builder_t {
    pub builder: Option<DbBuilder<String>>,
}

#[allow(non_camel_case_types)]
pub struct slatedb_db_t {
    pub runtime: Arc<Runtime>,
    pub db: Db,
}

#[allow(non_camel_case_types)]
pub struct slatedb_iterator_t {
    pub runtime: Arc<Runtime>,
    pub iter: DbIterator,
}

#[allow(non_camel_case_types)]
pub struct slatedb_write_batch_t {
    pub batch: Option<WriteBatch>,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum slatedb_error_t {
    SLATEDB_SUCCESS = 0,
    SLATEDB_INVALID_ARGUMENT = 1,
    SLATEDB_NULL_POINTER = 2,
    SLATEDB_INVALID_HANDLE = 3,
    SLATEDB_TRANSACTION = 4,
    SLATEDB_CLOSED = 5,
    SLATEDB_UNAVAILABLE = 6,
    SLATEDB_INVALID = 7,
    SLATEDB_DATA = 8,
    SLATEDB_INTERNAL = 9,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct slatedb_result_t {
    pub error: slatedb_error_t,
    pub none: bool,
    pub message: *mut c_char,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_value_t {
    pub data: *mut u8,
    pub len: usize,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_key_value_t {
    pub key: slatedb_value_t,
    pub value: slatedb_value_t,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_read_options_t {
    pub durability_filter: u8,
    pub dirty: bool,
    pub cache_blocks: bool,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_scan_options_t {
    pub durability_filter: u8,
    pub dirty: bool,
    pub read_ahead_bytes: u64,
    pub cache_blocks: bool,
    pub max_fetch_tasks: u64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_write_options_t {
    pub await_durable: bool,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_put_options_t {
    pub ttl_type: u8,
    pub ttl_value: u64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_merge_options_t {
    pub ttl_type: u8,
    pub ttl_value: u64,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_flush_options_t {
    pub flush_type: u8,
}

#[allow(non_camel_case_types)]
pub type slatedb_sst_block_size_t = u8;

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_bound_t {
    pub kind: u8,
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, Copy)]
pub struct slatedb_range_t {
    pub start: slatedb_bound_t,
    pub end: slatedb_bound_t,
}

pub(crate) fn success_result() -> slatedb_result_t {
    slatedb_result_t {
        error: slatedb_error_t::SLATEDB_SUCCESS,
        none: false,
        message: ptr::null_mut(),
    }
}

pub(crate) fn none_result() -> slatedb_result_t {
    slatedb_result_t {
        error: slatedb_error_t::SLATEDB_SUCCESS,
        none: true,
        message: ptr::null_mut(),
    }
}

pub(crate) fn error_result(error: slatedb_error_t, message: &str) -> slatedb_result_t {
    slatedb_result_t {
        error,
        none: false,
        message: message_to_cstring(message).into_raw(),
    }
}

fn message_to_cstring(message: &str) -> CString {
    CString::new(message).unwrap_or_else(|_| CString::new("invalid error message").unwrap())
}

pub(crate) fn map_error(err: &slatedb::Error) -> slatedb_error_t {
    match err.kind() {
        ErrorKind::Transaction => slatedb_error_t::SLATEDB_TRANSACTION,
        ErrorKind::Closed(_) => slatedb_error_t::SLATEDB_CLOSED,
        ErrorKind::Unavailable => slatedb_error_t::SLATEDB_UNAVAILABLE,
        ErrorKind::Invalid => slatedb_error_t::SLATEDB_INVALID,
        ErrorKind::Data => slatedb_error_t::SLATEDB_DATA,
        ErrorKind::Internal => slatedb_error_t::SLATEDB_INTERNAL,
        _ => slatedb_error_t::SLATEDB_INTERNAL,
    }
}

pub(crate) fn create_runtime() -> Result<Arc<Runtime>, slatedb_result_t> {
    RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .map(Arc::new)
        .map_err(|e| {
            error_result(
                slatedb_error_t::SLATEDB_INTERNAL,
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
            slatedb_error_t::SLATEDB_NULL_POINTER,
            &format!("{field_name} pointer is null"),
        ));
    }

    let cstr = CStr::from_ptr(ptr);
    cstr.to_str().map(|s| s.to_owned()).map_err(|_| {
        error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
            slatedb_error_t::SLATEDB_NULL_POINTER,
            &format!("{field_name} pointer is null"),
        ));
    }

    Ok(std::slice::from_raw_parts(ptr, len))
}

pub(crate) fn bytes_to_value(bytes: &[u8]) -> slatedb_value_t {
    let boxed = bytes.to_vec().into_boxed_slice();
    let len = boxed.len();
    let data = Box::into_raw(boxed) as *mut u8;
    slatedb_value_t { data, len }
}

pub(crate) fn durability_level_from_u8(
    durability_filter: u8,
) -> Result<DurabilityLevel, slatedb_result_t> {
    match durability_filter {
        0 => Ok(DurabilityLevel::Memory),
        1 => Ok(DurabilityLevel::Remote),
        _ => Err(error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
            "read_ahead_bytes does not fit in usize",
        )
    })?;
    let max_fetch_tasks = usize::try_from(options.max_fetch_tasks).map_err(|_| {
        error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
                slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
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
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
            "invalid bound kind (expected 0=Unbounded, 1=Included, 2=Excluded)",
        )),
    }
}

pub(crate) fn validate_write_key(key: &[u8]) -> Result<(), slatedb_result_t> {
    if key.is_empty() {
        return Err(error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
            "key cannot be empty",
        ));
    }
    if key.len() > u16::MAX as usize {
        return Err(error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
            "key size must be <= u16::MAX",
        ));
    }
    Ok(())
}

pub(crate) fn validate_write_key_value(key: &[u8], value: &[u8]) -> Result<(), slatedb_result_t> {
    validate_write_key(key)?;
    if value.len() > u32::MAX as usize {
        return Err(error_result(
            slatedb_error_t::SLATEDB_INVALID_ARGUMENT,
            "value size must be <= u32::MAX",
        ));
    }
    Ok(())
}
