#![allow(dead_code)]

use slatedb_c::{
    slatedb_bound_t, slatedb_bytes_free, slatedb_db_close, slatedb_db_open,
    slatedb_db_resolve_object_store, slatedb_error_kind_t, slatedb_object_store_close,
    slatedb_object_store_t, slatedb_range_t, slatedb_result_free, slatedb_result_t,
    SLATEDB_BOUND_KIND_UNBOUNDED,
};
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn cstring(input: &str) -> CString {
    CString::new(input).expect("failed to create CString")
}

pub(crate) fn unique_temp_dir(prefix: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "slatedb-c-{prefix}-pid{}-{now}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).expect("failed to create temp dir");
    path
}

pub(crate) fn file_url_for_path(path: &Path) -> CString {
    cstring(&format!("file://{}", path.display()))
}

pub(crate) fn cleanup_temp_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

pub(crate) fn assert_result_ok(result: slatedb_result_t) {
    let message = result_message(&result);
    assert_eq!(
        result.kind,
        slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE,
        "expected success result, got kind={:?}, message={:?}",
        result.kind,
        message
    );
    slatedb_result_free(result);
}

pub(crate) fn assert_result_invalid_contains(result: slatedb_result_t, message_substring: &str) {
    let message = result_message(&result);
    assert_eq!(
        result.kind,
        slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        "expected invalid result, got kind={:?}, message={:?}",
        result.kind,
        message
    );
    let message = message.expect("expected error message for invalid result");
    assert!(
        message.contains(message_substring),
        "expected error message to contain {:?}, got {:?}",
        message_substring,
        message
    );
    slatedb_result_free(result);
}

fn result_message(result: &slatedb_result_t) -> Option<String> {
    if result.message.is_null() {
        None
    } else {
        let message = unsafe { CStr::from_ptr(result.message) };
        Some(message.to_string_lossy().into_owned())
    }
}

pub(crate) unsafe fn resolve_object_store(url: &CString) -> *mut slatedb_object_store_t {
    let mut object_store = ptr::null_mut();
    assert_result_ok(slatedb_db_resolve_object_store(
        url.as_ptr(),
        &mut object_store,
    ));
    assert!(
        !object_store.is_null(),
        "object store handle should not be null"
    );
    object_store
}

pub(crate) unsafe fn open_db(
    db_path: &CString,
    object_store: *const slatedb_object_store_t,
) -> *mut slatedb_c::slatedb_db_t {
    let mut db = ptr::null_mut();
    assert_result_ok(slatedb_db_open(db_path.as_ptr(), object_store, &mut db));
    assert!(!db.is_null(), "db handle should not be null");
    db
}

pub(crate) unsafe fn close_db_if_not_null(db: *mut slatedb_c::slatedb_db_t) {
    if !db.is_null() {
        assert_result_ok(slatedb_db_close(db));
    }
}

pub(crate) unsafe fn close_object_store_if_not_null(object_store: *mut slatedb_object_store_t) {
    if !object_store.is_null() {
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

pub(crate) unsafe fn take_bytes(ptr: *mut u8, len: usize) -> Vec<u8> {
    if ptr.is_null() {
        return Vec::new();
    }
    let bytes = std::slice::from_raw_parts(ptr as *const u8, len).to_vec();
    slatedb_bytes_free(ptr, len);
    bytes
}

pub(crate) fn unbounded_range() -> slatedb_range_t {
    slatedb_range_t {
        start: slatedb_bound_t {
            kind: SLATEDB_BOUND_KIND_UNBOUNDED,
            data: ptr::null(),
            len: 0,
        },
        end: slatedb_bound_t {
            kind: SLATEDB_BOUND_KIND_UNBOUNDED,
            data: ptr::null(),
            len: 0,
        },
    }
}
