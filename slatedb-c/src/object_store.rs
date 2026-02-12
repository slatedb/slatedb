//! Object store handle APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for resolving and freeing object store
//! handles used by `slatedb_db_open` and builder APIs.

use crate::ffi::{
    cstr_to_string, error_from_slate_error, require_handle, require_out_ptr,
    slatedb_object_store_t, slatedb_result_t, success_result,
};
use slatedb::Db;

/// Resolves an object store from a URL and returns an opaque handle.
///
/// ## Arguments
/// - `url`: Null-terminated UTF-8 URL string (for example `file:///tmp/db`).
/// - `out_object_store`: Output pointer populated with a newly allocated
///   `slatedb_object_store_t*` on success.
///
/// ## Returns
/// - `slatedb_result_t` with `kind == SLATEDB_ERROR_KIND_NONE` on success.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid UTF-8.
/// - Returns mapped SlateDB error kinds when URL resolution fails.
///
/// ## Safety
/// - `url` must be a valid null-terminated C string.
/// - `out_object_store` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_resolve_object_store(
    url: *const std::os::raw::c_char,
    out_object_store: *mut *mut slatedb_object_store_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_object_store, "out_object_store") {
        return err;
    }

    let url = match cstr_to_string(url, "url") {
        Ok(url) => url,
        Err(err) => return err,
    };

    match Db::resolve_object_store(&url) {
        Ok(object_store) => {
            let handle = Box::new(slatedb_object_store_t { object_store });
            *out_object_store = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err, &format!("failed to resolve object store: {err}")),
    }
}

/// Closes and frees an object store handle previously returned by
/// `slatedb_db_resolve_object_store`.
///
/// ## Arguments
/// - `object_store`: Opaque object store handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating whether close succeeded.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` if `object_store` is null.
///
/// ## Safety
/// - `object_store` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_object_store_close(
    object_store: *mut slatedb_object_store_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(object_store, "object store") {
        return err;
    }

    let _ = Box::from_raw(object_store);

    success_result()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::slatedb_error_kind_t;
    use std::ffi::{CStr, CString};

    fn assert_result_kind(result: slatedb_result_t, expected: slatedb_error_kind_t) {
        let kind = result.kind;
        let message = if result.message.is_null() {
            String::new()
        } else {
            unsafe {
                CStr::from_ptr(result.message)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        crate::memory::slatedb_result_free(result);
        assert_eq!(
            kind, expected,
            "unexpected result kind with message: {message}"
        );
    }

    #[test]
    fn test_object_store_close_rejects_null_handle() {
        assert_result_kind(
            unsafe { slatedb_object_store_close(std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }

    #[test]
    fn test_db_resolve_object_store_requires_out_pointer() {
        let url = CString::new("memory:///").expect("CString failed");
        assert_result_kind(
            unsafe { slatedb_db_resolve_object_store(url.as_ptr(), std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }

    #[test]
    fn test_db_resolve_object_store_rejects_invalid_utf8_url() {
        let invalid_utf8 = [0xFF_u8, 0];
        let mut object_store: *mut slatedb_object_store_t = std::ptr::null_mut();
        assert_result_kind(
            unsafe {
                slatedb_db_resolve_object_store(
                    invalid_utf8.as_ptr() as *const std::os::raw::c_char,
                    &mut object_store,
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert!(object_store.is_null());
    }
}
