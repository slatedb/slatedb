//! Object store handle APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for resolving and freeing object store
//! handles used by `slatedb_db_open` and builder APIs.

use crate::ffi::{
    cstr_to_string, error_from_slate_error, error_result, require_handle, require_out_ptr,
    slatedb_error_kind_t, slatedb_object_store_t, slatedb_result_t, success_result,
};
use slatedb::{admin, Db};
use std::os::raw::c_char;

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
pub unsafe extern "C" fn slatedb_object_store_from_url(
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
        Err(err) => error_from_slate_error(&err),
    }
}

/// Resolves an object store from environment variables and returns an opaque
/// handle.
///
/// Provider configuration follows `slatedb::admin::load_object_store_from_env`.
///
/// ## Arguments
/// - `env_file`: Optional null-terminated UTF-8 path to a `.env` file. Pass
///   null or empty string to use default `.env` loading behavior.
/// - `out_object_store`: Output pointer populated with a newly allocated
///   `slatedb_object_store_t*` on success.
///
/// ## Returns
/// - `slatedb_result_t` with `kind == SLATEDB_ERROR_KIND_NONE` on success.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/UTF-8 and
///   invalid environment configuration.
///
/// ## Safety
/// - `env_file` must be null or a valid null-terminated C string.
/// - `out_object_store` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_object_store_from_env(
    env_file: *const c_char,
    out_object_store: *mut *mut slatedb_object_store_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_object_store, "out_object_store") {
        return err;
    }

    let env_file = match optional_cstr_to_string(env_file, "env_file") {
        Ok(env_file) => env_file,
        Err(err) => return err,
    };

    match admin::load_object_store_from_env(env_file) {
        Ok(object_store) => {
            let handle = Box::new(slatedb_object_store_t { object_store });
            *out_object_store = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("failed to resolve object store from environment: {err}"),
        ),
    }
}

/// Closes and frees an object store handle previously returned by
/// `slatedb_object_store_from_url` or `slatedb_object_store_from_env`.
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

unsafe fn optional_cstr_to_string(
    ptr: *const c_char,
    field_name: &str,
) -> Result<Option<String>, slatedb_result_t> {
    if ptr.is_null() {
        return Ok(None);
    }

    let value = cstr_to_string(ptr, field_name)?;
    let value = value.trim();
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_owned()))
    }
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
    fn test_object_store_from_url_requires_out_pointer() {
        let url = CString::new("memory:///").expect("CString failed");
        assert_result_kind(
            unsafe { slatedb_object_store_from_url(url.as_ptr(), std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }

    #[test]
    fn test_object_store_from_env_requires_out_pointer() {
        assert_result_kind(
            unsafe { slatedb_object_store_from_env(std::ptr::null(), std::ptr::null_mut()) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
    }

    #[test]
    fn test_object_store_from_env_rejects_invalid_utf8_env_file() {
        let invalid_utf8 = [0xFF_u8, 0];
        let mut object_store: *mut slatedb_object_store_t = std::ptr::null_mut();
        assert_result_kind(
            unsafe {
                slatedb_object_store_from_env(
                    invalid_utf8.as_ptr() as *const std::os::raw::c_char,
                    &mut object_store,
                )
            },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert!(object_store.is_null());
    }
}
