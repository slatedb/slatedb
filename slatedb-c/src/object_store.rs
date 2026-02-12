use crate::ffi::{
    cstr_to_string, error_result, map_error, slatedb_error_t, slatedb_object_store_t,
    slatedb_result_t, success_result,
};
use slatedb::Db;

/// Resolve an object store using `Db::resolve_object_store`.
///
/// # Safety
/// - `url` must be a valid, null-terminated C string.
/// - `out_object_store` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_resolve_object_store(
    url: *const std::os::raw::c_char,
    out_object_store: *mut *mut slatedb_object_store_t,
) -> slatedb_result_t {
    if out_object_store.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_object_store pointer is null",
        );
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
        Err(err) => {
            let code = map_error(&err);
            error_result(code, &format!("failed to resolve object store: {err}"))
        }
    }
}

#[no_mangle]
pub extern "C" fn slatedb_object_store_close(
    object_store: *mut slatedb_object_store_t,
) -> slatedb_result_t {
    if object_store.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid object store handle",
        );
    }

    unsafe {
        let _ = Box::from_raw(object_store);
    }

    success_result()
}
