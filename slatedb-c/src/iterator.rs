use crate::ffi::{
    bytes_from_ptr, bytes_to_value, error_result, map_error, none_result, slatedb_error_t,
    slatedb_iterator_t, slatedb_key_value_t, slatedb_result_t, success_result,
};

#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next(
    iterator: *mut slatedb_iterator_t,
    out_key_value: *mut slatedb_key_value_t,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid iterator handle",
        );
    }
    if out_key_value.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_key_value pointer is null",
        );
    }

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next()) {
        Ok(Some(kv)) => {
            (*out_key_value).key = bytes_to_value(kv.key.as_ref());
            (*out_key_value).value = bytes_to_value(kv.value.as_ref());
            success_result()
        }
        Ok(None) => none_result(),
        Err(err) => error_result(map_error(&err), &format!("iterator next failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_seek(
    iterator: *mut slatedb_iterator_t,
    key: *const u8,
    key_len: usize,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid iterator handle",
        );
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.seek(key)) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("iterator seek failed: {err}")),
    }
}

#[no_mangle]
pub extern "C" fn slatedb_iterator_close(iterator: *mut slatedb_iterator_t) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid iterator handle",
        );
    }

    unsafe {
        let _ = Box::from_raw(iterator);
    }

    success_result()
}
