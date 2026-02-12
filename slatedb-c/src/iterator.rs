use crate::ffi::{
    alloc_bytes, bytes_from_ptr, error_result, map_error, slatedb_error_t, slatedb_iterator_t,
    slatedb_result_t, success_result,
};

#[no_mangle]
pub unsafe extern "C" fn slatedb_iterator_next(
    iterator: *mut slatedb_iterator_t,
    out_has_item: *mut bool,
    out_key: *mut *mut u8,
    out_key_len: *mut usize,
    out_val: *mut *mut u8,
    out_val_len: *mut usize,
) -> slatedb_result_t {
    if iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid iterator handle",
        );
    }
    if out_key.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_key pointer is null",
        );
    }
    if out_key_len.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_key_len pointer is null",
        );
    }
    if out_val.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_val pointer is null",
        );
    }
    if out_val_len.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_val_len pointer is null",
        );
    }
    if out_has_item.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_has_item pointer is null",
        );
    }

    *out_has_item = false;
    *out_key = std::ptr::null_mut();
    *out_key_len = 0;
    *out_val = std::ptr::null_mut();
    *out_val_len = 0;

    let handle = &mut *iterator;
    match handle.runtime.block_on(handle.iter.next()) {
        Ok(Some(kv)) => {
            let (key, key_len) = alloc_bytes(kv.key.as_ref());
            let (val, val_len) = alloc_bytes(kv.value.as_ref());
            *out_key = key;
            *out_key_len = key_len;
            *out_val = val;
            *out_val_len = val_len;
            *out_has_item = true;
            success_result()
        }
        Ok(None) => {
            *out_has_item = false;
            success_result()
        }
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
