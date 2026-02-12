use crate::ffi::{
    bytes_from_ptr, bytes_to_value, create_runtime, cstr_to_string, error_result,
    flush_options_from_ptr, map_error, merge_options_from_ptr, none_result, put_options_from_ptr,
    range_from_c, read_options_from_ptr, scan_options_from_ptr, slatedb_db_builder_t, slatedb_db_t,
    slatedb_error_t, slatedb_flush_options_t, slatedb_iterator_t, slatedb_merge_options_t,
    slatedb_object_store_t, slatedb_put_options_t, slatedb_range_t, slatedb_read_options_t,
    slatedb_result_t, slatedb_scan_options_t, slatedb_sst_block_size_t, slatedb_value_t,
    slatedb_write_batch_t, slatedb_write_options_t, sst_block_size_from_u8, success_result,
    validate_write_key, validate_write_key_value, write_options_from_ptr,
};
use slatedb::Db;

/// # Safety
/// - `path` must be a valid C string.
/// - `object_store` must be a valid object store handle.
/// - `out_db` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_open(
    path: *const std::os::raw::c_char,
    object_store: *const slatedb_object_store_t,
    out_db: *mut *mut slatedb_db_t,
) -> slatedb_result_t {
    if out_db.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_db pointer is null",
        );
    }
    if object_store.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid object_store handle",
        );
    }

    let path = match cstr_to_string(path, "path") {
        Ok(path) => path,
        Err(err) => return err,
    };

    let runtime = match create_runtime() {
        Ok(runtime) => runtime,
        Err(err) => return err,
    };

    let object_store = (&*object_store).object_store.clone();
    match runtime.block_on(Db::open(path, object_store)) {
        Ok(db) => {
            let handle = Box::new(slatedb_db_t { runtime, db });
            *out_db = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_result(map_error(&err), &format!("db open failed: {err}")),
    }
}

/// # Safety
/// - `path` must be a valid C string.
/// - `object_store` must be a valid object store handle.
/// - `out_builder` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_new(
    path: *const std::os::raw::c_char,
    object_store: *const slatedb_object_store_t,
    out_builder: *mut *mut slatedb_db_builder_t,
) -> slatedb_result_t {
    if out_builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_builder pointer is null",
        );
    }
    if object_store.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid object_store handle",
        );
    }

    let path = match cstr_to_string(path, "path") {
        Ok(path) => path,
        Err(err) => return err,
    };

    let object_store = (&*object_store).object_store.clone();
    let builder = Db::builder(path, object_store);

    let handle = Box::new(slatedb_db_builder_t {
        builder: Some(builder),
    });
    *out_builder = Box::into_raw(handle);
    success_result()
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_wal_object_store(
    builder: *mut slatedb_db_builder_t,
    wal_object_store: *const slatedb_object_store_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid builder handle",
        );
    }
    if wal_object_store.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid wal_object_store handle",
        );
    }

    let handle = &mut *builder;
    let Some(current) = handle.builder.take() else {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "builder has been consumed",
        );
    };

    let wal_object_store = (&*wal_object_store).object_store.clone();
    handle.builder = Some(current.with_wal_object_store(wal_object_store));
    success_result()
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_seed(
    builder: *mut slatedb_db_builder_t,
    seed: u64,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid builder handle",
        );
    }

    let handle = &mut *builder;
    let Some(current) = handle.builder.take() else {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "builder has been consumed",
        );
    };

    handle.builder = Some(current.with_seed(seed));
    success_result()
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_sst_block_size(
    builder: *mut slatedb_db_builder_t,
    sst_block_size: slatedb_sst_block_size_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid builder handle",
        );
    }

    let block_size = match sst_block_size_from_u8(sst_block_size) {
        Ok(block_size) => block_size,
        Err(err) => return err,
    };

    let handle = &mut *builder;
    let Some(current) = handle.builder.take() else {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "builder has been consumed",
        );
    };

    handle.builder = Some(current.with_sst_block_size(block_size));
    success_result()
}

/// Consumes the builder, matching `DbBuilder::build(self)` semantics.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_build(
    builder: *mut slatedb_db_builder_t,
    out_db: *mut *mut slatedb_db_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid builder handle",
        );
    }
    if out_db.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_db pointer is null",
        );
    }

    let mut builder_handle = Box::from_raw(builder);
    let Some(builder) = builder_handle.builder.take() else {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "builder has been consumed",
        );
    };

    let runtime = match create_runtime() {
        Ok(runtime) => runtime,
        Err(err) => return err,
    };

    match runtime.block_on(builder.build()) {
        Ok(db) => {
            let db_handle = Box::new(slatedb_db_t { runtime, db });
            *out_db = Box::into_raw(db_handle);
            success_result()
        }
        Err(err) => error_result(map_error(&err), &format!("builder build failed: {err}")),
    }
}

#[no_mangle]
pub extern "C" fn slatedb_db_builder_close(builder: *mut slatedb_db_builder_t) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid builder handle",
        );
    }

    unsafe {
        let _ = Box::from_raw(builder);
    }

    success_result()
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_status(db: *const slatedb_db_t) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let handle = &*db;
    match handle.db.status() {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db status failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_get(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    out_value: *mut slatedb_value_t,
) -> slatedb_result_t {
    slatedb_db_get_with_options(db, key, key_len, std::ptr::null(), out_value)
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_get_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    read_options: *const slatedb_read_options_t,
    out_value: *mut slatedb_value_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }
    if out_value.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_value pointer is null",
        );
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };

    let read_options = match read_options_from_ptr(read_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.get_with_options(key, &read_options))
    {
        Ok(Some(value)) => {
            *out_value = bytes_to_value(value.as_ref());
            success_result()
        }
        Ok(None) => {
            (*out_value).data = std::ptr::null_mut();
            (*out_value).len = 0;
            none_result()
        }
        Err(err) => error_result(map_error(&err), &format!("db get failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_put(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.put(key, value)) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db put failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_put_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    put_options: *const slatedb_put_options_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let put_options = match put_options_from_ptr(put_options) {
        Ok(options) => options,
        Err(err) => return err,
    };
    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.put_with_options(
        key,
        value,
        &put_options,
        &write_options,
    )) {
        Ok(()) => success_result(),
        Err(err) => error_result(
            map_error(&err),
            &format!("db put_with_options failed: {err}"),
        ),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_delete(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.delete(key)) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db delete failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_delete_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    if let Err(err) = validate_write_key(key) {
        return err;
    }

    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.delete_with_options(key, &write_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_result(
            map_error(&err),
            &format!("db delete_with_options failed: {err}"),
        ),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_merge(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.merge(key, value)) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db merge failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_merge_with_options(
    db: *mut slatedb_db_t,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
    merge_options: *const slatedb_merge_options_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let key = match bytes_from_ptr(key, key_len, "key") {
        Ok(key) => key,
        Err(err) => return err,
    };
    let value = match bytes_from_ptr(value, value_len, "value") {
        Ok(value) => value,
        Err(err) => return err,
    };

    if let Err(err) = validate_write_key_value(key, value) {
        return err;
    }

    let merge_options = match merge_options_from_ptr(merge_options) {
        Ok(options) => options,
        Err(err) => return err,
    };
    let write_options = write_options_from_ptr(write_options);

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.merge_with_options(
        key,
        value,
        &merge_options,
        &write_options,
    )) {
        Ok(()) => success_result(),
        Err(err) => error_result(
            map_error(&err),
            &format!("db merge_with_options failed: {err}"),
        ),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_write(
    db: *mut slatedb_db_t,
    write_batch: *mut slatedb_write_batch_t,
) -> slatedb_result_t {
    slatedb_db_write_with_options(db, write_batch, std::ptr::null())
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_write_with_options(
    db: *mut slatedb_db_t,
    write_batch: *mut slatedb_write_batch_t,
    write_options: *const slatedb_write_options_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }
    if write_batch.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "invalid write_batch handle",
        );
    }

    let write_options = write_options_from_ptr(write_options);

    let write_batch_handle = &mut *write_batch;
    let Some(batch) = write_batch_handle.batch.take() else {
        return error_result(
            slatedb_error_t::SLATEDB_INVALID_HANDLE,
            "write batch has been consumed",
        );
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.write_with_options(batch, &write_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_result(
            map_error(&err),
            &format!("db write_with_options failed: {err}"),
        ),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan(
    db: *mut slatedb_db_t,
    range: slatedb_range_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_scan_with_options(db, range, std::ptr::null(), out_iterator)
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan_with_options(
    db: *mut slatedb_db_t,
    range: slatedb_range_t,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }
    if out_iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_iterator pointer is null",
        );
    }

    let range = match range_from_c(range) {
        Ok(range) => range,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.scan_with_options(range, &scan_options))
    {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: db_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_result(map_error(&err), &format!("db scan failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan_prefix(
    db: *mut slatedb_db_t,
    prefix: *const u8,
    prefix_len: usize,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    slatedb_db_scan_prefix_with_options(db, prefix, prefix_len, std::ptr::null(), out_iterator)
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_scan_prefix_with_options(
    db: *mut slatedb_db_t,
    prefix: *const u8,
    prefix_len: usize,
    scan_options: *const slatedb_scan_options_t,
    out_iterator: *mut *mut slatedb_iterator_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }
    if out_iterator.is_null() {
        return error_result(
            slatedb_error_t::SLATEDB_NULL_POINTER,
            "out_iterator pointer is null",
        );
    }

    let prefix = match bytes_from_ptr(prefix, prefix_len, "prefix") {
        Ok(prefix) => prefix,
        Err(err) => return err,
    };
    let scan_options = match scan_options_from_ptr(scan_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let db_handle = &mut *db;
    match db_handle
        .runtime
        .block_on(db_handle.db.scan_prefix_with_options(prefix, &scan_options))
    {
        Ok(iter) => {
            let iterator = Box::new(slatedb_iterator_t {
                runtime: db_handle.runtime.clone(),
                iter,
            });
            *out_iterator = Box::into_raw(iterator);
            success_result()
        }
        Err(err) => error_result(map_error(&err), &format!("db scan_prefix failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_flush(db: *mut slatedb_db_t) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let handle = &mut *db;
    match handle.runtime.block_on(handle.db.flush()) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db flush failed: {err}")),
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_db_flush_with_options(
    db: *mut slatedb_db_t,
    flush_options: *const slatedb_flush_options_t,
) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let flush_options = match flush_options_from_ptr(flush_options) {
        Ok(options) => options,
        Err(err) => return err,
    };

    let handle = &mut *db;
    match handle
        .runtime
        .block_on(handle.db.flush_with_options(flush_options))
    {
        Ok(()) => success_result(),
        Err(err) => error_result(
            map_error(&err),
            &format!("db flush_with_options failed: {err}"),
        ),
    }
}

/// Closes and frees the DB handle.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_close(db: *mut slatedb_db_t) -> slatedb_result_t {
    if db.is_null() {
        return error_result(slatedb_error_t::SLATEDB_INVALID_HANDLE, "invalid db handle");
    }

    let handle = Box::from_raw(db);
    match handle.runtime.block_on(handle.db.close()) {
        Ok(()) => success_result(),
        Err(err) => error_result(map_error(&err), &format!("db close failed: {err}")),
    }
}
