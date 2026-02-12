//! Database builder APIs for `slatedb-c`.
//!
//! This module exposes the C ABI surface for constructing and configuring a
//! `DbBuilder` before opening a database instance.

use crate::ffi::{
    create_runtime, cstr_to_string, error_from_slate_error, error_result, slatedb_db_builder_t,
    slatedb_db_t, slatedb_error_kind_t, slatedb_object_store_t, slatedb_result_t,
    slatedb_sst_block_size_t, sst_block_size_from_u8, success_result,
};
use slatedb::Db;

/// Creates a new database builder.
///
/// ## Arguments
/// - `path`: Database path as a null-terminated UTF-8 string.
/// - `object_store`: Opaque object store handle.
/// - `out_builder`: Output pointer populated with a `slatedb_db_builder_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null/invalid pointers.
///
/// ## Safety
/// - `path` must be a valid null-terminated C string.
/// - `object_store` and `out_builder` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_new(
    path: *const std::os::raw::c_char,
    object_store: *const slatedb_object_store_t,
    out_builder: *mut *mut slatedb_db_builder_t,
) -> slatedb_result_t {
    if out_builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_builder pointer is null",
        );
    }
    if object_store.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
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

/// Configures a dedicated WAL object store on an existing builder.
///
/// ## Arguments
/// - `builder`: Builder handle.
/// - `wal_object_store`: Object store handle for WAL files.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles or consumed builder.
///
/// ## Safety
/// - `builder` and `wal_object_store` must be valid handles.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_wal_object_store(
    builder: *mut slatedb_db_builder_t,
    wal_object_store: *const slatedb_object_store_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid builder handle",
        );
    }
    if wal_object_store.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid wal_object_store handle",
        );
    }

    let handle = &mut *builder;
    let Some(current) = handle.builder.take() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "builder has been consumed",
        );
    };

    let wal_object_store = (&*wal_object_store).object_store.clone();
    handle.builder = Some(current.with_wal_object_store(wal_object_store));
    success_result()
}

/// Configures RNG seed for a builder.
///
/// ## Arguments
/// - `builder`: Builder handle.
/// - `seed`: Seed value.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles or consumed builder.
///
/// ## Safety
/// - `builder` must be a valid builder handle.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_seed(
    builder: *mut slatedb_db_builder_t,
    seed: u64,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid builder handle",
        );
    }

    let handle = &mut *builder;
    let Some(current) = handle.builder.take() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "builder has been consumed",
        );
    };

    handle.builder = Some(current.with_seed(seed));
    success_result()
}

/// Configures SST block size for a builder.
///
/// ## Arguments
/// - `builder`: Builder handle.
/// - `sst_block_size`: Block-size selector (`1..=7`).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid handles, invalid block
///   size, or consumed builder.
///
/// ## Safety
/// - `builder` must be a valid builder handle.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_with_sst_block_size(
    builder: *mut slatedb_db_builder_t,
    sst_block_size: slatedb_sst_block_size_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
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
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "builder has been consumed",
        );
    };

    handle.builder = Some(current.with_sst_block_size(block_size));
    success_result()
}

/// Builds a database from a builder and consumes the builder handle.
///
/// ## Arguments
/// - `builder`: Builder handle to consume.
/// - `out_db`: Output pointer populated with a `slatedb_db_t*` on success.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles.
/// - Returns mapped SlateDB errors if build fails.
///
/// ## Safety
/// - `builder` and `out_db` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_build(
    builder: *mut slatedb_db_builder_t,
    out_db: *mut *mut slatedb_db_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid builder handle",
        );
    }
    if out_db.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "out_db pointer is null",
        );
    }

    let mut builder_handle = Box::from_raw(builder);
    let Some(builder) = builder_handle.builder.take() else {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
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
        Err(err) => error_from_slate_error(&err, &format!("builder build failed: {err}")),
    }
}

/// Closes and frees a builder handle.
///
/// ## Arguments
/// - `builder`: Builder handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` when `builder` is null.
///
/// ## Safety
/// - `builder` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_db_builder_close(
    builder: *mut slatedb_db_builder_t,
) -> slatedb_result_t {
    if builder.is_null() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid builder handle",
        );
    }

    let _ = Box::from_raw(builder);

    success_result()
}
