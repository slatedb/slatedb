mod common;

use common::{
    assert_result_invalid_contains, assert_result_ok, memory_object_store, take_bytes,
    unique_db_path,
};
use slatedb_c::{
    slatedb_db_builder_build, slatedb_db_builder_close, slatedb_db_builder_new,
    slatedb_db_builder_with_merge_operator, slatedb_db_builder_with_seed,
    slatedb_db_builder_with_sst_block_size, slatedb_db_builder_with_wal_object_store,
    slatedb_db_close, slatedb_db_get, slatedb_db_merge, slatedb_db_put, slatedb_db_t,
    slatedb_object_store_close, SLATEDB_SST_BLOCK_SIZE_8KIB,
};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct MergeOperatorContext {
    free_count: Arc<AtomicUsize>,
}

unsafe extern "C" fn concat_merge_operator(
    _key: *const u8,
    _key_len: usize,
    existing_value: *const u8,
    existing_value_len: usize,
    has_existing_value: bool,
    operand: *const u8,
    operand_len: usize,
    out_value: *mut *mut u8,
    out_value_len: *mut usize,
    _context: *mut c_void,
) -> bool {
    if out_value.is_null() || out_value_len.is_null() {
        return false;
    }
    if has_existing_value && existing_value.is_null() && existing_value_len > 0 {
        return false;
    }
    if operand.is_null() && operand_len > 0 {
        return false;
    }

    let mut merged = Vec::with_capacity(existing_value_len + operand_len);
    if has_existing_value && existing_value_len > 0 {
        // SAFETY: Guarded by pointer/length validation above.
        merged.extend_from_slice(unsafe {
            std::slice::from_raw_parts(existing_value, existing_value_len)
        });
    }
    if operand_len > 0 {
        // SAFETY: Guarded by pointer/length validation above.
        merged.extend_from_slice(unsafe { std::slice::from_raw_parts(operand, operand_len) });
    }

    if merged.is_empty() {
        // SAFETY: Guarded by pointer validation above.
        unsafe {
            *out_value = ptr::null_mut();
            *out_value_len = 0;
        }
        return true;
    }

    let mut merged = merged.into_boxed_slice();
    // SAFETY: Guarded by pointer validation above.
    unsafe {
        *out_value = merged.as_mut_ptr();
        *out_value_len = merged.len();
    }
    std::mem::forget(merged);
    true
}

unsafe extern "C" fn free_merge_result(value: *mut u8, value_len: usize, _context: *mut c_void) {
    if value.is_null() {
        return;
    }
    // SAFETY: `value` and `value_len` were produced by `concat_merge_operator`.
    unsafe {
        let value = std::ptr::slice_from_raw_parts_mut(value, value_len);
        drop(Box::from_raw(value));
    }
}

unsafe extern "C" fn free_merge_operator_context(context: *mut c_void) {
    if context.is_null() {
        return;
    }

    // SAFETY: `context` was created via `Box::into_raw` in the test.
    let context = unsafe { Box::from_raw(context as *mut MergeOperatorContext) };
    context.free_count.fetch_add(1, Ordering::Relaxed);
}

unsafe fn assert_roundtrip(db: *mut slatedb_db_t, key: &[u8], val: &[u8]) {
    assert_result_ok(slatedb_db_put(
        db,
        key.as_ptr(),
        key.len(),
        val.as_ptr(),
        val.len(),
    ));
    let mut present = false;
    let mut out_val = ptr::null_mut();
    let mut out_val_len = 0usize;
    assert_result_ok(slatedb_db_get(
        db,
        key.as_ptr(),
        key.len(),
        &mut present,
        &mut out_val,
        &mut out_val_len,
    ));
    assert!(present);
    assert_eq!(take_bytes(out_val, out_val_len), val);
}

#[test]
fn test_builder_new_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-new");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));
        assert!(!builder.is_null(), "builder handle should not be null");
        assert_result_ok(slatedb_db_builder_close(builder));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_builder_with_wal_object_store_happy_path() {
    unsafe {
        let data_object_store = memory_object_store();
        let wal_object_store = memory_object_store();
        let db_path = unique_db_path("builder-wal");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            data_object_store,
            &mut builder,
        ));
        assert_result_ok(slatedb_db_builder_with_wal_object_store(
            builder,
            wal_object_store,
        ));
        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert!(!db.is_null(), "db handle should not be null");
        assert_roundtrip(db, b"builder-wal-key", b"builder-wal-value");
        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(data_object_store));
        assert_result_ok(slatedb_object_store_close(wal_object_store));
    }
}

#[test]
fn test_builder_with_seed_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-seed");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));
        assert_result_ok(slatedb_db_builder_with_seed(builder, 42));
        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert_roundtrip(db, b"builder-seed-key", b"builder-seed-value");
        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_builder_with_sst_block_size_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-sst-block-size");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));
        assert_result_ok(slatedb_db_builder_with_sst_block_size(
            builder,
            SLATEDB_SST_BLOCK_SIZE_8KIB,
        ));
        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert_roundtrip(db, b"builder-sst-key", b"builder-sst-value");
        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_builder_with_merge_operator_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-merge-operator");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));

        let free_count = Arc::new(AtomicUsize::new(0));
        let context = Box::new(MergeOperatorContext {
            free_count: free_count.clone(),
        });
        assert_result_ok(slatedb_db_builder_with_merge_operator(
            builder,
            Some(concat_merge_operator),
            Box::into_raw(context) as *mut c_void,
            Some(free_merge_result),
            Some(free_merge_operator_context),
        ));

        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert!(!db.is_null(), "db handle should not be null");

        assert_result_ok(slatedb_db_put(
            db,
            b"merge-key".as_ptr(),
            b"merge-key".len(),
            b"base".as_ptr(),
            b"base".len(),
        ));
        assert_result_ok(slatedb_db_merge(
            db,
            b"merge-key".as_ptr(),
            b"merge-key".len(),
            b"-delta".as_ptr(),
            b"-delta".len(),
        ));

        let mut present = false;
        let mut out_val = ptr::null_mut();
        let mut out_val_len = 0usize;
        assert_result_ok(slatedb_db_get(
            db,
            b"merge-key".as_ptr(),
            b"merge-key".len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(present);
        assert_eq!(take_bytes(out_val, out_val_len), b"base-delta");

        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(object_store));
        assert_eq!(free_count.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn test_builder_build_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-build");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));
        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert!(!db.is_null(), "db handle should not be null");
        assert_roundtrip(db, b"builder-build-key", b"builder-build-value");
        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_builder_close_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-close");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));
        assert_result_ok(slatedb_db_builder_close(builder));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_builder_invalid_inputs_return_invalid() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("builder-invalid");

        assert_result_invalid_contains(
            slatedb_db_builder_new(db_path.as_ptr(), object_store, ptr::null_mut()),
            "out_builder pointer is null",
        );
        assert_result_invalid_contains(
            {
                let mut out_builder = ptr::null_mut();
                slatedb_db_builder_new(db_path.as_ptr(), ptr::null(), &mut out_builder)
            },
            "invalid object_store handle",
        );
        assert_result_invalid_contains(
            {
                let mut out_builder = ptr::null_mut();
                slatedb_db_builder_new(ptr::null(), object_store, &mut out_builder)
            },
            "path pointer is null",
        );
        assert_result_invalid_contains(
            slatedb_db_builder_close(ptr::null_mut()),
            "invalid builder handle",
        );
        assert_result_invalid_contains(
            slatedb_db_builder_with_seed(ptr::null_mut(), 7),
            "invalid builder handle",
        );

        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));

        assert_result_invalid_contains(
            slatedb_db_builder_with_wal_object_store(builder, ptr::null()),
            "invalid wal_object_store handle",
        );
        assert_result_invalid_contains(
            slatedb_db_builder_with_sst_block_size(builder, 0),
            "invalid sst_block_size",
        );
        assert_result_invalid_contains(
            slatedb_db_builder_with_merge_operator(builder, None, ptr::null_mut(), None, None),
            "merge_operator pointer is null",
        );
        assert_result_invalid_contains(
            slatedb_db_builder_build(builder, ptr::null_mut()),
            "out_db pointer is null",
        );

        assert_result_ok(slatedb_db_builder_close(builder));

        assert_result_ok(slatedb_object_store_close(object_store));
    }
}
