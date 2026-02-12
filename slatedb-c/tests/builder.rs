mod common;

use common::{
    assert_result_invalid_contains, assert_result_ok, memory_object_store, take_bytes,
    unique_db_path,
};
use slatedb_c::{
    slatedb_db_builder_build, slatedb_db_builder_close, slatedb_db_builder_new,
    slatedb_db_builder_with_seed, slatedb_db_builder_with_sst_block_size,
    slatedb_db_builder_with_wal_object_store, slatedb_db_close, slatedb_db_get, slatedb_db_put,
    slatedb_db_t, slatedb_object_store_close, SLATEDB_SST_BLOCK_SIZE_8KIB,
};
use std::ptr;

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
            slatedb_db_builder_build(builder, ptr::null_mut()),
            "out_db pointer is null",
        );

        assert_result_ok(slatedb_db_builder_close(builder));

        assert_result_ok(slatedb_object_store_close(object_store));
    }
}
