mod common;

use common::{
    assert_result_invalid_contains, assert_result_ok, cleanup_temp_dir,
    close_object_store_if_not_null, cstring, file_url_for_path, resolve_object_store, take_bytes,
    unique_temp_dir,
};
use slatedb_c::{
    slatedb_db_builder_build, slatedb_db_builder_close, slatedb_db_builder_new,
    slatedb_db_builder_with_seed, slatedb_db_builder_with_sst_block_size,
    slatedb_db_builder_with_wal_object_store, slatedb_db_close, slatedb_db_get, slatedb_db_put,
    slatedb_db_t, SLATEDB_SST_BLOCK_SIZE_8KIB,
};
use std::ptr;

#[test]
fn test_builder_happy_path_all_entrypoints() {
    unsafe {
        let data_root_dir = unique_temp_dir("builder-happy-data");
        let wal_root_dir = unique_temp_dir("builder-happy-wal");
        let data_store_url = file_url_for_path(&data_root_dir);
        let wal_store_url = file_url_for_path(&wal_root_dir);
        let data_object_store = resolve_object_store(&data_store_url);
        let wal_object_store = resolve_object_store(&wal_store_url);

        let db_path = cstring("builder-happy");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            data_object_store,
            &mut builder,
        ));
        assert!(!builder.is_null(), "builder handle should not be null");

        assert_result_ok(slatedb_db_builder_with_wal_object_store(
            builder,
            wal_object_store,
        ));
        assert_result_ok(slatedb_db_builder_with_seed(builder, 42));
        assert_result_ok(slatedb_db_builder_with_sst_block_size(
            builder,
            SLATEDB_SST_BLOCK_SIZE_8KIB,
        ));

        let mut db: *mut slatedb_db_t = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_build(builder, &mut db));
        assert!(!db.is_null(), "db handle should not be null");

        let key = b"builder-key";
        let val = b"builder-value";
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

        assert_result_ok(slatedb_db_close(db));

        close_object_store_if_not_null(data_object_store);
        close_object_store_if_not_null(wal_object_store);
        cleanup_temp_dir(&data_root_dir);
        cleanup_temp_dir(&wal_root_dir);
    }
}

#[test]
fn test_builder_close_happy_path() {
    unsafe {
        let root_dir = unique_temp_dir("builder-close");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);

        let db_path = cstring("builder-close");
        let mut builder = ptr::null_mut();
        assert_result_ok(slatedb_db_builder_new(
            db_path.as_ptr(),
            object_store,
            &mut builder,
        ));

        assert_result_ok(slatedb_db_builder_close(builder));

        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}

#[test]
fn test_builder_invalid_inputs_return_invalid() {
    unsafe {
        let root_dir = unique_temp_dir("builder-invalid");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);
        let db_path = cstring("builder-invalid");

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

        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}
