mod common;

use common::{
    assert_result_invalid_contains, assert_result_ok, cleanup_temp_dir, close_db_if_not_null,
    close_object_store_if_not_null, cstring, file_url_for_path, open_db, resolve_object_store,
    take_bytes, unbounded_range, unique_temp_dir,
};
use slatedb_c::{
    slatedb_bound_t, slatedb_db_close, slatedb_db_delete, slatedb_db_delete_with_options,
    slatedb_db_flush, slatedb_db_flush_with_options, slatedb_db_get, slatedb_db_get_with_options,
    slatedb_db_merge, slatedb_db_merge_with_options, slatedb_db_open, slatedb_db_put,
    slatedb_db_put_with_options, slatedb_db_scan, slatedb_db_scan_prefix,
    slatedb_db_scan_prefix_with_options, slatedb_db_scan_with_options, slatedb_db_status,
    slatedb_db_write, slatedb_db_write_with_options, slatedb_flush_options_t,
    slatedb_iterator_close, slatedb_iterator_next, slatedb_merge_options_t, slatedb_put_options_t,
    slatedb_range_t, slatedb_read_options_t, slatedb_scan_options_t, slatedb_write_batch_close,
    slatedb_write_batch_new, slatedb_write_batch_put, slatedb_write_options_t,
    SLATEDB_BOUND_KIND_UNBOUNDED, SLATEDB_DURABILITY_FILTER_MEMORY, SLATEDB_FLUSH_TYPE_WAL,
    SLATEDB_TTL_TYPE_DEFAULT, SLATEDB_TTL_TYPE_NO_EXPIRY,
};
use std::ptr::{self, NonNull};

#[test]
fn test_db_happy_path_all_entrypoints() {
    unsafe {
        let root_dir = unique_temp_dir("db-happy");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);

        let db_path = cstring("db-happy-path");
        let db = open_db(&db_path, object_store);

        assert_result_ok(slatedb_db_status(db));

        let key_primary = b"primary-key";
        let value_primary = b"primary-value";
        assert_result_ok(slatedb_db_put(
            db,
            key_primary.as_ptr(),
            key_primary.len(),
            value_primary.as_ptr(),
            value_primary.len(),
        ));

        let mut found_primary = false;
        let mut found_primary_val = ptr::null_mut();
        let mut found_primary_val_len = 0usize;
        assert_result_ok(slatedb_db_get(
            db,
            key_primary.as_ptr(),
            key_primary.len(),
            &mut found_primary,
            &mut found_primary_val,
            &mut found_primary_val_len,
        ));
        assert!(found_primary);
        assert_eq!(
            take_bytes(found_primary_val, found_primary_val_len),
            value_primary
        );

        let missing_key = b"missing-key";
        let mut missing_present = true;
        let mut missing_val = NonNull::<u8>::dangling().as_ptr();
        let mut missing_val_len = usize::MAX;
        assert_result_ok(slatedb_db_get(
            db,
            missing_key.as_ptr(),
            missing_key.len(),
            &mut missing_present,
            &mut missing_val,
            &mut missing_val_len,
        ));
        assert!(!missing_present);
        assert!(missing_val.is_null());
        assert_eq!(missing_val_len, 0);

        let put_options = slatedb_put_options_t {
            ttl_type: SLATEDB_TTL_TYPE_NO_EXPIRY,
            ttl_value: 0,
        };
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        let key_with_options = b"key-with-options";
        let value_with_options = b"value-with-options";
        assert_result_ok(slatedb_db_put_with_options(
            db,
            key_with_options.as_ptr(),
            key_with_options.len(),
            value_with_options.as_ptr(),
            value_with_options.len(),
            &put_options,
            &write_options,
        ));

        let read_options = slatedb_read_options_t {
            durability_filter: SLATEDB_DURABILITY_FILTER_MEMORY,
            dirty: true,
            cache_blocks: false,
        };
        let mut found_with_options = false;
        let mut found_with_options_val = ptr::null_mut();
        let mut found_with_options_val_len = 0usize;
        assert_result_ok(slatedb_db_get_with_options(
            db,
            key_with_options.as_ptr(),
            key_with_options.len(),
            &read_options,
            &mut found_with_options,
            &mut found_with_options_val,
            &mut found_with_options_val_len,
        ));
        assert!(found_with_options);
        assert_eq!(
            take_bytes(found_with_options_val, found_with_options_val_len),
            value_with_options
        );

        let key_delete = b"delete-key";
        let value_delete = b"delete-value";
        assert_result_ok(slatedb_db_put(
            db,
            key_delete.as_ptr(),
            key_delete.len(),
            value_delete.as_ptr(),
            value_delete.len(),
        ));
        assert_result_ok(slatedb_db_delete(db, key_delete.as_ptr(), key_delete.len()));

        let mut deleted_present = true;
        let mut deleted_val = NonNull::<u8>::dangling().as_ptr();
        let mut deleted_val_len = usize::MAX;
        assert_result_ok(slatedb_db_get(
            db,
            key_delete.as_ptr(),
            key_delete.len(),
            &mut deleted_present,
            &mut deleted_val,
            &mut deleted_val_len,
        ));
        assert!(!deleted_present);
        assert!(deleted_val.is_null());
        assert_eq!(deleted_val_len, 0);

        let key_delete_with_options = b"delete-with-options-key";
        let value_delete_with_options = b"delete-with-options-value";
        assert_result_ok(slatedb_db_put(
            db,
            key_delete_with_options.as_ptr(),
            key_delete_with_options.len(),
            value_delete_with_options.as_ptr(),
            value_delete_with_options.len(),
        ));
        assert_result_ok(slatedb_db_delete_with_options(
            db,
            key_delete_with_options.as_ptr(),
            key_delete_with_options.len(),
            &write_options,
        ));

        let key_merge = b"merge-key";
        let value_merge = b"merge-value";
        assert_result_ok(slatedb_db_merge(
            db,
            key_merge.as_ptr(),
            key_merge.len(),
            value_merge.as_ptr(),
            value_merge.len(),
        ));

        let merge_options = slatedb_merge_options_t {
            ttl_type: SLATEDB_TTL_TYPE_DEFAULT,
            ttl_value: 0,
        };
        let key_merge_with_options = b"merge-with-options-key";
        let value_merge_with_options = b"merge-with-options-value";
        assert_result_ok(slatedb_db_merge_with_options(
            db,
            key_merge_with_options.as_ptr(),
            key_merge_with_options.len(),
            value_merge_with_options.as_ptr(),
            value_merge_with_options.len(),
            &merge_options,
            &write_options,
        ));

        let mut write_batch = ptr::null_mut();
        assert_result_ok(slatedb_write_batch_new(&mut write_batch));
        let key_batch = b"batch-key";
        let value_batch = b"batch-value";
        assert_result_ok(slatedb_write_batch_put(
            write_batch,
            key_batch.as_ptr(),
            key_batch.len(),
            value_batch.as_ptr(),
            value_batch.len(),
        ));
        assert_result_ok(slatedb_db_write(db, write_batch));
        assert_result_ok(slatedb_write_batch_close(write_batch));

        let mut write_batch_with_options = ptr::null_mut();
        assert_result_ok(slatedb_write_batch_new(&mut write_batch_with_options));
        let key_batch_with_options = b"batch-with-options-key";
        let value_batch_with_options = b"batch-with-options-value";
        assert_result_ok(slatedb_write_batch_put(
            write_batch_with_options,
            key_batch_with_options.as_ptr(),
            key_batch_with_options.len(),
            value_batch_with_options.as_ptr(),
            value_batch_with_options.len(),
        ));
        assert_result_ok(slatedb_db_write_with_options(
            db,
            write_batch_with_options,
            &write_options,
        ));
        assert_result_ok(slatedb_write_batch_close(write_batch_with_options));

        let key_prefix_1 = b"prefix:one";
        let key_prefix_2 = b"prefix:two";
        assert_result_ok(slatedb_db_put(
            db,
            key_prefix_1.as_ptr(),
            key_prefix_1.len(),
            b"value-1".as_ptr(),
            b"value-1".len(),
        ));
        assert_result_ok(slatedb_db_put(
            db,
            key_prefix_2.as_ptr(),
            key_prefix_2.len(),
            b"value-2".as_ptr(),
            b"value-2".len(),
        ));

        let mut range_iter = ptr::null_mut();
        assert_result_ok(slatedb_db_scan(db, unbounded_range(), &mut range_iter));
        assert!(!range_iter.is_null());

        let mut range_present = false;
        let mut range_key = ptr::null_mut();
        let mut range_key_len = 0usize;
        let mut range_val = ptr::null_mut();
        let mut range_val_len = 0usize;
        assert_result_ok(slatedb_iterator_next(
            range_iter,
            &mut range_present,
            &mut range_key,
            &mut range_key_len,
            &mut range_val,
            &mut range_val_len,
        ));
        assert!(range_present, "scan should return at least one row");
        assert!(!take_bytes(range_key, range_key_len).is_empty());
        let _ = take_bytes(range_val, range_val_len);
        assert_result_ok(slatedb_iterator_close(range_iter));

        let scan_options = slatedb_scan_options_t {
            durability_filter: SLATEDB_DURABILITY_FILTER_MEMORY,
            dirty: true,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        };
        let mut range_iter_with_options = ptr::null_mut();
        assert_result_ok(slatedb_db_scan_with_options(
            db,
            unbounded_range(),
            &scan_options,
            &mut range_iter_with_options,
        ));
        assert_result_ok(slatedb_iterator_close(range_iter_with_options));

        let mut prefix_iter = ptr::null_mut();
        let prefix = b"prefix:";
        assert_result_ok(slatedb_db_scan_prefix(
            db,
            prefix.as_ptr(),
            prefix.len(),
            &mut prefix_iter,
        ));
        assert!(!prefix_iter.is_null());

        let mut prefix_present = false;
        let mut prefix_key = ptr::null_mut();
        let mut prefix_key_len = 0usize;
        let mut prefix_val = ptr::null_mut();
        let mut prefix_val_len = 0usize;
        assert_result_ok(slatedb_iterator_next(
            prefix_iter,
            &mut prefix_present,
            &mut prefix_key,
            &mut prefix_key_len,
            &mut prefix_val,
            &mut prefix_val_len,
        ));
        assert!(prefix_present);
        let prefix_key_bytes = take_bytes(prefix_key, prefix_key_len);
        assert!(prefix_key_bytes.starts_with(prefix));
        let _ = take_bytes(prefix_val, prefix_val_len);
        assert_result_ok(slatedb_iterator_close(prefix_iter));

        let mut missing_prefix_iter = ptr::null_mut();
        let missing_prefix = b"prefix:not-present";
        assert_result_ok(slatedb_db_scan_prefix_with_options(
            db,
            missing_prefix.as_ptr(),
            missing_prefix.len(),
            &scan_options,
            &mut missing_prefix_iter,
        ));

        let mut missing_prefix_present = true;
        let mut missing_prefix_key = NonNull::<u8>::dangling().as_ptr();
        let mut missing_prefix_key_len = usize::MAX;
        let mut missing_prefix_val = NonNull::<u8>::dangling().as_ptr();
        let mut missing_prefix_val_len = usize::MAX;
        assert_result_ok(slatedb_iterator_next(
            missing_prefix_iter,
            &mut missing_prefix_present,
            &mut missing_prefix_key,
            &mut missing_prefix_key_len,
            &mut missing_prefix_val,
            &mut missing_prefix_val_len,
        ));
        assert!(!missing_prefix_present);
        assert!(missing_prefix_key.is_null());
        assert_eq!(missing_prefix_key_len, 0);
        assert!(missing_prefix_val.is_null());
        assert_eq!(missing_prefix_val_len, 0);
        assert_result_ok(slatedb_iterator_close(missing_prefix_iter));

        assert_result_ok(slatedb_db_flush(db));
        let flush_options = slatedb_flush_options_t {
            flush_type: SLATEDB_FLUSH_TYPE_WAL,
        };
        assert_result_ok(slatedb_db_flush_with_options(db, &flush_options));

        assert_result_ok(slatedb_db_close(db));

        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}

#[test]
fn test_db_null_required_pointers_return_invalid() {
    unsafe {
        let root_dir = unique_temp_dir("db-null-required");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);
        let db_path = cstring("db-null-required");

        assert_result_invalid_contains(
            slatedb_db_open(db_path.as_ptr(), object_store, ptr::null_mut()),
            "out_db pointer is null",
        );

        let db = open_db(&db_path, object_store);
        let key = b"k";
        let mut out_val = ptr::null_mut();
        let mut out_val_len = 0usize;
        assert_result_invalid_contains(
            slatedb_db_get(
                db,
                key.as_ptr(),
                key.len(),
                ptr::null_mut(),
                &mut out_val,
                &mut out_val_len,
            ),
            "out_present pointer is null",
        );

        assert_result_invalid_contains(
            slatedb_db_scan(db, unbounded_range(), ptr::null_mut()),
            "out_iterator pointer is null",
        );

        close_db_if_not_null(db);
        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}

#[test]
fn test_db_invalid_selectors_return_invalid() {
    unsafe {
        let root_dir = unique_temp_dir("db-invalid-selectors");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);
        let db_path = cstring("db-invalid-selectors");
        let db = open_db(&db_path, object_store);

        let key = b"selector-key";
        let value = b"selector-value";
        assert_result_ok(slatedb_db_put(
            db,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
        ));

        let invalid_read_options = slatedb_read_options_t {
            durability_filter: 99,
            dirty: false,
            cache_blocks: false,
        };
        let mut present = true;
        let mut out_val = NonNull::<u8>::dangling().as_ptr();
        let mut out_val_len = usize::MAX;
        assert_result_invalid_contains(
            slatedb_db_get_with_options(
                db,
                key.as_ptr(),
                key.len(),
                &invalid_read_options,
                &mut present,
                &mut out_val,
                &mut out_val_len,
            ),
            "invalid durability_filter",
        );
        assert!(!present);
        assert!(out_val.is_null());
        assert_eq!(out_val_len, 0);

        let invalid_put_options = slatedb_put_options_t {
            ttl_type: 99,
            ttl_value: 1,
        };
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_invalid_contains(
            slatedb_db_put_with_options(
                db,
                b"put-selector".as_ptr(),
                b"put-selector".len(),
                b"value".as_ptr(),
                b"value".len(),
                &invalid_put_options,
                &write_options,
            ),
            "invalid ttl_type",
        );

        let invalid_merge_options = slatedb_merge_options_t {
            ttl_type: 99,
            ttl_value: 1,
        };
        assert_result_invalid_contains(
            slatedb_db_merge_with_options(
                db,
                b"merge-selector".as_ptr(),
                b"merge-selector".len(),
                b"value".as_ptr(),
                b"value".len(),
                &invalid_merge_options,
                &write_options,
            ),
            "invalid ttl_type",
        );

        let invalid_flush_options = slatedb_flush_options_t { flush_type: 99 };
        assert_result_invalid_contains(
            slatedb_db_flush_with_options(db, &invalid_flush_options),
            "invalid flush_type",
        );

        let invalid_scan_options = slatedb_scan_options_t {
            durability_filter: 99,
            dirty: false,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        };
        let mut out_iter = ptr::null_mut();
        assert_result_invalid_contains(
            slatedb_db_scan_with_options(
                db,
                unbounded_range(),
                &invalid_scan_options,
                &mut out_iter,
            ),
            "invalid durability_filter",
        );
        assert!(out_iter.is_null());

        let invalid_range = slatedb_range_t {
            start: slatedb_bound_t {
                kind: 99,
                data: ptr::null(),
                len: 0,
            },
            end: slatedb_bound_t {
                kind: SLATEDB_BOUND_KIND_UNBOUNDED,
                data: ptr::null(),
                len: 0,
            },
        };
        let mut out_iter_invalid_range = ptr::null_mut();
        assert_result_invalid_contains(
            slatedb_db_scan(db, invalid_range, &mut out_iter_invalid_range),
            "invalid bound kind",
        );
        assert!(out_iter_invalid_range.is_null());

        close_db_if_not_null(db);
        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}

#[test]
fn test_db_write_consumes_write_batch_handle() {
    unsafe {
        let root_dir = unique_temp_dir("db-write-batch-consumed");
        let object_store_url = file_url_for_path(&root_dir);
        let object_store = resolve_object_store(&object_store_url);
        let db_path = cstring("db-write-batch-consumed");
        let db = open_db(&db_path, object_store);

        let mut write_batch = ptr::null_mut();
        assert_result_ok(slatedb_write_batch_new(&mut write_batch));
        assert_result_ok(slatedb_write_batch_put(
            write_batch,
            b"consumed-key".as_ptr(),
            b"consumed-key".len(),
            b"consumed-value".as_ptr(),
            b"consumed-value".len(),
        ));
        assert_result_ok(slatedb_db_write(db, write_batch));

        assert_result_invalid_contains(
            slatedb_db_write(db, write_batch),
            "write batch has been consumed",
        );

        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_invalid_contains(
            slatedb_db_write_with_options(db, write_batch, &write_options),
            "write batch has been consumed",
        );

        assert_result_invalid_contains(
            slatedb_write_batch_put(write_batch, b"x".as_ptr(), 1, b"y".as_ptr(), 1),
            "write batch has been consumed",
        );

        assert_result_ok(slatedb_write_batch_close(write_batch));
        close_db_if_not_null(db);
        close_object_store_if_not_null(object_store);
        cleanup_temp_dir(&root_dir);
    }
}
