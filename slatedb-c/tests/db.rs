mod common;

use common::{
    assert_result_invalid_contains, assert_result_ok, close_db_if_not_null, memory_object_store,
    take_bytes, unbounded_range, unique_db_path,
};
use slatedb_c::{
    slatedb_bound_t, slatedb_db_close, slatedb_db_delete, slatedb_db_delete_with_options,
    slatedb_db_flush, slatedb_db_flush_with_options, slatedb_db_get, slatedb_db_get_with_options,
    slatedb_db_merge, slatedb_db_merge_with_options, slatedb_db_open, slatedb_db_put,
    slatedb_db_put_with_options, slatedb_db_scan, slatedb_db_scan_prefix,
    slatedb_db_scan_prefix_with_options, slatedb_db_scan_with_options, slatedb_db_status,
    slatedb_db_write, slatedb_db_write_with_options, slatedb_flush_options_t,
    slatedb_iterator_close, slatedb_iterator_next, slatedb_merge_options_t,
    slatedb_object_store_close, slatedb_object_store_t, slatedb_put_options_t, slatedb_range_t,
    slatedb_read_options_t, slatedb_scan_options_t, slatedb_write_batch_close,
    slatedb_write_batch_new, slatedb_write_batch_put, slatedb_write_options_t,
    SLATEDB_BOUND_KIND_UNBOUNDED, SLATEDB_DURABILITY_FILTER_MEMORY, SLATEDB_FLUSH_TYPE_WAL,
    SLATEDB_TTL_TYPE_DEFAULT, SLATEDB_TTL_TYPE_NO_EXPIRY,
};
use std::ptr::{self, NonNull};

unsafe fn open_memory_db(
    prefix: &str,
) -> (*mut slatedb_object_store_t, *mut slatedb_c::slatedb_db_t) {
    let object_store = memory_object_store();
    let db_path = unique_db_path(prefix);
    let mut db = ptr::null_mut();
    assert_result_ok(slatedb_db_open(db_path.as_ptr(), object_store, &mut db));
    assert!(!db.is_null(), "db handle should not be null");
    (object_store, db)
}

unsafe fn close_memory_db(
    object_store: *mut slatedb_object_store_t,
    db: *mut slatedb_c::slatedb_db_t,
) {
    close_db_if_not_null(db);
    assert_result_ok(slatedb_object_store_close(object_store));
}

#[test]
fn test_db_open_status_close_happy_path() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("db-open-status");
        let mut db = ptr::null_mut();
        assert_result_ok(slatedb_db_open(db_path.as_ptr(), object_store, &mut db));
        assert!(!db.is_null(), "db handle should not be null");
        assert_result_ok(slatedb_db_status(db));
        assert_result_ok(slatedb_db_close(db));
        assert_result_ok(slatedb_object_store_close(object_store));
    }
}

#[test]
fn test_db_put_get_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-put-get");
        let key = b"primary-key";
        let value = b"primary-value";
        assert_result_ok(slatedb_db_put(
            db,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
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
        assert_eq!(take_bytes(out_val, out_val_len), value);

        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_get_missing_sets_optional_outputs() {
    unsafe {
        let (object_store, db) = open_memory_db("db-get-missing");
        let missing_key = b"missing-key";
        let mut present = true;
        let mut out_val = NonNull::<u8>::dangling().as_ptr();
        let mut out_val_len = usize::MAX;
        assert_result_ok(slatedb_db_get(
            db,
            missing_key.as_ptr(),
            missing_key.len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(!present);
        assert!(out_val.is_null());
        assert_eq!(out_val_len, 0);
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_put_with_options_get_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-put-get-options");
        let key = b"key-with-options";
        let value = b"value-with-options";
        let put_options = slatedb_put_options_t {
            ttl_type: SLATEDB_TTL_TYPE_NO_EXPIRY,
            ttl_value: 0,
        };
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_ok(slatedb_db_put_with_options(
            db,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
            &put_options,
            &write_options,
        ));

        let read_options = slatedb_read_options_t {
            durability_filter: SLATEDB_DURABILITY_FILTER_MEMORY,
            dirty: true,
            cache_blocks: false,
        };
        let mut present = false;
        let mut out_val = ptr::null_mut();
        let mut out_val_len = 0usize;
        assert_result_ok(slatedb_db_get_with_options(
            db,
            key.as_ptr(),
            key.len(),
            &read_options,
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(present);
        assert_eq!(take_bytes(out_val, out_val_len), value);
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_delete_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-delete");
        let key = b"delete-key";
        let value = b"delete-value";
        assert_result_ok(slatedb_db_put(
            db,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
        ));
        assert_result_ok(slatedb_db_delete(db, key.as_ptr(), key.len()));

        let mut present = true;
        let mut out_val = NonNull::<u8>::dangling().as_ptr();
        let mut out_val_len = usize::MAX;
        assert_result_ok(slatedb_db_get(
            db,
            key.as_ptr(),
            key.len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(!present);
        assert!(out_val.is_null());
        assert_eq!(out_val_len, 0);
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_delete_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-delete-options");
        let key = b"delete-options-key";
        let value = b"delete-options-value";
        assert_result_ok(slatedb_db_put(
            db,
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
        ));
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_ok(slatedb_db_delete_with_options(
            db,
            key.as_ptr(),
            key.len(),
            &write_options,
        ));
        let mut present = true;
        let mut out_val = NonNull::<u8>::dangling().as_ptr();
        let mut out_val_len = usize::MAX;
        assert_result_ok(slatedb_db_get(
            db,
            key.as_ptr(),
            key.len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(!present);
        assert!(out_val.is_null());
        assert_eq!(out_val_len, 0);
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_merge_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-merge");
        assert_result_ok(slatedb_db_merge(
            db,
            b"merge-key".as_ptr(),
            b"merge-key".len(),
            b"merge-value".as_ptr(),
            b"merge-value".len(),
        ));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_merge_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-merge-options");
        let merge_options = slatedb_merge_options_t {
            ttl_type: SLATEDB_TTL_TYPE_DEFAULT,
            ttl_value: 0,
        };
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_ok(slatedb_db_merge_with_options(
            db,
            b"merge-options-key".as_ptr(),
            b"merge-options-key".len(),
            b"merge-options-value".as_ptr(),
            b"merge-options-value".len(),
            &merge_options,
            &write_options,
        ));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_write_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-write");
        let mut write_batch = ptr::null_mut();
        assert_result_ok(slatedb_write_batch_new(&mut write_batch));
        assert_result_ok(slatedb_write_batch_put(
            write_batch,
            b"batch-key".as_ptr(),
            b"batch-key".len(),
            b"batch-value".as_ptr(),
            b"batch-value".len(),
        ));
        assert_result_ok(slatedb_db_write(db, write_batch));
        assert_result_ok(slatedb_write_batch_close(write_batch));

        let mut present = false;
        let mut out_val = ptr::null_mut();
        let mut out_val_len = 0usize;
        assert_result_ok(slatedb_db_get(
            db,
            b"batch-key".as_ptr(),
            b"batch-key".len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(present);
        assert_eq!(take_bytes(out_val, out_val_len), b"batch-value");
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_write_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-write-options");
        let mut write_batch = ptr::null_mut();
        assert_result_ok(slatedb_write_batch_new(&mut write_batch));
        assert_result_ok(slatedb_write_batch_put(
            write_batch,
            b"batch-options-key".as_ptr(),
            b"batch-options-key".len(),
            b"batch-options-value".as_ptr(),
            b"batch-options-value".len(),
        ));
        let write_options = slatedb_write_options_t {
            await_durable: true,
        };
        assert_result_ok(slatedb_db_write_with_options(
            db,
            write_batch,
            &write_options,
        ));
        assert_result_ok(slatedb_write_batch_close(write_batch));

        let mut present = false;
        let mut out_val = ptr::null_mut();
        let mut out_val_len = 0usize;
        assert_result_ok(slatedb_db_get(
            db,
            b"batch-options-key".as_ptr(),
            b"batch-options-key".len(),
            &mut present,
            &mut out_val,
            &mut out_val_len,
        ));
        assert!(present);
        assert_eq!(take_bytes(out_val, out_val_len), b"batch-options-value");
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_scan_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-scan");
        assert_result_ok(slatedb_db_put(
            db,
            b"a".as_ptr(),
            b"a".len(),
            b"1".as_ptr(),
            b"1".len(),
        ));
        assert_result_ok(slatedb_db_put(
            db,
            b"b".as_ptr(),
            b"b".len(),
            b"2".as_ptr(),
            b"2".len(),
        ));

        let mut iter = ptr::null_mut();
        assert_result_ok(slatedb_db_scan(db, unbounded_range(), &mut iter));
        assert!(!iter.is_null());

        let mut present = false;
        let mut key = ptr::null_mut();
        let mut key_len = 0usize;
        let mut val = ptr::null_mut();
        let mut val_len = 0usize;
        assert_result_ok(slatedb_iterator_next(
            iter,
            &mut present,
            &mut key,
            &mut key_len,
            &mut val,
            &mut val_len,
        ));
        assert!(present);
        assert!(!take_bytes(key, key_len).is_empty());
        assert!(!take_bytes(val, val_len).is_empty());
        assert_result_ok(slatedb_iterator_close(iter));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_scan_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-scan-options");
        assert_result_ok(slatedb_db_put(
            db,
            b"scan-opt-key".as_ptr(),
            b"scan-opt-key".len(),
            b"scan-opt-val".as_ptr(),
            b"scan-opt-val".len(),
        ));
        let scan_options = slatedb_scan_options_t {
            durability_filter: SLATEDB_DURABILITY_FILTER_MEMORY,
            dirty: true,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        };
        let mut iter = ptr::null_mut();
        assert_result_ok(slatedb_db_scan_with_options(
            db,
            unbounded_range(),
            &scan_options,
            &mut iter,
        ));
        assert!(!iter.is_null());

        let mut present = false;
        let mut key = ptr::null_mut();
        let mut key_len = 0usize;
        let mut val = ptr::null_mut();
        let mut val_len = 0usize;
        assert_result_ok(slatedb_iterator_next(
            iter,
            &mut present,
            &mut key,
            &mut key_len,
            &mut val,
            &mut val_len,
        ));
        assert!(present);
        let _ = take_bytes(key, key_len);
        let _ = take_bytes(val, val_len);
        assert_result_ok(slatedb_iterator_close(iter));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_scan_prefix_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-scan-prefix");
        assert_result_ok(slatedb_db_put(
            db,
            b"prefix:one".as_ptr(),
            b"prefix:one".len(),
            b"value-1".as_ptr(),
            b"value-1".len(),
        ));
        assert_result_ok(slatedb_db_put(
            db,
            b"prefix:two".as_ptr(),
            b"prefix:two".len(),
            b"value-2".as_ptr(),
            b"value-2".len(),
        ));
        assert_result_ok(slatedb_db_put(
            db,
            b"other".as_ptr(),
            b"other".len(),
            b"value-3".as_ptr(),
            b"value-3".len(),
        ));

        let prefix = b"prefix:";
        let mut iter = ptr::null_mut();
        assert_result_ok(slatedb_db_scan_prefix(
            db,
            prefix.as_ptr(),
            prefix.len(),
            &mut iter,
        ));
        assert!(!iter.is_null());

        let mut present = false;
        let mut key = ptr::null_mut();
        let mut key_len = 0usize;
        let mut val = ptr::null_mut();
        let mut val_len = 0usize;
        assert_result_ok(slatedb_iterator_next(
            iter,
            &mut present,
            &mut key,
            &mut key_len,
            &mut val,
            &mut val_len,
        ));
        assert!(present);
        let key_bytes = take_bytes(key, key_len);
        assert!(key_bytes.starts_with(prefix));
        let _ = take_bytes(val, val_len);
        assert_result_ok(slatedb_iterator_close(iter));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_scan_prefix_with_options_missing_prefix_outputs_empty() {
    unsafe {
        let (object_store, db) = open_memory_db("db-scan-prefix-options");
        assert_result_ok(slatedb_db_put(
            db,
            b"prefix:one".as_ptr(),
            b"prefix:one".len(),
            b"value-1".as_ptr(),
            b"value-1".len(),
        ));
        let scan_options = slatedb_scan_options_t {
            durability_filter: SLATEDB_DURABILITY_FILTER_MEMORY,
            dirty: true,
            read_ahead_bytes: 1,
            cache_blocks: false,
            max_fetch_tasks: 1,
        };
        let mut iter = ptr::null_mut();
        let missing_prefix = b"prefix:not-present";
        assert_result_ok(slatedb_db_scan_prefix_with_options(
            db,
            missing_prefix.as_ptr(),
            missing_prefix.len(),
            &scan_options,
            &mut iter,
        ));

        let mut present = true;
        let mut key = NonNull::<u8>::dangling().as_ptr();
        let mut key_len = usize::MAX;
        let mut val = NonNull::<u8>::dangling().as_ptr();
        let mut val_len = usize::MAX;
        assert_result_ok(slatedb_iterator_next(
            iter,
            &mut present,
            &mut key,
            &mut key_len,
            &mut val,
            &mut val_len,
        ));
        assert!(!present);
        assert!(key.is_null());
        assert_eq!(key_len, 0);
        assert!(val.is_null());
        assert_eq!(val_len, 0);
        assert_result_ok(slatedb_iterator_close(iter));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_flush_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-flush");
        assert_result_ok(slatedb_db_put(
            db,
            b"flush-key".as_ptr(),
            b"flush-key".len(),
            b"flush-value".as_ptr(),
            b"flush-value".len(),
        ));
        assert_result_ok(slatedb_db_flush(db));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_flush_with_options_happy_path() {
    unsafe {
        let (object_store, db) = open_memory_db("db-flush-options");
        assert_result_ok(slatedb_db_put(
            db,
            b"flush-options-key".as_ptr(),
            b"flush-options-key".len(),
            b"flush-options-value".as_ptr(),
            b"flush-options-value".len(),
        ));
        let flush_options = slatedb_flush_options_t {
            flush_type: SLATEDB_FLUSH_TYPE_WAL,
        };
        assert_result_ok(slatedb_db_flush_with_options(db, &flush_options));
        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_null_required_pointers_return_invalid() {
    unsafe {
        let object_store = memory_object_store();
        let db_path = unique_db_path("db-null-required");

        assert_result_invalid_contains(
            slatedb_db_open(db_path.as_ptr(), object_store, ptr::null_mut()),
            "out_db pointer is null",
        );

        let mut db = ptr::null_mut();
        assert_result_ok(slatedb_db_open(db_path.as_ptr(), object_store, &mut db));
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

        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_invalid_selectors_return_invalid() {
    unsafe {
        let (object_store, db) = open_memory_db("db-invalid-selectors");
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

        close_memory_db(object_store, db);
    }
}

#[test]
fn test_db_write_consumes_write_batch_handle() {
    unsafe {
        let (object_store, db) = open_memory_db("db-write-batch-consumed");
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
        close_memory_db(object_store, db);
    }
}
