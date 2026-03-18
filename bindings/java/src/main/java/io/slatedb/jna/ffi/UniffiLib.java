package io.slatedb.jna.ffi;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

// A JNA Library to expose the extern-C FFI definitions.
// This is an implementation detail which will be called internally by the public API.
interface UniffiLib extends Library {
  UniffiLib INSTANCE = UniffiLibInitializer.load();

  // The Cleaner for the whole library
  static UniffiCleaner CLEANER = UniffiCleaner.create();

  Pointer uniffi_slatedb_ffi_fn_clone_db(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_db(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_db_begin(Pointer ptr, RustBuffer.ByValue isolationLevel);

  Long uniffi_slatedb_ffi_fn_method_db_delete(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_db_delete_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_flush(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_db_flush_with_options(Pointer ptr, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_db_get_key_value(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_db_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_merge(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand);

  Long uniffi_slatedb_ffi_fn_method_db_merge_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      RustBuffer.ByValue mergeOptions,
      RustBuffer.ByValue writeOptions);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_db_metrics(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_db_put(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value);

  Long uniffi_slatedb_ffi_fn_method_db_put_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      RustBuffer.ByValue putOptions,
      RustBuffer.ByValue writeOptions);

  Long uniffi_slatedb_ffi_fn_method_db_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_db_scan_prefix(Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_db_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_db_shutdown(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_db_snapshot(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_db_status(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_db_write(Pointer ptr, Pointer batch);

  Long uniffi_slatedb_ffi_fn_method_db_write_with_options(
      Pointer ptr, Pointer batch, RustBuffer.ByValue options);

  Pointer uniffi_slatedb_ffi_fn_clone_dbbuilder(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbbuilder(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_dbbuilder_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbbuilder_build(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_db_cache_disabled(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_merge_operator(
      Pointer ptr, Pointer mergeOperator, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_seed(
      Pointer ptr, Long seed, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_settings(
      Pointer ptr, Pointer settings, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_sst_block_size(
      Pointer ptr, RustBuffer.ByValue sstBlockSize, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbbuilder_with_wal_object_store(
      Pointer ptr, Pointer walObjectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_dbiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbiterator(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbiterator_next(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_dbiterator_seek(Pointer ptr, RustBuffer.ByValue key);

  Pointer uniffi_slatedb_ffi_fn_clone_dbreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbreader_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbreader_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbreader_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_dbreader_scan_prefix(Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_dbreader_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbreader_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbreader_shutdown(Pointer ptr);

  Pointer uniffi_slatedb_ffi_fn_clone_dbreaderbuilder(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbreaderbuilder(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_dbreaderbuilder_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbreaderbuilder_build(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_checkpoint_id(
      Pointer ptr, RustBuffer.ByValue checkpointId, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_merge_operator(
      Pointer ptr, Pointer mergeOperator, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_options(
      Pointer ptr, RustBuffer.ByValue options, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_dbreaderbuilder_with_wal_object_store(
      Pointer ptr, Pointer walObjectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_dbsnapshot(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbsnapshot(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_get_key_value(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_prefix(Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbsnapshot_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Pointer uniffi_slatedb_ffi_fn_clone_dbtransaction(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_dbtransaction(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_commit(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_commit_with_options(
      Pointer ptr, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_delete(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_get_key_value(
      Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_dbtransaction_id(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_mark_read(Pointer ptr, RustBuffer.ByValue keys);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_merge(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_merge_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_put(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_put_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_rollback(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_scan_prefix(
      Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_seqnum(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_dbtransaction_unmark_write(
      Pointer ptr, RustBuffer.ByValue keys);

  Pointer uniffi_slatedb_ffi_fn_clone_logcallback(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_logcallback(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_init_callback_vtable_logcallback(
      UniffiVTableCallbackInterfaceLogCallback vtable);

  void uniffi_slatedb_ffi_fn_method_logcallback_log(
      Pointer ptr, RustBuffer.ByValue record, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_mergeoperator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_mergeoperator(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_init_callback_vtable_mergeoperator(
      UniffiVTableCallbackInterfaceMergeOperator vtable);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_mergeoperator_merge(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue existingValue,
      RustBuffer.ByValue operand,
      UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_objectstore(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_objectstore(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_objectstore_from_env(
      RustBuffer.ByValue envFile, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_objectstore_resolve(
      RustBuffer.ByValue url, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_settings(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_settings(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_default(UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_from_env(
      RustBuffer.ByValue prefix, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_from_env_with_default(
      RustBuffer.ByValue prefix, Pointer defaultSettings, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_from_file(
      RustBuffer.ByValue path, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_from_json_string(
      RustBuffer.ByValue json, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_settings_load(UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_settings_set(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue valueJson,
      UniffiRustCallStatus uniffi_out_errmk);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_settings_to_json_string(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_walfile(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_walfile(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_walfile_id(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_walfile_iterator(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_walfile_metadata(Pointer ptr);

  Pointer uniffi_slatedb_ffi_fn_method_walfile_next_file(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_walfile_next_id(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_walfile_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_walfileiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_walfileiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_walfileiterator_next(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_walfileiterator_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_walreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_walreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_walreader_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_method_walreader_get(
      Pointer ptr, Long id, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_walreader_list(
      Pointer ptr, RustBuffer.ByValue startId, RustBuffer.ByValue endId);

  void uniffi_slatedb_ffi_fn_method_walreader_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_writebatch(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_writebatch(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_writebatch_new(UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_writebatch_delete(
      Pointer ptr, RustBuffer.ByValue key, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_writebatch_merge(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_writebatch_merge_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      RustBuffer.ByValue options,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_writebatch_put(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_writebatch_put_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      RustBuffer.ByValue options,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_func_init_logging(
      RustBuffer.ByValue level, RustBuffer.ByValue callback, UniffiRustCallStatus uniffi_out_errmk);

  RustBuffer.ByValue ffi_slatedb_ffi_rustbuffer_alloc(
      Long size, UniffiRustCallStatus uniffi_out_errmk);

  RustBuffer.ByValue ffi_slatedb_ffi_rustbuffer_from_bytes(
      ForeignBytes.ByValue bytes, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rustbuffer_free(
      RustBuffer.ByValue buf, UniffiRustCallStatus uniffi_out_errmk);

  RustBuffer.ByValue ffi_slatedb_ffi_rustbuffer_reserve(
      RustBuffer.ByValue buf, Long additional, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_u8(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_u8(Long handle);

  void ffi_slatedb_ffi_rust_future_free_u8(Long handle);

  Byte ffi_slatedb_ffi_rust_future_complete_u8(Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_i8(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_i8(Long handle);

  void ffi_slatedb_ffi_rust_future_free_i8(Long handle);

  Byte ffi_slatedb_ffi_rust_future_complete_i8(Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_u16(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_u16(Long handle);

  void ffi_slatedb_ffi_rust_future_free_u16(Long handle);

  Short ffi_slatedb_ffi_rust_future_complete_u16(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_i16(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_i16(Long handle);

  void ffi_slatedb_ffi_rust_future_free_i16(Long handle);

  Short ffi_slatedb_ffi_rust_future_complete_i16(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_u32(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_u32(Long handle);

  void ffi_slatedb_ffi_rust_future_free_u32(Long handle);

  Integer ffi_slatedb_ffi_rust_future_complete_u32(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_i32(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_i32(Long handle);

  void ffi_slatedb_ffi_rust_future_free_i32(Long handle);

  Integer ffi_slatedb_ffi_rust_future_complete_i32(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_u64(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_u64(Long handle);

  void ffi_slatedb_ffi_rust_future_free_u64(Long handle);

  Long ffi_slatedb_ffi_rust_future_complete_u64(Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_i64(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_i64(Long handle);

  void ffi_slatedb_ffi_rust_future_free_i64(Long handle);

  Long ffi_slatedb_ffi_rust_future_complete_i64(Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_f32(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_f32(Long handle);

  void ffi_slatedb_ffi_rust_future_free_f32(Long handle);

  Float ffi_slatedb_ffi_rust_future_complete_f32(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_f64(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_f64(Long handle);

  void ffi_slatedb_ffi_rust_future_free_f64(Long handle);

  Double ffi_slatedb_ffi_rust_future_complete_f64(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_pointer(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_pointer(Long handle);

  void ffi_slatedb_ffi_rust_future_free_pointer(Long handle);

  Pointer ffi_slatedb_ffi_rust_future_complete_pointer(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_rust_buffer(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_rust_buffer(Long handle);

  void ffi_slatedb_ffi_rust_future_free_rust_buffer(Long handle);

  RustBuffer.ByValue ffi_slatedb_ffi_rust_future_complete_rust_buffer(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  void ffi_slatedb_ffi_rust_future_poll_void(
      Long handle, UniffiRustFutureContinuationCallback callback, Long callbackData);

  void ffi_slatedb_ffi_rust_future_cancel_void(Long handle);

  void ffi_slatedb_ffi_rust_future_free_void(Long handle);

  void ffi_slatedb_ffi_rust_future_complete_void(
      Long handle, UniffiRustCallStatus uniffi_out_errmk);

  Short uniffi_slatedb_ffi_checksum_func_init_logging();

  Short uniffi_slatedb_ffi_checksum_method_db_begin();

  Short uniffi_slatedb_ffi_checksum_method_db_delete();

  Short uniffi_slatedb_ffi_checksum_method_db_delete_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_flush();

  Short uniffi_slatedb_ffi_checksum_method_db_flush_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_get();

  Short uniffi_slatedb_ffi_checksum_method_db_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_db_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_merge();

  Short uniffi_slatedb_ffi_checksum_method_db_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_metrics();

  Short uniffi_slatedb_ffi_checksum_method_db_put();

  Short uniffi_slatedb_ffi_checksum_method_db_put_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_scan();

  Short uniffi_slatedb_ffi_checksum_method_db_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_db_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_db_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_db_snapshot();

  Short uniffi_slatedb_ffi_checksum_method_db_status();

  Short uniffi_slatedb_ffi_checksum_method_db_write();

  Short uniffi_slatedb_ffi_checksum_method_db_write_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_build();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_db_cache_disabled();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_merge_operator();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_seed();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_settings();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_sst_block_size();

  Short uniffi_slatedb_ffi_checksum_method_dbbuilder_with_wal_object_store();

  Short uniffi_slatedb_ffi_checksum_method_dbiterator_next();

  Short uniffi_slatedb_ffi_checksum_method_dbiterator_seek();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_get();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_scan();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbreader_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_build();

  Short uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_checkpoint_id();

  Short uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_merge_operator();

  Short uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbreaderbuilder_with_wal_object_store();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_get();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbsnapshot_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_commit();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_commit_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_delete();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_get();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_id();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_mark_read();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_merge();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_put();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_put_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_rollback();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_scan();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_seqnum();

  Short uniffi_slatedb_ffi_checksum_method_dbtransaction_unmark_write();

  Short uniffi_slatedb_ffi_checksum_method_logcallback_log();

  Short uniffi_slatedb_ffi_checksum_method_mergeoperator_merge();

  Short uniffi_slatedb_ffi_checksum_method_settings_set();

  Short uniffi_slatedb_ffi_checksum_method_settings_to_json_string();

  Short uniffi_slatedb_ffi_checksum_method_walfile_id();

  Short uniffi_slatedb_ffi_checksum_method_walfile_iterator();

  Short uniffi_slatedb_ffi_checksum_method_walfile_metadata();

  Short uniffi_slatedb_ffi_checksum_method_walfile_next_file();

  Short uniffi_slatedb_ffi_checksum_method_walfile_next_id();

  Short uniffi_slatedb_ffi_checksum_method_walfile_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_walfileiterator_next();

  Short uniffi_slatedb_ffi_checksum_method_walfileiterator_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_walreader_get();

  Short uniffi_slatedb_ffi_checksum_method_walreader_list();

  Short uniffi_slatedb_ffi_checksum_method_walreader_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_writebatch_delete();

  Short uniffi_slatedb_ffi_checksum_method_writebatch_merge();

  Short uniffi_slatedb_ffi_checksum_method_writebatch_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_writebatch_put();

  Short uniffi_slatedb_ffi_checksum_method_writebatch_put_with_options();

  Short uniffi_slatedb_ffi_checksum_constructor_dbbuilder_new();

  Short uniffi_slatedb_ffi_checksum_constructor_dbreaderbuilder_new();

  Short uniffi_slatedb_ffi_checksum_constructor_objectstore_from_env();

  Short uniffi_slatedb_ffi_checksum_constructor_objectstore_resolve();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_default();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_from_env();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_from_env_with_default();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_from_file();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_from_json_string();

  Short uniffi_slatedb_ffi_checksum_constructor_settings_load();

  Short uniffi_slatedb_ffi_checksum_constructor_walreader_new();

  Short uniffi_slatedb_ffi_checksum_constructor_writebatch_new();

  Integer ffi_slatedb_ffi_uniffi_contract_version();
}
