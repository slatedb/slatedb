package io.slatedb.jna.ffi;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

// A JNA Library to expose the extern-C FFI definitions.
// This is an implementation detail which will be called internally by the public API.
interface UniffiLib extends Library {
  UniffiLib INSTANCE = UniffiLibInitializer.load();

  // The Cleaner for the whole library
  static UniffiCleaner CLEANER = UniffiCleaner.create();

  Pointer uniffi_slatedb_ffi_fn_clone_ffidb(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidb(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidb_begin(Pointer ptr, RustBuffer.ByValue isolationLevel);

  Long uniffi_slatedb_ffi_fn_method_ffidb_delete(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidb_delete_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_flush(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffidb_flush_with_options(
      Pointer ptr, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidb_get_key_value(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidb_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_merge(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand);

  Long uniffi_slatedb_ffi_fn_method_ffidb_merge_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      RustBuffer.ByValue mergeOptions,
      RustBuffer.ByValue writeOptions);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_ffidb_metrics(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidb_put(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value);

  Long uniffi_slatedb_ffi_fn_method_ffidb_put_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      RustBuffer.ByValue putOptions,
      RustBuffer.ByValue writeOptions);

  Long uniffi_slatedb_ffi_fn_method_ffidb_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_ffidb_scan_prefix(Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_ffidb_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidb_shutdown(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffidb_snapshot(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_ffidb_status(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidb_write(Pointer ptr, Pointer batch);

  Long uniffi_slatedb_ffi_fn_method_ffidb_write_with_options(
      Pointer ptr, Pointer batch, RustBuffer.ByValue options);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbbuilder(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbbuilder(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffidbbuilder_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbbuilder_build(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_db_cache_disabled(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_merge_operator(
      Pointer ptr, Pointer mergeOperator, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_seed(
      Pointer ptr, Long seed, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_settings(
      Pointer ptr, Pointer settings, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_sst_block_size(
      Pointer ptr, RustBuffer.ByValue sstBlockSize, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbbuilder_with_wal_object_store(
      Pointer ptr, Pointer walObjectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbiterator(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbiterator_next(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffidbiterator_seek(Pointer ptr, RustBuffer.ByValue key);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbreader(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix(Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbreader_shutdown(Pointer ptr);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbreaderbuilder(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbreaderbuilder(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffidbreaderbuilder_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_build(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_checkpoint_id(
      Pointer ptr, RustBuffer.ByValue checkpointId, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_merge_operator(
      Pointer ptr, Pointer mergeOperator, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_options(
      Pointer ptr, RustBuffer.ByValue options, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffidbreaderbuilder_with_wal_object_store(
      Pointer ptr, Pointer walObjectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbsnapshot(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbsnapshot(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_key_value(
      Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_prefix(
      Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbsnapshot_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Pointer uniffi_slatedb_ffi_fn_clone_ffidbtransaction(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffidbtransaction(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_commit(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_commit_with_options(
      Pointer ptr, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_delete(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_get(Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_key_value(
      Pointer ptr, RustBuffer.ByValue key);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_key_value_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_get_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue options);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_ffidbtransaction_id(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_mark_read(
      Pointer ptr, RustBuffer.ByValue keys);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_merge(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_merge_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue operand, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_put(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_put_with_options(
      Pointer ptr, RustBuffer.ByValue key, RustBuffer.ByValue value, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_rollback(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan(Pointer ptr, RustBuffer.ByValue range);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_prefix(
      Pointer ptr, RustBuffer.ByValue prefix);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_prefix_with_options(
      Pointer ptr, RustBuffer.ByValue prefix, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_scan_with_options(
      Pointer ptr, RustBuffer.ByValue range, RustBuffer.ByValue options);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_seqnum(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffidbtransaction_unmark_write(
      Pointer ptr, RustBuffer.ByValue keys);

  Pointer uniffi_slatedb_ffi_fn_clone_ffilogcallback(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffilogcallback(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_init_callback_vtable_ffilogcallback(
      UniffiVTableCallbackInterfaceFfiLogCallback vtable);

  void uniffi_slatedb_ffi_fn_method_ffilogcallback_log(
      Pointer ptr, RustBuffer.ByValue record, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffimergeoperator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffimergeoperator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_init_callback_vtable_ffimergeoperator(
      UniffiVTableCallbackInterfaceFfiMergeOperator vtable);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_ffimergeoperator_merge(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue existingValue,
      RustBuffer.ByValue operand,
      UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffiobjectstore(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffiobjectstore(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffiobjectstore_from_env(
      RustBuffer.ByValue envFile, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffiobjectstore_resolve(
      RustBuffer.ByValue url, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffisettings(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffisettings(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_default(
      UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env(
      RustBuffer.ByValue prefix, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env_with_default(
      RustBuffer.ByValue prefix, Pointer defaultSettings, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_from_file(
      RustBuffer.ByValue path, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_from_json_string(
      RustBuffer.ByValue json, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffisettings_load(UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffisettings_set(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue valueJson,
      UniffiRustCallStatus uniffi_out_errmk);

  RustBuffer.ByValue uniffi_slatedb_ffi_fn_method_ffisettings_to_json_string(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffiwalfile(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffiwalfile(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffiwalfile_id(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffiwalfile_iterator(Pointer ptr);

  Long uniffi_slatedb_ffi_fn_method_ffiwalfile_metadata(Pointer ptr);

  Pointer uniffi_slatedb_ffi_fn_method_ffiwalfile_next_file(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffiwalfile_next_id(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwalfile_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffiwalfileiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffiwalfileiterator(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffiwalfileiterator_next(Pointer ptr);

  void uniffi_slatedb_ffi_fn_method_ffiwalfileiterator_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffiwalreader(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffiwalreader(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffiwalreader_new(
      RustBuffer.ByValue path, Pointer objectStore, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_method_ffiwalreader_get(
      Pointer ptr, Long id, UniffiRustCallStatus uniffi_out_errmk);

  Long uniffi_slatedb_ffi_fn_method_ffiwalreader_list(
      Pointer ptr, RustBuffer.ByValue startId, RustBuffer.ByValue endId);

  void uniffi_slatedb_ffi_fn_method_ffiwalreader_shutdown(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_clone_ffiwritebatch(
      Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_free_ffiwritebatch(Pointer ptr, UniffiRustCallStatus uniffi_out_errmk);

  Pointer uniffi_slatedb_ffi_fn_constructor_ffiwritebatch_new(
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwritebatch_delete(
      Pointer ptr, RustBuffer.ByValue key, UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwritebatch_merge(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwritebatch_merge_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue operand,
      RustBuffer.ByValue options,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwritebatch_put(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_method_ffiwritebatch_put_with_options(
      Pointer ptr,
      RustBuffer.ByValue key,
      RustBuffer.ByValue value,
      RustBuffer.ByValue options,
      UniffiRustCallStatus uniffi_out_errmk);

  void uniffi_slatedb_ffi_fn_func_ffi_init_logging(
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

  Short uniffi_slatedb_ffi_checksum_func_ffi_init_logging();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_begin();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_delete();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_delete_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_flush();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_flush_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_get();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_merge();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_metrics();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_put();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_put_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_scan();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_snapshot();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_status();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_write();

  Short uniffi_slatedb_ffi_checksum_method_ffidb_write_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_build();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_db_cache_disabled();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_merge_operator();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_seed();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_settings();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_sst_block_size();

  Short uniffi_slatedb_ffi_checksum_method_ffidbbuilder_with_wal_object_store();

  Short uniffi_slatedb_ffi_checksum_method_ffidbiterator_next();

  Short uniffi_slatedb_ffi_checksum_method_ffidbiterator_seek();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_get();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_scan();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreader_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_build();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_checkpoint_id();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_merge_operator();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbreaderbuilder_with_wal_object_store();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbsnapshot_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_commit_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_delete();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_key_value_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_get_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_id();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_mark_read();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_put_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_rollback();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_prefix_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_scan_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_seqnum();

  Short uniffi_slatedb_ffi_checksum_method_ffidbtransaction_unmark_write();

  Short uniffi_slatedb_ffi_checksum_method_ffilogcallback_log();

  Short uniffi_slatedb_ffi_checksum_method_ffimergeoperator_merge();

  Short uniffi_slatedb_ffi_checksum_method_ffisettings_set();

  Short uniffi_slatedb_ffi_checksum_method_ffisettings_to_json_string();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_id();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_iterator();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_metadata();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_file();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_next_id();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfile_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_next();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalfileiterator_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalreader_get();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalreader_list();

  Short uniffi_slatedb_ffi_checksum_method_ffiwalreader_shutdown();

  Short uniffi_slatedb_ffi_checksum_method_ffiwritebatch_delete();

  Short uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge();

  Short uniffi_slatedb_ffi_checksum_method_ffiwritebatch_merge_with_options();

  Short uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put();

  Short uniffi_slatedb_ffi_checksum_method_ffiwritebatch_put_with_options();

  Short uniffi_slatedb_ffi_checksum_constructor_ffidbbuilder_new();

  Short uniffi_slatedb_ffi_checksum_constructor_ffidbreaderbuilder_new();

  Short uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_from_env();

  Short uniffi_slatedb_ffi_checksum_constructor_ffiobjectstore_resolve();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_default();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_env_with_default();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_file();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_from_json_string();

  Short uniffi_slatedb_ffi_checksum_constructor_ffisettings_load();

  Short uniffi_slatedb_ffi_checksum_constructor_ffiwalreader_new();

  Short uniffi_slatedb_ffi_checksum_constructor_ffiwritebatch_new();

  Integer ffi_slatedb_ffi_uniffi_contract_version();
}
