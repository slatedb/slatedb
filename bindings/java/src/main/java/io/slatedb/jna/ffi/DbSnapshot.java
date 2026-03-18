package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class DbSnapshot implements AutoCloseable, DbSnapshotInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public DbSnapshot(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any attempt to
   * actually use an object constructed this way will fail as there is no connected Rust object.
   */
  public DbSnapshot(NoPointer noPointer) {
    this.pointer = null;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  @Override
  public synchronized void close() {
    // Only allow a single call to this method.
    // TODO(uniffi): maybe we should log a warning if called more than once?
    if (this.wasDestroyed.compareAndSet(false, true)) {
      // This decrement always matches the initial count of 1 given at creation time.
      if (this.callCounter.decrementAndGet() == 0L) {
        cleanable.clean();
      }
    }
  }

  public <R> R callWithPointer(Function<Pointer, R> block) {
    // Check and increment the call counter, to keep the object alive.
    // This needs a compare-and-set retry loop in case of concurrent updates.
    long c;
    do {
      c = this.callCounter.get();
      if (c == 0L) {
        throw new IllegalStateException("DbSnapshot object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("DbSnapshot call counter would overflow");
      }
    } while (!this.callCounter.compareAndSet(c, c + 1L));
    // Now we can safely do the method call without the pointer being freed concurrently.
    try {
      return block.apply(this.uniffiClonePointer());
    } finally {
      // This decrement always matches the increment we performed above.
      if (this.callCounter.decrementAndGet() == 0L) {
        cleanable.clean();
      }
    }
  }

  public void callWithPointer(Consumer<Pointer> block) {
    callWithPointer(
        (Pointer p) -> {
          block.accept(p);
          return (Void) null;
        });
  }

  private class UniffiCleanAction implements Runnable {
    private final Pointer pointer;

    public UniffiCleanAction(Pointer pointer) {
      this.pointer = pointer;
    }

    @Override
    public void run() {
      if (pointer != null) {
        UniffiHelpers.uniffiRustCall(
            status -> {
              UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_dbsnapshot(pointer, status);
              return null;
            });
      }
    }
  }

  Pointer uniffiClonePointer() {
    return UniffiHelpers.uniffiRustCall(
        status -> {
          if (pointer == null) {
            throw new NullPointerException();
          }
          return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_dbsnapshot(pointer, status);
        });
  }

  @Override
  public CompletableFuture<byte[]> get(byte[] key) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get(
                  thisPtr, FfiConverterByteArray.INSTANCE.lower(key));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<KeyValue> getKeyValue(byte[] key) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_key_value(
                  thisPtr, FfiConverterByteArray.INSTANCE.lower(key));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE
                  .uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_key_value_with_options(
                      thisPtr,
                      FfiConverterByteArray.INSTANCE.lower(key),
                      FfiConverterTypeReadOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbsnapshot_get_with_options(
                  thisPtr,
                  FfiConverterByteArray.INSTANCE.lower(key),
                  FfiConverterTypeReadOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<DbIterator> scan(KeyRange range) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan(
                  thisPtr, FfiConverterTypeKeyRange.INSTANCE.lower(range));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<DbIterator> scanPrefix(byte[] prefix) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix(
                  thisPtr, FfiConverterByteArray.INSTANCE.lower(prefix));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE
                  .uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_prefix_with_options(
                      thisPtr,
                      FfiConverterByteArray.INSTANCE.lower(prefix),
                      FfiConverterTypeScanOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE
                  .uniffi_slatedb_uniffi_fn_method_dbsnapshot_scan_with_options(
                      thisPtr,
                      FfiConverterTypeKeyRange.INSTANCE.lower(range),
                      FfiConverterTypeScanOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new DbExceptionErrorHandler());
  }
}
