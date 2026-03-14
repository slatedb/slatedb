package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class FfiDbReader implements AutoCloseable, FfiDbReaderInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public FfiDbReader(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any attempt to
   * actually use an object constructed this way will fail as there is no connected Rust object.
   */
  public FfiDbReader(NoPointer noPointer) {
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
        throw new IllegalStateException("FfiDbReader object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("FfiDbReader call counter would overflow");
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
              UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_free_ffidbreader(pointer, status);
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
          return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_clone_ffidbreader(pointer, status);
        });
  }

  @Override
  public CompletableFuture<byte[]> get(byte[] key) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_get(
                  thisPtr, FfiConverterByteArray.INSTANCE.lower(key));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<byte[]> getWithOptions(byte[] key, FfiReadOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_get_with_options(
                  thisPtr,
                  FfiConverterByteArray.INSTANCE.lower(key),
                  FfiConverterTypeFfiReadOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(
                future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<FfiDbIterator> scan(FfiKeyRange range) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_scan(
                  thisPtr, FfiConverterTypeFfiKeyRange.INSTANCE.lower(range));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeFfiDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<FfiDbIterator> scanPrefix(byte[] prefix) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix(
                  thisPtr, FfiConverterByteArray.INSTANCE.lower(prefix));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeFfiDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<FfiDbIterator> scanPrefixWithOptions(
      byte[] prefix, FfiScanOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE
                  .uniffi_slatedb_ffi_fn_method_ffidbreader_scan_prefix_with_options(
                      thisPtr,
                      FfiConverterByteArray.INSTANCE.lower(prefix),
                      FfiConverterTypeFfiScanOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeFfiDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<FfiDbIterator> scanWithOptions(
      FfiKeyRange range, FfiScanOptions options) {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_scan_with_options(
                  thisPtr,
                  FfiConverterTypeFfiKeyRange.INSTANCE.lower(range),
                  FfiConverterTypeFfiScanOptions.INSTANCE.lower(options));
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeFfiDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(
            thisPtr -> {
              return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffidbreader_shutdown(thisPtr);
            }),
        (future, callback, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_void(
                future, callback, continuation),
        (future, continuation) ->
            UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new FfiExceptionErrorHandler());
  }
}
