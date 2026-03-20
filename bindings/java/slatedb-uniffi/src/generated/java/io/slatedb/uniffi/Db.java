package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Consumer;
import com.sun.jna.Pointer;
import java.util.concurrent.CompletableFuture;
/**
 * A writable SlateDB handle.
 */
public class Db implements AutoCloseable, DbInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public Db(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public Db(NoPointer noPointer) {
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
        throw new IllegalStateException("Db object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("Db call counter would overflow");
      }
    } while (! this.callCounter.compareAndSet(c, c + 1L));
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
    callWithPointer((Pointer p) -> {
      block.accept(p);
      return (Void)null;
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
        UniffiHelpers.uniffiRustCall(status -> {
          UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_db(pointer, status);
          return null;
        });
      }
    }
  }

  Pointer uniffiClonePointer() {
    return UniffiHelpers.uniffiRustCall(status -> {
      if (pointer == null) {
        throw new NullPointerException();
      }
      return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_db(pointer, status);
    });
  }

  
    /**
     * Starts a transaction at the requested isolation level.
     */
    @Override
    
    public CompletableFuture<DbTransaction> begin(IsolationLevel isolationLevel){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_begin(
                thisPtr,
                FfiConverterTypeIsolationLevel.INSTANCE.lower(isolationLevel)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbTransaction.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Deletes `key` and returns metadata for the write.
     */
    @Override
    
    public CompletableFuture<WriteHandle> delete(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_delete(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Deletes `key` using custom write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> deleteWithOptions(byte[] key, WriteOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_delete_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeWriteOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Flushes the default storage layer.
     */
    @Override
    
    public CompletableFuture<Void> flush(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_flush(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Flushes according to the provided flush options.
     */
    @Override
    
    public CompletableFuture<Void> flushWithOptions(FlushOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_flush_with_options(
                thisPtr,
                FfiConverterTypeFlushOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Reads the current value for `key`.
     */
    @Override
    
    public CompletableFuture<byte[]> get(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_get(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Reads the current row version for `key`, including metadata.
     */
    @Override
    
    public CompletableFuture<KeyValue> getKeyValue(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_get_key_value(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Reads the current row version for `key` using custom read options.
     */
    @Override
    
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, ReadOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_get_key_value_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeReadOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Reads the current value for `key` using custom read options.
     */
    @Override
    
    public CompletableFuture<byte[]> getWithOptions(byte[] key, ReadOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_get_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeReadOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Appends a merge operand for `key` and returns metadata for the write.
     */
    @Override
    
    public CompletableFuture<WriteHandle> merge(byte[] key, byte[] operand){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_merge(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Appends a merge operand using custom merge and write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> mergeWithOptions(byte[] key, byte[] operand, MergeOptions mergeOptions, WriteOptions writeOptions){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_merge_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand), FfiConverterTypeMergeOptions.INSTANCE.lower(mergeOptions), FfiConverterTypeWriteOptions.INSTANCE.lower(writeOptions)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Returns a snapshot of the current integer metrics registry.
     */
    @Override
    public Map<String, Long> metrics() throws Error {
            try {
                return FfiConverterMapStringLong.INSTANCE.lift(
    callWithPointer(it -> {
        try {
    
            return
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_metrics(
            it, _status);
    });
    
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    })
    );
            } catch (RuntimeException _e) {
                
                if (Error.class.isInstance(_e.getCause())) {
                    throw (Error)_e.getCause();
                }
                
                if (InternalException.class.isInstance(_e.getCause())) {
                    throw (InternalException)_e.getCause();
                }
                throw _e;
            }
    }
    

  
    /**
     * Inserts or overwrites a value and returns metadata for the write.
     *
     * Keys must be non-empty and at most `u16::MAX` bytes. Values must be at
     * most `u32::MAX` bytes.
     */
    @Override
    
    public CompletableFuture<WriteHandle> put(byte[] key, byte[] value){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_put(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Inserts or overwrites a value using custom put and write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> putWithOptions(byte[] key, byte[] value, PutOptions putOptions, WriteOptions writeOptions){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_put_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value), FfiConverterTypePutOptions.INSTANCE.lower(putOptions), FfiConverterTypeWriteOptions.INSTANCE.lower(writeOptions)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Scans rows inside `range`.
     */
    @Override
    
    public CompletableFuture<DbIterator> scan(KeyRange range){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_scan(
                thisPtr,
                FfiConverterTypeKeyRange.INSTANCE.lower(range)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Scans rows whose keys start with `prefix`.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_scan_prefix(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(prefix)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Scans rows whose keys start with `prefix` using custom scan options.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, ScanOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_scan_prefix_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(prefix), FfiConverterTypeScanOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Scans rows inside `range` using custom scan options.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanWithOptions(KeyRange range, ScanOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_scan_with_options(
                thisPtr,
                FfiConverterTypeKeyRange.INSTANCE.lower(range), FfiConverterTypeScanOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Flushes outstanding work and closes the database.
     */
    @Override
    
    public CompletableFuture<Void> shutdown(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_shutdown(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Creates a read-only snapshot representing a consistent point in time.
     */
    @Override
    
    public CompletableFuture<DbSnapshot> snapshot(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_snapshot(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbSnapshot.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Returns an error if the database is not currently healthy and open.
     */
    @Override
    public void status() throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_status(
            it, _status);
    });
    
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    })
    ;
            } catch (RuntimeException _e) {
                
                if (Error.class.isInstance(_e.getCause())) {
                    throw (Error)_e.getCause();
                }
                
                if (InternalException.class.isInstance(_e.getCause())) {
                    throw (InternalException)_e.getCause();
                }
                throw _e;
            }
    }
    

  
    /**
     * Applies all operations in `batch` atomically.
     *
     * The provided batch is consumed and cannot be reused afterwards.
     */
    @Override
    
    public CompletableFuture<WriteHandle> write(WriteBatch batch){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_write(
                thisPtr,
                FfiConverterTypeWriteBatch.INSTANCE.lower(batch)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Applies all operations in `batch` atomically using custom write options.
     *
     * The provided batch is consumed and cannot be reused afterwards.
     */
    @Override
    
    public CompletableFuture<WriteHandle> writeWithOptions(WriteBatch batch, WriteOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_db_write_with_options(
                thisPtr,
                FfiConverterTypeWriteBatch.INSTANCE.lower(batch), FfiConverterTypeWriteOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  

  
}



