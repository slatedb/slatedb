package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Consumer;
import com.sun.jna.Pointer;
import java.util.concurrent.CompletableFuture;
/**
 * Handle to an open SlateDB database.
 *
 * Instances of this type are created by [`crate::DbBuilder::build`].
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
          UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_free_db(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_clone_db(pointer, status);
    });
  }

  
    /**
     * Begin a new transaction at the requested isolation level.
     */
    @Override
    
    public CompletableFuture<DbTransaction> begin(IsolationLevel isolationLevel){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_begin(
                thisPtr,
                FfiConverterTypeIsolationLevel.INSTANCE.lower(isolationLevel)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbTransaction.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Delete a key using default write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> delete(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_delete(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Delete a key using custom write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> deleteWithOptions(byte[] key, DbWriteOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_delete_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeDbWriteOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Flush in-memory state using the database defaults.
     */
    @Override
    
    public CompletableFuture<Void> flush(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_flush(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Flush in-memory state using explicit flush options.
     */
    @Override
    
    public CompletableFuture<Void> flushWithOptions(DbFlushOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_flush_with_options(
                thisPtr,
                FfiConverterTypeDbFlushOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Get the value for a key using default read options.
     */
    @Override
    
    public CompletableFuture<byte[]> get(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_get(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Get the full row metadata for a key using default read options.
     */
    @Override
    
    public CompletableFuture<KeyValue> getKeyValue(byte[] key){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_get_key_value(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Get the full row metadata for a key using custom read options.
     */
    @Override
    
    public CompletableFuture<KeyValue> getKeyValueWithOptions(byte[] key, DbReadOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_get_key_value_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeDbReadOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalTypeKeyValue.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Get the value for a key using custom read options.
     */
    @Override
    
    public CompletableFuture<byte[]> getWithOptions(byte[] key, DbReadOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_get_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterTypeDbReadOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterOptionalByteArray.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Merge an operand into a key using default options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> merge(byte[] key, byte[] operand){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_merge(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Merge an operand into a key using custom merge and write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> mergeWithOptions(byte[] key, byte[] operand, DbMergeOptions mergeOptions, DbWriteOptions writeOptions){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_merge_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand), FfiConverterTypeDbMergeOptions.INSTANCE.lower(mergeOptions), FfiConverterTypeDbWriteOptions.INSTANCE.lower(writeOptions)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Put a value for a key using default options.
     *
     * ## Errors
     * - `SlatedbError::Invalid`: if the key is empty or exceeds SlateDB limits.
     */
    @Override
    
    public CompletableFuture<WriteHandle> put(byte[] key, byte[] value){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_put(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Put a value for a key using custom put and write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> putWithOptions(byte[] key, byte[] value, DbPutOptions putOptions, DbWriteOptions writeOptions){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_put_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value), FfiConverterTypeDbPutOptions.INSTANCE.lower(putOptions), FfiConverterTypeDbWriteOptions.INSTANCE.lower(writeOptions)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Scan a key range using default scan options.
     */
    @Override
    
    public CompletableFuture<DbIterator> scan(DbKeyRange range){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_scan(
                thisPtr,
                FfiConverterTypeDbKeyRange.INSTANCE.lower(range)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Scan all keys that share the provided prefix.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanPrefix(byte[] prefix){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_scan_prefix(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(prefix)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Scan all keys that share the provided prefix using custom scan options.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanPrefixWithOptions(byte[] prefix, DbScanOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_scan_prefix_with_options(
                thisPtr,
                FfiConverterByteArray.INSTANCE.lower(prefix), FfiConverterTypeDbScanOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Scan a key range using custom scan options.
     */
    @Override
    
    public CompletableFuture<DbIterator> scanWithOptions(DbKeyRange range, DbScanOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_scan_with_options(
                thisPtr,
                FfiConverterTypeDbKeyRange.INSTANCE.lower(range), FfiConverterTypeDbScanOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbIterator.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Close the database.
     */
    @Override
    
    public CompletableFuture<Void> shutdown(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_shutdown(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_void(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_void(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_void(future),
        // lift function
        () -> {},
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Create a point-in-time snapshot of the database.
     */
    @Override
    
    public CompletableFuture<DbSnapshot> snapshot(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_snapshot(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbSnapshot.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Check whether the database is still open.
     *
     * ## Returns
     * - `Result<(), SlatedbError>`: `Ok(())` if the database is open.
     */
    @Override
    public void status() throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_status(
            it, _status);
    });
    
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    })
    ;
            } catch (RuntimeException _e) {
                
                if (SlatedbException.class.isInstance(_e.getCause())) {
                    throw (SlatedbException)_e.getCause();
                }
                
                if (InternalException.class.isInstance(_e.getCause())) {
                    throw (InternalException)_e.getCause();
                }
                throw _e;
            }
    }
    

  
    /**
     * Apply a batch of operations atomically using default write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> write(List<DbWriteOperation> operations){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_write(
                thisPtr,
                FfiConverterSequenceTypeDbWriteOperation.INSTANCE.lower(operations)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Apply a batch of operations atomically using custom write options.
     */
    @Override
    
    public CompletableFuture<WriteHandle> writeWithOptions(List<DbWriteOperation> operations, DbWriteOptions options){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_db_write_with_options(
                thisPtr,
                FfiConverterSequenceTypeDbWriteOperation.INSTANCE.lower(operations), FfiConverterTypeDbWriteOptions.INSTANCE.lower(options)
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_rust_buffer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_rust_buffer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_rust_buffer(future),
        // lift function
        (it) -> FfiConverterTypeWriteHandle.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  

  
}



