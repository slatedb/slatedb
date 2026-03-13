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
 * A mutable batch of write operations that can be written atomically.
 */
public class WriteBatch implements AutoCloseable, WriteBatchInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public WriteBatch(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public WriteBatch(NoPointer noPointer) {
    this.pointer = null;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  
    /**
     * Create a new empty write batch.
     */
  public WriteBatch() {
    this((Pointer)
    UniffiHelpers.uniffiRustCall( _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_writebatch_new(
            _status);
    })
    );
  }
  public synchronized void destroy() {
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
        throw new IllegalStateException("WriteBatch object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("WriteBatch call counter would overflow");
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
          UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_free_writebatch(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_clone_writebatch(pointer, status);
    });
  }

  
    /**
     * Explicitly close the batch handle.
     */
    @Override
    public void close() throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_close(
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
     * Append a delete operation.
     */
    @Override
    public void delete(byte[] key) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_delete(
            it, FfiConverterByteArray.INSTANCE.lower(key), _status);
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
     * Append a merge operation using default merge options.
     */
    @Override
    public void merge(byte[] key, byte[] operand) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_merge(
            it, FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand), _status);
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
     * Append a merge operation using explicit merge options.
     */
    @Override
    public void mergeWithOptions(byte[] key, byte[] operand, DbMergeOptions options) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_merge_with_options(
            it, FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(operand), FfiConverterTypeDbMergeOptions.INSTANCE.lower(options), _status);
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
     * Append a put operation using default put options.
     */
    @Override
    public void put(byte[] key, byte[] value) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_put(
            it, FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value), _status);
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
     * Append a put operation using explicit put options.
     */
    @Override
    public void putWithOptions(byte[] key, byte[] value, DbPutOptions options) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_writebatch_put_with_options(
            it, FfiConverterByteArray.INSTANCE.lower(key), FfiConverterByteArray.INSTANCE.lower(value), FfiConverterTypeDbPutOptions.INSTANCE.lower(options), _status);
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
    

  

  
}



