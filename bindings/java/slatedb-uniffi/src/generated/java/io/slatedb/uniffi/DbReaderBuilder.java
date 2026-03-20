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
 * Builder for opening a read-only [`crate::DbReader`].
 *
 * Builders are single-use: calling [`DbReaderBuilder::build`] consumes the builder.
 */
public class DbReaderBuilder implements AutoCloseable, DbReaderBuilderInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public DbReaderBuilder(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public DbReaderBuilder(NoPointer noPointer) {
    this.pointer = null;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  
    /**
     * Creates a new reader builder for `path` in `object_store`.
     */
  public DbReaderBuilder(String path, ObjectStore objectStore) {
    this((Pointer)
    UniffiHelpers.uniffiRustCall( _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_dbreaderbuilder_new(
            FfiConverterString.INSTANCE.lower(path), FfiConverterTypeObjectStore.INSTANCE.lower(objectStore), _status);
    })
    );
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
        throw new IllegalStateException("DbReaderBuilder object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("DbReaderBuilder call counter would overflow");
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
          UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_dbreaderbuilder(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_dbreaderbuilder(pointer, status);
    });
  }

  
    /**
     * Opens the reader and consumes this builder.
     */
    @Override
    
    public CompletableFuture<DbReader> build(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_build(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_uniffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDbReader.INSTANCE.lift(it),
        // Error FFI converter
        new ErrorErrorHandler()
    );
    }

  
    /**
     * Pins the reader to an existing checkpoint UUID string.
     */
    @Override
    public void withCheckpointId(String checkpointId) throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_checkpoint_id(
            it, FfiConverterString.INSTANCE.lower(checkpointId), _status);
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
     * Installs an application-defined merge operator used while reading merge rows.
     */
    @Override
    public void withMergeOperator(MergeOperator mergeOperator) throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_merge_operator(
            it, FfiConverterTypeMergeOperator.INSTANCE.lower(mergeOperator), _status);
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
     * Applies custom reader options.
     */
    @Override
    public void withOptions(ReaderOptions options) throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_options(
            it, FfiConverterTypeReaderOptions.INSTANCE.lower(options), _status);
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
     * Uses a separate object store for WAL files.
     */
    @Override
    public void withWalObjectStore(ObjectStore walObjectStore) throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_dbreaderbuilder_with_wal_object_store(
            it, FfiConverterTypeObjectStore.INSTANCE.lower(walObjectStore), _status);
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
    

  

  
}



