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
 * Builder used to configure and open a [`Db`].
 */
public class DbBuilder implements AutoCloseable, DbBuilderInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public DbBuilder(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public DbBuilder(NoPointer noPointer) {
    this.pointer = null;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  
    /**
     * Create a new builder for a database.
     *
     * ## Arguments
     * - `path`: the database path within the object store.
     * - `object_store`: the object store that will back the database.
     *
     * ## Returns
     * - `Arc<DbBuilder>`: a new builder instance.
     */
  public DbBuilder(String path, ObjectStore objectStore) {
    this((Pointer)
    UniffiHelpers.uniffiRustCall( _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_dbbuilder_new(
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
        throw new IllegalStateException("DbBuilder object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("DbBuilder call counter would overflow");
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
          UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_free_dbbuilder(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_clone_dbbuilder(pointer, status);
    });
  }

  
    /**
     * Open the database using the builder's current configuration.
     *
     * This consumes the builder state. Reusing the same builder after a
     * successful or failed call to `build()` returns an error.
     *
     * ## Returns
     * - `Result<Arc<Db>, SlatedbError>`: the opened database handle.
     *
     * ## Errors
     * - `SlatedbError`: if the builder was already consumed or the database cannot be opened.
     */
    @Override
    
    public CompletableFuture<Db> build(){
        return UniffiAsyncHelpers.uniffiRustCallAsync(
        callWithPointer(thisPtr -> {
            return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_build(
                thisPtr
                
            );
        }),
        (future, callback, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_poll_pointer(future, callback, continuation),
        (future, continuation) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_complete_pointer(future, continuation),
        (future) -> UniffiLib.INSTANCE.ffi_slatedb_ffi_rust_future_free_pointer(future),
        // lift function
        (it) -> FfiConverterTypeDb.INSTANCE.lift(it),
        // Error FFI converter
        new SlatedbExceptionErrorHandler()
    );
    }

  
    /**
     * Disable the database-level cache created by the builder.
     */
    @Override
    public void withDbCacheDisabled() throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_db_cache_disabled(
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
     * Configure the merge operator used for merge reads and writes.
     *
     * ## Arguments
     * - `merge_operator`: the callback implementation to use.
     */
    @Override
    public void withMergeOperator(MergeOperator mergeOperator) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_merge_operator(
            it, FfiConverterTypeMergeOperator.INSTANCE.lower(mergeOperator), _status);
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
     * Set the random seed used by the database.
     *
     * ## Arguments
     * - `seed`: the seed to use when constructing the database.
     */
    @Override
    public void withSeed(Long seed) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_seed(
            it, FfiConverterLong.INSTANCE.lower(seed), _status);
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
     * Replace the default database settings with a JSON-encoded [`slatedb::Settings`] document.
     *
     * ## Arguments
     * - `settings_json`: the full settings document encoded as JSON.
     *
     * ## Errors
     * - `SlatedbError::Invalid`: if the JSON cannot be parsed.
     */
    @Override
    public void withSettingsJson(String settingsJson) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_settings_json(
            it, FfiConverterString.INSTANCE.lower(settingsJson), _status);
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
     * Override the SST block size used for new SSTs.
     *
     * ## Arguments
     * - `sst_block_size`: the block size to use.
     */
    @Override
    public void withSstBlockSize(SstBlockSize sstBlockSize) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_sst_block_size(
            it, FfiConverterTypeSstBlockSize.INSTANCE.lower(sstBlockSize), _status);
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
     * Configure a separate object store for WAL data.
     *
     * ## Arguments
     * - `wal_object_store`: the object store to use for WAL files.
     */
    @Override
    public void withWalObjectStore(ObjectStore walObjectStore) throws SlatedbException {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_dbbuilder_with_wal_object_store(
            it, FfiConverterTypeObjectStore.INSTANCE.lower(walObjectStore), _status);
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



