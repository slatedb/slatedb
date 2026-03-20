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
 * Object store handle used when opening databases, readers, and WAL readers.
 */
public class ObjectStore implements AutoCloseable, ObjectStoreInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public ObjectStore(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public ObjectStore(NoPointer noPointer) {
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
        throw new IllegalStateException("ObjectStore object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("ObjectStore call counter would overflow");
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
          UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_objectstore(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_objectstore(pointer, status);
    });
  }

  

  
    /**
     * Builds an object store from environment configuration.
     *
     * When `env_file` is provided, environment variables are loaded from that
     * file before constructing the store.
     */public static ObjectStore fromEnv(String envFile) throws Error {
            try {
                return FfiConverterTypeObjectStore.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_objectstore_from_env(
            FfiConverterOptionalString.INSTANCE.lower(envFile), _status);
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
     * Resolves an object store from a URL understood by SlateDB.
     */public static ObjectStore resolve(String url) throws Error {
            try {
                return FfiConverterTypeObjectStore.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_objectstore_resolve(
            FfiConverterString.INSTANCE.lower(url), _status);
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
    

  
  
}



