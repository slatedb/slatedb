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
 * Mutable database settings object used to configure a [`crate::DbBuilder`].
 */
public class Settings implements AutoCloseable, SettingsInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public Settings(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any
   * attempt to actually use an object constructed this way will fail as there is no
   * connected Rust object.
   */
  public Settings(NoPointer noPointer) {
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
        throw new IllegalStateException("Settings object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("Settings call counter would overflow");
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
          UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_free_settings(pointer, status);
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
      return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_clone_settings(pointer, status);
    });
  }

  
    /**
     * Sets a settings field by dotted path using a JSON literal value.
     *
     * `key` identifies the field to update. Use `.` to address nested objects,
     * for example `compactor_options.max_sst_size` or
     * `object_store_cache_options.root_folder`.
     *
     * `value_json` must be a valid JSON literal matching the target field's
     * expected type. That means strings must be quoted JSON strings, numbers
     * should be passed as JSON numbers, booleans as `true`/`false`, and
     * optional fields can be cleared with `null`.
     *
     * Missing or `null` intermediate objects in the dotted path are created
     * automatically. If the update would produce an invalid `slatedb::Settings`
     * value, the method returns an error and leaves the current settings
     * unchanged.
     *
     * Examples:
     *
     * - `set("flush_interval", "\"250ms\"")`
     * - `set("default_ttl", "42")`
     * - `set("default_ttl", "null")`
     * - `set("compactor_options.max_sst_size", "33554432")`
     * - `set("object_store_cache_options.root_folder", "\"/tmp/slatedb-cache\"")`
     */
    @Override
    public void set(String key, String valueJson) throws Error {
            try {
                
    callWithPointer(it -> {
        try {
    
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_settings_set(
            it, FfiConverterString.INSTANCE.lower(key), FfiConverterString.INSTANCE.lower(valueJson), _status);
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
     * Serializes the current settings value to a JSON string.
     */
    @Override
    public String toJsonString() throws Error {
            try {
                return FfiConverterString.INSTANCE.lift(
    callWithPointer(it -> {
        try {
    
            return
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_method_settings_to_json_string(
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
     * Creates a settings object populated with SlateDB defaults.
     */public static Settings _default()  {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCall( _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_default(
            _status);
    })
    );
            } catch (RuntimeException _e) {
                
                
                if (InternalException.class.isInstance(_e.getCause())) {
                    throw (InternalException)_e.getCause();
                }
                throw _e;
            }
    }
    

  
    /**
     * Loads settings from environment variables using `prefix`.
     */public static Settings fromEnv(String prefix) throws Error {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_from_env(
            FfiConverterString.INSTANCE.lower(prefix), _status);
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
     * Loads settings from environment variables, falling back to `default_settings`.
     */public static Settings fromEnvWithDefault(String prefix, Settings defaultSettings) throws Error {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_from_env_with_default(
            FfiConverterString.INSTANCE.lower(prefix), FfiConverterTypeSettings.INSTANCE.lower(defaultSettings), _status);
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
     * Loads settings from a JSON, TOML, or YAML file based on its extension.
     */public static Settings fromFile(String path) throws Error {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_from_file(
            FfiConverterString.INSTANCE.lower(path), _status);
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
     * Parses settings from a JSON string.
     */public static Settings fromJsonString(String json) throws Error {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_from_json_string(
            FfiConverterString.INSTANCE.lower(json), _status);
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
     * Loads settings from SlateDB's default file and environment lookup order.
     */public static Settings load() throws Error {
            try {
                return FfiConverterTypeSettings.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_constructor_settings_load(
            _status);
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



