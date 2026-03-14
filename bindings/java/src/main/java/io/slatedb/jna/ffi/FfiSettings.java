package io.slatedb.jna.ffi;

import com.sun.jna.Pointer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class FfiSettings implements AutoCloseable, FfiSettingsInterface {
  protected Pointer pointer;
  protected UniffiCleaner.Cleanable cleanable;

  private AtomicBoolean wasDestroyed = new AtomicBoolean(false);
  private AtomicLong callCounter = new AtomicLong(1);

  public FfiSettings(Pointer pointer) {
    this.pointer = pointer;
    this.cleanable = UniffiLib.CLEANER.register(this, new UniffiCleanAction(pointer));
  }

  /**
   * This constructor can be used to instantiate a fake object. Only used for tests. Any attempt to
   * actually use an object constructed this way will fail as there is no connected Rust object.
   */
  public FfiSettings(NoPointer noPointer) {
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
        throw new IllegalStateException("FfiSettings object has already been destroyed");
      }
      if (c == Long.MAX_VALUE) {
        throw new IllegalStateException("FfiSettings call counter would overflow");
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
              UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_free_ffisettings(pointer, status);
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
          return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_clone_ffisettings(pointer, status);
        });
  }

  /**
   * Sets a settings field by dotted path using a JSON literal value.
   *
   * <p>`key` identifies the field to update. Use `.` to address nested objects, for example
   * `compactor_options.max_sst_size` or `object_store_cache_options.root_folder`.
   *
   * <p>`value_json` must be a valid JSON literal matching the target field's expected type. That
   * means strings must be quoted JSON strings, numbers should be passed as JSON numbers, booleans
   * as `true`/`false`, and optional fields can be cleared with `null`.
   *
   * <p>Missing or `null` intermediate objects in the dotted path are created automatically. If the
   * update would produce an invalid `slatedb::Settings` value, the method returns an error and
   * leaves the current settings unchanged.
   *
   * <p>Examples:
   *
   * <p>- `set("flush_interval", "\"250ms\"")` - `set("default_ttl", "42")` - `set("default_ttl",
   * "null")` - `set("compactor_options.max_sst_size", "33554432")` -
   * `set("object_store_cache_options.root_folder", "\"/tmp/slatedb-cache\"")`
   */
  @Override
  public void set(String key, String valueJson) throws FfiException {
    try {

      callWithPointer(
          it -> {
            try {

              UniffiHelpers.uniffiRustCallWithError(
                  new FfiExceptionErrorHandler(),
                  _status -> {
                    UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_method_ffisettings_set(
                        it,
                        FfiConverterString.INSTANCE.lower(key),
                        FfiConverterString.INSTANCE.lower(valueJson),
                        _status);
                  });

            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  @Override
  public String toJsonString() throws FfiException {
    try {
      return FfiConverterString.INSTANCE.lift(
          callWithPointer(
              it -> {
                try {

                  return UniffiHelpers.uniffiRustCallWithError(
                      new FfiExceptionErrorHandler(),
                      _status -> {
                        return UniffiLib.INSTANCE
                            .uniffi_slatedb_ffi_fn_method_ffisettings_to_json_string(it, _status);
                      });

                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings _default() {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCall(
              _status -> {
                return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_ffisettings_default(
                    _status);
              }));
    } catch (RuntimeException _e) {

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings fromEnv(String prefix) throws FfiException {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCallWithError(
              new FfiExceptionErrorHandler(),
              _status -> {
                return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env(
                    FfiConverterString.INSTANCE.lower(prefix), _status);
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings fromEnvWithDefault(String prefix, FfiSettings defaultSettings)
      throws FfiException {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCallWithError(
              new FfiExceptionErrorHandler(),
              _status -> {
                return UniffiLib.INSTANCE
                    .uniffi_slatedb_ffi_fn_constructor_ffisettings_from_env_with_default(
                        FfiConverterString.INSTANCE.lower(prefix),
                        FfiConverterTypeFfiSettings.INSTANCE.lower(defaultSettings),
                        _status);
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings fromFile(String path) throws FfiException {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCallWithError(
              new FfiExceptionErrorHandler(),
              _status -> {
                return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_ffisettings_from_file(
                    FfiConverterString.INSTANCE.lower(path), _status);
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings fromJsonString(String json) throws FfiException {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCallWithError(
              new FfiExceptionErrorHandler(),
              _status -> {
                return UniffiLib.INSTANCE
                    .uniffi_slatedb_ffi_fn_constructor_ffisettings_from_json_string(
                        FfiConverterString.INSTANCE.lower(json), _status);
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }

  public static FfiSettings load() throws FfiException {
    try {
      return FfiConverterTypeFfiSettings.INSTANCE.lift(
          UniffiHelpers.uniffiRustCallWithError(
              new FfiExceptionErrorHandler(),
              _status -> {
                return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_constructor_ffisettings_load(
                    _status);
              }));
    } catch (RuntimeException _e) {

      if (FfiException.class.isInstance(_e.getCause())) {
        throw (FfiException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }
}
