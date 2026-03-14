package io.slatedb.jna.ffi;


public class Slatedb {
  public static void ffiInitLogging(FfiLogLevel level, FfiLogCallback callback)
      throws FfiException {
    try {

      UniffiHelpers.uniffiRustCallWithError(
          new FfiExceptionErrorHandler(),
          _status -> {
            UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_func_ffi_init_logging(
                FfiConverterTypeFfiLogLevel.INSTANCE.lower(level),
                FfiConverterOptionalTypeFfiLogCallback.INSTANCE.lower(callback),
                _status);
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
}
