package io.slatedb.jna.ffi;


public class Slatedb {
  public static void initLogging(LogLevel level, LogCallback callback) throws DbException {
    try {

      UniffiHelpers.uniffiRustCallWithError(
          new DbExceptionErrorHandler(),
          _status -> {
            UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_func_init_logging(
                FfiConverterTypeLogLevel.INSTANCE.lower(level),
                FfiConverterOptionalTypeLogCallback.INSTANCE.lower(callback),
                _status);
          });
    } catch (RuntimeException _e) {

      if (DbException.class.isInstance(_e.getCause())) {
        throw (DbException) _e.getCause();
      }

      if (InternalException.class.isInstance(_e.getCause())) {
        throw (InternalException) _e.getCause();
      }
      throw _e;
    }
  }
}
