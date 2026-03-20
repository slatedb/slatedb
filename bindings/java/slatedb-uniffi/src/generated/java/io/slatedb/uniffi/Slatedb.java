package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
public class Slatedb {
  
    /**
     * Installs SlateDB logging exactly once for the current process.
     *
     * If `callback` is provided, log records are forwarded to it. Otherwise logs
     * are written to standard error using the default tracing formatter.
     */public static void initLogging(LogLevel level, LogCallback callback) throws Error {
            try {
                
    UniffiHelpers.uniffiRustCallWithError(new ErrorErrorHandler(), _status -> {
        UniffiLib.INSTANCE.uniffi_slatedb_uniffi_fn_func_init_logging(
            FfiConverterTypeLogLevel.INSTANCE.lower(level), FfiConverterOptionalTypeLogCallback.INSTANCE.lower(callback), _status);
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

