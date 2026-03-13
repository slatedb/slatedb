package io.slatedb;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
public class Slatedb {
  
    /**
     * Return the default [`slatedb::Settings`] value as JSON.
     *
     * This is useful for FFI callers that want to start from the Rust default
     * configuration, modify selected fields, and pass the full JSON document back
     * to [`crate::DbBuilder::with_settings_json`].
     *
     * ## Returns
     * - `Result<String, SlatedbError>`: the default settings encoded as JSON.
     */public static String defaultSettingsJson() throws SlatedbException {
            try {
                return FfiConverterString.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_func_default_settings_json(
            _status);
    })
    );
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
     * Resolve an object store from a URL.
     *
     * ## Arguments
     * - `url`: the object-store URL, for example `memory:///` or `s3://bucket/prefix`.
     *
     * ## Returns
     * - `Result<Arc<ObjectStore>, SlatedbError>`: the resolved object-store handle.
     *
     * ## Errors
     * - `SlatedbError`: if the URL cannot be parsed or the object-store backend is unsupported.
     */public static ObjectStore resolveObjectStore(String url) throws SlatedbException {
            try {
                return FfiConverterTypeObjectStore.INSTANCE.lift(
    UniffiHelpers.uniffiRustCallWithError(new SlatedbExceptionErrorHandler(), _status -> {
        return UniffiLib.INSTANCE.uniffi_slatedb_ffi_fn_func_resolve_object_store(
            FfiConverterString.INSTANCE.lower(url), _status);
    })
    );
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

