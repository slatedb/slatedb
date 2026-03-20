package io.slatedb.uniffi;


import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import com.sun.jna.*;
import com.sun.jna.ptr.*;
/**
 * Mutable database settings object used to configure a [`crate::DbBuilder`].
 */
public interface SettingsInterface {
    
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
    public void set(String key, String valueJson) throws Error;
    
    /**
     * Serializes the current settings value to a JSON string.
     */
    public String toJsonString() throws Error;
    
}

