//! Settings APIs for `slatedb-c`.
//!
//! This module exposes C ABI functions for loading, serializing, and freeing
//! SlateDB `Settings` handles.

use crate::ffi::{
    alloc_bytes, bytes_from_ptr, cstr_to_string, error_from_slate_error, error_result,
    require_handle, require_out_ptr, slatedb_error_kind_t, slatedb_result_t, slatedb_settings_t,
    success_result,
};
use serde_json::{Map, Value};
use slatedb::Settings;
use std::os::raw::c_char;

/// Creates a new settings handle initialized with default values.
///
/// ## Arguments
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null output pointers.
///
/// ## Safety
/// - `out_settings` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_default(
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    let handle = Box::new(slatedb_settings_t {
        settings: Settings::default(),
    });
    *out_settings = Box::into_raw(handle);
    success_result()
}

/// Loads settings from a configuration file.
///
/// ## Arguments
/// - `path`: Config file path (`.json`, `.toml`, `.yaml`, `.yml`) as a
///   null-terminated UTF-8 string.
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid UTF-8.
/// - Returns mapped SlateDB errors for file/parse failures.
///
/// ## Safety
/// - `path` must be a valid null-terminated C string.
/// - `out_settings` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_from_file(
    path: *const c_char,
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    let path = match cstr_to_string(path, "path") {
        Ok(path) => path,
        Err(err) => return err,
    };

    match Settings::from_file(path) {
        Ok(settings) => {
            let handle = Box::new(slatedb_settings_t { settings });
            *out_settings = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Loads settings from a JSON string.
///
/// ## Arguments
/// - `json`: JSON payload as a null-terminated UTF-8 string.
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers, invalid UTF-8, or
///   invalid JSON/settings schema.
///
/// ## Safety
/// - `json` must be a valid null-terminated C string.
/// - `out_settings` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_from_json(
    json: *const c_char,
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    let json = match cstr_to_string(json, "json") {
        Ok(json) => json,
        Err(err) => return err,
    };

    match serde_json::from_str::<Settings>(&json) {
        Ok(settings) => {
            let handle = Box::new(slatedb_settings_t { settings });
            *out_settings = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("settings from_json failed: {err}"),
        ),
    }
}

/// Loads settings from environment variables with the given prefix.
///
/// ## Arguments
/// - `prefix`: Environment variable prefix as a null-terminated UTF-8 string.
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid UTF-8.
/// - Returns mapped SlateDB errors for parse failures.
///
/// ## Safety
/// - `prefix` must be a valid null-terminated C string.
/// - `out_settings` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_from_env(
    prefix: *const c_char,
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    let prefix = match cstr_to_string(prefix, "prefix") {
        Ok(prefix) => prefix,
        Err(err) => return err,
    };

    match Settings::from_env(&prefix) {
        Ok(settings) => {
            let handle = Box::new(slatedb_settings_t { settings });
            *out_settings = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Loads settings from environment variables using a default settings handle.
///
/// ## Arguments
/// - `prefix`: Environment variable prefix as a null-terminated UTF-8 string.
/// - `default_settings`: Default settings handle to merge environment overrides
///   into.
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null pointers or invalid UTF-8.
/// - Returns mapped SlateDB errors for parse failures.
///
/// ## Safety
/// - `prefix` must be a valid null-terminated C string.
/// - `default_settings` and `out_settings` must be valid non-null pointers.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_from_env_with_default(
    prefix: *const c_char,
    default_settings: *const slatedb_settings_t,
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(default_settings, "default_settings") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    let prefix = match cstr_to_string(prefix, "prefix") {
        Ok(prefix) => prefix,
        Err(err) => return err,
    };
    let default_settings = (&*default_settings).settings.clone();

    match Settings::from_env_with_default(&prefix, default_settings) {
        Ok(settings) => {
            let handle = Box::new(slatedb_settings_t { settings });
            *out_settings = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Loads settings from default files and `SLATEDB_` environment variables.
///
/// ## Arguments
/// - `out_settings`: Output pointer populated with a `slatedb_settings_t*`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for null output pointers.
/// - Returns mapped SlateDB errors for parse failures.
///
/// ## Safety
/// - `out_settings` must be a valid non-null writable pointer.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_load(
    out_settings: *mut *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_out_ptr(out_settings, "out_settings") {
        return err;
    }

    match Settings::load() {
        Ok(settings) => {
            let handle = Box::new(slatedb_settings_t { settings });
            *out_settings = Box::into_raw(handle);
            success_result()
        }
        Err(err) => error_from_slate_error(&err),
    }
}

/// Applies key/value JSON updates to an existing settings handle.
///
/// Uses a dotted field path in `key` and a JSON literal payload in
/// `value_json`. Intermediate objects in a dotted path are materialized as
/// needed when absent or null.
///
/// ## Arguments
/// - `settings`: Settings handle to mutate.
/// - `key`: UTF-8 dotted field path bytes.
/// - `key_len`: Number of bytes in `key`.
/// - `value_json`: UTF-8 JSON literal bytes assigned at `key`.
/// - `value_json_len`: Number of bytes in `value_json`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles, invalid
///   keys, invalid JSON literals, or schema-invalid resulting settings.
/// - Returns `SLATEDB_ERROR_KIND_INTERNAL` for unexpected serialization errors.
///
/// ## Safety
/// - `settings` must be a valid non-null settings handle.
/// - If `key_len > 0`, `key` must point to at least `key_len` readable bytes.
/// - If `value_json_len > 0`, `value_json` must point to at least
///   `value_json_len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_apply_kv(
    settings: *mut slatedb_settings_t,
    key: *const c_char,
    key_len: usize,
    value_json: *const c_char,
    value_json_len: usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(settings, "settings") {
        return err;
    }

    let handle = &mut *settings;
    let mut settings_json = match serde_json::to_value(&handle.settings) {
        Ok(settings_json) => settings_json,
        Err(err) => {
            return error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
                &format!("settings apply_kv serialization failed: {err}"),
            );
        }
    };

    if !settings_json.is_object() {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
            "settings apply_kv expected settings JSON object",
        );
    }

    let key_bytes = match bytes_from_ptr(key as *const u8, key_len, "key") {
        Ok(key_bytes) => key_bytes,
        Err(err) => return err,
    };
    let key = match std::str::from_utf8(key_bytes) {
        Ok(key) => key,
        Err(_) => {
            return error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "key is not valid UTF-8",
            );
        }
    };

    let value_json_bytes =
        match bytes_from_ptr(value_json as *const u8, value_json_len, "value_json") {
            Ok(value_json_bytes) => value_json_bytes,
            Err(err) => return err,
        };
    let value = match serde_json::from_slice::<Value>(value_json_bytes) {
        Ok(value) => value,
        Err(err) => {
            return error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                &format!("value_json is not valid JSON: {err}"),
            );
        }
    };

    if let Err(message) = apply_dotted_json_path(&mut settings_json, key, value) {
        return error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("key invalid: {message}"),
        );
    }

    match serde_json::from_value::<Settings>(settings_json) {
        Ok(settings) => {
            handle.settings = settings;
            success_result()
        }
        Err(err) => error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            &format!("settings apply_kv produced invalid settings: {err}"),
        ),
    }
}

/// Serializes settings to a UTF-8 JSON payload.
///
/// ## Arguments
/// - `settings`: Settings handle.
/// - `out_json`: Output pointer to Rust-allocated UTF-8 bytes.
/// - `out_json_len`: Output length for `out_json`.
///
/// ## Returns
/// - `slatedb_result_t` indicating success or failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` for invalid pointers/handles.
/// - Returns `SLATEDB_ERROR_KIND_INTERNAL` when serialization fails.
///
/// ## Safety
/// - `settings`, `out_json`, and `out_json_len` must be valid non-null
///   pointers.
/// - `out_json` must be freed with `slatedb_bytes_free`.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_to_json(
    settings: *const slatedb_settings_t,
    out_json: *mut *mut u8,
    out_json_len: *mut usize,
) -> slatedb_result_t {
    if let Err(err) = require_handle(settings, "settings") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_json, "out_json") {
        return err;
    }
    if let Err(err) = require_out_ptr(out_json_len, "out_json_len") {
        return err;
    }

    *out_json = std::ptr::null_mut();
    *out_json_len = 0;

    let settings = &(&*settings).settings;
    match settings.to_json_string() {
        Ok(json) => {
            let (json_data, json_len) = alloc_bytes(json.as_bytes());
            *out_json = json_data;
            *out_json_len = json_len;
            success_result()
        }
        Err(err) => error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INTERNAL,
            &format!("settings to_json failed: {err}"),
        ),
    }
}

/// Closes and frees a settings handle.
///
/// ## Arguments
/// - `settings`: Settings handle.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` when `settings` is null.
///
/// ## Safety
/// - `settings` must be a valid non-null handle obtained from this library.
#[no_mangle]
pub unsafe extern "C" fn slatedb_settings_close(
    settings: *mut slatedb_settings_t,
) -> slatedb_result_t {
    if let Err(err) = require_handle(settings, "settings") {
        return err;
    }

    let _ = Box::from_raw(settings);
    success_result()
}

fn apply_dotted_json_path(root: &mut Value, key: &str, value: Value) -> Result<(), String> {
    if key.is_empty() {
        return Err("key cannot be empty".to_owned());
    }

    let mut current = root;
    let mut parts = key.split('.').peekable();
    while let Some(part) = parts.next() {
        if part.is_empty() {
            return Err("key has an empty path segment".to_owned());
        }

        let is_last = parts.peek().is_none();
        if is_last {
            let object = current
                .as_object_mut()
                .ok_or_else(|| format!("segment '{part}' parent is not an object"))?;
            object.insert(part.to_owned(), value);
            return Ok(());
        }

        let object = current
            .as_object_mut()
            .ok_or_else(|| format!("segment '{part}' parent is not an object"))?;
        let next = object
            .entry(part.to_owned())
            .or_insert_with(|| Value::Object(Map::new()));

        if next.is_null() {
            *next = Value::Object(Map::new());
        } else if !next.is_object() {
            return Err(format!("segment '{part}' is not an object"));
        }

        current = next;
    }

    Err("key cannot be empty".to_owned())
}

#[cfg(test)]
mod tests {
    use super::apply_dotted_json_path;
    use serde_json::json;

    #[test]
    fn test_apply_dotted_json_path_sets_top_level_key() {
        let mut root = json!({
            "flush_interval": "100ms",
        });

        apply_dotted_json_path(&mut root, "flush_interval", json!("250ms")).unwrap();

        assert_eq!(root["flush_interval"], "250ms");
    }

    #[test]
    fn test_apply_dotted_json_path_materializes_nested_option_object() {
        let mut root = json!({
            "compactor_options": null,
        });

        apply_dotted_json_path(
            &mut root,
            "compactor_options.max_sst_size",
            json!(64 * 1024 * 1024),
        )
        .unwrap();

        assert_eq!(root["compactor_options"]["max_sst_size"], 64 * 1024 * 1024);
    }

    #[test]
    fn test_apply_dotted_json_path_rejects_empty_segment() {
        let mut root = json!({});

        let err = apply_dotted_json_path(&mut root, "compactor_options..max_sst_size", json!(1))
            .expect_err("expected invalid key");

        assert!(err.contains("empty path segment"), "unexpected err: {err}");
    }

    #[test]
    fn test_apply_dotted_json_path_rejects_non_object_intermediate() {
        let mut root = json!({
            "compactor_options": 1,
        });

        let err = apply_dotted_json_path(&mut root, "compactor_options.max_sst_size", json!(1))
            .expect_err("expected invalid key");

        assert!(
            err.contains("not an object"),
            "unexpected error message: {err}"
        );
    }
}
