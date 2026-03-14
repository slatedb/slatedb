use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{Map, Value};

use crate::error::FfiSlatedbError;

#[derive(uniffi::Object)]
pub struct FfiSettings {
    inner: Mutex<slatedb::Settings>,
}

impl FfiSettings {
    pub(crate) fn new(inner: slatedb::Settings) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(crate) fn inner(&self) -> slatedb::Settings {
        self.inner.lock().clone()
    }
}

#[uniffi::export]
impl FfiSettings {
    #[uniffi::constructor]
    pub fn default() -> Arc<Self> {
        Arc::new(Self::new(slatedb::Settings::default()))
    }

    #[uniffi::constructor]
    pub fn from_file(path: String) -> Result<Arc<Self>, FfiSlatedbError> {
        Ok(Arc::new(Self::new(slatedb::Settings::from_file(path)?)))
    }

    #[uniffi::constructor]
    pub fn from_json_string(json: String) -> Result<Arc<Self>, FfiSlatedbError> {
        Ok(Arc::new(Self::new(serde_json::from_str::<
            slatedb::Settings,
        >(&json)?)))
    }

    #[uniffi::constructor]
    pub fn from_env(prefix: String) -> Result<Arc<Self>, FfiSlatedbError> {
        Ok(Arc::new(Self::new(slatedb::Settings::from_env(&prefix)?)))
    }

    #[uniffi::constructor]
    pub fn from_env_with_default(
        prefix: String,
        default_settings: Arc<FfiSettings>,
    ) -> Result<Arc<Self>, FfiSlatedbError> {
        Ok(Arc::new(Self::new(
            slatedb::Settings::from_env_with_default(&prefix, default_settings.inner())?,
        )))
    }

    #[uniffi::constructor]
    pub fn load() -> Result<Arc<Self>, FfiSlatedbError> {
        Ok(Arc::new(Self::new(slatedb::Settings::load()?)))
    }

    /// Sets a settings field by dotted path using a JSON literal value.
    ///
    /// `key` identifies the field to update. Use `.` to address nested objects,
    /// for example `compactor_options.max_sst_size` or
    /// `object_store_cache_options.root_folder`.
    ///
    /// `value_json` must be a valid JSON literal matching the target field's
    /// expected type. That means strings must be quoted JSON strings, numbers
    /// should be passed as JSON numbers, booleans as `true`/`false`, and
    /// optional fields can be cleared with `null`.
    ///
    /// Missing or `null` intermediate objects in the dotted path are created
    /// automatically. If the update would produce an invalid `slatedb::Settings`
    /// value, the method returns an error and leaves the current settings
    /// unchanged.
    ///
    /// Examples:
    ///
    /// - `set("flush_interval", "\"250ms\"")`
    /// - `set("default_ttl", "42")`
    /// - `set("default_ttl", "null")`
    /// - `set("compactor_options.max_sst_size", "33554432")`
    /// - `set("object_store_cache_options.root_folder", "\"/tmp/slatedb-cache\"")`
    pub fn set(&self, key: String, value_json: String) -> Result<(), FfiSlatedbError> {
        let mut guard = self.inner.lock();
        let mut settings_json =
            serde_json::to_value(&*guard).map_err(|error| FfiSlatedbError::Internal {
                message: format!("settings serialization failed: {error}"),
            })?;

        if !settings_json.is_object() {
            return Err(FfiSlatedbError::Internal {
                message: "settings JSON root was not an object".to_owned(),
            });
        }

        let value = serde_json::from_str::<Value>(&value_json).map_err(|error| {
            FfiSlatedbError::Invalid {
                message: format!("value_json is not valid JSON: {error}"),
            }
        })?;

        apply_dotted_json_path(&mut settings_json, &key, value).map_err(|message| {
            FfiSlatedbError::Invalid {
                message: format!("key invalid: {message}"),
            }
        })?;

        let settings =
            serde_json::from_value::<slatedb::Settings>(settings_json).map_err(|error| {
                FfiSlatedbError::Invalid {
                    message: format!("settings update produced invalid settings: {error}"),
                }
            })?;
        *guard = settings;
        Ok(())
    }

    pub fn to_json_string(&self) -> Result<String, FfiSlatedbError> {
        self.inner
            .lock()
            .to_json_string()
            .map_err(|error| FfiSlatedbError::Internal {
                message: format!("settings serialization failed: {error}"),
            })
    }
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
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{apply_dotted_json_path, FfiSettings};
    use crate::error::FfiSlatedbError;

    #[test]
    fn apply_dotted_json_path_sets_top_level_key() {
        let mut root = json!({
            "flush_interval": "100ms",
        });

        apply_dotted_json_path(&mut root, "flush_interval", json!("250ms")).unwrap();

        assert_eq!(root["flush_interval"], "250ms");
    }

    #[test]
    fn apply_dotted_json_path_materializes_nested_option_object() {
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
    fn apply_dotted_json_path_rejects_empty_segment() {
        let mut root = json!({});

        let err = apply_dotted_json_path(&mut root, "compactor_options..max_sst_size", json!(1))
            .expect_err("expected invalid key");

        assert!(err.contains("empty path segment"), "unexpected err: {err}");
    }

    #[test]
    fn apply_dotted_json_path_rejects_non_object_intermediate() {
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

    #[test]
    fn settings_set_json_updates_top_level_field() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));

        settings
            .set("flush_interval".to_owned(), "\"250ms\"".to_owned())
            .unwrap();

        assert_eq!(
            settings.inner().flush_interval,
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn settings_set_json_materializes_nested_option_object() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));

        settings
            .set(
                "compactor_options.max_sst_size".to_owned(),
                "33554432".to_owned(),
            )
            .unwrap();

        assert_eq!(
            settings
                .inner()
                .compactor_options
                .expect("compactor options should exist")
                .max_sst_size,
            33_554_432
        );
    }

    #[test]
    fn settings_set_json_can_clear_optional_field() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));

        settings
            .set("default_ttl".to_owned(), "100".to_owned())
            .unwrap();
        settings
            .set("default_ttl".to_owned(), "null".to_owned())
            .unwrap();

        assert_eq!(settings.inner().default_ttl, None);
    }

    #[test]
    fn settings_set_json_rejects_invalid_json_literal() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));

        let err = settings
            .set("flush_interval".to_owned(), "not-json".to_owned())
            .expect_err("expected invalid JSON");

        assert!(
            matches!(err, FfiSlatedbError::Invalid { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn settings_set_json_preserves_previous_value_on_invalid_update() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));
        let before = settings.inner();

        let err = settings
            .set("flush_interval".to_owned(), "123".to_owned())
            .expect_err("expected invalid settings");

        assert!(
            matches!(err, FfiSlatedbError::Invalid { .. }),
            "unexpected error: {err:?}"
        );
        assert_eq!(settings.inner().flush_interval, before.flush_interval);
    }

    #[test]
    fn settings_round_trip_json_string() {
        let settings = Arc::new(FfiSettings::new(slatedb::Settings::default()));

        settings
            .set("default_ttl".to_owned(), "42".to_owned())
            .unwrap();

        let encoded = settings.to_json_string().unwrap();
        let decoded = FfiSettings::from_json_string(encoded).unwrap();

        assert_eq!(decoded.inner().default_ttl, Some(42));
    }

    #[test]
    fn settings_from_file_reads_config() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
flush_interval = "1s"
"#,
            )?;

            let settings = FfiSettings::from_file("config.toml".to_owned()).unwrap();
            assert_eq!(
                settings.inner().flush_interval,
                Some(Duration::from_secs(1))
            );

            Ok(())
        });
    }

    #[test]
    fn settings_from_env_reads_config() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("FFI_SETTINGS_FLUSH_INTERVAL", "1s");

            let settings = FfiSettings::from_env("FFI_SETTINGS_".to_owned()).unwrap();
            assert_eq!(
                settings.inner().flush_interval,
                Some(Duration::from_secs(1))
            );

            Ok(())
        });
    }

    #[test]
    fn settings_from_env_with_default_uses_default_snapshot() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("FFI_SETTINGS_DEFAULT_TTL", "42");

            let defaults = Arc::new(FfiSettings::new(slatedb::Settings::default()));
            defaults
                .set("flush_interval".to_owned(), "\"250ms\"".to_owned())
                .unwrap();

            let settings =
                FfiSettings::from_env_with_default("FFI_SETTINGS_".to_owned(), defaults.clone())
                    .unwrap();

            assert_eq!(
                settings.inner().flush_interval,
                Some(Duration::from_millis(250))
            );
            assert_eq!(settings.inner().default_ttl, Some(42));

            Ok(())
        });
    }

    #[test]
    fn settings_load_uses_default_locations() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "SlateDb.toml",
                r#"
flush_interval = "2s"
"#,
            )?;

            let settings = FfiSettings::load().unwrap();
            assert_eq!(
                settings.inner().flush_interval,
                Some(Duration::from_secs(2))
            );

            Ok(())
        });
    }
}
