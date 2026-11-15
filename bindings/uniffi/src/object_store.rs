use std::collections::HashMap;
use std::sync::Arc;

use slatedb::{admin, Db};

use crate::error::Error;
use crate::object_store_builder::validate_url_config_key;

/// Object store handle used when opening databases, readers, and WAL readers.
#[derive(uniffi::Object)]
pub struct ObjectStore {
    pub(crate) inner: Arc<dyn object_store::ObjectStore>,
}

#[uniffi::export]
impl ObjectStore {
    /// Resolves an object store from a URL understood by SlateDB.
    #[uniffi::constructor]
    pub fn resolve(url: String) -> Result<Arc<Self>, Error> {
        let inner = Db::resolve_object_store(&url)?;
        Ok(Arc::new(Self { inner }))
    }

    /// Resolves an object store from a URL, like `resolve`, and applies
    /// `options` to it.
    ///
    /// Option keys are the `object_store` crate's config keys, for example
    /// `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, and
    /// `aws_region` for S3. Keys are case-insensitive. An option overrides an
    /// environment variable with the same key. The options apply only to this
    /// store. They do not change the process environment.
    ///
    /// Returns an error if a key is not a valid config key for an S3, Azure,
    /// or GCS URL. Options for other URLs are ignored.
    #[uniffi::constructor]
    pub fn resolve_with_options(
        url: String,
        options: HashMap<String, String>,
    ) -> Result<Arc<Self>, Error> {
        for key in options.keys() {
            validate_url_config_key(&url, key)?;
        }
        let inner = Db::resolve_object_store_with_options(&url, options)?;
        Ok(Arc::new(Self { inner }))
    }

    /// Builds an object store from environment configuration.
    ///
    /// When `env_file` is provided, environment variables are loaded from that
    /// file before constructing the store.
    #[uniffi::constructor]
    pub fn from_env(env_file: Option<String>) -> Result<Arc<Self>, Error> {
        let inner = admin::load_object_store_from_env(env_file)?;
        Ok(Arc::new(Self { inner }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SlateDbError;
    use object_store::ObjectStoreExt;

    fn options(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn resolve_with_options_signs_requests_with_given_credentials() {
        use std::io::{BufRead, BufReader, Write};
        use std::net::TcpListener;

        // A fake S3 endpoint that records the request headers and returns 404.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut headers = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                headers.push_str(&line.to_ascii_lowercase());
            }
            let mut stream = stream;
            stream
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            headers
        });

        let store = ObjectStore::resolve_with_options(
            "s3://my-bucket".to_string(),
            options(&[
                ("aws_access_key_id", "AKIDEXAMPLE"),
                ("aws_secret_access_key", "secret"),
                ("aws_session_token", "token"),
                ("aws_region", "us-east-1"),
                ("aws_endpoint", &endpoint),
                ("aws_allow_http", "true"),
            ]),
        )
        .unwrap();
        let _ = store
            .inner
            .get(&object_store::path::Path::from("key"))
            .await;

        let headers = server.join().unwrap();
        assert!(headers.contains("credential=akidexample/"), "{headers}");
        assert!(headers.contains("x-amz-security-token: token"), "{headers}");
    }

    #[test]
    fn resolve_with_options_accepts_upper_case_keys() {
        ObjectStore::resolve_with_options(
            "s3://my-bucket".to_string(),
            options(&[("AWS_REGION", "us-east-1")]),
        )
        .unwrap();
    }

    #[test]
    fn resolve_with_options_rejects_unknown_s3_key() {
        let result = ObjectStore::resolve_with_options(
            "s3://my-bucket".to_string(),
            options(&[("aws_acess_key_id", "AKIDEXAMPLE")]),
        );
        assert!(result.is_err());
        assert!(matches!(
            validate_url_config_key("s3://my-bucket", "aws_acess_key_id"),
            Err(SlateDbError::InvalidObjectStoreConfigKey { provider: "S3", .. })
        ));
    }

    #[test]
    fn resolve_with_options_rejects_unknown_azure_and_gcs_keys() {
        assert!(matches!(
            validate_url_config_key("az://container", "bogus"),
            Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "Azure",
                ..
            })
        ));
        assert!(matches!(
            validate_url_config_key("gs://bucket", "bogus"),
            Err(SlateDbError::InvalidObjectStoreConfigKey {
                provider: "GCS",
                ..
            })
        ));
    }

    #[test]
    fn resolve_with_options_ignores_options_for_memory() {
        ObjectStore::resolve_with_options(
            "memory:///".to_string(),
            options(&[("anything", "value")]),
        )
        .unwrap();
    }

    #[test]
    fn resolve_with_options_rejects_path_component() {
        let result =
            ObjectStore::resolve_with_options("s3://my-bucket/path".to_string(), HashMap::new());
        assert!(result.is_err());
    }
}
