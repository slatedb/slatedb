use ::slatedb::admin::load_object_store_from_env;
use ::slatedb::config::Settings;
use ::slatedb::object_store::memory::InMemory;
use ::slatedb::Db;
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use std::backtrace::Backtrace;
use std::sync::Arc;
use tokio::runtime::Runtime;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

fn create_value_error(msg: impl std::fmt::Display) -> PyErr {
    let bt = Backtrace::capture();
    let error_msg = format!("{}.\nBacktrace:\n{}", msg, bt);
    PyValueError::new_err(error_msg)
}

/// A Python module implemented in Rust.
#[pymodule]
fn slatedb(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySlateDB>()?;
    Ok(())
}

#[pyclass(name = "SlateDB")]
struct PySlateDB {
    db: Arc<Db>,
}

impl PySlateDB {
    fn inner_get_bytes(&self, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async {
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
        })
    }
}

#[pymethods]
impl PySlateDB {
    #[new]
    #[pyo3(signature = (path, env_file = None, *, **kwargs))]
    fn new(
        path: String,
        env_file: Option<String>,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let object_store = if let Some(env_file) = env_file {
            load_object_store_from_env(Some(env_file)).map_err(create_value_error)?
        } else {
            Arc::new(InMemory::new())
        };
        let db = rt.block_on(async {
            let settings = match kwargs.and_then(|k| k.get_item("settings").ok().flatten()) {
                Some(settings_item) => {
                    let settings_path = settings_item
                        .extract::<String>()
                        .map_err(create_value_error)?;
                    Settings::from_file(settings_path).map_err(create_value_error)?
                }
                None => Settings::load().map_err(create_value_error)?,
            };

            Db::builder(path, object_store)
                .with_settings(settings)
                .build()
                .await
                .map_err(create_value_error)
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    #[pyo3(signature = (key, value))]
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async { db.put(&key, &value).await.map_err(create_value_error) })
    }

    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        match self.inner_get_bytes(key)? {
            Some(bytes) => Ok(Some(PyBytes::new(py, &bytes))),
            None => Ok(None),
        }
    }

    #[pyo3(signature = (start, end = None))]
    fn scan<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        if start.is_empty() {
            return Err(create_value_error("start cannot be empty"));
        }
        let start = start.clone();
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });

        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async {
            let mut iter = db.scan(start..end).await.map_err(create_value_error)?;
            let mut tuples = Vec::new();
            while let Some(entry) = iter.next().await.map_err(create_value_error)? {
                let key = PyBytes::new(py, &entry.key);
                let value = PyBytes::new(py, &entry.value);
                let tuple = PyTuple::new(py, vec![key, value])?;
                tuples.push(tuple);
            }
            Ok(tuples)
        })
    }

    fn delete(&self, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async { db.delete(&key).await.map_err(create_value_error) })
    }

    fn close(&self) -> PyResult<()> {
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async { db.close().await.map_err(create_value_error) })
    }

    fn put_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        future_into_py(py, async move {
            db.put(&key, &value).await.map_err(create_value_error)
        })
    }

    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        future_into_py(py, async move {
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
        })
    }

    fn delete_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        future_into_py(py, async move {
            db.delete(&key).await.map_err(create_value_error)
        })
    }
}
