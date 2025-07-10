use ::slatedb::admin::{load_object_store_from_env, Admin};
use ::slatedb::config::{CheckpointOptions, DbReaderOptions, Settings};
use ::slatedb::object_store::memory::InMemory;
use ::slatedb::object_store::ObjectStore;
use ::slatedb::Db;
use ::slatedb::DbReader;
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use std::backtrace::Backtrace;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;
use uuid::Uuid;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

fn create_value_error(msg: impl std::fmt::Display) -> PyErr {
    let bt = Backtrace::capture();
    let error_msg = format!("{}.\nBacktrace:\n{}", msg, bt);
    PyValueError::new_err(error_msg)
}

fn load_object_store(env_file: Option<String>) -> PyResult<Arc<dyn ObjectStore>> {
    if let Some(env_file) = env_file {
        Ok(load_object_store_from_env(Some(env_file)).map_err(create_value_error)?)
    } else {
        Ok(Arc::new(InMemory::new()))
    }
}

fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A Python module implemented in Rust.
#[pymodule]
fn slatedb(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySlateDB>()?;
    m.add_class::<PySlateDBReader>()?;
    m.add_class::<PySlateDBAdmin>()?;
    Ok(())
}

#[pyclass(name = "SlateDB")]
struct PySlateDB {
    inner: Arc<Db>,
}

impl PySlateDB {
    fn inner_get_bytes(&self, key: Vec<u8>) -> PyResult<Option<Vec<u8>>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
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
        let object_store = load_object_store(env_file)?;
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
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    #[pyo3(signature = (key, value))]
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async { db.put(&key, &value).await.map_err(create_value_error) })
    }

    #[pyo3(signature = (key))]
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

        let db = self.inner.clone();
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

    #[pyo3(signature = (key))]
    fn delete(&self, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async { db.delete(&key).await.map_err(create_value_error) })
    }

    fn close(&self) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async { db.close().await.map_err(create_value_error) })
    }

    #[pyo3(signature = (key, value))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move {
            db.put(&key, &value).await.map_err(create_value_error)
        })
    }

    #[pyo3(signature = (key))]
    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move {
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn delete_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move {
            db.delete(&key).await.map_err(create_value_error)
        })
    }
}

#[pyclass(name = "SlateDBReader")]
struct PySlateDBReader {
    inner: Arc<DbReader>,
}

#[pymethods]
impl PySlateDBReader {
    #[new]
    #[pyo3(signature = (path, env_file = None, checkpoint_id = None))]
    fn new(
        path: String,
        env_file: Option<String>,
        checkpoint_id: Option<String>,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let object_store = load_object_store(env_file)?;
        let db_reader = rt.block_on(async {
            let options = DbReaderOptions::default();
            DbReader::open(
                path,
                object_store,
                checkpoint_id
                    .map(|id| Uuid::parse_str(&id).map_err(create_value_error))
                    .transpose()?,
                options,
            )
            .await
            .map_err(create_value_error)
        })?;
        Ok(Self {
            inner: Arc::new(db_reader),
        })
    }

    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db_reader = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async {
            match db_reader.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(PyBytes::new(py, &bytes))),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db_reader = self.inner.clone();
        future_into_py(py, async move {
            match db_reader.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
        })
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

        let db_reader = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async {
            let mut iter = db_reader
                .scan(start..end)
                .await
                .map_err(create_value_error)?;
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

    fn close(&self) -> PyResult<()> {
        let db_reader = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async { db_reader.close().await.map_err(create_value_error) })
    }
}

#[pyclass(name = "SlateDBAdmin")]
struct PySlateDBAdmin {
    inner: Arc<Admin>,
}

#[pymethods]
impl PySlateDBAdmin {
    #[new]
    #[pyo3(signature = (path, env_file = None))]
    fn new(path: String, env_file: Option<String>) -> PyResult<Self> {
        let object_store = load_object_store(env_file)?;
        let admin = Admin::builder(path, object_store).build();
        Ok(Self {
            inner: Arc::new(admin),
        })
    }

    #[pyo3(signature = (lifetime = None, source = None))]
    fn create_checkpoint<'py>(
        &self,
        py: Python<'py>,
        lifetime: Option<u64>,
        source: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let admin = self.inner.clone();
        let rt = get_runtime();
        let result = rt.block_on(async {
            let lifetime = lifetime.map(Duration::from_millis);
            let source = source
                .map(|s| Uuid::parse_str(&s).map_err(create_value_error))
                .transpose()?;
            admin
                .create_checkpoint(&CheckpointOptions { lifetime, source })
                .await
                .map_err(create_value_error)
        })?;
        let dict = PyDict::new(py);
        dict.set_item("id", result.id.to_string())?;
        dict.set_item("manifest_id", result.manifest_id)?;
        Ok(dict)
    }

    fn list_checkpoints<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let admin = self.inner.clone();
        let rt = get_runtime();
        let result =
            rt.block_on(async { admin.list_checkpoints().await.map_err(create_value_error) })?;
        result
            .into_iter()
            .map(|c| {
                let dict = PyDict::new(py);
                dict.set_item("id", c.id.to_string())?;
                dict.set_item("manifest_id", c.manifest_id)?;
                dict.set_item("expire_time", c.expire_time.map(to_millis))?;
                dict.set_item("create_time", to_millis(c.create_time))?;
                Ok(dict)
            })
            .collect::<PyResult<Vec<Bound<PyDict>>>>()
    }
}
