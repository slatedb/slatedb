use ::slatedb::admin::{load_object_store_from_env, Admin};
use ::slatedb::config::{CheckpointOptions, DbReaderOptions, Settings};
use ::slatedb::object_store::memory::InMemory;
use ::slatedb::object_store::ObjectStore;
use ::slatedb::Db;
use ::slatedb::DbReader;
use ::slatedb::{Error, KeyValue};
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use std::backtrace::Backtrace;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Mutex};
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

/// A Python module implemented in Rust.
#[pymodule]
fn slatedb(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySlateDB>()?;
    m.add_class::<PySlateDBReader>()?;
    m.add_class::<PySlateDBAdmin>()?;
    m.add_class::<PyDbIterator>()?;
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

    #[pyo3(signature = (start, end = None))]
    fn scan_iter(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(create_value_error("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });

        Ok(PyDbIterator::new_from_db(self.inner.clone(), start, end))
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

    #[pyo3(signature = (start, end = None))]
    fn scan_iter(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(create_value_error("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });

        Ok(PyDbIterator::new_from_reader(
            self.inner.clone(),
            start,
            end,
        ))
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
                .create_detached_checkpoint(&CheckpointOptions { lifetime, source })
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
                dict.set_item("expire_time", c.expire_time.map(|t| t.timestamp_millis()))?;
                dict.set_item("create_time", c.create_time.timestamp_millis())?;
                Ok(dict)
            })
            .collect::<PyResult<Vec<Bound<PyDict>>>>()
    }
}

type IteratorReceiver = Arc<Mutex<mpsc::UnboundedReceiver<Result<Option<KeyValue>, Error>>>>;

#[pyclass(name = "DbIterator")]
pub struct PyDbIterator {
    receiver: Option<IteratorReceiver>,
    db: Option<Arc<Db>>,
    reader: Option<Arc<DbReader>>,
    start: Vec<u8>,
    end: Vec<u8>,
    _task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PyDbIterator {
    fn new_from_db(db: Arc<Db>, start: Vec<u8>, end: Vec<u8>) -> Self {
        Self {
            receiver: None,
            db: Some(db),
            reader: None,
            start,
            end,
            _task_handle: None,
        }
    }

    fn new_from_reader(reader: Arc<DbReader>, start: Vec<u8>, end: Vec<u8>) -> Self {
        Self {
            receiver: None,
            db: None,
            reader: Some(reader),
            start,
            end,
            _task_handle: None,
        }
    }

    fn ensure_initialized(&mut self) -> PyResult<()> {
        if self.receiver.is_some() {
            return Ok(());
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let receiver = Arc::new(Mutex::new(receiver));

        let task_handle = if let Some(db) = &self.db {
            let db = db.clone();
            let start = self.start.clone();
            let end = self.end.clone();
            get_runtime().spawn(async move {
                let result = async {
                    let mut iter = db.scan(start..end).await?;
                    while let Some(kv) = iter.next().await? {
                        if sender.send(Ok(Some(kv))).is_err() {
                            break; // Receiver dropped
                        }
                    }
                    let _ = sender.send(Ok(None)); // End of iteration
                    Ok::<(), Error>(())
                }
                .await;

                if let Err(e) = result {
                    let _ = sender.send(Err(e));
                }
            })
        } else if let Some(reader) = &self.reader {
            let reader = reader.clone();
            let start = self.start.clone();
            let end = self.end.clone();
            get_runtime().spawn(async move {
                let result = async {
                    let mut iter = reader.scan(start..end).await?;
                    while let Some(kv) = iter.next().await? {
                        if sender.send(Ok(Some(kv))).is_err() {
                            break; // Receiver dropped
                        }
                    }
                    let _ = sender.send(Ok(None)); // End of iteration
                    Ok::<(), Error>(())
                }
                .await;

                if let Err(e) = result {
                    let _ = sender.send(Err(e));
                }
            })
        } else {
            return Err(create_value_error("Iterator not properly initialized"));
        };

        self.receiver = Some(receiver);
        self._task_handle = Some(task_handle);
        Ok(())
    }
}

#[pymethods]
impl PyDbIterator {
    fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        self.ensure_initialized()?;

        let receiver = self.receiver.as_ref().unwrap().clone();
        let rt = get_runtime();

        let kv_result = rt.block_on(async {
            let mut guard = receiver.lock().await;
            guard.recv().await
        });

        match kv_result {
            Some(Ok(Some(kv))) => {
                let key = PyBytes::new(py, &kv.key);
                let value = PyBytes::new(py, &kv.value);
                let tuple = PyTuple::new(py, vec![key, value])?;
                Ok(tuple.into())
            }
            Some(Ok(None)) | None => {
                // End of iteration or channel closed - raise StopIteration
                Err(pyo3::exceptions::PyStopIteration::new_err(""))
            }
            Some(Err(e)) => Err(create_value_error(e)),
        }
    }
}
