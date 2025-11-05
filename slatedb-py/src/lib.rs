use ::slatedb::admin::{load_object_store_from_env, Admin};
use ::slatedb::config::{CheckpointOptions, DbReaderOptions, Settings};
use ::slatedb::object_store::memory::InMemory;
use ::slatedb::object_store::ObjectStore;
use ::slatedb::Db;
use ::slatedb::DbReader;
use ::slatedb::MergeOperator;
use ::slatedb::MergeOperatorError;
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

async fn load_db_from_url(
    path: &str,
    url: &str,
    settings: Settings,
    merge_operator: Option<MergeOperatorType>,
) -> PyResult<Db> {
    let object_store = Db::resolve_object_store(url).map_err(create_value_error)?;

    let builder = Db::builder(path, object_store).with_settings(settings);
    let builder = if let Some(op) = merge_operator {
        builder.with_merge_operator(op)
    } else {
        builder
    };
    builder.build().await.map_err(create_value_error)
}

async fn load_db_from_env(
    path: &str,
    env_file: Option<String>,
    settings: Settings,
    merge_operator: Option<MergeOperatorType>,
) -> PyResult<Db> {
    let object_store = load_object_store(env_file)?;

    let builder = Db::builder(path, object_store).with_settings(settings);
    let builder = if let Some(op) = merge_operator {
        builder.with_merge_operator(op)
    } else {
        builder
    };
    builder.build().await.map_err(create_value_error)
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

// Merge operator bridging to Python
//
// The merge operator may call back into Python during reads/compactions. To avoid
// deadlocks, callers of DB methods must not hold the Python GIL while awaiting
// Rust futures that may in turn invoke the merge operator. The DB methods below
// use `py.allow_threads(|| rt.block_on(...))` to release the GIL before awaiting.
// Here we only briefly acquire the GIL to run the Python callable and convert args/results.
type MergeOperatorType = Arc<dyn MergeOperator + Send + Sync>;

struct PyMergeOperator {
    callable: Py<PyAny>,
}

impl MergeOperator for PyMergeOperator {
    fn merge(
        &self,
        existing_value: Option<bytes::Bytes>,
        value: bytes::Bytes,
    ) -> Result<bytes::Bytes, MergeOperatorError> {
        // Fall back to returning the operand on any Python error.
        let fallback = value.clone();
        Python::with_gil(|py| {
            let existing_arg: Option<Vec<u8>> = existing_value.as_ref().map(|b| b.to_vec());
            let value_arg: Vec<u8> = value.to_vec();
            match self.callable.call1(py, (existing_arg, value_arg)) {
                Ok(obj) => {
                    let obj = obj.bind(py);
                    // Expect bytes return
                    if let Ok(pybytes) = obj.downcast::<PyBytes>() {
                        Ok(bytes::Bytes::copy_from_slice(pybytes.as_bytes()))
                    } else if let Ok(v) = obj.extract::<Vec<u8>>() {
                        Ok(bytes::Bytes::from(v))
                    } else {
                        log::warn!("merge operator returned non-bytes; using operand");
                        Ok(fallback)
                    }
                }
                Err(err) => {
                    // Log and return operand as best-effort behavior (merge operator is infallible)
                    log::warn!("merge operator raised exception: {}", err);
                    Ok(fallback)
                }
            }
        })
    }
}

fn parse_merge_operator(py_obj: Option<&Bound<PyAny>>) -> PyResult<Option<MergeOperatorType>> {
    if let Some(obj) = py_obj {
        // Expect a Python callable: merge(existing: Optional[bytes], value: bytes) -> bytes
        if obj.is_callable() {
            let callable: Py<PyAny> = obj.clone().unbind();
            let op: MergeOperatorType = Arc::new(PyMergeOperator { callable });
            Ok(Some(op))
        } else {
            Err(create_value_error("merge_operator must be a callable (merge(existing: Optional[bytes], value: bytes) -> bytes)"))
        }
    } else {
        Ok(None)
    }
}

#[pyclass(name = "SlateDB")]
struct PySlateDB {
    inner: Arc<Db>,
}

impl PySlateDB {}

#[pymethods]
impl PySlateDB {
    #[new]
    #[pyo3(signature = (path, url = None, env_file = None, *, merge_operator = None, **kwargs))]
    fn new(
        path: String,
        url: Option<String>,
        env_file: Option<String>,
        merge_operator: Option<&Bound<PyAny>>,
        kwargs: Option<&Bound<PyDict>>,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let settings = match kwargs.and_then(|k| k.get_item("settings").ok().flatten()) {
            Some(settings_item) => {
                let settings_path = settings_item
                    .extract::<String>()
                    .map_err(create_value_error)?;
                Settings::from_file(settings_path).map_err(create_value_error)?
            }
            None => Settings::load().map_err(create_value_error)?,
        };

        // Parse the optional merge operator
        let merge_operator = parse_merge_operator(merge_operator)?;

        let db = rt.block_on(async move {
            if let Some(url) = url {
                load_db_from_url(&path, &url, settings, merge_operator).await
            } else {
                load_db_from_env(&path, env_file, settings, merge_operator).await
            }
        })?;
        Ok(Self {
            inner: Arc::new(db),
        })
    }

    #[pyo3(signature = (key, value))]
    fn put(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        py.allow_threads(|| {
            rt.block_on(async { db.put(&key, &value).await.map_err(create_value_error) })
        })
    }

    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match db.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.as_ref().to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(create_value_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
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
        // Collect key-values with GIL released
        let kvs = py.allow_threads(|| {
            rt.block_on(async {
                let mut iter = db.scan(start..end).await.map_err(create_value_error)?;
                let mut out = Vec::new();
                while let Some(entry) = iter.next().await.map_err(create_value_error)? {
                    out.push((entry.key.to_vec(), entry.value.to_vec()));
                }
                Ok::<Vec<(Vec<u8>, Vec<u8>)>, PyErr>(out)
            })
        })?;
        // Build Python tuples under GIL
        kvs.into_iter()
            .map(|(k, v)| {
                let key = PyBytes::new(py, &k);
                let value = PyBytes::new(py, &v);
                PyTuple::new(py, vec![key, value])
            })
            .collect()
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
    fn delete(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while awaiting Rust I/O
        py.allow_threads(|| {
            rt.block_on(async { db.delete(&key).await.map_err(create_value_error) })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while awaiting shutdown
        py.allow_threads(|| rt.block_on(async { db.close().await.map_err(create_value_error) }))
    }

    /// Merge a value into the database using the configured merge operator.
    #[pyo3(signature = (key, value))]
    fn merge(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while DB.merge may invoke Python merge operator
        py.allow_threads(|| {
            rt.block_on(async { db.merge(&key, &value).await.map_err(create_value_error) })
        })
    }

    /// Async version of merge
    #[pyo3(signature = (key, value))]
    fn merge_async<'py>(
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
            db.merge(&key, &value).await.map_err(create_value_error)
        })
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
    #[pyo3(signature = (path, env_file = None, checkpoint_id = None, *, merge_operator = None))]
    fn new(
        path: String,
        env_file: Option<String>,
        checkpoint_id: Option<String>,
        merge_operator: Option<&Bound<PyAny>>,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let object_store = load_object_store(env_file)?;
        let db_reader = rt.block_on(async {
            let mut options = DbReaderOptions::default();
            options.merge_operator = parse_merge_operator(merge_operator)?;
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
        // Release the GIL while awaiting Rust; merge operator may need to reacquire it.
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match db_reader.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(create_value_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
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
        // Collect key-values with GIL released to avoid deadlocks when merge operator calls Python
        let kvs = py.allow_threads(|| {
            rt.block_on(async {
                let mut iter = db_reader
                    .scan(start..end)
                    .await
                    .map_err(create_value_error)?;
                let mut out = Vec::new();
                while let Some(entry) = iter.next().await.map_err(create_value_error)? {
                    out.push((entry.key.to_vec(), entry.value.to_vec()));
                }
                Ok::<Vec<(Vec<u8>, Vec<u8>)>, PyErr>(out)
            })
        })?;
        kvs.into_iter()
            .map(|(k, v)| {
                let key = PyBytes::new(py, &k);
                let value = PyBytes::new(py, &v);
                PyTuple::new(py, vec![key, value])
            })
            .collect()
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

        // Release GIL while waiting on Rust channel to avoid blocking Python
        let kv_result = py.allow_threads(|| {
            rt.block_on(async {
                let mut guard = receiver.lock().await;
                guard.recv().await
            })
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
