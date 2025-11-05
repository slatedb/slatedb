use ::slatedb::admin::{load_object_store_from_env, Admin};
use ::slatedb::config::{
    CheckpointOptions, CheckpointScope, DbReaderOptions, DurabilityLevel, FlushOptions, FlushType,
    MergeOptions, PutOptions, ReadOptions, ScanOptions, Settings, Ttl, WriteOptions,
};
use ::slatedb::object_store::memory::InMemory;
use ::slatedb::object_store::ObjectStore;
use ::slatedb::DBTransaction;
use ::slatedb::Db;
use ::slatedb::DbReader;
use ::slatedb::DbSnapshot;
use ::slatedb::Error;
use ::slatedb::IsolationLevel;
use ::slatedb::MergeOperator;
use ::slatedb::MergeOperatorError;
use once_cell::sync::OnceCell;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyTuple, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use std::backtrace::Backtrace;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use uuid::Uuid;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// Python exception types mirroring slatedb::Error kinds
create_exception!(slatedb, TransactionError, PyException);
create_exception!(slatedb, ClosedError, PyException);
create_exception!(slatedb, UnavailableError, PyException);
create_exception!(slatedb, InvalidError, PyException);
create_exception!(slatedb, DataError, PyException);
create_exception!(slatedb, InternalError, PyException);

// Map slatedb::Error into the appropriate Python exception type
fn map_error(err: Error) -> PyErr {
    let bt = Backtrace::capture();
    let msg = format!("{}\nBacktrace:\n{}", err, bt);
    match err.kind() {
        ::slatedb::ErrorKind::Transaction => TransactionError::new_err(msg),
        ::slatedb::ErrorKind::Closed(_) => ClosedError::new_err(msg),
        ::slatedb::ErrorKind::Unavailable => UnavailableError::new_err(msg),
        ::slatedb::ErrorKind::Invalid => InvalidError::new_err(msg),
        ::slatedb::ErrorKind::Data => DataError::new_err(msg),
        ::slatedb::ErrorKind::Internal => InternalError::new_err(msg),
        _ => InternalError::new_err(msg),
    }
}

async fn load_db_from_url(
    path: &str,
    url: &str,
    settings: Settings,
    merge_operator: Option<MergeOperatorType>,
) -> PyResult<Db> {
    let object_store = Db::resolve_object_store(url).map_err(map_error)?;

    let builder = Db::builder(path, object_store).with_settings(settings);
    let builder = if let Some(op) = merge_operator {
        builder.with_merge_operator(op)
    } else {
        builder
    };
    builder.build().await.map_err(map_error)
}

fn resolve_object_store_py(
    url: Option<&str>,
    env_file: Option<String>,
) -> PyResult<Arc<dyn ObjectStore>> {
    if let Some(u) = url {
        return Db::resolve_object_store(u).map_err(map_error);
    }
    if let Some(env) = env_file {
        return load_object_store_from_env(Some(env))
            .map_err(|e| InvalidError::new_err(e.to_string()));
    }
    Ok(Arc::new(InMemory::new()))
}

async fn load_db_from_env(
    path: &str,
    env_file: Option<String>,
    settings: Settings,
    merge_operator: Option<MergeOperatorType>,
) -> PyResult<Db> {
    let object_store = resolve_object_store_py(None, env_file)?;
    let builder = Db::builder(path, object_store).with_settings(settings);
    let builder = if let Some(op) = merge_operator {
        builder.with_merge_operator(op)
    } else {
        builder
    };
    builder.build().await.map_err(map_error)
}

/// A Python module implemented in Rust.
#[pymodule]
fn slatedb(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySlateDB>()?;
    m.add_class::<PySlateDBReader>()?;
    m.add_class::<PySlateDBSnapshot>()?;
    m.add_class::<PySlateDBTransaction>()?;
    m.add_class::<PySlateDBAdmin>()?;
    m.add_class::<PyWriteBatch>()?;
    m.add_class::<PyDbIterator>()?;
    // Export exception types
    m.add("TransactionError", py.get_type::<TransactionError>())?;
    m.add("ClosedError", py.get_type::<ClosedError>())?;
    m.add("UnavailableError", py.get_type::<UnavailableError>())?;
    m.add("InvalidError", py.get_type::<InvalidError>())?;
    m.add("DataError", py.get_type::<DataError>())?;
    m.add("InternalError", py.get_type::<InternalError>())?;
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
            Err(InvalidError::new_err("merge_operator must be a callable (merge(existing: Optional[bytes], value: bytes) -> bytes)"))
        }
    } else {
        Ok(None)
    }
}

fn build_scan_options(
    durability_filter: Option<String>,
    dirty: Option<bool>,
    read_ahead_bytes: Option<usize>,
    cache_blocks: Option<bool>,
    max_fetch_tasks: Option<usize>,
) -> PyResult<ScanOptions> {
    let mut opts = ScanOptions::default();
    if let Some(df) = durability_filter {
        opts.durability_filter = match df.to_lowercase().as_str() {
            "remote" => DurabilityLevel::Remote,
            "memory" => DurabilityLevel::Memory,
            other => {
                return Err(InvalidError::new_err(format!(
                    "invalid durability_filter: {other} (expected 'remote' or 'memory')"
                )))
            }
        };
    }
    if let Some(d) = dirty {
        opts.dirty = d;
    }
    if let Some(rab) = read_ahead_bytes {
        opts.read_ahead_bytes = rab;
    }
    if let Some(cb) = cache_blocks {
        opts.cache_blocks = cb;
    }
    if let Some(mft) = max_fetch_tasks {
        opts.max_fetch_tasks = mft;
    }
    Ok(opts)
}

fn build_read_options(
    durability_filter: Option<String>,
    dirty: Option<bool>,
) -> PyResult<ReadOptions> {
    let mut opts = ReadOptions::default();
    if let Some(df) = durability_filter {
        opts.durability_filter = match df.to_lowercase().as_str() {
            "remote" => DurabilityLevel::Remote,
            "memory" => DurabilityLevel::Memory,
            other => {
                return Err(InvalidError::new_err(format!(
                    "invalid durability_filter: {other} (expected 'remote' or 'memory')"
                )))
            }
        };
    }
    if let Some(d) = dirty {
        opts.dirty = d;
    }
    Ok(opts)
}

fn build_put_options(ttl: Option<u64>) -> PutOptions {
    PutOptions {
        ttl: match ttl {
            Some(v) => Ttl::ExpireAfter(v),
            None => Ttl::Default,
        },
    }
}

fn build_merge_options(ttl: Option<u64>) -> MergeOptions {
    MergeOptions {
        ttl: match ttl {
            Some(v) => Ttl::ExpireAfter(v),
            None => Ttl::Default,
        },
    }
}

fn build_write_options(await_durable: Option<bool>) -> WriteOptions {
    WriteOptions {
        await_durable: await_durable.unwrap_or(true),
    }
}

#[pyclass(name = "SlateDB")]
struct PySlateDB {
    inner: Arc<Db>,
}

impl PySlateDB {
    fn parse_isolation_level(level: Option<String>) -> PyResult<IsolationLevel> {
        match level
            .as_deref()
            .unwrap_or("si")
            .to_ascii_lowercase()
            .as_str()
        {
            "si" | "snapshot" => Ok(IsolationLevel::Snapshot),
            "ssi" | "serializable" | "serializable_snapshot" => {
                Ok(IsolationLevel::SerializableSnapshot)
            }
            other => Err(InvalidError::new_err(format!(
                "invalid isolation level: {other} (expected 'si' or 'ssi')"
            ))),
        }
    }
}

#[pymethods]
impl PySlateDB {
    #[classmethod]
    #[pyo3(signature = (path, url = None, env_file = None, *, merge_operator = None, **kwargs))]
    fn open_async<'py>(
        _cls: &'py Bound<'py, PyType>,
        py: Python<'py>,
        path: String,
        url: Option<String>,
        env_file: Option<String>,
        merge_operator: Option<&'py Bound<'py, PyAny>>,
        kwargs: Option<&'py Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let settings = match kwargs.and_then(|k| k.get_item("settings").ok().flatten()) {
            Some(settings_item) => {
                let settings_path = settings_item
                    .extract::<String>()
                    .map_err(|e| InvalidError::new_err(e.to_string()))?;
                Settings::from_file(settings_path).map_err(map_error)?
            }
            None => Settings::load().map_err(map_error)?,
        };
        let merge_operator = parse_merge_operator(merge_operator)?;
        future_into_py(py, async move {
            let db = if let Some(url) = url {
                load_db_from_url(&path, &url, settings, merge_operator).await?
            } else {
                load_db_from_env(&path, env_file, settings, merge_operator).await?
            };
            Ok(PySlateDB {
                inner: Arc::new(db),
            })
        })
    }

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
                    .map_err(|e| InvalidError::new_err(e.to_string()))?;
                Settings::from_file(settings_path).map_err(map_error)?
            }
            None => Settings::load().map_err(map_error)?,
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

    /// Create a read-only snapshot of the database at the current committed state.
    ///
    /// The snapshot provides a consistent view that is not affected by subsequent
    /// writes to the database. Use `SlateDBSnapshot` to perform read operations.
    fn snapshot(&self, py: Python<'_>) -> PyResult<PySlateDBSnapshot> {
        let db = self.inner.clone();
        let rt = get_runtime();
        let snapshot =
            py.allow_threads(|| rt.block_on(async { db.snapshot().await.map_err(map_error) }))?;
        Ok(PySlateDBSnapshot {
            inner: Some(snapshot),
        })
    }

    fn snapshot_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        future_into_py(py, async move {
            let snapshot = db.snapshot().await.map_err(map_error)?;
            Ok(PySlateDBSnapshot {
                inner: Some(snapshot),
            })
        })
    }

    #[pyo3(signature = (isolation = None))]
    fn begin(&self, py: Python<'_>, isolation: Option<String>) -> PyResult<PySlateDBTransaction> {
        let db = self.inner.clone();
        let level = Self::parse_isolation_level(isolation)?;
        let rt = get_runtime();
        let txn =
            py.allow_threads(|| rt.block_on(async { db.begin(level).await.map_err(map_error) }))?;
        Ok(PySlateDBTransaction { inner: Some(txn) })
    }

    #[pyo3(signature = (isolation = None))]
    fn begin_async<'py>(
        &self,
        py: Python<'py>,
        isolation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let level = Self::parse_isolation_level(isolation)?;
        future_into_py(py, async move {
            db.begin(level)
                .await
                .map(|txn| PySlateDBTransaction { inner: Some(txn) })
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (key, value))]
    fn put(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        py.allow_threads(|| rt.block_on(async { db.put(&key, &value).await.map_err(map_error) }))
    }

    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match db.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.as_ref().to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(map_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (key, *, durability_filter = None, dirty = None))]
    fn get_with_options<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let opts = build_read_options(durability_filter, dirty)?;
        let rt = get_runtime();
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match db.get_with_options(&key, &opts).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.as_ref().to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(map_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (start, end = None))]
    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });

        let db = self.inner.clone();
        let rt = get_runtime();
        let iter = rt.block_on(async { db.scan(start..end).await.map_err(map_error) })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None))]
    fn scan_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let db = self.inner.clone();
        future_into_py(py, async move {
            let iter = db.scan(start..end).await.map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    #[pyo3(signature = (key))]
    fn delete(&self, py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while awaiting Rust I/O
        py.allow_threads(|| rt.block_on(async { db.delete(&key).await.map_err(map_error) }))
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let db = self.inner.clone();
        let rt = get_runtime();
        let iter = rt.block_on(async {
            db.scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)
        })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let db = self.inner.clone();
        future_into_py(py, async move {
            let iter = db
                .scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        py.allow_threads(|| rt.block_on(async { db.close().await.map_err(map_error) }))
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        future_into_py(py, async move { db.close().await.map_err(map_error) })
    }

    /// Merge a value into the database using the configured merge operator.
    #[pyo3(signature = (key, value))]
    fn merge(&self, py: Python<'_>, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while DB.merge may invoke Python merge operator
        py.allow_threads(|| rt.block_on(async { db.merge(&key, &value).await.map_err(map_error) }))
    }

    #[pyo3(signature = (key, value, *, ttl = None, await_durable = None))]
    fn merge_with_options(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
        await_durable: Option<bool>,
    ) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        let merge_opts = build_merge_options(ttl);
        let write_opts = build_write_options(await_durable);
        py.allow_threads(|| {
            rt.block_on(async move {
                db.merge_with_options(&key, &value, &merge_opts, &write_opts)
                    .await
                    .map_err(map_error)
            })
        })
    }

    #[pyo3(signature = (key, value, *, ttl = None, await_durable = None))]
    fn merge_with_options_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
        await_durable: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let merge_opts = build_merge_options(ttl);
        let write_opts = build_write_options(await_durable);
        future_into_py(py, async move {
            db.merge_with_options(&key, &value, &merge_opts, &write_opts)
                .await
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (key, value))]
    fn merge_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move {
            db.merge(&key, &value).await.map_err(map_error)
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
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(
            py,
            async move { db.put(&key, &value).await.map_err(map_error) },
        )
    }

    #[pyo3(signature = (key))]
    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move {
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(map_error(e)),
            }
        })
    }

    #[pyo3(signature = (key, *, durability_filter = None, dirty = None))]
    fn get_with_options_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let opts = build_read_options(durability_filter, dirty)?;
        future_into_py(py, async move {
            match db.get_with_options(&key, &opts).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(map_error(e)),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn delete_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        future_into_py(py, async move { db.delete(&key).await.map_err(map_error) })
    }

    #[pyo3(signature = (key, value, *, ttl = None, await_durable = None))]
    fn put_with_options(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
        await_durable: Option<bool>,
    ) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        let put_opts = build_put_options(ttl);
        let write_opts = build_write_options(await_durable);
        py.allow_threads(|| {
            rt.block_on(async move {
                db.put_with_options(&key, &value, &put_opts, &write_opts)
                    .await
                    .map_err(map_error)
            })
        })
    }

    #[pyo3(signature = (key, value, *, ttl = None, await_durable = None))]
    fn put_with_options_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
        await_durable: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let put_opts = build_put_options(ttl);
        let write_opts = build_write_options(await_durable);
        future_into_py(py, async move {
            db.put_with_options(&key, &value, &put_opts, &write_opts)
                .await
                .map_err(map_error)
        })
    }

    #[pyo3(signature = (key, *, await_durable = None))]
    fn delete_with_options(
        &self,
        py: Python<'_>,
        key: Vec<u8>,
        await_durable: Option<bool>,
    ) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let rt = get_runtime();
        let write_opts = build_write_options(await_durable);
        py.allow_threads(|| {
            rt.block_on(async move {
                db.delete_with_options(&key, &write_opts)
                    .await
                    .map_err(map_error)
            })
        })
    }

    fn write(&self, py: Python<'_>, batch: &PyWriteBatch) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        let b = batch.inner.clone();
        py.allow_threads(|| rt.block_on(async move { db.write(b).await.map_err(map_error) }))
    }

    #[pyo3(signature = (batch, *, await_durable = None))]
    fn write_with_options(
        &self,
        py: Python<'_>,
        batch: &PyWriteBatch,
        await_durable: Option<bool>,
    ) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        let b = batch.inner.clone();
        let opts = build_write_options(await_durable);
        py.allow_threads(|| {
            rt.block_on(async move { db.write_with_options(b, &opts).await.map_err(map_error) })
        })
    }

    fn write_async<'py>(&self, py: Python<'py>, batch: &PyWriteBatch) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let b = batch.inner.clone();
        future_into_py(py, async move { db.write(b).await.map_err(map_error) })
    }

    #[pyo3(signature = (batch, *, await_durable = None))]
    fn write_with_options_async<'py>(
        &self,
        py: Python<'py>,
        batch: &PyWriteBatch,
        await_durable: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let b = batch.inner.clone();
        let opts = build_write_options(await_durable);
        future_into_py(py, async move { db.write_with_options(b, &opts).await.map_err(map_error) })
    }

    #[pyo3(signature = (key, *, await_durable = None))]
    fn delete_with_options_async<'py>(
        &self,
        py: Python<'py>,
        key: Vec<u8>,
        await_durable: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db = self.inner.clone();
        let write_opts = build_write_options(await_durable);
        future_into_py(py, async move {
            db.delete_with_options(&key, &write_opts)
                .await
                .map_err(map_error)
        })
    }

    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        py.allow_threads(|| rt.block_on(async move { db.flush().await.map_err(map_error) }))
    }

    fn flush_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        future_into_py(py, async move { db.flush().await.map_err(map_error) })
    }

    #[pyo3(signature = (flush_type))]
    fn flush_with_options(&self, py: Python<'_>, flush_type: String) -> PyResult<()> {
        let db = self.inner.clone();
        let rt = get_runtime();
        let options = FlushOptions {
            flush_type: match flush_type.to_lowercase().as_str() {
                "wal" => FlushType::Wal,
                "memtable" => FlushType::MemTable,
                other => {
                    return Err(InvalidError::new_err(format!(
                        "invalid flush_type: {other} (expected 'wal' or 'memtable')"
                    )))
                }
            },
        };
        py.allow_threads(|| {
            rt.block_on(async move { db.flush_with_options(options).await.map_err(map_error) })
        })
    }

    #[pyo3(signature = (flush_type))]
    fn flush_with_options_async<'py>(
        &self,
        py: Python<'py>,
        flush_type: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let options = FlushOptions {
            flush_type: match flush_type.to_lowercase().as_str() {
                "wal" => FlushType::Wal,
                "memtable" => FlushType::MemTable,
                other => {
                    return Err(InvalidError::new_err(format!(
                        "invalid flush_type: {other} (expected 'wal' or 'memtable')"
                    )))
                }
            },
        };
        future_into_py(py, async move {
            db.flush_with_options(options).await.map_err(map_error)
        })
    }

    #[pyo3(signature = (scope = "all", *, lifetime = None, source = None))]
    fn create_checkpoint<'py>(
        &self,
        py: Python<'py>,
        scope: &str,
        lifetime: Option<u64>,
        source: Option<String>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let db = self.inner.clone();
        let rt = get_runtime();
        let scope_enum = match scope.to_ascii_lowercase().as_str() {
            "all" => CheckpointScope::All,
            "durable" => CheckpointScope::Durable,
            other => {
                return Err(InvalidError::new_err(format!(
                    "invalid scope: {other} (expected 'all' or 'durable')"
                )))
            }
        };
        let result = rt.block_on(async move {
            let lifetime = lifetime.map(Duration::from_millis);
            let source = source
                .map(|s| {
                    Uuid::parse_str(&s)
                        .map_err(|e| InvalidError::new_err(format!("invalid source UUID: {e}")))
                })
                .transpose()?;

            db.create_checkpoint(scope_enum, &CheckpointOptions { lifetime, source })
                .await
                .map_err(map_error)
        })?;
        let dict = PyDict::new(py);
        dict.set_item("id", result.id.to_string())?;
        dict.set_item("manifest_id", result.manifest_id)?;
        Ok(dict)
    }

    #[pyo3(signature = (scope = "all", *, lifetime = None, source = None))]
    fn create_checkpoint_async<'py>(
        &self,
        py: Python<'py>,
        scope: &str,
        lifetime: Option<u64>,
        source: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let db = self.inner.clone();
        let scope_enum = match scope.to_ascii_lowercase().as_str() {
            "all" => CheckpointScope::All,
            "durable" => CheckpointScope::Durable,
            other => {
                return Err(InvalidError::new_err(format!(
                    "invalid scope: {other} (expected 'all' or 'durable')"
                )))
            }
        };
        future_into_py(py, async move {
            let lifetime = lifetime.map(Duration::from_millis);
            let source = source
                .map(|s| {
                    Uuid::parse_str(&s)
                        .map_err(|e| InvalidError::new_err(format!("invalid source UUID: {e}")))
                })
                .transpose()?;
            let result = db
                .create_checkpoint(scope_enum, &CheckpointOptions { lifetime, source })
                .await
                .map_err(map_error)?;
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                dict.set_item("id", result.id.to_string())?;
                dict.set_item("manifest_id", result.manifest_id)?;
                Ok(dict.into_any().unbind())
            })
        })
    }
}

#[pyclass(name = "SlateDBSnapshot")]
struct PySlateDBSnapshot {
    inner: Option<Arc<DbSnapshot>>, // None after close()
}

impl PySlateDBSnapshot {
    fn inner_ref(&self) -> PyResult<Arc<DbSnapshot>> {
        self.inner
            .as_ref()
            .cloned()
            .ok_or_else(|| ClosedError::new_err("snapshot is closed"))
    }
}

#[pymethods]
impl PySlateDBSnapshot {
    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let snapshot = self.inner_ref()?;
        let rt = get_runtime();
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match snapshot.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.as_ref().to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(map_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (key))]
    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let snapshot = self.inner_ref()?;
        future_into_py(py, async move {
            match snapshot.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(map_error(e)),
            }
        })
    }

    #[pyo3(signature = (start, end = None))]
    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let snapshot = self.inner_ref()?;
        let rt = get_runtime();
        let iter = rt.block_on(async { snapshot.scan(start..end).await.map_err(map_error) })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None))]
    fn scan_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let snapshot = self.inner_ref()?;
        future_into_py(py, async move {
            let iter = snapshot.scan(start..end).await.map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let snapshot = self.inner_ref()?;
        let rt = get_runtime();
        let iter = rt.block_on(async {
            snapshot
                .scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)
        })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let snapshot = self.inner_ref()?;
        future_into_py(py, async move {
            let iter = snapshot
                .scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner = None; // drop snapshot; unregisters in Drop
        Ok(())
    }

    fn close_async<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Drop immediately and return a ready future
        self.inner = None;
        future_into_py(py, async move { Ok(()) })
    }
}

#[pyclass(name = "SlateDBTransaction")]
struct PySlateDBTransaction {
    inner: Option<DBTransaction>, // None after commit/rollback
}

impl PySlateDBTransaction {
    fn inner_ref(&self) -> PyResult<&DBTransaction> {
        self.inner
            .as_ref()
            .ok_or_else(|| ClosedError::new_err("transaction is closed"))
    }
}

#[pyclass(name = "WriteBatch")]
struct PyWriteBatch {
    inner: ::slatedb::WriteBatch,
}

#[pymethods]
impl PyWriteBatch {
    #[new]
    fn new() -> Self {
        Self {
            inner: ::slatedb::WriteBatch::new(),
        }
    }

    #[pyo3(signature = (key, value))]
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        self.inner.put(&key, &value);
        Ok(())
    }

    #[pyo3(signature = (key, value, *, ttl = None))]
    fn put_with_options(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let opts = build_put_options(ttl);
        self.inner.put_with_options(&key, &value, &opts);
        Ok(())
    }

    #[pyo3(signature = (key))]
    fn delete(&mut self, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        self.inner.delete(&key);
        Ok(())
    }

    #[pyo3(signature = (key, value))]
    fn merge(&mut self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        self.inner.merge(&key, &value);
        Ok(())
    }

    #[pyo3(signature = (key, value, *, ttl = None))]
    fn merge_with_options(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<u64>,
    ) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let opts = build_merge_options(ttl);
        self.inner.merge_with_options(&key, &value, &opts);
        Ok(())
    }
}

#[pymethods]
impl PySlateDBTransaction {
    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let txn = self.inner_ref()?;
        let rt = get_runtime();
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match txn.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.as_ref().to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(map_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (start, end = None))]
    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let txn = self.inner_ref()?;
        let rt = get_runtime();
        let iter = rt.block_on(async { txn.scan(start..end).await.map_err(map_error) })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let txn = self.inner_ref()?;
        let rt = get_runtime();
        let iter = rt.block_on(async {
            txn.scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)
        })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (key, value))]
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let txn = self.inner_ref()?;
        txn.put(&key, &value).map_err(map_error)
    }

    #[pyo3(signature = (key))]
    fn delete(&self, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let txn = self.inner_ref()?;
        txn.delete(&key).map_err(map_error)
    }

    /// Merge within a transaction (requires merge operator configured on DB)
    #[pyo3(signature = (key, value))]
    fn merge(&self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let txn = self.inner_ref()?;
        txn.merge(&key, &value).map_err(map_error)
    }

    fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
        let txn = self
            .inner
            .take()
            .ok_or_else(|| ClosedError::new_err("transaction is closed"))?;
        let rt = get_runtime();
        py.allow_threads(|| rt.block_on(async { txn.commit().await.map_err(map_error) }))
    }

    fn commit_async<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let txn = self
            .inner
            .take()
            .ok_or_else(|| ClosedError::new_err("transaction is closed"))?;
        future_into_py(py, async move { txn.commit().await.map_err(map_error) })
    }

    fn rollback(&mut self) -> PyResult<()> {
        if let Some(txn) = self.inner.take() {
            txn.rollback();
        }
        Ok(())
    }
}

#[pyclass(name = "SlateDBReader")]
struct PySlateDBReader {
    inner: Arc<DbReader>,
}

#[pymethods]
impl PySlateDBReader {
    #[classmethod]
    #[pyo3(signature = (path, url = None, env_file = None, checkpoint_id = None, *, merge_operator = None))]
    fn open_async<'py>(
        _cls: &'py Bound<'py, PyType>,
        py: Python<'py>,
        path: String,
        url: Option<String>,
        env_file: Option<String>,
        checkpoint_id: Option<String>,
        merge_operator: Option<&'py Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let object_store = resolve_object_store_py(url.as_deref(), env_file.clone())?;
        let merge_operator = parse_merge_operator(merge_operator)?;
        future_into_py(py, async move {
            let mut options = DbReaderOptions::default();
            options.merge_operator = merge_operator;
            let checkpoint = checkpoint_id
                .map(|id| {
                    Uuid::parse_str(&id)
                        .map_err(|e| InvalidError::new_err(format!("invalid checkpoint_id: {e}")))
                })
                .transpose()?;
            let db_reader = DbReader::open(path, object_store, checkpoint, options)
                .await
                .map_err(map_error)?;
            Ok(PySlateDBReader {
                inner: Arc::new(db_reader),
            })
        })
    }
    #[new]
    #[pyo3(signature = (path, url = None, env_file = None, checkpoint_id = None, *, merge_operator = None))]
    fn new(
        path: String,
        url: Option<String>,
        env_file: Option<String>,
        checkpoint_id: Option<String>,
        merge_operator: Option<&Bound<PyAny>>,
    ) -> PyResult<Self> {
        let rt = get_runtime();
        let object_store = resolve_object_store_py(url.as_deref(), env_file)?;
        let db_reader = rt.block_on(async {
            let mut options = DbReaderOptions::default();
            options.merge_operator = parse_merge_operator(merge_operator)?;
            DbReader::open(
                path,
                object_store,
                checkpoint_id
                    .map(|id| {
                        Uuid::parse_str(&id).map_err(|e| {
                            InvalidError::new_err(format!("invalid checkpoint_id: {e}"))
                        })
                    })
                    .transpose()?,
                options,
            )
            .await
            .map_err(map_error)
        })?;
        Ok(Self {
            inner: Arc::new(db_reader),
        })
    }

    #[pyo3(signature = (key))]
    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db_reader = self.inner.clone();
        let rt = get_runtime();
        // Release the GIL while awaiting Rust; merge operator may need to reacquire it.
        let res: Option<Vec<u8>> = py.allow_threads(|| {
            rt.block_on(async {
                match db_reader.get(&key).await {
                    Ok(Some(bytes)) => Ok::<Option<Vec<u8>>, PyErr>(Some(bytes.to_vec())),
                    Ok(None) => Ok(None),
                    Err(e) => Err(map_error(e)),
                }
            })
        })?;
        Ok(res.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (key))]
    fn get_async<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let db_reader = self.inner.clone();
        future_into_py(py, async move {
            match db_reader.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(map_error(e)),
            }
        })
    }

    #[pyo3(signature = (start, end = None))]
    fn scan(&self, start: Vec<u8>, end: Option<Vec<u8>>) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });

        let reader = self.inner.clone();
        let rt = get_runtime();
        let iter = rt.block_on(async { reader.scan(start..end).await.map_err(map_error) })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None))]
    fn scan_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let reader = self.inner.clone();
        future_into_py(py, async move {
            let iter = reader.scan(start..end).await.map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options(
        &self,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<PyDbIterator> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let reader = self.inner.clone();
        let rt = get_runtime();
        let iter = rt.block_on(async {
            reader
                .scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)
        })?;
        Ok(PyDbIterator::from_iter(iter))
    }

    #[pyo3(signature = (start, end = None, *, durability_filter = None, dirty = None, read_ahead_bytes = None, cache_blocks = None, max_fetch_tasks = None))]
    fn scan_with_options_async<'py>(
        &self,
        py: Python<'py>,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        durability_filter: Option<String>,
        dirty: Option<bool>,
        read_ahead_bytes: Option<usize>,
        cache_blocks: Option<bool>,
        max_fetch_tasks: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if start.is_empty() {
            return Err(InvalidError::new_err("start cannot be empty"));
        }
        let end = end.unwrap_or_else(|| {
            let mut end = start.clone();
            end.push(0xff);
            end
        });
        let opts = build_scan_options(
            durability_filter,
            dirty,
            read_ahead_bytes,
            cache_blocks,
            max_fetch_tasks,
        )?;
        let reader = self.inner.clone();
        future_into_py(py, async move {
            let iter = reader
                .scan_with_options(start..end, &opts)
                .await
                .map_err(map_error)?;
            Ok(PyDbIterator::from_iter(iter))
        })
    }

    fn close(&self) -> PyResult<()> {
        let db_reader = self.inner.clone();
        let rt = get_runtime();
        rt.block_on(async { db_reader.close().await.map_err(map_error) })
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db_reader = self.inner.clone();
        future_into_py(
            py,
            async move { db_reader.close().await.map_err(map_error) },
        )
    }
}

#[pyclass(name = "SlateDBAdmin")]
struct PySlateDBAdmin {
    inner: Arc<Admin>,
}

#[pymethods]
impl PySlateDBAdmin {
    #[new]
    #[pyo3(signature = (path, url = None, env_file = None))]
    fn new(path: String, url: Option<String>, env_file: Option<String>) -> PyResult<Self> {
        let object_store = resolve_object_store_py(url.as_deref(), env_file)?;
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
                .map(|s| {
                    Uuid::parse_str(&s)
                        .map_err(|e| InvalidError::new_err(format!("invalid source UUID: {e}")))
                })
                .transpose()?;
            admin
                .create_detached_checkpoint(&CheckpointOptions { lifetime, source })
                .await
                .map_err(map_error)
        })?;
        let dict = PyDict::new(py);
        dict.set_item("id", result.id.to_string())?;
        dict.set_item("manifest_id", result.manifest_id)?;
        Ok(dict)
    }

    #[pyo3(signature = (lifetime = None, source = None))]
    fn create_checkpoint_async<'py>(
        &self,
        py: Python<'py>,
        lifetime: Option<u64>,
        source: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.inner.clone();
        future_into_py(py, async move {
            let lifetime = lifetime.map(Duration::from_millis);
            let source = source
                .map(|s| {
                    Uuid::parse_str(&s)
                        .map_err(|e| InvalidError::new_err(format!("invalid source UUID: {e}")))
                })
                .transpose()?;
            let result = admin
                .create_detached_checkpoint(&CheckpointOptions { lifetime, source })
                .await
                .map_err(map_error)?;
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                dict.set_item("id", result.id.to_string())?;
                dict.set_item("manifest_id", result.manifest_id)?;
                // Convert to a GIL-independent PyObject for return
                Ok(dict.into_any().unbind())
            })
        })
    }

    fn list_checkpoints<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let admin = self.inner.clone();
        let rt = get_runtime();
        let result = rt.block_on(async {
            admin
                .list_checkpoints()
                .await
                .map_err(|e| UnavailableError::new_err(e.to_string()))
        })?;
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

    fn list_checkpoints_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let admin = self.inner.clone();
        future_into_py(py, async move {
            let result = admin
                .list_checkpoints()
                .await
                .map_err(|e| UnavailableError::new_err(e.to_string()))?;
            Python::with_gil(|py| {
                let out: PyResult<Vec<PyObject>> = result
                    .into_iter()
                    .map(|c| {
                        let dict = PyDict::new(py);
                        dict.set_item("id", c.id.to_string())?;
                        dict.set_item("manifest_id", c.manifest_id)?;
                        dict.set_item("expire_time", c.expire_time.map(|t| t.timestamp_millis()))?;
                        dict.set_item("create_time", c.create_time.timestamp_millis())?;
                        // Convert to a GIL-independent PyObject for return
                        Ok(dict.into_any().unbind())
                    })
                    .collect();
                Ok(out?)
            })
        })
    }
}

#[pyclass(name = "DbIterator")]
pub struct PyDbIterator {
    inner_iter: Arc<Mutex<Option<::slatedb::DbIterator>>>,
}

impl PyDbIterator {
    fn from_iter(iter: ::slatedb::DbIterator) -> Self {
        Self {
            inner_iter: Arc::new(Mutex::new(Some(iter))),
        }
    }
}

#[pymethods]
impl PyDbIterator {
    fn __iter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let inner = self.inner_iter.clone();
        let rt = get_runtime();
        let kv_opt = py.allow_threads(|| {
            rt.block_on(async {
                let mut guard = inner.lock().await;
                let iter = guard
                    .as_mut()
                    .ok_or_else(|| InternalError::new_err("iterator not initialized"))?;
                let next = iter.next().await.map_err(map_error)?;
                Ok::<_, PyErr>(next)
            })
        })?;
        match kv_opt {
            Some(kv) => {
                let key = PyBytes::new(py, &kv.key);
                let value = PyBytes::new(py, &kv.value);
                let tuple = PyTuple::new(py, vec![key, value])?;
                Ok(tuple.into())
            }
            None => Err(pyo3::exceptions::PyStopIteration::new_err("")),
        }
    }

    /// Enable `async for` by providing an async iterator protocol.
    fn __aiter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    fn __anext__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner_iter.clone();
        future_into_py::<_, PyObject>(py, async move {
            let mut guard = inner.lock().await;
            let iter = guard
                .as_mut()
                .ok_or_else(|| InternalError::new_err("iterator not initialized"))?;
            let kv_opt = iter.next().await.map_err(map_error)?;
            match kv_opt {
                Some(kv) => Python::with_gil(|py| {
                    let key = PyBytes::new(py, &kv.key);
                    let value = PyBytes::new(py, &kv.value);
                    let tuple = PyTuple::new(py, vec![key, value])?;
                    Ok::<PyObject, PyErr>(tuple.into())
                }),
                None => Err(pyo3::exceptions::PyStopAsyncIteration::new_err("")),
            }
        })
    }

    #[pyo3(signature = (key))]
    fn seek(&mut self, py: Python<'_>, key: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let inner = self.inner_iter.clone();
        let rt = get_runtime();
        py.allow_threads(|| {
            rt.block_on(async {
                let mut guard = inner.lock().await;
                let iter = guard
                    .as_mut()
                    .ok_or_else(|| InternalError::new_err("iterator not initialized"))?;
                iter.seek(&key).await.map_err(map_error)
            })
        })?;
        Ok(())
    }

    #[pyo3(signature = (key))]
    fn seek_async<'py>(&mut self, py: Python<'py>, key: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        if key.is_empty() {
            return Err(InvalidError::new_err("key cannot be empty"));
        }
        let inner = self.inner_iter.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let iter = guard
                .as_mut()
                .ok_or_else(|| InternalError::new_err("iterator not initialized"))?;
            iter.seek(&key).await.map_err(map_error)
        })
    }
}
