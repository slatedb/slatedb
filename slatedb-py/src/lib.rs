use ::slatedb::object_store::{memory::InMemory, ObjectStore};
use ::slatedb::Db;
use once_cell::sync::OnceCell;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
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

#[pymethods]
impl PySlateDB {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let rt = get_runtime();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = rt.block_on(async {
            Db::open(path.as_str(), object_store)
                .await
                .map_err(create_value_error)
        })?;
        Ok(Self { db: Arc::new(db) })
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> PyResult<()> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async { db.put(&key, &value).await.map_err(create_value_error) })
    }

    fn get<'py>(&self, py: Python<'py>, key: Vec<u8>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        if key.is_empty() {
            return Err(create_value_error("key cannot be empty"));
        }
        let db = self.db.clone();
        let rt = get_runtime();
        rt.block_on(async {
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(PyBytes::new(py, &bytes))),
                Ok(None) => Ok(None),
                Err(e) => Err(create_value_error(e)),
            }
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

    // Async API methods
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
                Ok(Some(bytes)) => Ok(Some(bytes.to_vec())),
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
