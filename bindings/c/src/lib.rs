use ::slatedb as core;
use core::config::DbOptions;
use core::db::Db;
use object_store::{
    aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory, path::Path, ObjectStore,
};
use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Arc;

mod error;
use error::slatedb_error;

mod config;
use config::{slatedb_config_source, slatedb_object_store};

// TODO:
// better comments and docs
// add other cloud storages in config option
// expose the remanining API methods
// tests
#[repr(C)]
pub struct slatedb_instance {
    // pointer to slatedb::Db
    inner: *mut c_void,
    // pointer to tokio::runtime::Runtime
    rt: *mut c_void,
}

impl slatedb_instance {
    pub(crate) fn deref(&self) -> &core::db::Db {
        unsafe { &*(self.inner as *mut core::db::Db) }
    }

    fn runtime(&self) -> &tokio::runtime::Runtime {
        unsafe { &*(self.rt as *mut tokio::runtime::Runtime) }
    }
}

#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_free(ptr: *const slatedb_instance) {
    if !ptr.is_null() {
        drop(Box::from_raw((*ptr).inner as *mut core::db::Db));
        drop(Box::from_raw((*ptr).rt as *mut tokio::runtime::Runtime));
        drop(Box::from_raw(ptr as *mut slatedb_instance));
    }
}

// Corresponds to Result<slatedb::Db, SlateDBError>
#[repr(C)]
pub struct slatedb_new_result {
    instance: *mut slatedb_instance,
    error: *mut slatedb_error,
}

/// Creates a new instance of slatedb based on the config provided
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_new(
    path: *const c_char,
    config: slatedb_config_source,
    object_store: slatedb_object_store,
) -> slatedb_new_result {
    let rt = Box::new(tokio::runtime::Runtime::new().unwrap());
    let path = CStr::from_ptr(path);
    let path = Path::from(path.to_str().unwrap());
    let object_store = match object_store {
        slatedb_object_store::IN_MEMORY => Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
        slatedb_object_store::LOCAL_FILESYSTEM => {
            Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>
        }
        slatedb_object_store::AWS(config) => {
            let bucket = CStr::from_ptr(config.bucket_name).to_str().unwrap();
            let obj_store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
                .build()
                .unwrap();
            Arc::new(obj_store) as Arc<dyn ObjectStore>
        }
    };

    let result = match config {
        slatedb_config_source::DEFAULT => rt.block_on(Db::open(path, object_store)),
        slatedb_config_source::FROM_FILE(config_path) => {
            let options =
                DbOptions::from_file(CStr::from_ptr(config_path).to_str().unwrap()).unwrap();
            rt.block_on(Db::open_with_opts(path, options, object_store))
        }
        slatedb_config_source::FROM_ENV(prefix) => {
            let options = DbOptions::from_env(CStr::from_ptr(prefix).to_str().unwrap()).unwrap();
            rt.block_on(Db::open_with_opts(path, options, object_store))
        }
        slatedb_config_source::LOAD => {
            let options = DbOptions::load().unwrap();
            rt.block_on(Db::open_with_opts(path, options, object_store))
        }
    };

    match result {
        Ok(db) => {
            let instance = Box::into_raw(Box::new(slatedb_instance {
                inner: Box::into_raw(Box::new(db)) as _,
                rt: Box::into_raw(rt) as _,
            }));
            slatedb_new_result {
                instance,
                error: std::ptr::null_mut(),
            }
        }
        Err(err) => slatedb_new_result {
            instance: std::ptr::null_mut(),
            error: slatedb_error::new(err),
        },
    }
}

#[repr(C)]
pub struct slatedb_instance_get_result {
    // Wrap value in an enum to make it like an Option<u8>
    value: *const c_char,
    error: *mut slatedb_error,
}

/// Corresponds to slatedb::Db::get
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_get(
    instance: &slatedb_instance,
    key: *const c_char,
) -> slatedb_instance_get_result {
    let key = CStr::from_ptr(key);
    let key = key.to_str().unwrap();
    match instance
        .runtime()
        .block_on(instance.deref().get(key.as_bytes()))
    {
        Ok(v) => match v {
            Some(v) => {
                let value = CString::new(v.as_ref()).unwrap();
                slatedb_instance_get_result {
                    value: value.into_raw(),
                    error: std::ptr::null_mut(),
                }
            }
            None => slatedb_instance_get_result {
                value: std::ptr::null_mut(),
                error: std::ptr::null_mut(),
            },
        },
        Err(err) => slatedb_instance_get_result {
            value: std::ptr::null_mut(),
            error: slatedb_error::new(err),
        },
    }
}

/// Corresponds to slatedb::Db::put
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_put(
    instance: &slatedb_instance,
    key: *const c_char,
    value: *const c_char,
) -> *mut slatedb_error {
    let key = CStr::from_ptr(key);
    let value = CStr::from_ptr(value);
    let key = key.to_str().unwrap();
    let value = value.to_str().unwrap();
    match instance
        .runtime()
        .block_on(instance.deref().put(key.as_bytes(), value.as_bytes()))
    {
        Ok(_) => std::ptr::null_mut(),
        Err(err) => slatedb_error::new(err),
    }
}

/// Corresponds to slatedb::Db::delete
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_delete(
    instance: &slatedb_instance,
    key: *const c_char,
) -> *mut slatedb_error {
    let key = CStr::from_ptr(key);
    let key = key.to_str().unwrap();
    match instance
        .runtime()
        .block_on(instance.deref().delete(key.as_bytes()))
    {
        Ok(_) => std::ptr::null_mut(),
        Err(err) => slatedb_error::new(err),
    }
}

/// Corresponds to slatedb::Db::flush
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_flush(instance: &slatedb_instance) -> *mut slatedb_error {
    match instance.runtime().block_on(instance.deref().flush()) {
        Ok(_) => std::ptr::null_mut(),
        Err(err) => slatedb_error::new(err),
    }
}

/// Corresponds to slatedb::Db::close
#[no_mangle]
pub unsafe extern "C" fn slatedb_instance_close(instance: &slatedb_instance) -> *mut slatedb_error {
    match instance.runtime().block_on(instance.deref().close()) {
        Ok(_) => std::ptr::null_mut(),
        Err(err) => slatedb_error::new(err),
    }
}
