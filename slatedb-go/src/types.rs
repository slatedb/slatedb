use slatedb::DbIterator;
use slatedb::{Db, WriteBatch};
use std::ptr;
use tokio::runtime::Runtime;

/// Internal struct that owns a Tokio runtime and a SlateDB instance.
/// This eliminates the need for a global handle map and shared runtime.
pub struct SlateDbFFI {
    pub rt: Runtime,
    pub db: Db,
}

impl SlateDbFFI {
    /// Convenience helper to run an async block on the internal runtime.
    pub fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }
}

/// Type-safe wrapper around a pointer to SlateDbFFI.
/// This provides better type safety than raw u64 handles.
#[repr(C)]
pub struct CSdbHandle(pub *mut SlateDbFFI);

impl CSdbHandle {
    pub fn null() -> Self {
        CSdbHandle(ptr::null_mut())
    }

    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// # Safety
    ///
    /// Caller must ensure the pointer is valid and properly aligned.
    /// The returned mutable reference must not outlive the pointer's validity.
    pub unsafe fn as_inner(&mut self) -> &mut SlateDbFFI {
        &mut *self.0
    }
}

// Value type for returning data from get operations
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbValue {
    pub data: *mut u8,
    pub len: usize,
}

// Key-Value pair for scan results
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbKeyValue {
    pub key: CSdbValue,
    pub value: CSdbValue,
}

// Scan result containing multiple key-value pairs
#[repr(C)]
pub struct CSdbScanResult {
    pub items: *mut CSdbKeyValue,
    pub count: usize,
    pub has_more: bool,
    pub next_key: CSdbValue, // Next key for pagination (empty if no more)
}

// Write options for controlling write operation behavior
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbWriteOptions {
    pub await_durable: bool,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbPutOptions {
    /// TTL type: 0=Default, 1=NoExpiry, 2=ExpireAfter
    pub ttl_type: u32,
    /// TTL value in milliseconds (only used when ttl_type=2)
    pub ttl_value: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbReadOptions {
    /// Durability filter: 0=Memory, 1=Remote
    pub durability_filter: u32,
    /// Whether to include dirty/uncommitted data
    pub dirty: bool,
}

// Scan options for controlling scan behavior
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CSdbScanOptions {
    pub durability_filter: i32, // 0 = Memory (default), 1 = Remote
    pub dirty: bool,            // Include uncommitted writes (default: false)
    pub read_ahead_bytes: u64,  // Buffer size for read-ahead (default: 1)
    pub cache_blocks: bool,     // Whether to cache fetched blocks (default: false)
    pub max_fetch_tasks: u64,   // Maximum concurrent fetch tasks (default: 1)
}

/// Internal struct for managing database iterators in FFI
/// Contains the iterator and a reference to the database to ensure proper lifetime management
pub struct CSdbIterator {
    pub db_ptr: *mut SlateDbFFI, // Keep DB alive via pointer reference
    pub iter: DbIterator,
}

impl CSdbIterator {
    /// Create a new iterator FFI wrapper
    pub fn new(db_ptr: *mut SlateDbFFI, iter: DbIterator) -> Box<Self> {
        Box::new(CSdbIterator { db_ptr, iter })
    }
}

/// Internal struct for managing WriteBatch operations in FFI
/// Contains the WriteBatch that can be moved out when writing to the database
pub struct CSdbWriteBatch {
    pub batch: Option<WriteBatch>, // Can be moved out on write
}
