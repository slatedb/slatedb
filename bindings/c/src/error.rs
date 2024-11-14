use ::slatedb::error::SlateDBError;

use std::ffi::{c_char, CString};

// Wrapper type for SlateDBError types
#[repr(C)]
#[allow(non_camel_case_types)]
pub enum slatedb_code {
    IO_ERROR,
    CHECKSUM_MISMATCH,
    EMPTY_SSTABLE,
    EMPTY_BLOCK_META,
    EMPTY_BLOCK,
    OBJECT_STORE_ERROR,
    MANIFEST_VERSION_EXISTS,
    MANIFEST_MISSING,
    INVALID_DELETION,
    INVALID_FLATBUFFER,
    INVALID_DB_STATE,
    INVALID_COMPACTION,
    FENCED,
    INVALID_CACHE_PART_SIZE,
    INVALID_COMPRESSION_CODEC,
    BLOCK_DECOMPRESSION_ERROR,
    BLOCK_COMPRESSION_ERROR,
    INVALID_ROW_FLAGS,
}
impl From<SlateDBError> for slatedb_code {
    fn from(err: SlateDBError) -> Self {
        match err {
            SlateDBError::IoError(_) => slatedb_code::IO_ERROR,
            SlateDBError::ChecksumMismatch => slatedb_code::CHECKSUM_MISMATCH,
            SlateDBError::EmptySSTable => slatedb_code::EMPTY_SSTABLE,
            SlateDBError::EmptyBlockMeta => slatedb_code::EMPTY_BLOCK_META,
            SlateDBError::EmptyBlock => slatedb_code::EMPTY_BLOCK,
            SlateDBError::ObjectStoreError(_) => slatedb_code::OBJECT_STORE_ERROR,
            SlateDBError::ManifestVersionExists => slatedb_code::MANIFEST_VERSION_EXISTS,
            SlateDBError::ManifestMissing => slatedb_code::MANIFEST_MISSING,
            SlateDBError::InvalidDeletion => slatedb_code::INVALID_DELETION,
            SlateDBError::InvalidFlatbuffer(_) => slatedb_code::INVALID_FLATBUFFER,
            SlateDBError::InvalidDBState => slatedb_code::INVALID_DB_STATE,
            SlateDBError::InvalidCompaction => slatedb_code::INVALID_COMPACTION,
            SlateDBError::Fenced => slatedb_code::FENCED,
            SlateDBError::InvalidCachePartSize => slatedb_code::INVALID_CACHE_PART_SIZE,
            SlateDBError::InvalidCompressionCodec => slatedb_code::INVALID_COMPRESSION_CODEC,
            SlateDBError::BlockDecompressionError => slatedb_code::BLOCK_DECOMPRESSION_ERROR,
            SlateDBError::BlockCompressionError => slatedb_code::BLOCK_COMPRESSION_ERROR,
            SlateDBError::InvalidRowFlags => slatedb_code::INVALID_ROW_FLAGS,
            _ => unimplemented!("This error type has not been implemented yet"),
        }
    }
}

#[repr(C)]
pub struct slatedb_error {
    code: slatedb_code,
    message: *const c_char,
}

impl slatedb_error {
    /// Create a new slatedb error via `core::error::SlateDBError`.
    ///
    /// We will call `Box::leak()` to leak this error, so the caller should be responsible for
    /// free this error.
    pub fn new(err: SlateDBError) -> *mut slatedb_error {
        let message = CString::new(err.to_string()).unwrap().into_raw();
        let code = slatedb_code::from(err);

        Box::into_raw(Box::new(slatedb_error { code, message }))
    }

    /// Frees the slatedb_error, ok to call on NULL
    #[no_mangle]
    pub unsafe extern "C" fn slatedb_error_free(ptr: *mut slatedb_error) {
        if !ptr.is_null() {
            drop(Box::from_raw(ptr));
        }
    }
}
