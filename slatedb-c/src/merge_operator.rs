//! Merge-operator bridge for `slatedb-c`.
//!
//! This module adapts C callbacks into SlateDB's Rust `MergeOperator` trait so
//! builder APIs can register merge operators from C callers.

use crate::ffi::{slatedb_merge_operator_context_free_fn, slatedb_merge_operator_result_free_fn};
use slatedb::bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};
use std::ffi::c_void;
use std::ptr;

/// Merge-operator bridge that forwards merge resolution to C callbacks.
pub(crate) struct CMergeOperator {
    merge_fn: unsafe extern "C" fn(
        key: *const u8,
        key_len: usize,
        existing_value: *const u8,
        existing_value_len: usize,
        has_existing_value: bool,
        operand: *const u8,
        operand_len: usize,
        out_value: *mut *mut u8,
        out_value_len: *mut usize,
        context: *mut c_void,
    ) -> bool,
    free_result_fn: slatedb_merge_operator_result_free_fn,
    free_context_fn: slatedb_merge_operator_context_free_fn,
    context_addr: usize,
}

impl CMergeOperator {
    /// Creates a new C callback-backed merge operator.
    pub(crate) fn new(
        merge_fn: unsafe extern "C" fn(
            key: *const u8,
            key_len: usize,
            existing_value: *const u8,
            existing_value_len: usize,
            has_existing_value: bool,
            operand: *const u8,
            operand_len: usize,
            out_value: *mut *mut u8,
            out_value_len: *mut usize,
            context: *mut c_void,
        ) -> bool,
        context: *mut c_void,
        free_result_fn: slatedb_merge_operator_result_free_fn,
        free_context_fn: slatedb_merge_operator_context_free_fn,
    ) -> Self {
        Self {
            merge_fn,
            free_result_fn,
            free_context_fn,
            context_addr: context as usize,
        }
    }

    fn context_ptr(&self) -> *mut c_void {
        self.context_addr as *mut c_void
    }

    fn maybe_free_result(&self, value: *mut u8, value_len: usize) {
        if value.is_null() {
            return;
        }
        if let Some(free_result_fn) = self.free_result_fn {
            // SAFETY: The callback pointer is provided by the user and only called
            // with the exact value pointer/length produced by the user callback.
            unsafe { free_result_fn(value, value_len, self.context_ptr()) };
        }
    }
}

impl Drop for CMergeOperator {
    fn drop(&mut self) {
        if let Some(free_context_fn) = self.free_context_fn {
            // SAFETY: The context pointer and optional free callback are provided
            // by the caller and are invoked at most once when this operator drops.
            unsafe { free_context_fn(self.context_ptr()) };
        }
    }
}

impl MergeOperator for CMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operand: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        let mut out_value = ptr::null_mut();
        let mut out_value_len = 0usize;
        let (existing_ptr, existing_len, has_existing_value) = match existing_value.as_ref() {
            Some(existing_value) => (existing_value.as_ptr(), existing_value.len(), true),
            None => (ptr::null(), 0, false),
        };

        // SAFETY: The callback pointer comes from validated FFI input and all
        // passed pointers/lengths are valid for the duration of this call.
        let merged = unsafe {
            (self.merge_fn)(
                key.as_ptr(),
                key.len(),
                existing_ptr,
                existing_len,
                has_existing_value,
                operand.as_ptr(),
                operand.len(),
                &mut out_value,
                &mut out_value_len,
                self.context_ptr(),
            )
        };

        if !merged {
            self.maybe_free_result(out_value, out_value_len);
            return Err(MergeOperatorError::EmptyBatch);
        }

        if out_value_len == 0 {
            self.maybe_free_result(out_value, out_value_len);
            return Ok(Bytes::new());
        }

        if out_value.is_null() {
            return Err(MergeOperatorError::EmptyBatch);
        }

        // SAFETY: `out_value` is non-null and `out_value_len` is supplied by the
        // callback contract for a readable contiguous byte region.
        let merged_value = Bytes::copy_from_slice(unsafe {
            std::slice::from_raw_parts(out_value as *const u8, out_value_len)
        });
        self.maybe_free_result(out_value, out_value_len);
        Ok(merged_value)
    }
}
