//! Merge-operator bridge for `slatedb-c`.
//!
//! This module adapts C callbacks into SlateDB's Rust `MergeOperator` trait so
//! builder APIs can register merge operators from C callers.

use crate::ffi::slatedb_merge_operator_out_free_fn;
use slatedb::bytes::Bytes;
use slatedb::{MergeOperator, MergeOperatorError};
use std::ptr;

/// Merge-operator bridge that forwards merge resolution to C callbacks.
pub(crate) struct CMergeOperator {
    merge_fn: unsafe extern "C" fn(
        key: *const u8,
        key_len: usize,
        existing_value_present: bool,
        existing_value: *const u8,
        existing_value_len: usize,
        operand: *const u8,
        operand_len: usize,
        out_value: *mut *mut u8,
        out_value_len: *mut usize,
    ) -> bool,
    free_result_fn: slatedb_merge_operator_out_free_fn,
}

impl CMergeOperator {
    /// Creates a new C callback-backed merge operator.
    pub(crate) fn new(
        merge_fn: unsafe extern "C" fn(
            key: *const u8,
            key_len: usize,
            existing_value_present: bool,
            existing_value: *const u8,
            existing_value_len: usize,
            operand: *const u8,
            operand_len: usize,
            out_value: *mut *mut u8,
            out_value_len: *mut usize,
        ) -> bool,
        free_result_fn: slatedb_merge_operator_out_free_fn,
    ) -> Self {
        Self {
            merge_fn,
            free_result_fn,
        }
    }

    fn maybe_free_result(&self, value: *mut u8, value_len: usize) {
        if value.is_null() {
            return;
        }
        if let Some(free_result_fn) = self.free_result_fn {
            // SAFETY: The callback pointer is provided by the user and only called
            // with the exact value pointer/length produced by the user callback.
            unsafe { free_result_fn(value, value_len) };
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
        let (existing_value_present, existing_ptr, existing_len) = match existing_value.as_ref() {
            Some(existing_value) => (true, existing_value.as_ptr(), existing_value.len()),
            None => (false, ptr::null(), 0),
        };

        // SAFETY: The callback pointer comes from validated FFI input and all
        // passed pointers/lengths are valid for the duration of this call.
        let merged = unsafe {
            (self.merge_fn)(
                key.as_ptr(),
                key.len(),
                existing_value_present,
                existing_ptr,
                existing_len,
                operand.as_ptr(),
                operand.len(),
                &mut out_value,
                &mut out_value_len,
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

#[cfg(test)]
mod tests {
    use super::CMergeOperator;
    use slatedb::bytes::Bytes;
    use slatedb::{MergeOperator, MergeOperatorError};
    use std::cell::RefCell;
    use std::ptr;

    static MERGED_BYTES: &[u8] = b"merged-value";
    static SENTINEL_BYTE: u8 = 0xAB;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
    enum CallbackMode {
        #[default]
        SuccessBytes,
        SuccessEmptyWithNonNullPtr,
        SuccessNonEmptyWithNullPtr,
        FailureWithNonNullPtr,
    }

    #[derive(Clone, Debug, Default)]
    struct CallbackState {
        mode: CallbackMode,
        last_key: Vec<u8>,
        last_existing: Option<Vec<u8>>,
        last_operand: Vec<u8>,
        last_existing_value_present: bool,
        free_result_calls: usize,
        last_free_result_len: usize,
        last_free_result_non_null: bool,
    }

    thread_local! {
        static CALLBACK_STATE: RefCell<CallbackState> = RefCell::new(CallbackState::default());
    }

    fn reset_state(mode: CallbackMode) {
        CALLBACK_STATE.with(|state| {
            *state.borrow_mut() = CallbackState {
                mode,
                ..CallbackState::default()
            };
        });
    }

    fn state_snapshot() -> CallbackState {
        CALLBACK_STATE.with(|state| state.borrow().clone())
    }

    unsafe extern "C" fn test_merge_fn(
        key: *const u8,
        key_len: usize,
        existing_value_present: bool,
        existing_value: *const u8,
        existing_value_len: usize,
        operand: *const u8,
        operand_len: usize,
        out_value: *mut *mut u8,
        out_value_len: *mut usize,
    ) -> bool {
        CALLBACK_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.last_key = std::slice::from_raw_parts(key, key_len).to_vec();
            state.last_operand = std::slice::from_raw_parts(operand, operand_len).to_vec();
            state.last_existing_value_present = existing_value_present;
            state.last_existing = if existing_value_present {
                Some(std::slice::from_raw_parts(existing_value, existing_value_len).to_vec())
            } else {
                None
            };

            match state.mode {
                CallbackMode::SuccessBytes => {
                    *out_value = MERGED_BYTES.as_ptr() as *mut u8;
                    *out_value_len = MERGED_BYTES.len();
                    true
                }
                CallbackMode::SuccessEmptyWithNonNullPtr => {
                    *out_value = (&SENTINEL_BYTE as *const u8) as *mut u8;
                    *out_value_len = 0;
                    true
                }
                CallbackMode::SuccessNonEmptyWithNullPtr => {
                    *out_value = ptr::null_mut();
                    *out_value_len = 4;
                    true
                }
                CallbackMode::FailureWithNonNullPtr => {
                    *out_value = (&SENTINEL_BYTE as *const u8) as *mut u8;
                    *out_value_len = 1;
                    false
                }
            }
        })
    }

    unsafe extern "C" fn test_free_result_fn(value: *mut u8, value_len: usize) {
        CALLBACK_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.free_result_calls += 1;
            state.last_free_result_non_null = !value.is_null();
            state.last_free_result_len = value_len;
        });
    }

    fn new_operator(mode: CallbackMode, free_result: bool) -> CMergeOperator {
        reset_state(mode);
        CMergeOperator::new(
            test_merge_fn,
            if free_result {
                Some(test_free_result_fn)
            } else {
                None
            },
        )
    }

    #[test]
    fn test_merge_forwards_args_and_frees_result() {
        let operator = new_operator(CallbackMode::SuccessBytes, true);

        let merged = operator
            .merge(
                &Bytes::from_static(b"key"),
                Some(Bytes::from_static(b"existing")),
                Bytes::from_static(b"operand"),
            )
            .unwrap();

        assert_eq!(merged, Bytes::from_static(MERGED_BYTES));

        let state = state_snapshot();
        assert_eq!(state.last_key, b"key");
        assert_eq!(state.last_existing, Some(b"existing".to_vec()));
        assert_eq!(state.last_operand, b"operand");
        assert!(state.last_existing_value_present);
        assert_eq!(state.free_result_calls, 1);
        assert!(state.last_free_result_non_null);
        assert_eq!(state.last_free_result_len, MERGED_BYTES.len());
    }

    #[test]
    fn test_merge_with_none_existing_sets_existing_value_present_false() {
        let operator = new_operator(CallbackMode::SuccessBytes, false);

        let merged = operator
            .merge(
                &Bytes::from_static(b"key"),
                None,
                Bytes::from_static(b"operand"),
            )
            .unwrap();

        assert_eq!(merged, Bytes::from_static(MERGED_BYTES));

        let state = state_snapshot();
        assert!(!state.last_existing_value_present);
        assert_eq!(state.last_existing, None);
        assert_eq!(state.free_result_calls, 0);
    }

    #[test]
    fn test_merge_empty_value_calls_free_result_and_returns_empty_bytes() {
        let operator = new_operator(CallbackMode::SuccessEmptyWithNonNullPtr, true);

        let merged = operator
            .merge(&Bytes::from_static(b"k"), None, Bytes::from_static(b"v"))
            .unwrap();
        assert!(merged.is_empty());

        let state = state_snapshot();
        assert_eq!(state.free_result_calls, 1);
        assert_eq!(state.last_free_result_len, 0);
        assert!(state.last_free_result_non_null);
    }

    #[test]
    fn test_merge_failure_calls_free_result_and_returns_error() {
        let operator = new_operator(CallbackMode::FailureWithNonNullPtr, true);

        let err = operator
            .merge(&Bytes::from_static(b"k"), None, Bytes::from_static(b"v"))
            .expect_err("expected merge callback failure");
        assert!(matches!(err, MergeOperatorError::EmptyBatch));

        let state = state_snapshot();
        assert_eq!(state.free_result_calls, 1);
        assert_eq!(state.last_free_result_len, 1);
        assert!(state.last_free_result_non_null);
    }

    #[test]
    fn test_merge_non_empty_null_ptr_returns_error_without_free_result() {
        let operator = new_operator(CallbackMode::SuccessNonEmptyWithNullPtr, true);

        let err = operator
            .merge(&Bytes::from_static(b"k"), None, Bytes::from_static(b"v"))
            .expect_err("expected null-pointer merge failure");
        assert!(matches!(err, MergeOperatorError::EmptyBatch));

        let state = state_snapshot();
        assert_eq!(state.free_result_calls, 0);
    }
}
