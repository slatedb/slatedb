//! Logging APIs for `slatedb-c`.
//!
//! This module installs a process-global logger bridge and exposes C ABI
//! helpers to configure log level and optional callback forwarding.

use crate::ffi::{
    error_result, slatedb_error_kind_t, slatedb_log_callback_fn, slatedb_log_context_free_fn,
    slatedb_log_level_t, slatedb_result_t, success_result, SLATEDB_LOG_LEVEL_DEBUG,
    SLATEDB_LOG_LEVEL_ERROR, SLATEDB_LOG_LEVEL_INFO, SLATEDB_LOG_LEVEL_OFF,
    SLATEDB_LOG_LEVEL_TRACE, SLATEDB_LOG_LEVEL_WARN,
};
use log::{Level, LevelFilter, Metadata, Record};
use std::cell::Cell;
use std::ffi::c_void;
use std::os::raw::c_char;
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

type CLogCallback = unsafe extern "C" fn(
    level: slatedb_log_level_t,
    target: *const c_char,
    target_len: usize,
    message: *const c_char,
    message_len: usize,
    module_path: *const c_char,
    module_path_len: usize,
    file: *const c_char,
    file_len: usize,
    line: u32,
    context: *mut c_void,
);

struct LogCallbackState {
    callback: CLogCallback,
    context_addr: usize,
    free_context: slatedb_log_context_free_fn,
}

impl LogCallbackState {
    fn context_ptr(&self) -> *mut c_void {
        self.context_addr as *mut c_void
    }
}

impl Drop for LogCallbackState {
    fn drop(&mut self) {
        if let Some(free_context) = self.free_context {
            // SAFETY: Callback/context come from user configuration. We invoke
            // it exactly once when this callback state is dropped.
            unsafe { free_context(self.context_ptr()) };
        }
    }
}

#[derive(Default)]
struct LoggingState {
    callback: Option<Arc<LogCallbackState>>,
}

fn logging_state() -> &'static Mutex<LoggingState> {
    static LOGGING_STATE: OnceLock<Mutex<LoggingState>> = OnceLock::new();
    LOGGING_STATE.get_or_init(|| Mutex::new(LoggingState::default()))
}

fn install_lock() -> &'static Mutex<()> {
    static INSTALL_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    INSTALL_LOCK.get_or_init(|| Mutex::new(()))
}

/// 0 = unknown/unset, 1 = installed by `slatedb-c`, 2 = external logger.
static LOGGER_OWNER: AtomicU8 = AtomicU8::new(0);

struct SlateDbLogger;

static SLATEDB_LOGGER: SlateDbLogger = SlateDbLogger;

thread_local! {
    static LOGGING_REENTRANT: Cell<bool> = const { Cell::new(false) };
}

struct ReentrantLogGuard;

impl ReentrantLogGuard {
    fn enter() -> Option<Self> {
        LOGGING_REENTRANT.with(|active| {
            if active.get() {
                None
            } else {
                active.set(true);
                Some(Self)
            }
        })
    }
}

impl Drop for ReentrantLogGuard {
    fn drop(&mut self) {
        LOGGING_REENTRANT.with(|active| active.set(false));
    }
}

impl log::Log for SlateDbLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        match log::max_level().to_level() {
            Some(level) => metadata.level() <= level,
            None => false,
        }
    }

    fn log(&self, record: &Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let Some(_guard) = ReentrantLogGuard::enter() else {
            return;
        };

        let callback = {
            let guard = logging_state().lock().expect("lock poisoned");
            guard.callback.clone()
        };

        match callback {
            Some(callback) => dispatch_callback(&callback, record),
            None => default_stderr_log(record),
        }
    }

    fn flush(&self) {}
}

fn ensure_logger_installed() -> Result<(), slatedb_result_t> {
    match LOGGER_OWNER.load(Ordering::Acquire) {
        1 => return Ok(()),
        2 => {
            return Err(error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "global logger already initialized by another library",
            ));
        }
        _ => {}
    }

    let _guard = install_lock().lock().expect("lock poisoned");
    match LOGGER_OWNER.load(Ordering::Acquire) {
        1 => return Ok(()),
        2 => {
            return Err(error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "global logger already initialized by another library",
            ));
        }
        _ => {}
    }

    match log::set_logger(&SLATEDB_LOGGER) {
        Ok(()) => {
            LOGGER_OWNER.store(1, Ordering::Release);
            Ok(())
        }
        Err(_) => {
            LOGGER_OWNER.store(2, Ordering::Release);
            Err(error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "global logger already initialized by another library",
            ))
        }
    }
}

fn level_filter_from_u8(level: slatedb_log_level_t) -> Result<LevelFilter, slatedb_result_t> {
    match level {
        SLATEDB_LOG_LEVEL_OFF => Ok(LevelFilter::Off),
        SLATEDB_LOG_LEVEL_ERROR => Ok(LevelFilter::Error),
        SLATEDB_LOG_LEVEL_WARN => Ok(LevelFilter::Warn),
        SLATEDB_LOG_LEVEL_INFO => Ok(LevelFilter::Info),
        SLATEDB_LOG_LEVEL_DEBUG => Ok(LevelFilter::Debug),
        SLATEDB_LOG_LEVEL_TRACE => Ok(LevelFilter::Trace),
        _ => Err(error_result(
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
            "invalid log level (use SLATEDB_LOG_LEVEL_* constants)",
        )),
    }
}

fn level_to_u8(level: Level) -> slatedb_log_level_t {
    match level {
        Level::Error => SLATEDB_LOG_LEVEL_ERROR,
        Level::Warn => SLATEDB_LOG_LEVEL_WARN,
        Level::Info => SLATEDB_LOG_LEVEL_INFO,
        Level::Debug => SLATEDB_LOG_LEVEL_DEBUG,
        Level::Trace => SLATEDB_LOG_LEVEL_TRACE,
    }
}

fn str_ptr_len(value: &str) -> (*const c_char, usize) {
    if value.is_empty() {
        (ptr::null(), 0)
    } else {
        (value.as_ptr() as *const c_char, value.len())
    }
}

fn opt_str_ptr_len(value: Option<&str>) -> (*const c_char, usize) {
    match value {
        Some(value) => str_ptr_len(value),
        None => (ptr::null(), 0),
    }
}

fn dispatch_callback(callback: &LogCallbackState, record: &Record<'_>) {
    let message = record.args().to_string();
    let (target_ptr, target_len) = str_ptr_len(record.target());
    let (message_ptr, message_len) = str_ptr_len(&message);
    let (module_ptr, module_len) = opt_str_ptr_len(record.module_path_static());
    let (file_ptr, file_len) = opt_str_ptr_len(record.file_static());
    let line = record.line().unwrap_or(0);

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: Function pointer/context come from user configuration and all
        // string pointers reference live memory for the duration of this call.
        unsafe {
            (callback.callback)(
                level_to_u8(record.level()),
                target_ptr,
                target_len,
                message_ptr,
                message_len,
                module_ptr,
                module_len,
                file_ptr,
                file_len,
                line,
                callback.context_ptr(),
            );
        }
    }));
}

fn default_stderr_log(record: &Record<'_>) {
    eprintln!(
        "[slatedb][{}][{}] {}",
        record.level(),
        record.target(),
        record.args()
    );
}

/// Initializes `slatedb-c` logging with a process-global logger.
///
/// Calling this function multiple times is allowed when `slatedb-c` already
/// owns the global logger. If another library has already installed a global
/// logger, this function returns `SLATEDB_ERROR_KIND_INVALID`.
///
/// ## Arguments
/// - `level`: Logging level selector (`SLATEDB_LOG_LEVEL_*`).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
#[no_mangle]
pub extern "C" fn slatedb_logging_init(level: slatedb_log_level_t) -> slatedb_result_t {
    if let Err(err) = ensure_logger_installed() {
        return err;
    }
    slatedb_logging_set_level(level)
}

/// Updates the global logging level for `slatedb-c` logger output.
///
/// ## Arguments
/// - `level`: Logging level selector (`SLATEDB_LOG_LEVEL_*`).
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
#[no_mangle]
pub extern "C" fn slatedb_logging_set_level(level: slatedb_log_level_t) -> slatedb_result_t {
    if let Err(err) = ensure_logger_installed() {
        return err;
    }

    let level = match level_filter_from_u8(level) {
        Ok(level) => level,
        Err(err) => return err,
    };

    log::set_max_level(level);
    success_result()
}

/// Sets a callback for receiving SlateDB log messages.
///
/// Replaces any existing callback. When replaced, the old callback context is
/// released using the previous `free_context` callback after in-flight log
/// callbacks complete.
///
/// ## Arguments
/// - `callback`: Log callback function pointer (must be non-null).
/// - `context`: Opaque callback context passed to every invocation.
/// - `free_context`: Optional context free callback.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
///
/// ## Errors
/// - Returns `SLATEDB_ERROR_KIND_INVALID` when `callback` is null.
///
/// ## Safety
/// - `callback` and `context` must remain valid while configured.
/// - Callback must be thread-safe and must not retain borrowed pointer
///   arguments after it returns.
#[no_mangle]
pub unsafe extern "C" fn slatedb_logging_set_callback(
    callback: slatedb_log_callback_fn,
    context: *mut c_void,
    free_context: slatedb_log_context_free_fn,
) -> slatedb_result_t {
    if let Err(err) = ensure_logger_installed() {
        return err;
    }

    let callback = match callback {
        Some(callback) => callback,
        None => {
            return error_result(
                slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
                "callback pointer is null",
            );
        }
    };

    let new_callback = Arc::new(LogCallbackState {
        callback,
        context_addr: context as usize,
        free_context,
    });

    let old_callback = {
        let mut guard = logging_state().lock().expect("lock poisoned");
        guard.callback.replace(new_callback)
    };
    drop(old_callback);

    success_result()
}

/// Clears the configured log callback.
///
/// If a callback is configured with `free_context`, the context is released
/// after in-flight callback invocations complete.
///
/// ## Returns
/// - `slatedb_result_t` indicating success/failure.
#[no_mangle]
pub extern "C" fn slatedb_logging_clear_callback() -> slatedb_result_t {
    if let Err(err) = ensure_logger_installed() {
        return err;
    }

    let old_callback = {
        let mut guard = logging_state().lock().expect("lock poisoned");
        guard.callback.take()
    };
    drop(old_callback);

    success_result()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::slatedb_result_free;
    use std::slice;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Clone)]
    struct LogEvent {
        level: u8,
        target: String,
        message: String,
    }

    fn test_lock() -> &'static Mutex<()> {
        static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn events() -> &'static Mutex<Vec<LogEvent>> {
        static EVENTS: OnceLock<Mutex<Vec<LogEvent>>> = OnceLock::new();
        EVENTS.get_or_init(|| Mutex::new(Vec::new()))
    }

    static FREE_COUNT: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn collect_log_callback(
        level: slatedb_log_level_t,
        target: *const c_char,
        target_len: usize,
        message: *const c_char,
        message_len: usize,
        _module_path: *const c_char,
        _module_path_len: usize,
        _file: *const c_char,
        _file_len: usize,
        _line: u32,
        _context: *mut c_void,
    ) {
        let target = if target.is_null() {
            String::new()
        } else {
            String::from_utf8_lossy(slice::from_raw_parts(target as *const u8, target_len))
                .into_owned()
        };
        let message = if message.is_null() {
            String::new()
        } else {
            String::from_utf8_lossy(slice::from_raw_parts(message as *const u8, message_len))
                .into_owned()
        };
        events().lock().expect("lock poisoned").push(LogEvent {
            level,
            target,
            message,
        });
    }

    unsafe extern "C" fn noop_log_callback(
        _level: slatedb_log_level_t,
        _target: *const c_char,
        _target_len: usize,
        _message: *const c_char,
        _message_len: usize,
        _module_path: *const c_char,
        _module_path_len: usize,
        _file: *const c_char,
        _file_len: usize,
        _line: u32,
        _context: *mut c_void,
    ) {
    }

    unsafe extern "C" fn free_context_callback(context: *mut c_void) {
        if !context.is_null() {
            let _ = Box::from_raw(context as *mut usize);
        }
        FREE_COUNT.fetch_add(1, Ordering::SeqCst);
    }

    fn assert_result_kind(result: slatedb_result_t, expected: slatedb_error_kind_t) {
        let kind = result.kind;
        slatedb_result_free(result);
        assert_eq!(kind, expected);
    }

    fn assert_ok(result: slatedb_result_t) {
        assert_result_kind(result, slatedb_error_kind_t::SLATEDB_ERROR_KIND_NONE);
    }

    #[test]
    fn test_logging_init_is_idempotent() {
        let _guard = test_lock().lock().expect("lock poisoned");
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_DEBUG));
        assert_ok(slatedb_logging_clear_callback());
    }

    #[test]
    fn test_logging_set_level_filters_messages() {
        let _guard = test_lock().lock().expect("lock poisoned");
        events().lock().expect("lock poisoned").clear();

        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_WARN));
        assert_ok(unsafe {
            slatedb_logging_set_callback(Some(collect_log_callback), ptr::null_mut(), None)
        });

        log::info!(target: "slatedb_c/logging_test", "this should be filtered");
        log::warn!(target: "slatedb_c/logging_test", "this should pass");

        let matching_events: Vec<LogEvent> = events()
            .lock()
            .expect("lock poisoned")
            .iter()
            .filter(|event| event.target == "slatedb_c/logging_test")
            .cloned()
            .collect();
        assert_eq!(matching_events.len(), 1);
        assert_eq!(matching_events[0].level, SLATEDB_LOG_LEVEL_WARN);
        assert_eq!(matching_events[0].message, "this should pass");

        assert_ok(slatedb_logging_clear_callback());
    }

    #[test]
    fn test_logging_set_callback_rejects_null_callback() {
        let _guard = test_lock().lock().expect("lock poisoned");
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));
        assert_result_kind(
            unsafe { slatedb_logging_set_callback(None, ptr::null_mut(), None) },
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert_ok(slatedb_logging_clear_callback());
    }

    #[test]
    fn test_logging_clear_callback_frees_context_once() {
        let _guard = test_lock().lock().expect("lock poisoned");
        FREE_COUNT.store(0, Ordering::SeqCst);
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));

        let context = Box::into_raw(Box::new(1usize)) as *mut c_void;
        assert_ok(unsafe {
            slatedb_logging_set_callback(
                Some(noop_log_callback),
                context,
                Some(free_context_callback),
            )
        });
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 0);

        assert_ok(slatedb_logging_clear_callback());
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);

        assert_ok(slatedb_logging_clear_callback());
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_logging_replacing_callback_frees_old_context() {
        let _guard = test_lock().lock().expect("lock poisoned");
        FREE_COUNT.store(0, Ordering::SeqCst);
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));

        let context1 = Box::into_raw(Box::new(1usize)) as *mut c_void;
        assert_ok(unsafe {
            slatedb_logging_set_callback(
                Some(noop_log_callback),
                context1,
                Some(free_context_callback),
            )
        });
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 0);

        let context2 = Box::into_raw(Box::new(1usize)) as *mut c_void;
        assert_ok(unsafe {
            slatedb_logging_set_callback(
                Some(noop_log_callback),
                context2,
                Some(free_context_callback),
            )
        });
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 1);

        assert_ok(slatedb_logging_clear_callback());
        assert_eq!(FREE_COUNT.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_logging_set_level_rejects_invalid_selector() {
        let _guard = test_lock().lock().expect("lock poisoned");
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));
        assert_result_kind(
            slatedb_logging_set_level(255),
            slatedb_error_kind_t::SLATEDB_ERROR_KIND_INVALID,
        );
        assert_ok(slatedb_logging_clear_callback());
    }

    #[test]
    fn test_logging_clear_callback_is_usable_without_prior_callback() {
        let _guard = test_lock().lock().expect("lock poisoned");
        assert_ok(slatedb_logging_init(SLATEDB_LOG_LEVEL_INFO));
        assert_ok(slatedb_logging_clear_callback());
    }
}
