//! Dedicated Tokio runtime support for the UniFFI bindings.
//!
//! UniFFI's `async_runtime = "tokio"` support can drive exported futures through
//! `async-compat`'s current-thread runtime. SlateDB captures `Handle::current()`
//! while opening a `Db` or `DbReader` and uses that handle for long-lived
//! background tasks, so opening directly on that runtime can leave WAL flush,
//! memtable flush, compaction, GC, and reader polling on a single thread.
//!
//! We cannot simply `spawn` the whole open future onto our runtime, because open
//! can synchronously invoke foreign callbacks such as custom metrics recorders.
//! Some bindings, notably Node, expect those callbacks on the original binding
//! call path. `EnterRuntime` keeps polling on the caller's future path while
//! making `Handle::current()` resolve to the dedicated multi-threaded runtime
//! for the duration of each poll.

use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};

use tokio::runtime::{Builder, Handle, Runtime};

const RUNTIME_THREADS_ENV: &str = "SLATEDB_UNIFFI_RUNTIME_THREADS";
const FALLBACK_WORKER_THREADS: usize = 4;

/// Polls `future` with the dedicated UniFFI runtime entered as the current
/// Tokio context. This lets SlateDB capture the multi-threaded runtime handle
/// during open without moving synchronous foreign callbacks onto runtime worker
/// threads.
pub(crate) fn enter<F>(future: F) -> impl Future<Output = F::Output>
where
    F: Future,
{
    EnterRuntime {
        handle: runtime().handle().clone(),
        future: Box::pin(future),
    }
}

// Wraps an open future so every poll occurs inside `handle.enter()`.
//
// Entering once around `.await` would hold the enter guard across suspension,
// leaking the runtime context back to the caller. Entering per poll ensures the
// guard is dropped before returning `Poll::Pending` or `Poll::Ready`.
struct EnterRuntime<F> {
    handle: Handle,
    future: Pin<Box<F>>,
}

impl<F: Future> Future for EnterRuntime<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = this.handle.enter();
        this.future.as_mut().poll(cx)
    }
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        // The runtime must outlive every Db/DbReader opened through UniFFI,
        // because their background tasks keep using the handle captured at open.
        Builder::new_multi_thread()
            .worker_threads(configured_worker_threads(
                std::env::var(RUNTIME_THREADS_ENV).ok().as_deref(),
            ))
            .enable_all()
            .thread_name("slatedb-uniffi-rt")
            .build()
            .expect("failed to build SlateDB UniFFI runtime")
    })
}

fn configured_worker_threads(value: Option<&str>) -> usize {
    // Invalid env values are ignored so a bad deployment knob cannot prevent
    // the binding from opening databases.
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(FALLBACK_WORKER_THREADS)
        })
}

#[cfg(test)]
mod tests {
    use tokio::runtime::{Handle, RuntimeFlavor};

    use super::configured_worker_threads;

    #[test]
    fn worker_threads_uses_positive_env_value() {
        assert_eq!(configured_worker_threads(Some("8")), 8);
        assert_eq!(configured_worker_threads(Some(" 3 ")), 3);
    }

    #[test]
    fn worker_threads_falls_back_for_invalid_env_values() {
        let default = configured_worker_threads(None);

        assert_eq!(configured_worker_threads(Some("0")), default);
        assert_eq!(configured_worker_threads(Some("-1")), default);
        assert_eq!(configured_worker_threads(Some("not-a-number")), default);
    }

    #[test]
    fn default_worker_threads_is_non_zero() {
        assert!(configured_worker_threads(None) > 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn enter_uses_multi_thread_runtime_context() {
        let flavor = super::enter(async { Handle::current().runtime_flavor() }).await;

        assert_eq!(flavor, RuntimeFlavor::MultiThread);
    }
}
