use crate::db::FlushMsg;
use crate::error::SlateDBError;
use crate::error::SlateDBError::{BackgroundTaskPanic, BackgroundTaskShutdown};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedReceiver;

pub(crate) struct WatchableOnceCell<T: Clone> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
    tx: tokio::sync::watch::Sender<Option<T>>,
}

pub(crate) struct WatchableOnceCellReader<T: Clone> {
    rx: tokio::sync::watch::Receiver<Option<T>>,
}

impl<T: Clone> WatchableOnceCell<T> {
    pub(crate) fn new() -> Self {
        let (tx, rx) = tokio::sync::watch::channel(None);
        Self { rx, tx }
    }

    pub(crate) fn write(&self, val: T) {
        self.tx.send_if_modified(|v| {
            if v.is_some() {
                return false;
            }
            v.replace(val);
            true
        });
    }

    pub(crate) fn reader(&self) -> WatchableOnceCellReader<T> {
        WatchableOnceCellReader {
            rx: self.rx.clone(),
        }
    }
}

impl<T: Clone> WatchableOnceCellReader<T> {
    pub(crate) fn read(&self) -> Option<T> {
        self.rx.borrow().clone()
    }

    pub(crate) async fn await_value(&mut self) -> T {
        self.rx
            .wait_for(|v| v.is_some())
            .await
            .expect("watch channel closed")
            .clone()
            .expect("no value found")
    }
}

/// Spawn a monitored background tokio task. The task must return a Result<T, SlateDBError>.
/// The task is spawned by a monitor task. When the task exits, the monitor task
/// calls a provided cleanup fn with the returned error, or Err(BackgroundTaskShutdown)
/// if the task exited with Ok. If the spawned task panics, the cleanup fn is called with
/// Err(BackgroundTaskFailed).
pub(crate) fn spawn_bg_task<F, T, C>(
    handle: &tokio::runtime::Handle,
    cleanup_fn: C,
    future: F,
) -> tokio::task::JoinHandle<Result<T, SlateDBError>>
where
    F: Future<Output = Result<T, SlateDBError>> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&SlateDBError) + Send + 'static,
{
    let inner_handle = handle.clone();
    handle.spawn(async move {
        let jh = inner_handle.spawn(future);
        match jh.await {
            Ok(Err(err)) => {
                // task exited with an error
                cleanup_fn(&err);
                Err(err)
            }
            Ok(result) => {
                cleanup_fn(&BackgroundTaskShutdown);
                result
            }
            Err(join_err) => {
                // task panic'd or was cancelled
                let err = BackgroundTaskPanic(Arc::new(Mutex::new(
                    join_err
                        .try_into_panic()
                        .unwrap_or_else(|_| Box::new("background task was aborted")),
                )));
                cleanup_fn(&err);
                Err(err)
            }
        }
    })
}

/// Spawn a monitored background os thread. The thread must return a Result<T, SlateDBError>.
/// The thread is spawned by a monitor thread. When the thread exits, the monitor thread
/// calls a provided cleanup fn with the returned error, or Err(BackgroundTaskShutdown)
/// if the thread exited with Ok. If the spawned thread panics, the cleanup fn is called with
/// Err(BackgroundTaskFailed).
pub(crate) fn spawn_bg_thread<F, T, C>(
    name: &str,
    cleanup_fn: C,
    f: F,
) -> std::thread::JoinHandle<Result<T, SlateDBError>>
where
    F: FnOnce() -> Result<T, SlateDBError> + Send + 'static,
    T: Send + 'static,
    C: FnOnce(&SlateDBError) + Send + 'static,
{
    let monitored_name = String::from(name);
    let monitor_name = format!("{}-monitor", name);
    std::thread::Builder::new()
        .name(monitor_name)
        .spawn(move || {
            let inner = std::thread::Builder::new()
                .name(monitored_name)
                .spawn(f)
                .expect("failed to create monitored thread");
            let result = inner.join();
            match result {
                Err(err) => {
                    // the thread panic'd
                    let err = BackgroundTaskPanic(Arc::new(Mutex::new(err)));
                    cleanup_fn(&err);
                    Err(err)
                }
                Ok(Err(err)) => {
                    // thread exited with an error
                    cleanup_fn(&err);
                    Err(err)
                }
                Ok(r) => {
                    cleanup_fn(&BackgroundTaskShutdown);
                    r
                }
            }
        })
        .expect("failed to create monitor thread")
}

pub(crate) async fn close_and_drain_receiver<T>(
    rx: &mut UnboundedReceiver<FlushMsg<T>>,
    error: &SlateDBError,
) {
    rx.close();
    while !rx.is_empty() {
        let (rsp_sender, _) = rx.recv().await.expect("channel unexpectedly closed");
        if let Some(sender) = rsp_sender {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::SlateDBError;
    use crate::utils::{spawn_bg_task, spawn_bg_thread, WatchableOnceCell};
    use parking_lot::Mutex;
    use std::sync::Arc;

    struct ErrorCaptor {
        error: Mutex<Option<SlateDBError>>,
    }

    impl ErrorCaptor {
        fn new() -> Self {
            Self {
                error: Mutex::new(None),
            }
        }

        fn capture(&self, error: &SlateDBError) {
            let mut guard = self.error.lock();
            let prev = guard.replace(error.clone());
            assert!(prev.is_none());
        }

        fn captured(&self) -> Option<SlateDBError> {
            self.error.lock().clone()
        }
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits_with_error() {
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async {
            Err(SlateDBError::Fenced)
        });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(SlateDBError::Fenced)));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_panics() {
        let monitored = async {
            panic!("oops");
        };
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), monitored);

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskPanic(_))
        ));
    }

    #[tokio::test]
    async fn test_should_cleanup_when_task_exits() {
        let captor = Arc::new(ErrorCaptor::new());
        let handle = tokio::runtime::Handle::current();
        let captor2 = captor.clone();

        let task = spawn_bg_task(&handle, move |err| captor2.capture(err), async { Ok(()) });

        let result: Result<(), SlateDBError> = task.await.expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits_with_error() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread(
            "test",
            move |err| captor2.capture(err),
            || Err(SlateDBError::Fenced),
        );

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(captor.captured(), Some(SlateDBError::Fenced)));
    }

    #[test]
    fn test_should_cleanup_when_thread_panics() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || panic!("oops"));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskPanic(_))
        ));
    }

    #[test]
    fn test_should_cleanup_when_thread_exits() {
        let captor = Arc::new(ErrorCaptor::new());
        let captor2 = captor.clone();

        let thread = spawn_bg_thread("test", move |err| captor2.capture(err), || Ok(()));

        let result: Result<(), SlateDBError> = thread.join().expect("join failure");
        assert!(matches!(result, Ok(())));
        assert!(matches!(
            captor.captured(),
            Some(SlateDBError::BackgroundTaskShutdown)
        ));
    }

    #[tokio::test]
    async fn test_should_only_write_register_once() {
        let register = WatchableOnceCell::new();
        let reader = register.reader();
        assert_eq!(reader.read(), None);
        register.write(123);
        assert_eq!(reader.read(), Some(123));
        register.write(456);
        assert_eq!(reader.read(), Some(123));
    }

    #[tokio::test]
    async fn test_should_return_on_await_written_register() {
        let register = WatchableOnceCell::new();
        let mut reader = register.reader();
        let h = tokio::spawn(async move {
            assert_eq!(reader.await_value().await, 123);
            assert_eq!(reader.await_value().await, 123);
        });
        register.write(123);
        h.await.unwrap();
    }
}
