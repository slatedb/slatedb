//! SlateDB makes use of [mpsc::channel] to offload work to background tasks when
//! possible. Examples of these tasks include [crate::mem_table_flush],
//! [crate::checkpoint], and [crate::compactor]. Each task's lifecycle is fairly
//! similar; they:
//!
//! 1. Receive messages and perform some work
//! 2. Perform work based on a fixed schedule
//! 3. Manage error states when failures occur
//! 4. Drain messages on shutdown
//! 5. Clean up resources on shutdown
//!
//! [MessageDispatcher], [MessageHandlerExecutor], and [MessageHandler] standardize
//! this pattern in a single set of event loops.
//!
//! - [MessageHandlerExecutor] spawns background tasks, monitors them for completion, and
//!   updates [crate::db_state::DbState::error] accordingly.
//! - [MessageDispatcher] runs an event loop for a single background task. It receives
//!   messages and ticks, and passes them to the [MessageHandler] for processing.
//! - [MessageHandler] receives callbacks from the dispatcher based on lifetime events
//!   (messages, ticks, etc.) and performs work based on those events.
//!
//! SlateDB instantiates a [MessageHandlerExecutor] and calls
//! [MessageHandlerExecutor::add_handler] for each [MessageHandler]. The DB then calls
//! [MessageHandlerExecutor::monitor_on] to start the event loop and monitor tasks until
//! shutdown.
//!
//! ## Example
//!
//! ```ignore
//! # use slatedb::dispatcher::{MessageHandlerExecutor, MessageHandler, MessageFactory};
//! # use slatedb::error::SlateDBError;
//! # use slatedb::clock::DefaultSystemClock;
//! # use slatedb::utils::WatchableOnceCell;
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # use tokio::sync::mpsc;
//! # use tokio::runtime::Handle;
//! # use futures::stream::BoxStream;
//! # use futures::StreamExt;
//! # #[tokio::main]
//! # async fn main() {
//! enum Message {
//!     Say(String),
//! }
//!
//! struct PrintMessageHandler;
//!
//! #[async_trait::async_trait]
//! impl MessageHandler<Message> for PrintMessageHandler {
//!     fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<Message>>)> {
//!         let mut tickers = Vec::new();
//!         tickers.push((Duration::from_secs(1), Box::new(|| Message::Say("tick".to_string()))));
//!         tickers
//!     }
//!
//!     async fn handle(
//!         &mut self,
//!         message: Message,
//!     ) -> Result<(), SlateDBError> {
//!         match message {
//!             Message::Say(msg) => println!("{}", msg),
//!         }
//!         Ok(())
//!     }
//!
//!     async fn cleanup(
//!         &mut self,
//!         mut messages: BoxStream<'async_trait, Message>,
//!         result: Result<(), SlateDBError>,
//!     ) -> Result<(), SlateDBError> {
//!         match result {
//!             Ok(_) | Err(SlateDBError::Closed) => {
//!                 while let Some(m) = messages.next().await {
//!                     let _ = self.handle(m).await;
//!                 }
//!             },
//!             // skipping drain messages on unclean shutdown
//!             _ => {},
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let clock = Arc::new(DefaultSystemClock::default());
//! let (tx, rx) = mpsc::unbounded_channel();
//! let error_state = WatchableOnceCell::new();
//! let handle = Handle::current();
//! let task_executor = MessageHandlerExecutor::new(
//!     error_state,
//!     clock,
//! );
//! task_executor.add_handler(
//!     "print_message_handler".to_string(),
//!     Box::new(PrintMessageHandler),
//!     rx,
//!     &handle,
//! ).expect("failed to add handler");
//! let join_handle = task_executor.monitor_on(&handle)
//!     .expect("failed to start monitoring");
//! tx.send(Message::Say("hello".to_string())).unwrap();
//! task_executor.shutdown_task("print_message_handler").await.ok();
//! join_handle.await.ok();
//! # }
//! ```

use std::{
    any::Any, future::Future, mem::take, panic::AssertUnwindSafe, pin::Pin, sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use fail_parallel::{fail_point, FailPointRegistry};
use futures::{
    future::BoxFuture,
    stream::{BoxStream, FuturesUnordered},
    FutureExt, StreamExt,
};
use log::warn;
use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::mpsc, task::JoinHandle};
use tokio_util::{sync::CancellationToken, task::JoinMap};

use crate::{
    clock::{SystemClock, SystemClockTicker},
    error::SlateDBError,
    utils::{panic_string, split_join_result, split_unwind_result, WatchableOnceCell},
};

/// A factory for creating messages when a [MessageDispatcherTicker] ticks.
pub(crate) type MessageFactory<T> = dyn Fn() -> T + Send;

/// A dispatcher that invokes [MessageHandler] callbacks when events occur.
///
/// [MessageDispatcher] receives a [MessageHandler] and [mpsc::UnboundedReceiver<T>].
/// Messages sent to the receiver are passed to the [MessageHandler] for processing.
///
/// [MessageDispatcher::run] is the primary entry point for running the dispatcher;
/// it is responsible for running the main event loop. The function receives messages
/// from the [mpsc::UnboundedReceiver<T>] and from any [MessageDispatcherTicker]s, and
/// passes them to the [MessageHandler] for processing.
struct MessageDispatcher<T: Send + std::fmt::Debug> {
    handler: Box<dyn MessageHandler<T>>,
    rx: mpsc::UnboundedReceiver<T>,
    clock: Arc<dyn SystemClock>,
    cancellation_token: CancellationToken,
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
}

impl<T: Send + std::fmt::Debug> MessageDispatcher<T> {
    /// Creates a new [MessageDispatcher]. Messages sent to the channel are passed to
    /// the [MessageHandler] for processing.
    ///
    /// ## Arguments
    ///
    /// * `handler`: The [MessageHandler] to use for processing messages.
    /// * `rx`: The [mpsc::UnboundedReceiver<T>] to use for receiving messages.
    /// * `clock`: The [SystemClock] to use for time.
    /// * `cancellation_token`: The [CancellationToken] to use for shutdown.
    #[allow(dead_code)]
    fn new(
        handler: Box<dyn MessageHandler<T>>,
        rx: mpsc::UnboundedReceiver<T>,
        clock: Arc<dyn SystemClock>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            handler,
            rx,
            clock,
            cancellation_token,
            fp_registry: Arc::new(FailPointRegistry::new()),
        }
    }

    /// Creates a new [MessageDispatcher]. Messages sent to the channel are passed to
    /// the [MessageHandler] for processing. A fail point registry is provided for
    /// fail points.
    ///
    /// ## Arguments
    ///
    /// * `handler`: The [MessageHandler] to use for processing messages.
    /// * `rx`: The [mpsc::UnboundedReceiver<T>] to use for receiving messages.
    /// * `clock`: The [SystemClock] to use for time.
    /// * `cancellation_token`: The [CancellationToken] to use for shutdown.
    /// * `fp_registry`: The [FailPointRegistry] to use for fail points.
    #[allow(dead_code)]
    fn with_fp_registry(mut self, fp_registry: Arc<FailPointRegistry>) -> Self {
        self.fp_registry = fp_registry;
        self
    }

    /// Runs the main event loop for the dispatcher. This is where messages and ticker
    /// events are processed.
    ///
    /// [MessageDispatcher::run] contains a message loop with the following control
    /// flow:
    ///
    /// 1. Break immediately if the task is cancelled and returns `Ok(())`.
    /// 2. Else, if there is a message, read it and invoke [MessageHandler::handle].
    /// 3. Else, if there is a ticker event, read it and invoke [MessageHandler::handle].
    ///
    /// ## Returns
    ///
    /// A [Result] containing `Ok(())` on clean shutdown, or an error if the handler
    /// fails for any reason.
    async fn run(&mut self) -> Result<(), SlateDBError> {
        let mut tickers = self
            .handler
            .tickers()
            .into_iter()
            .map(|(dur, factory)| MessageDispatcherTicker::new(self.clock.ticker(dur), factory))
            .collect::<Vec<_>>();
        let mut ticker_futures: FuturesUnordered<_> =
            tickers.iter_mut().map(|t| t.tick()).collect();
        loop {
            fail_point!(Arc::clone(&self.fp_registry), "dispatcher-run-loop", |_| {
                Err(SlateDBError::Fenced)
            });
            tokio::select! {
                biased;
                // stop the loop if we're in an error state or cancelled
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                // if no errors, prioritize messages
                Some(message) = self.rx.recv() => {
                    self.handler.handle(message).await?;
                },
                // if no messages, check tickers
                Some((message, ticker)) = ticker_futures.next() => {
                    self.handler.handle(message).await?;
                    ticker_futures.push(ticker.tick());
                },
            }
        }
        Ok(())
    }

    /// Tells the handler to clean up any resources.
    ///
    /// If cleanup fails, the error is returned.
    ///
    /// ## Arguments
    ///
    /// * `result`: The value of [crate::db_state::DbState::error] when
    ///   [MessageDispatcher::run] returns.
    ///
    /// ## Returns
    ///
    /// The [Result] after cleaning up resources.
    async fn cleanup(&mut self, result: Result<(), SlateDBError>) -> Result<(), SlateDBError> {
        fail_point!(Arc::clone(&self.fp_registry), "dispatcher-cleanup", |_| {
            Err(SlateDBError::Fenced)
        });
        self.rx.close();
        let messages = futures::stream::unfold(&mut self.rx, |rx| async move {
            rx.recv().await.map(|message| (message, rx))
        });
        self.handler.cleanup(Box::pin(messages), result).await
    }
}

/// A ticker that generates messages at regular intervals.
///
/// On each [MessageDispatcherTicker::tick], the message factory is called to generate a
/// message, and the message is returned as a [Future].
struct MessageDispatcherTicker<'a, T: Send> {
    inner: SystemClockTicker<'a>,
    message_factory: Box<MessageFactory<T>>,
}

impl<'a, T: Send> MessageDispatcherTicker<'a, T> {
    /// Creates a new [MessageDispatcherTicker].
    ///
    /// ## Arguments
    ///
    /// * `inner`: The [SystemClockTicker] to use.
    /// * `message_factory`: A factory for generating messages.
    ///
    /// ## Returns
    ///
    /// The new [MessageDispatcherTicker].
    fn new(inner: SystemClockTicker<'a>, message_factory: Box<MessageFactory<T>>) -> Self {
        Self {
            inner,
            message_factory,
        }
    }

    /// Returns a [Future] that resolves when the ticker ticks, and returns the message
    /// generated by the message factory.
    ///
    /// ## Returns
    ///
    /// A [Future] that resolves when the ticker ticks.
    fn tick(&mut self) -> Pin<Box<dyn Future<Output = (T, &mut Self)> + Send + '_>> {
        let message = (self.message_factory)();
        Box::pin(async move {
            self.inner.tick().await;
            (message, self)
        })
    }
}

/// [MessageDispatcher] event loop callbacks are implemented in a [MessageHandler].
/// Handlers are responsible for:
///
/// 1. Processing messages ([MessageHandler::handle])
/// 2. Defining ticker schedules (when to tick a certain message) ([MessageHandler::tickers])
/// 3. Cleaning up resources ([MessageHandler::cleanup])
///
/// It is safe to return errors on failure or panic; the [MessageDispatcher] and
/// [MessageHandlerExecutor] will handle them appropriately.
#[async_trait]
pub(crate) trait MessageHandler<T: Send>: Send {
    /// Defines message ticker schedules. [MessageDispatcher::run] instantiates a
    /// [MessageDispatcherTicker] for each ticker defined here. Whenever each ticker
    /// ticks, the message factory generates a message, and [MessageDispatcher] sends the
    /// message to this [MessageHandler].
    ///
    /// ## Returns
    ///
    /// A vector of tuples containing the duration when a message should be sent to the
    /// [MessageDispatcher], and a message factory to generate a new message on each tick.
    fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<T>>)> {
        vec![]
    }

    /// Handles a message. Messages can come from either a channel or a ticker. See
    /// [crate::dispatcher] for details.
    ///
    /// ## Arguments
    ///
    /// * `message`: The message to handle.
    /// * `error`: An optional error to pass to the handler. If set, this argument
    ///   signals to the [MessageHandler] that the database is in an error state during
    ///   shutdown.
    ///
    /// ## Returns
    ///
    /// The [Result] after handling the message.
    async fn handle(&mut self, message: T) -> Result<(), SlateDBError>;

    /// Cleans up resources. This method should not be used to continue attempting to
    /// write to the database.
    ///
    /// ## Arguments
    ///
    /// * `messages`: An iterator of messages still in the channel after
    ///   [MessageDispatcher::run] returns. If a handler fails, this iterator will not
    ///   include the message that triggered the failure.
    /// * `result`: The value of [crate::db_state::DbState::error] when
    ///   [MessageDispatcher::run] returns.
    ///
    /// ## Returns
    ///
    /// The [Result] after cleaning up resources.
    async fn cleanup(
        &mut self,
        messages: BoxStream<'async_trait, T>,
        result: Result<(), SlateDBError>,
    ) -> Result<(), SlateDBError>;
}

/// A builder data structure for [MessageHandlerExecutor]. The executor creates a
/// [MessageHandlerFuture] for each handler it receives in
/// [MessageHandlerExecutor::add_handler]. The future is used to run the handler's
/// dispatcher ([MessageDispatcher::run]) and handle cleanup
/// ([MessageDispatcher::cleanup]).
struct MessageHandlerFuture {
    /// The name of the task.
    name: String,
    /// A future that runs the handler's dispatcher and handles cleanup.
    future: BoxFuture<'static, Result<(), SlateDBError>>,
    /// A cancellation token used to cancel the handler's dispatcher.
    token: CancellationToken,
    /// A runtime handle used to spawn the future.
    handle: Handle,
}

impl std::fmt::Debug for MessageHandlerFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageHandlerFuture")
            .field("name", &self.name)
            .finish()
    }
}

/// The [MessageHandlerExecutor] is responsible for spawning, monitoring, and shutting
/// down [MessageDispatcher]s. Think of it as a dispatcher pool.
///
/// Each dispatcher is associated with a name, which is used to identify the dispatcher
/// in the [MessageHandlerExecutor]. As dispatchers complete, their results are stored
/// in the [MessageHandlerExecutor]. The first completed dispatcher's results are also
/// used for [crate::db_state::DbState::error].
///
/// The executor also catches panics, and will convert them to an appropriate
/// [SlateDBError].
#[derive(Debug)]
pub(crate) struct MessageHandlerExecutor {
    /// A vector of futures for each dispatcher. Set to `None` when executor starts.
    futures: Mutex<Option<Vec<MessageHandlerFuture>>>,
    /// A map of cancellation tokens for each dispatcher.
    tokens: SkipMap<String, CancellationToken>,
    /// A map of (task name, results) for dispatchers that have returned.
    results: Arc<SkipMap<String, WatchableOnceCell<Result<(), SlateDBError>>>>,
    /// A list of (task name, panic payloads) that have occurred.
    panics: Arc<Mutex<Vec<(String, Box<dyn Any + Send>)>>>,
    /// A watchable cell that stores the error state of the database.
    error_state: WatchableOnceCell<SlateDBError>,
    /// A system clock for time keeping.
    clock: Arc<dyn SystemClock>,
    /// A fail point registry for fail points.
    #[allow(dead_code)]
    fp_registry: Arc<FailPointRegistry>,
}

impl MessageHandlerExecutor {
    /// Creates a new [MessageHandlerExecutor].
    ///
    /// ## Arguments
    ///
    /// * `error_state`: A [WatchableOnceCell] that stores the error state of the database.
    /// * `clock`: A [SystemClock] to use for tickers and timekeeping.
    ///
    /// ## Returns
    ///
    /// The new [MessageHandlerExecutor].
    pub(crate) fn new(
        error_state: WatchableOnceCell<SlateDBError>,
        clock: Arc<dyn SystemClock>,
    ) -> Self {
        Self {
            futures: Mutex::new(Some(vec![])),
            error_state,
            clock,
            tokens: SkipMap::new(),
            results: Arc::new(SkipMap::new()),
            panics: Arc::new(Mutex::new(Vec::new())),
            fp_registry: Arc::new(FailPointRegistry::new()),
        }
    }

    /// Sets a custom [FailPointRegistry] for the executor.
    ///
    /// ## Arguments
    ///
    /// * `fp_registry`: A [FailPointRegistry] to use for fail points.
    ///
    /// ## Returns
    ///
    /// The [MessageHandlerExecutor] with the custom [FailPointRegistry].
    #[allow(dead_code)]
    pub(crate) fn with_fp_registry(mut self, fp_registry: Arc<FailPointRegistry>) -> Self {
        self.fp_registry = fp_registry;
        self
    }

    /// Adds a new [MessageHandlerFuture] for the provided [MessageHandler] on the given
    /// [Handle]. The future calls [MessageDispatcher::run] on the dispatcher. When
    /// [MessageDispatcher::run] returns, [MessageDispatcher::cleanup] is called. The
    /// future is _not_ spawned in this method. Instead, it is added to the executor's
    /// `futures` list and will be spawned when [MessageHandlerExecutor::monitor_on] is
    /// called.
    ///
    /// ## Arguments
    ///
    /// * `name`: The name of the dispatcher.
    /// * `handler`: The [MessageHandler] to use for handling messages.
    /// * `rx`: The [mpsc::UnboundedReceiver] to use for receiving messages on the
    ///   handler's behalf.
    /// * `handle`: The [Handle] to use for spawning the dispatcher.
    ///
    /// ## Returns
    ///
    /// - `Ok(())` if the dispatcher was successfully spawned.
    /// - Err([SlateDBError::BackgroundTaskExists]) if a dispatcher with the same name is
    ///   already added.
    pub(crate) fn add_handler<T: Send + std::fmt::Debug + 'static>(
        &self,
        name: String,
        handler: Box<dyn MessageHandler<T>>,
        rx: mpsc::UnboundedReceiver<T>,
        handle: &Handle,
    ) -> Result<(), SlateDBError> {
        let token = CancellationToken::new();
        let mut dispatcher = MessageDispatcher::new(handler, rx, self.clock.clone(), token.clone())
            .with_fp_registry(self.fp_registry.clone());
        let this_error_state = self.error_state.clone();
        let this_name = name.clone();
        let this_panics = self.panics.clone();
        // future that runs the dispatcher and handles cleanup
        let task_future = async move {
            // catch dispatcher panics using catch_unwind
            let run_unwind_result = AssertUnwindSafe(dispatcher.run()).catch_unwind().await;
            let (run_result, run_maybe_panic) =
                split_unwind_result(this_name.clone(), run_unwind_result);
            // if the dispatcher panicked, add it to the panics list
            if let Some(payload) = run_maybe_panic {
                let mut guard = this_panics.lock();
                guard.push((this_name.clone(), payload));
            }
            // set the error state or mark a clean shutdown
            this_error_state.write(run_result.clone().err().unwrap_or(SlateDBError::Closed));
            // error_state is write-once, so re-read error_state to get the first error
            let final_error_state = Err(this_error_state
                .reader()
                .read()
                .expect("error state was unexpectedly empty"));
            // catch cleanup panics using catch_unwind
            let cleanup_unwind_result = AssertUnwindSafe(dispatcher.cleanup(final_error_state))
                .catch_unwind()
                .await;
            // if the cleanup failed, log it but don't set error_state/panics
            let (cleanup_result, cleanup_maybe_panic) =
                split_unwind_result(this_name.clone(), cleanup_unwind_result);
            if let Err(err) = cleanup_result {
                warn!(
                    "background task failed to clean up on shutdown [name={}, error={:?}, panic={:?}]",
                    this_name.clone(),
                    err,
                    cleanup_maybe_panic.map(|p| panic_string(&p))
                );
            }
            run_result
        };
        // add the future to the executor's list of task definitions
        let mut guard = self.futures.lock();
        if let Some(task_definitions) = guard.as_mut() {
            if task_definitions.iter().any(|t| t.name == name) {
                return Err(SlateDBError::BackgroundTaskExists(name));
            }
            task_definitions.push(MessageHandlerFuture {
                name,
                future: Box::pin(task_future),
                token,
                handle: handle.clone(),
            });
            Ok(())
        } else {
            Err(SlateDBError::BackgroundTaskExecutorStarted)
        }
    }

    /// Starts all [MessageHandlerFuture]s and spawns a task that monitors for completed
    /// dispatchers. As dispatchers complete, their results are stored in the executor's
    /// `results` map.
    ///
    /// ## Arguments
    ///
    /// * `handle`: The [Handle] to spawn the monitor on.
    ///
    /// ## Returns
    ///
    /// - Ok([JoinHandle]) if the monitor was successfully spawned.
    /// - Err([SlateDBError::BackgroundTaskExecutorStarted]) if the executor is already
    ///   started.
    pub(crate) fn monitor_on(&self, handle: &Handle) -> Result<JoinHandle<()>, SlateDBError> {
        let mut task_definitions = {
            let mut guard = self.futures.lock();
            if let Some(task_definitions) = guard.take() {
                task_definitions
            } else {
                return Err(SlateDBError::BackgroundTaskExecutorStarted);
            }
        };
        let mut tasks = JoinMap::new();
        for task_definition in task_definitions.drain(..) {
            self.tokens
                .insert(task_definition.name.clone(), task_definition.token.clone());
            self.results
                .insert(task_definition.name.clone(), WatchableOnceCell::new());
            tasks.spawn_on(
                task_definition.name.clone(),
                task_definition.future,
                &task_definition.handle,
            );
        }
        let this_results = self.results.clone();
        let this_tokens = self
            .tokens
            .iter()
            .map(|e| e.value().clone())
            .collect::<Vec<_>>();
        let this_panics = self.panics.clone();
        // future that runs until all tasks are completed
        let monitor_future = async move {
            while !tasks.is_empty() {
                if let Some((name, join_result)) = tasks.join_next().await {
                    let (task_result, task_maybe_panic) =
                        split_join_result(name.clone(), join_result);
                    if let Some(payload) = task_maybe_panic {
                        let mut guard = this_panics.lock();
                        guard.push((name.clone(), payload));
                    }
                    let entry = this_results
                        .get(&name)
                        .expect("result cell isn't set when expected");
                    let result_cell = entry.value();
                    result_cell.write(task_result.clone());
                    if task_result.is_err() {
                        // db is in an error state, so cancel all other tasks
                        this_tokens.iter().for_each(|t| t.cancel());
                    }
                }
            }
        };
        Ok(handle.spawn(monitor_future))
    }

    /// Cancels a task by name.
    ///
    /// ## Arguments
    ///
    /// * `name`: The name of the task to cancel.
    pub(crate) fn cancel_task(&self, name: &str) {
        if let Some(entry) = self.tokens.get(name) {
            entry.value().cancel();
        }
    }

    /// Waits for a task to complete.
    ///
    /// ## Arguments
    ///
    /// * `name`: The name of the task to wait for.
    ///
    /// ## Returns
    ///
    /// The result of the task.
    pub(crate) async fn join_task(&self, name: &str) -> Result<(), SlateDBError> {
        if let Some(entry) = self.results.get(name) {
            return entry.value().reader().await_value().await;
        }
        Ok(())
    }

    /// Cancels a task and waits for it to complete.
    ///
    /// ## Arguments
    ///
    /// * `name`: The name of the task to cancel and wait for.
    ///
    /// ## Returns
    ///
    /// The result of the task.
    pub(crate) async fn shutdown_task(&self, name: &str) -> Result<(), SlateDBError> {
        self.cancel_task(name);
        self.join_task(name).await
    }

    /// Retrieves panic payloads for any background tasks that have panicked.
    ///
    /// ## Returns
    ///
    /// An iterator of _(task name, payload)_ pairs. The task name is a string that
    /// uniquely identifies the background task. The payload is a `Box<dyn Any + Send>`
    /// from the panic.
    ///
    /// ## Note
    /// Each panic is returned only once. Subsequent calls will return new panics, but
    /// previously returned panics will not be returned again.
    pub(crate) fn panics(&self) -> impl Iterator<Item = (String, Box<dyn Any + Send>)> + '_ {
        let mut guard = self.panics.lock();
        let panics = take(&mut *guard);
        panics.into_iter()
    }
}

#[cfg(all(test, feature = "test-util"))]
mod test {
    use super::{MessageDispatcher, MessageHandler};
    use crate::clock::{DefaultSystemClock, MockSystemClock, SystemClock};
    use crate::dispatcher::{MessageFactory, MessageHandlerExecutor};
    use crate::error::SlateDBError;
    use crate::utils::WatchableOnceCell;
    use fail_parallel::FailPointRegistry;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::runtime::Handle;
    use tokio::sync::mpsc;
    use tokio::task::yield_now;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestMessage {
        Channel(i32),
        Tick(i32),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Phase {
        Pre,
        Cleanup,
    }

    #[derive(Clone)]
    struct TestHandler {
        log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
        cleanup_called: WatchableOnceCell<Result<(), SlateDBError>>,
        tickers: Vec<(Duration, u8)>,
        clock: Arc<dyn SystemClock>,
        clock_schedule: VecDeque<Duration>,
    }

    impl TestHandler {
        fn new(
            log: Arc<Mutex<Vec<(Phase, TestMessage)>>>,
            cleanup_called: WatchableOnceCell<Result<(), SlateDBError>>,
            clock: Arc<dyn SystemClock>,
        ) -> Self {
            Self {
                log,
                cleanup_called,
                tickers: vec![],
                clock,
                clock_schedule: VecDeque::new(),
            }
        }

        fn add_ticker(mut self, d: Duration, id: u8) -> Self {
            self.tickers.push((d, id));
            self
        }

        /// Add a clock schedule to the handler. The clock will pop the first duration from the
        /// schedule and advance the clock by that duration after each message is processed.
        fn add_clock_schedule(mut self, ts: u64) -> Self {
            self.clock_schedule.push_back(Duration::from_millis(ts));
            self
        }
    }

    #[async_trait::async_trait]
    impl MessageHandler<TestMessage> for TestHandler {
        fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<TestMessage>>)> {
            let mut tickers: Vec<(Duration, Box<MessageFactory<_>>)> = vec![];
            for (interval, id) in self.tickers.iter() {
                let id = *id as i32;
                tickers.push((*interval, Box::new(move || TestMessage::Tick(id))));
            }
            tickers
        }

        async fn handle(&mut self, message: TestMessage) -> Result<(), SlateDBError> {
            self.log.lock().unwrap().push((Phase::Pre, message));
            if let Some(advance_duration) = self.clock_schedule.pop_front() {
                self.clock.advance(advance_duration).await;
            }
            Ok(())
        }

        async fn cleanup(
            &mut self,
            mut messages: futures::stream::BoxStream<'async_trait, TestMessage>,
            result: Result<(), SlateDBError>,
        ) -> Result<(), SlateDBError> {
            self.cleanup_called.write(result);
            while let Some(m) = messages.next().await {
                self.log.lock().unwrap().push((Phase::Cleanup, m));
            }
            Ok(())
        }
    }

    async fn wait_for_message_count(log: Arc<Mutex<Vec<(Phase, TestMessage)>>>, count: usize) {
        timeout(Duration::from_secs(30), async move {
            while log.lock().unwrap().len() < count {
                yield_now().await;
            }
        })
        .await
        .expect("timeout waiting for message count");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_run_happy_path() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), WatchableOnceCell::new(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1)
            .add_clock_schedule(5); // Advance clock by 5ms after first message
        let cancellation_token = CancellationToken::new();
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
        );
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Send a message successfully, then trigger a tick before processing the next message
        tx.send(TestMessage::Channel(10)).unwrap();
        wait_for_message_count(log.clone(), 2).await;
        tx.send(TestMessage::Channel(20)).unwrap();
        wait_for_message_count(log.clone(), 3).await;

        // Cancel and wait for cleanup to start
        cancellation_token.cancel();

        // Ensure run() completes and returns clean shutdown
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Ok(())));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(10)),
                (Phase::Pre, TestMessage::Tick(1)),
                (Phase::Pre, TestMessage::Channel(20))
            ]
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_executor_propagates_handler_error_drains_messages() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), cleanup_called.clone(), clock.clone());
        let error_state = WatchableOnceCell::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let task_executor = MessageHandlerExecutor::new(error_state.clone(), clock.clone())
            .with_fp_registry(fp_registry.clone());
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "1*off->return").unwrap();
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-cleanup", "pause").unwrap();
        task_executor
            .add_handler(
                "test".to_string(),
                Box::new(handler),
                rx,
                &Handle::current(),
            )
            .expect("spawn failed");
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");

        // Send a message successfully.
        tx.send(TestMessage::Channel(42)).unwrap();
        // Wait for the first message to be processed.
        wait_for_message_count(log.clone(), 1).await;
        // Send another message. The panic is guaranteed to be triggered before this
        // message is processed since the `dispatcher-run-loop` failpoint is set to
        // `1*off->return`, one message has been processed, and the point is hit before
        // the message selector.
        tx.send(TestMessage::Channel(77)).unwrap();
        // Now allow cleanup to proceed.
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-cleanup", "off").unwrap();

        // Wait for cleanup to start (unclean shutdown)
        let _ = cleanup_reader.await_value().await;

        // Wait for the dispatcher to stop
        let result = timeout(Duration::from_secs(30), task_executor.join_task("test"))
            .await
            .expect("dispatcher did not stop in time");

        // Verify final state
        assert!(matches!(result, Err(SlateDBError::Fenced)));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::Fenced)
        ));
        assert!(matches!(
            cleanup_reader.read(),
            Some(Err(SlateDBError::Fenced))
        ));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(42)),
                (Phase::Cleanup, TestMessage::Channel(77)),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_prioritizes_messages_over_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let (tx, rx) = mpsc::unbounded_channel();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), WatchableOnceCell::new(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1);
        let cancellation_token = CancellationToken::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
        )
        .with_fp_registry(fp_registry.clone());
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "pause").unwrap();
        let join = tokio::spawn(async move { dispatcher.run().await });

        // Trigger a tick and a message
        clock.advance(Duration::from_millis(5)).await;
        tx.send(TestMessage::Channel(99)).unwrap();
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "off").unwrap();
        wait_for_message_count(log.clone(), 2).await;

        // Shutdown cleanly
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Ok(())));
        let messages = log.lock().unwrap().clone();
        assert_eq!(
            messages,
            vec![
                (Phase::Pre, TestMessage::Channel(99)),
                (Phase::Pre, TestMessage::Tick(1)),
            ]
        );
    }

    // This test simulates the following timeline:
    // immediate tick(3) (tickers always return first ticks immediately)
    // immediate tick(5)
    // advance clock to 5ms
    // tick(5)
    // advance clock to 7ms
    // tick(7)
    // advance clock to 10ms
    // tick(5)
    // advance clock to 14ms
    // tick(7)
    // advance clock to 15ms
    // tick(5)
    // advance clock to 20ms
    // tick(5)
    // advance clock to 21ms
    // tick(7)
    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_multiple_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), WatchableOnceCell::new(), clock.clone())
            .add_ticker(Duration::from_millis(5), 1)
            .add_ticker(Duration::from_millis(7), 2)
            .add_clock_schedule(0) // 0ms (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(5) // 5ms
            .add_clock_schedule(2) // 7ms
            .add_clock_schedule(3) // 10ms
            .add_clock_schedule(4) // 14ms
            .add_clock_schedule(1) // 15ms
            .add_clock_schedule(5) // 20ms
            .add_clock_schedule(1); // 21ms
        let cancellation_token = CancellationToken::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
        )
        .with_fp_registry(fp_registry.clone());
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "pause").unwrap();
        let join = tokio::spawn(async move { dispatcher.run().await });
        assert_eq!(log.lock().unwrap().len(), 0);
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "off").unwrap();

        wait_for_message_count(log.clone(), 9).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                (Phase::Pre, TestMessage::Tick(1)), // immediate tick
                (Phase::Pre, TestMessage::Tick(2)), // immediate tick
                (Phase::Pre, TestMessage::Tick(1)), // 5
                (Phase::Pre, TestMessage::Tick(2)), // 7
                (Phase::Pre, TestMessage::Tick(1)), // 10
                (Phase::Pre, TestMessage::Tick(2)), // 14
                (Phase::Pre, TestMessage::Tick(1)), // 15
                (Phase::Pre, TestMessage::Tick(1)), // 20
                (Phase::Pre, TestMessage::Tick(2)), // 21
            ]
        );

        // Shutdown and wait for cleanup to start
        cancellation_token.cancel();

        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");

        // Verify final state
        assert!(matches!(result, Ok(())));
        assert_eq!(log.lock().unwrap().len(), 9);
    }

    // This test simulates the following timeline:
    // immediate tick(3) (tickers always return first ticks immediately)
    // immediate tick(5)
    // advance clock to 3ms
    // tick(3)
    // advance clock to 5ms
    // tick(5)
    // advance clock to 6ms
    // tick(3)
    // advance clock to 9ms
    // tick(3)
    // advance clock to 10ms
    // tick(5)
    // advance clock to 12ms
    // tick(3)
    // clock = 15ms: tick(3), tick(5)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_overlapping_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), WatchableOnceCell::new(), clock.clone())
            .add_ticker(Duration::from_millis(3), 3)
            .add_ticker(Duration::from_millis(5), 5)
            .add_clock_schedule(0) // 0 (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(3) // 3
            .add_clock_schedule(2) // 5
            .add_clock_schedule(1) // 6
            .add_clock_schedule(3) // 9
            .add_clock_schedule(1) // 10
            .add_clock_schedule(2) // 12
            .add_clock_schedule(3); // 15
        let cancellation_token = CancellationToken::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
        )
        .with_fp_registry(fp_registry.clone());
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "pause").unwrap();
        let join = tokio::spawn(async move { dispatcher.run().await });
        assert_eq!(log.lock().unwrap().len(), 0);
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "off").unwrap();

        wait_for_message_count(log.clone(), 10).await;
        assert_eq!(
            log.lock().unwrap().clone()[..8],
            vec![
                (Phase::Pre, TestMessage::Tick(3)), // immediate tick
                (Phase::Pre, TestMessage::Tick(5)), // immediate tick
                (Phase::Pre, TestMessage::Tick(3)), // 3
                (Phase::Pre, TestMessage::Tick(5)), // 5
                (Phase::Pre, TestMessage::Tick(3)), // 6
                (Phase::Pre, TestMessage::Tick(3)), // 9
                (Phase::Pre, TestMessage::Tick(5)), // 10
                (Phase::Pre, TestMessage::Tick(3)), // 12
            ]
        );
        // expect two back-to-back ticks at 15
        let mut last_two_ticks = log.lock().unwrap().clone()[8..].to_vec();
        last_two_ticks.sort_by(|a, b| match (a.1.clone(), b.1.clone()) {
            (TestMessage::Tick(a), TestMessage::Tick(b)) => a.cmp(&b),
            _ => panic!("expected ticks"),
        });
        assert_eq!(
            last_two_ticks,
            vec![
                (Phase::Pre, TestMessage::Tick(3)),
                (Phase::Pre, TestMessage::Tick(5))
            ]
        );

        // Shutdown cleanly
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Ok(())));
        assert_eq!(log.lock().unwrap().len(), 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dispatcher_supports_identical_tickers() {
        let log = Arc::new(Mutex::new(Vec::<(Phase, TestMessage)>::new()));
        let (_tx, rx) = mpsc::unbounded_channel::<TestMessage>();
        let clock = Arc::new(MockSystemClock::new());
        let handler = TestHandler::new(log.clone(), WatchableOnceCell::new(), clock.clone())
            .add_ticker(Duration::from_millis(3), 1)
            .add_ticker(Duration::from_millis(3), 2)
            .add_clock_schedule(0) // 0 (initial two ticks are immediate, so don't advance clock until second tick)
            .add_clock_schedule(3) // 3
            .add_clock_schedule(0) // 3 (process second)
            .add_clock_schedule(3) // 6
            .add_clock_schedule(0) // 6 (process second)
            .add_clock_schedule(3) // 9
            .add_clock_schedule(0) // 9 (process second)
            .add_clock_schedule(3) // 12
            .add_clock_schedule(0) // 12 (process second)
            .add_clock_schedule(3) // 15
            .add_clock_schedule(0) // 15 (process second)
            .add_clock_schedule(3) // 18
            .add_clock_schedule(0) // 18 (process second)
            .add_clock_schedule(3) // 21
            .add_clock_schedule(0); // 21 (process second)
        let cancellation_token = CancellationToken::new();
        let fp_registry = Arc::new(FailPointRegistry::default());
        let mut dispatcher = MessageDispatcher::new(
            Box::new(handler),
            rx,
            clock.clone(),
            cancellation_token.clone(),
        )
        .with_fp_registry(fp_registry.clone());
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "pause").unwrap();
        let join = tokio::spawn(async move { dispatcher.run().await });
        assert_eq!(log.lock().unwrap().len(), 0);
        fail_parallel::cfg(fp_registry.clone(), "dispatcher-run-loop", "off").unwrap();

        wait_for_message_count(log.clone(), 16).await;
        assert_eq!(
            log.lock().unwrap().clone(),
            vec![
                (Phase::Pre, TestMessage::Tick(1)), // immediate tick
                (Phase::Pre, TestMessage::Tick(2)), // immediate tick
                (Phase::Pre, TestMessage::Tick(1)), // 3
                (Phase::Pre, TestMessage::Tick(2)), // 3
                (Phase::Pre, TestMessage::Tick(1)), // 6
                (Phase::Pre, TestMessage::Tick(2)), // 6
                (Phase::Pre, TestMessage::Tick(1)), // 9
                (Phase::Pre, TestMessage::Tick(2)), // 9
                (Phase::Pre, TestMessage::Tick(1)), // 12
                (Phase::Pre, TestMessage::Tick(2)), // 12
                (Phase::Pre, TestMessage::Tick(1)), // 15
                (Phase::Pre, TestMessage::Tick(2)), // 15
                (Phase::Pre, TestMessage::Tick(1)), // 18
                (Phase::Pre, TestMessage::Tick(2)), // 18
                (Phase::Pre, TestMessage::Tick(1)), // 21
                (Phase::Pre, TestMessage::Tick(2)), // 21
            ]
        );

        // Shutdown cleanly
        cancellation_token.cancel();
        let result = timeout(Duration::from_secs(30), join)
            .await
            .expect("dispatcher did not stop in time")
            .expect("join failed");
        assert!(matches!(result, Ok(())));
        assert_eq!(log.lock().unwrap().len(), 16);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_executor_catches_panic_from_run_loop() {
        #[derive(Clone)]
        struct PanicHandler {
            cleanup_called: WatchableOnceCell<Result<(), SlateDBError>>,
        }

        #[async_trait::async_trait]
        impl MessageHandler<u8> for PanicHandler {
            fn tickers(&mut self) -> Vec<(Duration, Box<MessageFactory<u8>>)> {
                vec![]
            }

            async fn handle(&mut self, _message: u8) -> Result<(), SlateDBError> {
                panic!("intentional panic in handler");
            }

            async fn cleanup(
                &mut self,
                mut messages: BoxStream<'async_trait, u8>,
                result: Result<(), SlateDBError>,
            ) -> Result<(), SlateDBError> {
                // Record the shutdown result and drain any pending messages
                self.cleanup_called.write(result);
                while messages.next().await.is_some() {}
                Ok(())
            }
        }

        let cleanup_called = WatchableOnceCell::new();
        let mut cleanup_reader = cleanup_called.reader();
        let (tx, rx) = mpsc::unbounded_channel::<u8>();
        let clock = Arc::new(DefaultSystemClock::new());
        let handler = PanicHandler { cleanup_called };
        let error_state = WatchableOnceCell::new();
        let task_executor = MessageHandlerExecutor::new(error_state.clone(), clock.clone());

        // Spawn the panic task
        task_executor
            .add_handler(
                "test".to_string(),
                Box::new(handler),
                rx,
                &Handle::current(),
            )
            .expect("failed to spawn task");

        // Monitor the executor
        task_executor
            .monitor_on(&Handle::current())
            .expect("failed to monitor executor");

        // Trigger panic inside the run loop via handle()
        tx.send(1u8).unwrap();

        // Wait for cleanup to observe the panic result
        let _ = timeout(Duration::from_secs(30), cleanup_reader.await_value())
            .await
            .expect("timeout waiting for cleanup result");

        // Join dispatcher and verify panic was converted and propagated
        let result = timeout(Duration::from_secs(30), task_executor.join_task("test"))
            .await
            .expect("dispatcher did not stop in time");

        // Check final state
        assert!(matches!(result, Err(SlateDBError::BackgroundTaskPanic(_))));
        assert!(matches!(
            error_state.reader().read(),
            Some(SlateDBError::BackgroundTaskPanic(_))
        ));
        assert!(matches!(
            cleanup_reader.read(),
            Some(Err(SlateDBError::BackgroundTaskPanic(_)))
        ));
    }
}
